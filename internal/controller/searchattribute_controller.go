/*
 * Copyright 2026 Simon Emms <simon@simonemms.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package controller

import (
	"context"
	"errors"
	"fmt"

	sdkclient "go.temporal.io/sdk/client"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	temporalv1alpha1 "github.com/mrsimonemms/temporal-resource-operator/api/v1alpha1"
	"github.com/mrsimonemms/temporal-resource-operator/internal/connection"
	"github.com/mrsimonemms/temporal-resource-operator/internal/temporal"
)

// Reasons reported on a SearchAttribute's Ready condition, in addition to the
// Connection ones it shares with the other controllers.
const (
	// ReasonNamespaceNotFound means the referenced Namespace does not exist.
	ReasonNamespaceNotFound = "NamespaceNotFound"

	// ReasonNamespaceNotReady means the referenced Namespace exists but has not
	// reported Ready=True, so its Temporal namespace may not be there yet.
	ReasonNamespaceNotReady = "NamespaceNotReady"

	// ReasonTypeConflict means a search attribute of this name already exists
	// with a different type. Temporal cannot retype one, so the operator
	// reports the clash rather than pretending it can be resolved.
	ReasonTypeConflict = "TypeConflict"
)

// Field indexes registered on SearchAttribute, so that a dependency event can
// find its dependants with one indexed lookup rather than scanning.
const (
	searchAttributeConnectionRefIndex = ".spec.connectionRef.name"
	searchAttributeNamespaceRefIndex  = ".spec.namespaceRef.name"
)

// TemporalSearchAttributeClient is the part of the Temporal client wrapper the
// SearchAttribute reconciler uses. It exists so that controller tests can run
// without a Temporal Service.
type TemporalSearchAttributeClient interface {
	DescribeSearchAttribute(ctx context.Context, namespace, name string) (*temporal.SearchAttribute, error)
	CreateSearchAttribute(ctx context.Context, namespace, name string, t temporal.SearchAttributeType) error
	DeleteSearchAttribute(ctx context.Context, namespace, name string) error
	Close()
}

// SearchAttributeReconciler reconciles a SearchAttribute object
type SearchAttributeReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Resolver turns a Connection into Temporal client options.
	Resolver *connection.Resolver

	// Connect dials a Temporal Service. Defaults to the internal/temporal
	// package when unset.
	Connect func(ctx context.Context, opts *sdkclient.Options) (TemporalSearchAttributeClient, error)
}

// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=searchattributes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=searchattributes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=searchattributes/finalizers,verbs=update
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=connections,verbs=get;list;watch
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=namespaces,verbs=get;list;watch

// Reconcile ensures the Temporal search attribute named by this resource exists
// on the referenced namespace with the requested type, and is removed again if
// - and only if - this resource is what registered it.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
//
//nolint:dupl // parallel with the sibling controllers on purpose; see dependency.go
func (r *SearchAttributeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	attribute := &temporalv1alpha1.SearchAttribute{}
	if err := r.Get(ctx, req.NamespacedName, attribute); err != nil {
		if apierrors.IsNotFound(err) {
			// Deleted, or the cache is behind. Either way there is nothing to
			// reconcile - importantly, do not carry on with a zero-value
			// SearchAttribute.
			log.Info("SearchAttribute no longer exists")
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	// Snapshot the status as it is stored, before anything below mutates it in
	// memory, so a change made during reconciliation is never mistaken for a
	// no-op.
	observed := attribute.Status.DeepCopy()

	if !attribute.GetDeletionTimestamp().IsZero() {
		return r.finalise(ctx, attribute, observed)
	}

	// The finalizer has to be in place before anything is registered in
	// Temporal, or a deletion racing the create would orphan the attribute.
	if err := r.ensureFinalizer(ctx, attribute); err != nil {
		return ctrl.Result{}, err
	}

	reason, message, reconcileErr := r.reconcileSearchAttribute(ctx, attribute)

	condition := &metav1.Condition{
		Type:               temporalv1alpha1.ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: attribute.Generation,
	}
	if reconcileErr != nil {
		condition.Status = metav1.ConditionFalse
		condition.Message = reconcileErr.Error()
	}

	if err := r.updateSearchAttributeStatus(ctx, attribute, observed, condition); err != nil {
		return ctrl.Result{}, err
	}

	switch {
	case reconcileErr == nil:
		return ctrl.Result{RequeueAfter: namespaceResyncInterval}, nil
	case isDependencyReason(reason):
		// The dependency will become ready on its own schedule, and the watches
		// below wake us the moment it does. Waiting is not a failure, so retry
		// on a fixed interval rather than burning error backoff on it.
		log.Info("Waiting for a dependency", "reason", reason)
		return ctrl.Result{RequeueAfter: dependencyRetryInterval}, nil
	default:
		log.Error(reconcileErr, "Failed to reconcile SearchAttribute", "reason", reason)
		return ctrl.Result{}, reconcileErr
	}
}

// isDependencyReason reports whether a reason describes waiting on another
// resource rather than a failure worth backing off over.
func isDependencyReason(reason string) bool {
	switch reason {
	case ReasonConnectionNotFound, ReasonConnectionNotReady,
		ReasonNamespaceNotFound, ReasonNamespaceNotReady:
		return true
	default:
		return false
	}
}

// reconcileSearchAttribute brings the Temporal search attribute into line with
// the spec, returning the reason and, on success, the message to report.
func (r *SearchAttributeReconciler) reconcileSearchAttribute(
	ctx context.Context,
	attribute *temporalv1alpha1.SearchAttribute,
) (reason, message string, err error) {
	conn, reason, err := r.readyDependencies(ctx, attribute)
	if err != nil {
		return reason, "", err
	}

	ctx, cancel := context.WithTimeout(ctx, namespaceTimeout)
	defer cancel()

	temporalClient, reason, err := r.temporalClient(ctx, conn)
	if err != nil {
		return reason, "", err
	}
	defer temporalClient.Close()

	name := attribute.TemporalName()
	namespace := attribute.TemporalNamespace()
	desired := temporal.SearchAttributeType(attribute.Spec.Type)

	actual, err := temporalClient.DescribeSearchAttribute(ctx, namespace, name)
	switch {
	case err == nil:
		return r.reconcileExistingSearchAttribute(attribute, actual, desired)
	case !errors.Is(err, temporal.ErrSearchAttributeNotFound):
		return ReasonDescribeFailed, "", err
	}

	return r.createSearchAttribute(ctx, attribute, temporalClient, desired)
}

// reconcileExistingSearchAttribute settles ownership of a search attribute that
// already exists, refusing to touch one whose type does not match.
func (r *SearchAttributeReconciler) reconcileExistingSearchAttribute(
	attribute *temporalv1alpha1.SearchAttribute,
	actual *temporal.SearchAttribute,
	desired temporal.SearchAttributeType,
) (reason, message string, err error) {
	name := attribute.TemporalName()
	namespace := attribute.TemporalNamespace()

	if actual.Type != desired {
		// A search attribute's type is its schema. Temporal cannot change one -
		// it accepts the request and keeps the original - and dropping and
		// re-adding would discard whatever is indexed under it. So this is
		// reported, never resolved.
		return ReasonTypeConflict, "", fmt.Errorf(
			"search attribute %q on namespace %s is %s, not the requested %s",
			name, namespace, actual.Type, desired,
		)
	}

	if adopting := establishSearchAttributeOwnership(attribute); adopting {
		return ReasonAdopted, fmt.Sprintf(
			"Adopted existing %s search attribute %q on namespace %s", actual.Type, name, namespace,
		), nil
	}

	return ReasonReconciled, fmt.Sprintf(
		"Search attribute %q on namespace %s matches the spec", name, namespace,
	), nil
}

// createSearchAttribute registers a search attribute that does not exist and,
// if ownership has not been settled yet, takes ownership of it.
//
// Ownership is recorded as Creating before the registration call, so that a
// reconcile interrupted between registering and recording the result can tell
// that what it now finds is its own work. Unlike a Temporal namespace there is
// no marker to confirm that from the outside; see
// establishSearchAttributeOwnership for what that costs.
//
// An already-settled ownership is left alone: an adopted attribute that has
// since been removed behind the operator's back is put back, but that does not
// make it the operator's to delete.
func (r *SearchAttributeReconciler) createSearchAttribute(
	ctx context.Context,
	attribute *temporalv1alpha1.SearchAttribute,
	temporalClient TemporalSearchAttributeClient,
	desired temporal.SearchAttributeType,
) (reason, message string, err error) {
	establishing := !attribute.Status.Ownership.IsEstablished()

	if establishing {
		if err := r.persistSearchAttributeOwnership(
			ctx, attribute, temporalv1alpha1.SearchAttributeOwnershipCreating,
		); err != nil {
			return ReasonCreateFailed, "", fmt.Errorf("recording intent to create search attribute: %w", err)
		}
	}

	name := attribute.TemporalName()
	namespace := attribute.TemporalNamespace()

	if err := temporalClient.CreateSearchAttribute(ctx, namespace, name, desired); err != nil {
		return ReasonCreateFailed, "", err
	}

	if establishing {
		// Settled in memory; the caller's status write persists it alongside
		// the Ready condition. Should that write fail, ownership stays at
		// Creating and the next reconcile resolves it to Created.
		attribute.Status.Ownership = temporalv1alpha1.SearchAttributeOwnershipCreated
	}

	return ReasonCreated, fmt.Sprintf(
		"Registered %s search attribute %q on namespace %s", desired, name, namespace,
	), nil
}

// establishSearchAttributeOwnership settles, once, how this resource came to
// manage an existing search attribute. It reports whether that settled to a
// fresh adoption.
//
// A namespace can be asked who created it; a search attribute cannot. There is
// nowhere on one to record an owner, so Creating is the only evidence available
// that an interrupted reconcile registered what we are now looking at.
//
// That evidence is strong but not proof: the attribute was absent when Creating
// was written, and it now exists with exactly the requested type, so it is
// almost certainly this resource's own registration - but another actor could
// have registered the same name and type in the intervening moment. Reading it
// as Created is the deliberate choice, because the alternative would make the
// marker useless for the failure it exists to survive. A type that does not
// match is caught before this is ever reached.
func establishSearchAttributeOwnership(attribute *temporalv1alpha1.SearchAttribute) bool {
	if attribute.Status.Ownership.IsEstablished() {
		return false
	}

	if attribute.Status.Ownership == temporalv1alpha1.SearchAttributeOwnershipCreating {
		attribute.Status.Ownership = temporalv1alpha1.SearchAttributeOwnershipCreated

		return false
	}

	attribute.Status.Ownership = temporalv1alpha1.SearchAttributeOwnershipAdopted

	return true
}

// finalise runs the deletion flow: remove the Temporal search attribute if this
// resource registered it and has been asked to, then release the finalizer.
//
// This deliberately mirrors the Namespace controller's finalise step by step,
// because the two resources are meant to behave identically here. Folding them
// together would mean an abstraction parameterised by ownership type, finalizer,
// policy, external delete and log wording - more surface than the repetition it
// removes, for two callers. Worth revisiting at the third.
//
//nolint:dupl // mirrors Namespace.finalise on purpose; see above
func (r *SearchAttributeReconciler) finalise(
	ctx context.Context,
	attribute *temporalv1alpha1.SearchAttribute,
	observed *temporalv1alpha1.SearchAttributeStatus,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(attribute, temporalv1alpha1.SearchAttributeFinalizer) {
		return ctrl.Result{}, nil
	}

	name := attribute.TemporalName()
	ownership := attribute.Status.Ownership

	// Orphan is checked first, and deliberately before anything that needs a
	// Connection. It is the way out of a deletion that would otherwise be
	// blocked, so it must not depend on any of the machinery that could be what
	// is broken.
	if policy := attribute.Spec.DeletionPolicyValue(); policy == temporalv1alpha1.SearchAttributeDeletionPolicyOrphan {
		log.Info("Leaving Temporal search attribute in place",
			"searchAttribute", name, "deletionPolicy", string(policy))

		return ctrl.Result{}, r.removeSearchAttributeFinalizer(ctx, attribute)
	}

	if !ownership.OwnsSearchAttribute() {
		// Adopted, or never established. Either way this resource did not
		// register the attribute, so it has no business removing it.
		log.Info("Leaving Temporal search attribute in place",
			"searchAttribute", name, "ownership", string(ownership))

		return ctrl.Result{}, r.removeSearchAttributeFinalizer(ctx, attribute)
	}

	if reason, err := r.deleteSearchAttribute(ctx, attribute); err != nil {
		log.Error(err, "Failed to delete Temporal search attribute",
			"searchAttribute", name, "reason", reason)

		condition := &metav1.Condition{
			Type:               temporalv1alpha1.ConditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            err.Error(),
			ObservedGeneration: attribute.Generation,
		}
		if statusErr := r.updateSearchAttributeStatus(ctx, attribute, observed, condition); statusErr != nil {
			log.Error(statusErr, "Failed to record deletion failure")
		}

		return ctrl.Result{}, err
	}

	log.Info("Finished with Temporal search attribute", "searchAttribute", name)

	return ctrl.Result{}, r.removeSearchAttributeFinalizer(ctx, attribute)
}

// deleteSearchAttribute removes the search attribute this resource registered.
//
// The Temporal namespace comes from the reference on the spec rather than from
// the Namespace resource, which may well have gone by now - deletion ordering
// between the two is not guaranteed, and a Namespace resource being absent says
// nothing about whether its Temporal namespace still exists. Asking Temporal is
// the only way to know, and that is what this does.
func (r *SearchAttributeReconciler) deleteSearchAttribute(
	ctx context.Context,
	attribute *temporalv1alpha1.SearchAttribute,
) (string, error) {
	log := logf.FromContext(ctx)

	// Deletion needs a working Connection. Readiness is deliberately not
	// required: it is a cached judgement that may be stale, and giving up on an
	// attribute this resource owns because of a stale status would orphan it.
	conn, reason, err := getConnection(ctx, r.Client, attribute.Spec.ConnectionRef.Name, attribute.Namespace)
	if err != nil {
		return reason, err
	}

	ctx, cancel := context.WithTimeout(ctx, namespaceTimeout)
	defer cancel()

	temporalClient, reason, err := r.temporalClient(ctx, conn)
	if err != nil {
		return reason, err
	}
	defer temporalClient.Close()

	name := attribute.TemporalName()
	namespace := attribute.TemporalNamespace()

	// Removing an attribute that is not registered is success as far as
	// Temporal is concerned, so this covers "already gone" without a lookup
	// first.
	err = temporalClient.DeleteSearchAttribute(ctx, namespace, name)
	switch {
	case err == nil:
		return "", nil
	case errors.Is(err, temporal.ErrNamespaceNotFound):
		// The namespace holding the attribute has gone, taking every search
		// attribute registered on it. There is nothing left to remove, and
		// holding the finalizer open for a namespace that will never come back
		// would wedge the resource forever.
		//
		// This is a narrow judgement, not a blanket one: Temporal said the
		// namespace is absent. An unreachable Service or a missing Connection
		// proves nothing and is handled above.
		log.Info("Temporal namespace has gone, so its search attributes went with it",
			"searchAttribute", name, "temporalNamespace", namespace)

		return "", nil
	default:
		return ReasonDeleteFailed, err
	}
}

// readyDependencies fetches the Connection and Namespace this resource needs
// and checks that both have reported themselves Ready.
//
// The Namespace gate is what stops the operator registering an attribute on a
// Temporal namespace that does not exist yet.
//
//nolint:dupl // parallel with the sibling controllers on purpose; see dependency.go
func (r *SearchAttributeReconciler) readyDependencies(
	ctx context.Context,
	attribute *temporalv1alpha1.SearchAttribute,
) (*temporalv1alpha1.Connection, string, error) {
	conn, reason, err := getConnection(ctx, r.Client, attribute.Spec.ConnectionRef.Name, attribute.Namespace)
	if err != nil {
		return nil, reason, err
	}

	if reason, err := readyCondition(conn.Status.Conditions, "connection", client.ObjectKeyFromObject(conn),
		ReasonConnectionNotReady); err != nil {
		return nil, reason, err
	}

	namespace := &temporalv1alpha1.Namespace{}
	key := types.NamespacedName{Namespace: attribute.Namespace, Name: attribute.Spec.NamespaceRef.Name}

	if err := r.Get(ctx, key, namespace); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, ReasonNamespaceNotFound, fmt.Errorf("namespace %s not found: %w", key, err)
		}

		return nil, ReasonNamespaceNotReady, fmt.Errorf("getting namespace %s: %w", key, err)
	}

	if reason, err := readyCondition(namespace.Status.Conditions, "namespace", key,
		ReasonNamespaceNotReady); err != nil {
		return nil, reason, err
	}

	return conn, "", nil
}

// readyCondition checks a dependency's Ready condition, returning the given
// reason when it is missing or not True.
func readyCondition(
	conditions []metav1.Condition,
	kind string,
	key types.NamespacedName,
	notReady string,
) (string, error) {
	ready := meta.FindStatusCondition(conditions, temporalv1alpha1.ConditionTypeReady)

	switch {
	case ready == nil:
		return notReady, fmt.Errorf("%s %s has not been validated yet", kind, key)
	case ready.Status != metav1.ConditionTrue:
		return notReady, fmt.Errorf("%s %s is not ready: %s", kind, key, ready.Message)
	}

	return "", nil
}

// temporalClient resolves a Connection and dials the Temporal Service it
// describes.
func (r *SearchAttributeReconciler) temporalClient(
	ctx context.Context,
	conn *temporalv1alpha1.Connection,
) (TemporalSearchAttributeClient, string, error) {
	opts, err := r.Resolver.ResolveConnection(ctx, conn)
	if err != nil {
		return nil, ReasonInvalidConfiguration, err
	}

	if r.Connect != nil {
		temporalClient, err := r.Connect(ctx, opts)
		if err != nil {
			return nil, ReasonConnectionFailed, err
		}

		return temporalClient, "", nil
	}

	temporalClient, err := temporal.New(ctx, opts)
	if err != nil {
		return nil, ReasonConnectionFailed, err
	}

	return temporalClient, "", nil
}

// ensureFinalizer adds the finalizer if it is missing.
func (r *SearchAttributeReconciler) ensureFinalizer(
	ctx context.Context,
	attribute *temporalv1alpha1.SearchAttribute,
) error {
	patch := searchAttributeFinalizerPatch(attribute)

	if !controllerutil.AddFinalizer(attribute, temporalv1alpha1.SearchAttributeFinalizer) {
		return nil
	}

	return r.Patch(ctx, attribute, patch)
}

// removeSearchAttributeFinalizer releases the finalizer, allowing Kubernetes to
// remove the resource.
func (r *SearchAttributeReconciler) removeSearchAttributeFinalizer(
	ctx context.Context,
	attribute *temporalv1alpha1.SearchAttribute,
) error {
	patch := searchAttributeFinalizerPatch(attribute)

	if !controllerutil.RemoveFinalizer(attribute, temporalv1alpha1.SearchAttributeFinalizer) {
		return nil
	}

	return r.Patch(ctx, attribute, patch)
}

// searchAttributeFinalizerPatch captures the resource as it stands, so that
// only the finalizer change is sent.
//
// A full update would round-trip the spec, and the API server would read the
// re-marshalled result as a change and bump the generation, waking the
// controller again for no reason.
func searchAttributeFinalizerPatch(attribute *temporalv1alpha1.SearchAttribute) client.Patch {
	return client.MergeFromWithOptions(attribute.DeepCopy(), client.MergeFromWithOptimisticLock{})
}

// persistSearchAttributeOwnership writes an ownership transition straight to the
// API server, because the value has to be durable before the Temporal call it
// describes is made.
func (r *SearchAttributeReconciler) persistSearchAttributeOwnership(
	ctx context.Context,
	attribute *temporalv1alpha1.SearchAttribute,
	ownership temporalv1alpha1.SearchAttributeOwnership,
) error {
	if attribute.Status.Ownership == ownership {
		return nil
	}

	attribute.Status.Ownership = ownership

	return r.Status().Update(ctx, attribute)
}

// updateSearchAttributeStatus writes the condition, doing nothing if the stored
// status already says the same thing.
func (r *SearchAttributeReconciler) updateSearchAttributeStatus(
	ctx context.Context,
	attribute *temporalv1alpha1.SearchAttribute,
	observed *temporalv1alpha1.SearchAttributeStatus,
	condition *metav1.Condition,
) error {
	meta.SetStatusCondition(&attribute.Status.Conditions, *condition)
	attribute.Status.ObservedGeneration = attribute.Generation

	if equality.Semantic.DeepEqual(observed, &attribute.Status) {
		return nil
	}

	return r.Status().Update(ctx, attribute)
}

// indexSearchAttributeByConnection extracts the Connection a SearchAttribute
// references.
func indexSearchAttributeByConnection(obj client.Object) []string {
	return searchAttributeRef(obj, func(spec temporalv1alpha1.SearchAttributeSpec) string {
		return spec.ConnectionRef.Name
	})
}

// indexSearchAttributeByNamespace extracts the Namespace a SearchAttribute
// references.
func indexSearchAttributeByNamespace(obj client.Object) []string {
	return searchAttributeRef(obj, func(spec temporalv1alpha1.SearchAttributeSpec) string {
		return spec.NamespaceRef.Name
	})
}

// searchAttributeRef pulls one reference out of a SearchAttribute for indexing.
//
// It is called for every object the cache holds, including ones that are
// half-built or of the wrong type, so it never assumes anything about what it
// is handed. An unset reference is left out of the index rather than filed
// under the empty string.
func searchAttributeRef(obj client.Object, ref func(temporalv1alpha1.SearchAttributeSpec) string) []string {
	attribute, ok := obj.(*temporalv1alpha1.SearchAttribute)
	if !ok || attribute == nil {
		return nil
	}

	name := ref(attribute.Spec)
	if name == "" {
		return nil
	}

	return []string{name}
}

// searchAttributesForDependency maps an event on a dependency onto the
// SearchAttributes that reference it through the given index.
//
// Every kind involved is namespaced, and a SearchAttribute only ever references
// dependencies beside it, so the list is confined to the event's own Kubernetes
// namespace as well as being filtered by the index.
func (r *SearchAttributeReconciler) searchAttributesForDependency(
	index string,
) handler.MapFunc {
	return func(ctx context.Context, dependency client.Object) []ctrl.Request {
		return dependants(ctx, r.Client, &temporalv1alpha1.SearchAttributeList{}, index, dependency)
	}
}

// SetupWithManager sets up the controller with the Manager.
//
//nolint:dupl // parallel with the sibling controllers on purpose; see dependency.go
func (r *SearchAttributeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Resolver == nil {
		// Read through the API reader rather than the cache: caching Secrets
		// would mean watching every Secret in the cluster, and the operator
		// only ever needs to get the ones a Connection names.
		r.Resolver = connection.NewResolver(mgr.GetAPIReader())
	}

	indexes := map[string]client.IndexerFunc{
		searchAttributeConnectionRefIndex: indexSearchAttributeByConnection,
		searchAttributeNamespaceRefIndex:  indexSearchAttributeByNamespace,
	}

	for field, extract := range indexes {
		// The cache has not started yet, so this only registers the indexer and
		// returns; there is nothing for a caller's context to cancel.
		if err := mgr.GetFieldIndexer().IndexField(
			context.Background(), &temporalv1alpha1.SearchAttribute{}, field, extract,
		); err != nil {
			return fmt.Errorf("indexing search attributes by %s: %w", field, err)
		}
	}

	return ctrl.NewControllerManagedBy(mgr).
		// Only the spec matters here, so ignore the status writes this
		// controller makes itself. Kubernetes bumps the generation when it
		// stamps a deletion timestamp, so the finalizer flow still runs.
		For(&temporalv1alpha1.SearchAttribute{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		// Wake dependants as soon as a dependency changes. Both watches are
		// deliberately unfiltered: readiness lives in status, so the generation
		// predicate guarding the primary resource above would discard precisely
		// the events that matter - and predicates passed to For() apply only to
		// For(), so it does not reach these of its own accord.
		Watches(
			&temporalv1alpha1.Connection{},
			handler.EnqueueRequestsFromMapFunc(
				r.searchAttributesForDependency(searchAttributeConnectionRefIndex),
			),
		).
		Watches(
			&temporalv1alpha1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(
				r.searchAttributesForDependency(searchAttributeNamespaceRefIndex),
			),
		).
		Named("searchattribute").
		Complete(r)
}
