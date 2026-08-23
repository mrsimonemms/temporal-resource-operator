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

// Reasons reported on a NexusEndpoint's Ready condition, in addition to those
// it shares with the other controllers.
const (
	// ReasonEndpointTaken means an endpoint of this name already exists but the
	// operator has just been told to create one. Endpoint names are unique
	// Service-wide, so something else got there first.
	ReasonEndpointTaken = "EndpointTaken"

	// ReasonConflict means the endpoint was changed by something else between
	// being read and being written, so the change was refused. The next
	// reconcile starts again from a fresh read.
	ReasonConflict = "Conflict"
)

// ReasonUpdated and ReasonReconciled are shared with the Namespace controller,
// which reports configuration drift the same way.

// Field indexes registered on NexusEndpoint, so that a dependency event can
// find its dependants with one indexed lookup rather than scanning.
const (
	nexusEndpointConnectionRefIndex = ".spec.connectionRef.name"
	nexusEndpointNamespaceRefIndex  = ".spec.namespaceRef.name"
)

// TemporalNexusEndpointClient is the part of the Temporal client wrapper the
// NexusEndpoint reconciler uses. It exists so that controller tests can run
// without a Temporal Service.
type TemporalNexusEndpointClient interface {
	DescribeNexusEndpoint(ctx context.Context, name string) (*temporal.NexusEndpoint, error)
	CreateNexusEndpoint(ctx context.Context, name, namespace, taskQueue string) (*temporal.NexusEndpoint, error)
	UpdateNexusEndpointTarget(
		ctx context.Context, endpoint *temporal.NexusEndpoint, namespace, taskQueue string,
	) (*temporal.NexusEndpoint, error)
	DeleteNexusEndpoint(ctx context.Context, endpoint *temporal.NexusEndpoint) error
	Close()
}

// NexusEndpointReconciler reconciles a NexusEndpoint object
type NexusEndpointReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Resolver turns a Connection into Temporal client options.
	Resolver *connection.Resolver

	// Connect dials a Temporal Service. Defaults to the internal/temporal
	// package when unset.
	Connect func(ctx context.Context, opts *sdkclient.Options) (TemporalNexusEndpointClient, error)
}

// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=nexusendpoints,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=nexusendpoints/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=nexusendpoints/finalizers,verbs=update
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=connections,verbs=get;list;watch
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=namespaces,verbs=get;list;watch

// Reconcile ensures the Temporal Nexus endpoint named by this resource exists
// and routes where the spec says, and is removed again if - and only if - this
// resource is what created it.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
//
//nolint:dupl // parallel with the sibling controllers on purpose; see dependency.go
func (r *NexusEndpointReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	endpoint := &temporalv1alpha1.NexusEndpoint{}
	if err := r.Get(ctx, req.NamespacedName, endpoint); err != nil {
		if apierrors.IsNotFound(err) {
			// Deleted, or the cache is behind. Either way there is nothing to
			// reconcile - importantly, do not carry on with a zero-value
			// NexusEndpoint.
			log.Info("NexusEndpoint no longer exists")
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	// Snapshot the status as it is stored, before anything below mutates it in
	// memory, so a change made during reconciliation is never mistaken for a
	// no-op.
	observed := endpoint.Status.DeepCopy()

	if !endpoint.GetDeletionTimestamp().IsZero() {
		return r.finaliseEndpoint(ctx, endpoint, observed)
	}

	// The finalizer has to be in place before anything is created in Temporal,
	// or a deletion racing the create would orphan the endpoint.
	if err := r.ensureEndpointFinalizer(ctx, endpoint); err != nil {
		return ctrl.Result{}, err
	}

	reason, message, reconcileErr := r.reconcileEndpoint(ctx, endpoint)

	condition := &metav1.Condition{
		Type:               temporalv1alpha1.ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: endpoint.Generation,
	}
	if reconcileErr != nil {
		condition.Status = metav1.ConditionFalse
		condition.Message = reconcileErr.Error()
	}

	if err := r.updateEndpointStatus(ctx, endpoint, observed, condition); err != nil {
		return ctrl.Result{}, err
	}

	switch {
	case reconcileErr == nil:
		return ctrl.Result{RequeueAfter: namespaceResyncInterval}, nil
	case isDependencyReason(reason):
		// The dependency will become ready on its own schedule, and the watches
		// below wake us the moment it does.
		log.Info("Waiting for a dependency", "reason", reason)
		return ctrl.Result{RequeueAfter: dependencyRetryInterval}, nil
	default:
		log.Error(reconcileErr, "Failed to reconcile NexusEndpoint", "reason", reason)
		return ctrl.Result{}, reconcileErr
	}
}

// reconcileEndpoint brings the Temporal Nexus endpoint into line with the spec.
func (r *NexusEndpointReconciler) reconcileEndpoint(
	ctx context.Context,
	endpoint *temporalv1alpha1.NexusEndpoint,
) (reason, message string, err error) {
	conn, reason, err := r.readyEndpointDependencies(ctx, endpoint)
	if err != nil {
		return reason, "", err
	}

	ctx, cancel := context.WithTimeout(ctx, namespaceTimeout)
	defer cancel()

	temporalClient, reason, err := r.endpointClient(ctx, conn)
	if err != nil {
		return reason, "", err
	}
	defer temporalClient.Close()

	name := endpoint.TemporalName()

	actual, err := temporalClient.DescribeNexusEndpoint(ctx, name)
	switch {
	case err == nil:
		return r.reconcileExistingEndpoint(ctx, endpoint, temporalClient, actual)
	case !errors.Is(err, temporal.ErrNexusEndpointNotFound):
		return ReasonDescribeFailed, "", err
	}

	return r.createEndpoint(ctx, endpoint, temporalClient)
}

// reconcileExistingEndpoint settles ownership of an endpoint that already
// exists and corrects any drift in its target.
//
// Adoption decides who may delete the endpoint, not whether its configuration
// is managed: an adopted endpoint is kept in step with the spec just like one
// the operator created, and stays adopted while doing so.
func (r *NexusEndpointReconciler) reconcileExistingEndpoint(
	ctx context.Context,
	endpoint *temporalv1alpha1.NexusEndpoint,
	temporalClient TemporalNexusEndpointClient,
	actual *temporal.NexusEndpoint,
) (reason, message string, err error) {
	name := endpoint.TemporalName()
	namespace := endpoint.TemporalNamespace()
	taskQueue := endpoint.Spec.TaskQueue

	adopting := establishEndpointOwnership(endpoint)
	endpoint.Status.EndpointID = actual.ID

	if actual.TargetMatches(namespace, taskQueue) {
		if adopting {
			return ReasonAdopted, fmt.Sprintf("Adopted existing Nexus endpoint %q", name), nil
		}

		return ReasonReconciled, fmt.Sprintf("Nexus endpoint %q matches the spec", name), nil
	}

	// Update in place rather than replacing the endpoint: its ID is what
	// callers resolve through, and dropping and recreating it would break them
	// for no reason. The version carried by the endpoint just read is what
	// makes this safe against a concurrent change.
	updated, err := temporalClient.UpdateNexusEndpointTarget(ctx, actual, namespace, taskQueue)
	if err != nil {
		if errors.Is(err, temporal.ErrNexusEndpointChanged) {
			// Something else moved it between the read and the write. Report it
			// and let the retry start again from a fresh read, rather than
			// guessing at a newer version here.
			return ReasonConflict, "", err
		}

		return ReasonUpdateFailed, "", err
	}

	endpoint.Status.EndpointID = updated.ID

	return ReasonUpdated, fmt.Sprintf(
		"Pointed Nexus endpoint %q at %s/%s, was %s/%s",
		name, namespace, taskQueue, actual.TargetNamespace, actual.TaskQueue,
	), nil
}

// createEndpoint registers an endpoint that does not exist and, if ownership
// has not been settled yet, takes ownership of it.
//
// Ownership is recorded as Creating before the create call, so that a reconcile
// interrupted between creating and recording the result can tell that what it
// now finds is its own work. As with search attributes there is no marker on
// the endpoint to confirm that from the outside; see
// establishEndpointOwnership for what that costs.
//
// An already-settled ownership is left alone: an adopted endpoint that has
// since been removed behind the operator's back is put back, but that does not
// make it the operator's to delete.
func (r *NexusEndpointReconciler) createEndpoint(
	ctx context.Context,
	endpoint *temporalv1alpha1.NexusEndpoint,
	temporalClient TemporalNexusEndpointClient,
) (reason, message string, err error) {
	establishing := !endpoint.Status.Ownership.IsEstablished()

	if establishing {
		if err := r.persistEndpointOwnership(
			ctx, endpoint, temporalv1alpha1.NexusEndpointOwnershipCreating,
		); err != nil {
			return ReasonCreateFailed, "", fmt.Errorf("recording intent to create nexus endpoint: %w", err)
		}
	}

	name := endpoint.TemporalName()
	namespace := endpoint.TemporalNamespace()
	taskQueue := endpoint.Spec.TaskQueue

	created, err := temporalClient.CreateNexusEndpoint(ctx, name, namespace, taskQueue)
	if err != nil {
		if errors.Is(err, temporal.ErrNexusEndpointExists) {
			// The name was free a moment ago and is not any more. Whatever took
			// it is not this resource's to touch, so say so rather than
			// adopting something that appeared out from under us.
			return ReasonEndpointTaken, "", err
		}

		return ReasonCreateFailed, "", err
	}

	endpoint.Status.EndpointID = created.ID

	if establishing {
		// Settled in memory; the caller's status write persists it alongside
		// the Ready condition. Should that write fail, ownership stays at
		// Creating and the next reconcile resolves it to Created.
		endpoint.Status.Ownership = temporalv1alpha1.NexusEndpointOwnershipCreated
	}

	return ReasonCreated, fmt.Sprintf(
		"Created Nexus endpoint %q routing to %s/%s", name, namespace, taskQueue,
	), nil
}

// establishEndpointOwnership settles, once, how this resource came to manage an
// existing endpoint. It reports whether that settled to a fresh adoption.
//
// A namespace can be asked who created it; a Nexus endpoint cannot. Its only
// free-form field is a markdown description meant for people, and putting a
// marker there would be a misuse of it. Creating is therefore the only evidence
// available that an interrupted reconcile created what we are now looking at.
//
// That evidence is strong but not proof: the endpoint was absent when Creating
// was written and one of exactly that name is now there, so it is almost
// certainly this resource's own work - but endpoint names are Service-wide, so
// another actor could have taken the name in the intervening moment. Reading it
// as Created is the deliberate choice, because the alternative would make the
// marker useless for the failure it exists to survive.
func establishEndpointOwnership(endpoint *temporalv1alpha1.NexusEndpoint) bool {
	if endpoint.Status.Ownership.IsEstablished() {
		return false
	}

	if endpoint.Status.Ownership == temporalv1alpha1.NexusEndpointOwnershipCreating {
		endpoint.Status.Ownership = temporalv1alpha1.NexusEndpointOwnershipCreated

		return false
	}

	endpoint.Status.Ownership = temporalv1alpha1.NexusEndpointOwnershipAdopted

	return true
}

// finaliseEndpoint runs the deletion flow: remove the Temporal Nexus endpoint
// if this resource created it and has been asked to, then release the
// finalizer.
//
// This mirrors the Namespace and SearchAttribute finalisers step by step, on
// purpose; see the note on Namespace.finalise for why the three are not folded
// together.
//
//nolint:dupl // mirrors the other finalisers on purpose; see above
func (r *NexusEndpointReconciler) finaliseEndpoint(
	ctx context.Context,
	endpoint *temporalv1alpha1.NexusEndpoint,
	observed *temporalv1alpha1.NexusEndpointStatus,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(endpoint, temporalv1alpha1.NexusEndpointFinalizer) {
		return ctrl.Result{}, nil
	}

	name := endpoint.TemporalName()
	ownership := endpoint.Status.Ownership

	// Orphan is checked first, and deliberately before anything that needs a
	// Connection. It is the way out of a deletion that would otherwise be
	// blocked, so it must not depend on any of the machinery that could be what
	// is broken.
	if policy := endpoint.Spec.DeletionPolicyValue(); policy == temporalv1alpha1.NexusEndpointDeletionPolicyOrphan {
		log.Info("Leaving Temporal Nexus endpoint in place",
			"endpoint", name, "deletionPolicy", string(policy))

		return ctrl.Result{}, r.removeEndpointFinalizer(ctx, endpoint)
	}

	if !ownership.OwnsEndpoint() {
		// Adopted, or never established. Either way this resource did not
		// create the endpoint, so it has no business removing it.
		log.Info("Leaving Temporal Nexus endpoint in place",
			"endpoint", name, "ownership", string(ownership))

		return ctrl.Result{}, r.removeEndpointFinalizer(ctx, endpoint)
	}

	if reason, err := r.deleteEndpoint(ctx, endpoint); err != nil {
		log.Error(err, "Failed to delete Temporal Nexus endpoint", "endpoint", name, "reason", reason)

		condition := &metav1.Condition{
			Type:               temporalv1alpha1.ConditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            err.Error(),
			ObservedGeneration: endpoint.Generation,
		}
		if statusErr := r.updateEndpointStatus(ctx, endpoint, observed, condition); statusErr != nil {
			log.Error(statusErr, "Failed to record deletion failure")
		}

		return ctrl.Result{}, err
	}

	log.Info("Finished with Temporal Nexus endpoint", "endpoint", name)

	return ctrl.Result{}, r.removeEndpointFinalizer(ctx, endpoint)
}

// deleteEndpoint removes the Nexus endpoint this resource created.
//
// Nothing here needs the target namespace, and that is not an oversight. A
// Nexus endpoint belongs to the Service rather than to a namespace: it is found
// by its Service-wide name and removed by its own ID. The Namespace resource
// being gone, or its Temporal namespace being gone, has no bearing on whether
// the endpoint is still there - and Temporal will not let a namespace be
// deleted while an endpoint targets it anyway, so the awkward ordering that
// search attributes have to allow for cannot arise.
func (r *NexusEndpointReconciler) deleteEndpoint(
	ctx context.Context,
	endpoint *temporalv1alpha1.NexusEndpoint,
) (string, error) {
	log := logf.FromContext(ctx)

	// Deletion needs a working Connection. Readiness is deliberately not
	// required: it is a cached judgement that may be stale, and giving up on an
	// endpoint this resource owns because of a stale status would orphan it.
	conn, reason, err := getConnection(ctx, r.Client, endpoint.Spec.ConnectionRef.Name, endpoint.Namespace)
	if err != nil {
		return reason, err
	}

	ctx, cancel := context.WithTimeout(ctx, namespaceTimeout)
	defer cancel()

	temporalClient, reason, err := r.endpointClient(ctx, conn)
	if err != nil {
		return reason, err
	}
	defer temporalClient.Close()

	name := endpoint.TemporalName()

	actual, err := temporalClient.DescribeNexusEndpoint(ctx, name)
	if err != nil {
		if errors.Is(err, temporal.ErrNexusEndpointNotFound) {
			// Nothing left to remove.
			log.Info("Temporal Nexus endpoint is already gone", "endpoint", name)

			return "", nil
		}

		return ReasonDescribeFailed, err
	}

	if err := temporalClient.DeleteNexusEndpoint(ctx, actual); err != nil {
		if errors.Is(err, temporal.ErrNexusEndpointNotFound) {
			// Removed between the read and the delete.
			return "", nil
		}

		return ReasonDeleteFailed, err
	}

	return "", nil
}

// readyEndpointDependencies fetches the Connection and Namespace this resource
// needs and checks that both have reported themselves Ready.
//
// The Namespace gate matters here: Temporal refuses to create an endpoint whose
// target namespace does not exist.
//
//nolint:dupl // parallel with the sibling controllers on purpose; see dependency.go
func (r *NexusEndpointReconciler) readyEndpointDependencies(
	ctx context.Context,
	endpoint *temporalv1alpha1.NexusEndpoint,
) (*temporalv1alpha1.Connection, string, error) {
	conn, reason, err := getConnection(ctx, r.Client, endpoint.Spec.ConnectionRef.Name, endpoint.Namespace)
	if err != nil {
		return nil, reason, err
	}

	if reason, err := readyCondition(conn.Status.Conditions, "connection", client.ObjectKeyFromObject(conn),
		ReasonConnectionNotReady); err != nil {
		return nil, reason, err
	}

	namespace := &temporalv1alpha1.Namespace{}
	key := types.NamespacedName{Namespace: endpoint.Namespace, Name: endpoint.Spec.NamespaceRef.Name}

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

// endpointClient resolves a Connection and dials the Temporal Service it
// describes.
func (r *NexusEndpointReconciler) endpointClient(
	ctx context.Context,
	conn *temporalv1alpha1.Connection,
) (TemporalNexusEndpointClient, string, error) {
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

// ensureEndpointFinalizer adds the finalizer if it is missing.
func (r *NexusEndpointReconciler) ensureEndpointFinalizer(
	ctx context.Context,
	endpoint *temporalv1alpha1.NexusEndpoint,
) error {
	patch := endpointFinalizerPatch(endpoint)

	if !controllerutil.AddFinalizer(endpoint, temporalv1alpha1.NexusEndpointFinalizer) {
		return nil
	}

	return r.Patch(ctx, endpoint, patch)
}

// removeEndpointFinalizer releases the finalizer, allowing Kubernetes to remove
// the resource.
func (r *NexusEndpointReconciler) removeEndpointFinalizer(
	ctx context.Context,
	endpoint *temporalv1alpha1.NexusEndpoint,
) error {
	patch := endpointFinalizerPatch(endpoint)

	if !controllerutil.RemoveFinalizer(endpoint, temporalv1alpha1.NexusEndpointFinalizer) {
		return nil
	}

	return r.Patch(ctx, endpoint, patch)
}

// endpointFinalizerPatch captures the resource as it stands, so that only the
// finalizer change is sent.
//
// A full update would round-trip the spec, and the API server would read the
// re-marshalled result as a change and bump the generation, waking the
// controller again for no reason.
func endpointFinalizerPatch(endpoint *temporalv1alpha1.NexusEndpoint) client.Patch {
	return client.MergeFromWithOptions(endpoint.DeepCopy(), client.MergeFromWithOptimisticLock{})
}

// persistEndpointOwnership writes an ownership transition straight to the API
// server, because the value has to be durable before the Temporal call it
// describes is made.
func (r *NexusEndpointReconciler) persistEndpointOwnership(
	ctx context.Context,
	endpoint *temporalv1alpha1.NexusEndpoint,
	ownership temporalv1alpha1.NexusEndpointOwnership,
) error {
	if endpoint.Status.Ownership == ownership {
		return nil
	}

	endpoint.Status.Ownership = ownership

	return r.Status().Update(ctx, endpoint)
}

// updateEndpointStatus writes the condition, doing nothing if the stored status
// already says the same thing.
func (r *NexusEndpointReconciler) updateEndpointStatus(
	ctx context.Context,
	endpoint *temporalv1alpha1.NexusEndpoint,
	observed *temporalv1alpha1.NexusEndpointStatus,
	condition *metav1.Condition,
) error {
	meta.SetStatusCondition(&endpoint.Status.Conditions, *condition)
	endpoint.Status.ObservedGeneration = endpoint.Generation

	if equality.Semantic.DeepEqual(observed, &endpoint.Status) {
		return nil
	}

	return r.Status().Update(ctx, endpoint)
}

// indexNexusEndpointByConnection extracts the Connection a NexusEndpoint
// references.
func indexNexusEndpointByConnection(obj client.Object) []string {
	return nexusEndpointRef(obj, func(spec temporalv1alpha1.NexusEndpointSpec) string {
		return spec.ConnectionRef.Name
	})
}

// indexNexusEndpointByNamespace extracts the Namespace a NexusEndpoint
// references.
func indexNexusEndpointByNamespace(obj client.Object) []string {
	return nexusEndpointRef(obj, func(spec temporalv1alpha1.NexusEndpointSpec) string {
		return spec.NamespaceRef.Name
	})
}

// nexusEndpointRef pulls one reference out of a NexusEndpoint for indexing.
//
// It is called for every object the cache holds, including ones that are
// half-built or of the wrong type, so it never assumes anything about what it
// is handed.
func nexusEndpointRef(obj client.Object, ref func(temporalv1alpha1.NexusEndpointSpec) string) []string {
	endpoint, ok := obj.(*temporalv1alpha1.NexusEndpoint)
	if !ok || endpoint == nil {
		return nil
	}

	name := ref(endpoint.Spec)
	if name == "" {
		return nil
	}

	return []string{name}
}

// nexusEndpointsForDependency maps an event on a dependency onto the
// NexusEndpoints that reference it through the given index.
//
// Every kind involved is namespaced, and a NexusEndpoint only ever references
// dependencies beside it, so the list is confined to the event's own Kubernetes
// namespace as well as being filtered by the index.
func (r *NexusEndpointReconciler) nexusEndpointsForDependency(index string) handler.MapFunc {
	return func(ctx context.Context, dependency client.Object) []ctrl.Request {
		return dependants(ctx, r.Client, &temporalv1alpha1.NexusEndpointList{}, index, dependency)
	}
}

// SetupWithManager sets up the controller with the Manager.
//
//nolint:dupl // parallel with the sibling controllers on purpose; see dependency.go
func (r *NexusEndpointReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Resolver == nil {
		// Read through the API reader rather than the cache: caching Secrets
		// would mean watching every Secret in the cluster, and the operator
		// only ever needs to get the ones a Connection names.
		r.Resolver = connection.NewResolver(mgr.GetAPIReader())
	}

	indexes := map[string]client.IndexerFunc{
		nexusEndpointConnectionRefIndex: indexNexusEndpointByConnection,
		nexusEndpointNamespaceRefIndex:  indexNexusEndpointByNamespace,
	}

	for field, extract := range indexes {
		// The cache has not started yet, so this only registers the indexer and
		// returns; there is nothing for a caller's context to cancel.
		if err := mgr.GetFieldIndexer().IndexField(
			context.Background(), &temporalv1alpha1.NexusEndpoint{}, field, extract,
		); err != nil {
			return fmt.Errorf("indexing nexus endpoints by %s: %w", field, err)
		}
	}

	return ctrl.NewControllerManagedBy(mgr).
		// Only the spec matters here, so ignore the status writes this
		// controller makes itself. Kubernetes bumps the generation when it
		// stamps a deletion timestamp, so the finalizer flow still runs.
		For(&temporalv1alpha1.NexusEndpoint{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		// Wake dependants as soon as a dependency changes. Both watches are
		// deliberately unfiltered: readiness lives in status, so the generation
		// predicate guarding the primary resource above would discard precisely
		// the events that matter.
		Watches(
			&temporalv1alpha1.Connection{},
			handler.EnqueueRequestsFromMapFunc(
				r.nexusEndpointsForDependency(nexusEndpointConnectionRefIndex),
			),
		).
		Watches(
			&temporalv1alpha1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(
				r.nexusEndpointsForDependency(nexusEndpointNamespaceRefIndex),
			),
		).
		Named("nexusendpoint").
		Complete(r)
}
