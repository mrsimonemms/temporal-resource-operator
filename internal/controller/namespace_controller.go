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
	"time"

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

// Reasons reported on a Namespace's Ready condition. They describe what the
// reconcile did; the durable record of who owns the namespace is
// status.ownership.
const (
	// ReasonCreated means the Temporal namespace was registered by this
	// reconcile.
	ReasonCreated = "Created"

	// ReasonAdopted means a pre-existing Temporal namespace was taken under
	// management by this reconcile.
	ReasonAdopted = "Adopted"

	// ReasonUpdated means the Temporal namespace's configuration had drifted
	// and was corrected.
	ReasonUpdated = "Updated"

	// ReasonReconciled means the Temporal namespace already matched the spec.
	ReasonReconciled = "Reconciled"

	// ReasonConnectionNotFound means the referenced Connection does not exist.
	ReasonConnectionNotFound = "ConnectionNotFound"

	// ReasonConnectionNotReady means the referenced Connection exists but has
	// not reported Ready=True.
	ReasonConnectionNotReady = "ConnectionNotReady"

	// ReasonDescribeFailed means the Temporal namespace could not be looked up.
	ReasonDescribeFailed = "DescribeFailed"

	// ReasonCreateFailed means the Temporal namespace could not be registered.
	ReasonCreateFailed = "CreateFailed"

	// ReasonUpdateFailed means drifted configuration could not be corrected.
	ReasonUpdateFailed = "UpdateFailed"

	// ReasonDeleteFailed means an owned Temporal namespace could not be
	// removed, so the finalizer is being held.
	ReasonDeleteFailed = "DeleteFailed"

	// ReasonOwnershipConflict means the Temporal namespace carries another
	// Namespace resource's ownership marker. The operator will not adopt,
	// reconfigure or remove it.
	ReasonOwnershipConflict = "OwnershipConflict"

	// ReasonOwnershipUnverified means a namespace this resource believes it
	// owns cannot be confirmed as its own from Temporal's own metadata, so a
	// destructive operation has been refused.
	ReasonOwnershipUnverified = "OwnershipUnverified"
)

// namespaceOwnerKey is the Temporal namespace metadata key under which the
// operator records the UID of the Namespace resource that registered it.
//
// The UID, rather than the name, is what makes this useful: deleting a resource
// and recreating it under the same name produces a new UID, so the replacement
// cannot inherit the original's claim on a Temporal namespace.
const namespaceOwnerKey = "temporal.simonemms.com/owner-uid"

// namespaceOwner is what a Temporal namespace's own metadata says about who
// owns it. It is deliberately independent of status.ownership: the point of the
// marker is to check the operator's belief against the outside world.
type namespaceOwner int

const (
	// ownerUnmarked means the namespace carries no operator ownership marker.
	ownerUnmarked namespaceOwner = iota

	// ownerSelf means the marker names the resource being reconciled.
	ownerSelf

	// ownerOther means the marker names some other resource.
	ownerOther
)

// namespaceConnectionRefIndex indexes Namespace resources by the name of the
// Connection they reference. A Connection event can then find its dependants
// with one indexed lookup instead of listing and scanning every Namespace.
const namespaceConnectionRefIndex = ".spec.connectionRef.name"

const (
	// namespaceResyncInterval is how often a reconciled Namespace is checked
	// again. The Temporal Service is outside Kubernetes, so nothing will tell
	// us if the namespace disappears or is reconfigured behind our back.
	namespaceResyncInterval = 5 * time.Minute

	// dependencyRetryInterval is how soon to look again when the referenced
	// Connection is missing or not yet Ready. This is an expected transient
	// state rather than a failure, so it is retried on a fixed interval rather
	// than through error backoff.
	//
	// The watch on Connection is what normally ends the wait, and it does so
	// immediately. This stays as a backstop for the cases a watch cannot cover
	// - a dropped event, a cache resync gap - and is cheap enough at one
	// reconcile per waiting Namespace per interval to leave alone.
	dependencyRetryInterval = 30 * time.Second

	// namespaceTimeout bounds the Temporal calls made by a single reconcile, so
	// an unresponsive Service cannot wedge a reconcile worker.
	namespaceTimeout = 30 * time.Second
)

// TemporalNamespaceClient is the part of the Temporal client wrapper the
// Namespace reconciler uses. It exists so that controller tests can run without
// a Temporal Service.
type TemporalNamespaceClient interface {
	DescribeNamespace(ctx context.Context, name string) (*temporal.Namespace, error)
	CreateNamespace(ctx context.Context, name string, retention time.Duration, data map[string]string) error
	UpdateNamespaceRetention(ctx context.Context, name string, retention time.Duration) error
	DeleteNamespace(ctx context.Context, name string) error
	Close()
}

// NamespaceReconciler reconciles a Namespace object
type NamespaceReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Resolver turns a Connection into Temporal client options.
	Resolver *connection.Resolver

	// Connect dials a Temporal Service. Defaults to the internal/temporal
	// package when unset.
	Connect func(ctx context.Context, opts *sdkclient.Options) (TemporalNamespaceClient, error)
}

// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=namespaces,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=namespaces/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=namespaces/finalizers,verbs=update
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=connections,verbs=get;list;watch

// Reconcile ensures the Temporal namespace named by this resource exists on the
// referenced Connection's Temporal Service, matches the spec, and is removed
// again if - and only if - this resource is what created it.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *NamespaceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	ns := &temporalv1alpha1.Namespace{}
	if err := r.Get(ctx, req.NamespacedName, ns); err != nil {
		if apierrors.IsNotFound(err) {
			// Deleted, or the cache is behind. Either way there is nothing to
			// reconcile - importantly, do not carry on with a zero-value
			// Namespace.
			log.Info("Namespace no longer exists")
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	// Snapshot the status as it is stored, before anything below mutates it in
	// memory. The final write compares against this rather than against its own
	// starting point, so a change made during reconciliation - ownership in
	// particular - is never mistaken for a no-op.
	observed := ns.Status.DeepCopy()

	if !ns.GetDeletionTimestamp().IsZero() {
		return r.finalise(ctx, ns, observed)
	}

	// The finalizer has to be in place before anything is registered in
	// Temporal. Registering first would leave a window in which a deletion
	// races the create and orphans the namespace.
	if err := r.ensureFinalizer(ctx, ns); err != nil {
		return ctrl.Result{}, err
	}

	reason, message, reconcileErr := r.reconcileNamespace(ctx, ns)

	condition := &metav1.Condition{
		Type:               temporalv1alpha1.ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: ns.Generation,
	}
	if reconcileErr != nil {
		condition.Status = metav1.ConditionFalse
		condition.Message = reconcileErr.Error()
	}

	if err := r.updateNamespaceStatus(ctx, ns, observed, condition); err != nil {
		return ctrl.Result{}, err
	}

	switch {
	case reconcileErr == nil:
		return ctrl.Result{RequeueAfter: namespaceResyncInterval}, nil
	case reason == ReasonConnectionNotFound, reason == ReasonConnectionNotReady:
		// The dependency will become ready on its own schedule. Waiting for it
		// is not a failure, so retry on a fixed interval rather than burning
		// error backoff on it.
		log.Info("Waiting for Connection", "connection", ns.Spec.ConnectionRef.Name, "reason", reason)
		return ctrl.Result{RequeueAfter: dependencyRetryInterval}, nil
	default:
		log.Error(reconcileErr, "Failed to reconcile Namespace", "reason", reason)
		// Returning the error requeues with the controller's backoff.
		return ctrl.Result{}, reconcileErr
	}
}

// reconcileNamespace brings the Temporal namespace into line with the spec,
// returning the reason and, on success, the message to report. A non-nil error
// means the Namespace is not Ready.
func (r *NamespaceReconciler) reconcileNamespace(
	ctx context.Context,
	ns *temporalv1alpha1.Namespace,
) (reason, message string, err error) {
	conn, reason, err := r.readyConnection(ctx, ns)
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

	actual, err := temporalClient.DescribeNamespace(ctx, ns.TemporalName())
	switch {
	case err == nil:
		return r.reconcileExisting(ctx, ns, temporalClient, actual)
	case !errors.Is(err, temporal.ErrNamespaceNotFound):
		return ReasonDescribeFailed, "", err
	}

	return r.createNamespace(ctx, ns, temporalClient)
}

// createNamespace registers a Temporal namespace that does not exist and, if
// ownership has not been settled yet, takes ownership of it.
//
// Ownership is recorded as Creating *before* the registration call. That write
// is what makes the create survivable: if this reconcile dies between
// registering the namespace and recording the result, the next one finds the
// namespace present with ownership already at Creating, and so knows the
// namespace is its own work rather than something to adopt.
//
// An already-settled ownership is left alone. A namespace that was adopted and
// has since been removed behind the operator's back is put back, but that does
// not make the operator its owner - the user's namespace does not become the
// operator's to delete just because it had to be restored.
func (r *NamespaceReconciler) createNamespace(
	ctx context.Context,
	ns *temporalv1alpha1.Namespace,
	temporalClient TemporalNamespaceClient,
) (reason, message string, err error) {
	establishing := !ns.Status.Ownership.IsEstablished()

	if establishing {
		if err := r.persistOwnership(ctx, ns, temporalv1alpha1.NamespaceOwnershipCreating); err != nil {
			return ReasonCreateFailed, "", fmt.Errorf("recording intent to create Temporal namespace: %w", err)
		}
	}

	name := ns.TemporalName()
	retention := ns.Spec.RetentionDuration()

	// Stamp the namespace with this resource's identity as it is registered, so
	// that no namespace the operator owns has ever existed unmarked. A
	// namespace being restored under an adopted ownership is left unmarked: the
	// operator manages it, but it was never the operator's to own.
	var data map[string]string
	if ns.Status.Ownership != temporalv1alpha1.NamespaceOwnershipAdopted {
		data = map[string]string{namespaceOwnerKey: string(ns.UID)}
	}

	if err := temporalClient.CreateNamespace(ctx, name, retention, data); err != nil {
		return ReasonCreateFailed, "", err
	}

	if establishing {
		// Settled in memory; the caller's status write persists it alongside
		// the Ready condition. Should that write fail, ownership stays at
		// Creating and the next reconcile resolves it to Created.
		ns.Status.Ownership = temporalv1alpha1.NamespaceOwnershipCreated
	}

	return ReasonCreated, fmt.Sprintf("Registered Temporal namespace %q with retention %s", name, retention), nil
}

// reconcileExisting takes ownership of a Temporal namespace that already exists
// and corrects any drift in the settings the operator manages.
func (r *NamespaceReconciler) reconcileExisting(
	ctx context.Context,
	ns *temporalv1alpha1.Namespace,
	temporalClient TemporalNamespaceClient,
	actual *temporal.Namespace,
) (reason, message string, err error) {
	name := ns.TemporalName()

	owner, marker := ownerOf(ns, actual)
	if owner == ownerOther {
		// A conflict outranks everything else. This namespace is not ours to
		// adopt, to reconfigure, or - later - to delete, and the operator will
		// not take it over on its own initiative.
		return ReasonOwnershipConflict, "", fmt.Errorf(
			"temporal namespace %q belongs to Namespace UID %s, not %s", name, marker, ns.UID,
		)
	}

	adopting := establishOwnership(ns, owner)

	desired := ns.Spec.RetentionDuration()
	if actual.Retention == desired {
		if adopting {
			return ReasonAdopted, fmt.Sprintf("Adopted existing Temporal namespace %q", name), nil
		}

		return ReasonReconciled, fmt.Sprintf("Temporal namespace %q matches the spec", name), nil
	}

	if err := temporalClient.UpdateNamespaceRetention(ctx, name, desired); err != nil {
		return ReasonUpdateFailed, "", err
	}

	return ReasonUpdated, fmt.Sprintf(
		"Updated Temporal namespace %q retention from %s to %s", name, actual.Retention, desired,
	), nil
}

// ownerOf reads the ownership marker off a Temporal namespace and compares it
// with the resource in hand, also returning the marker's raw value so that a
// conflict can be reported usefully.
func ownerOf(ns *temporalv1alpha1.Namespace, actual *temporal.Namespace) (owner namespaceOwner, marker string) {
	if actual == nil {
		return ownerUnmarked, ""
	}

	marker = actual.Data[namespaceOwnerKey]

	switch {
	case marker == "":
		return ownerUnmarked, ""
	case marker == string(ns.UID):
		return ownerSelf, marker
	default:
		return ownerOther, marker
	}
}

// establishOwnership settles, once, how this resource came to manage an
// existing Temporal namespace. It reports whether that settled to a fresh
// adoption.
//
// The decision comes from Temporal's own metadata rather than from the mere
// existence of the namespace. An interrupted create is recognised by its
// marker, not by the coincidence of a namespace being there.
//
// An established ownership is never revisited: whether the namespace exists
// right now says nothing about who created it, so re-deriving ownership from
// its existence would let a Created namespace silently become Adopted.
func establishOwnership(ns *temporalv1alpha1.Namespace, owner namespaceOwner) bool {
	if ns.Status.Ownership.IsEstablished() {
		return false
	}

	if owner == ownerSelf {
		// Temporal says this resource registered the namespace, so it did -
		// however the reconcile that registered it happened to end.
		ns.Status.Ownership = temporalv1alpha1.NamespaceOwnershipCreated

		return false
	}

	// No marker. Whatever is there was not registered by this resource, even if
	// an earlier reconcile was part-way through creating one: the operator only
	// ever registers namespaces with the marker already on them. Adopting is
	// the conservative reading, because an adopted namespace is never deleted.
	ns.Status.Ownership = temporalv1alpha1.NamespaceOwnershipAdopted

	return true
}

// finalise runs the deletion flow: remove the Temporal namespace if this
// resource owns it and has been asked to, then release the finalizer.
func (r *NamespaceReconciler) finalise(
	ctx context.Context,
	ns *temporalv1alpha1.Namespace,
	observed *temporalv1alpha1.NamespaceStatus,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(ns, temporalv1alpha1.NamespaceFinalizer) {
		// Nothing is holding the resource open, so it is already on its way out.
		return ctrl.Result{}, nil
	}

	name := ns.TemporalName()
	ownership := ns.Status.Ownership

	// Orphan is checked first, and deliberately before anything that needs a
	// Connection. It is the way out of a deletion that would otherwise be
	// blocked forever - a namespace whose Connection has been deleted, or whose
	// ownership Temporal can no longer confirm - so it must not depend on any
	// of the machinery that could be what is broken.
	if policy := ns.Spec.DeletionPolicyValue(); policy == temporalv1alpha1.NamespaceDeletionPolicyOrphan {
		log.Info("Leaving Temporal namespace in place",
			"namespace", name, "deletionPolicy", string(policy))

		return ctrl.Result{}, r.removeFinalizer(ctx, ns)
	}

	if !ownership.OwnsTemporalNamespace() {
		// Adopted, or never established. Either way this resource did not
		// create the namespace, so it has no business deleting it.
		log.Info("Leaving Temporal namespace in place",
			"namespace", name, "ownership", string(ownership))

		return ctrl.Result{}, r.removeFinalizer(ctx, ns)
	}

	if reason, err := r.deleteTemporalNamespace(ctx, ns); err != nil {
		log.Error(err, "Failed to delete Temporal namespace", "namespace", name, "reason", reason)

		// Hold the finalizer. Releasing it now would either orphan a namespace
		// this resource is responsible for, or walk away from an unresolved
		// ownership problem without anyone noticing.
		condition := &metav1.Condition{
			Type:               temporalv1alpha1.ConditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            err.Error(),
			ObservedGeneration: ns.Generation,
		}
		if statusErr := r.updateNamespaceStatus(ctx, ns, observed, condition); statusErr != nil {
			// Reporting why deletion is stuck is a convenience; the deletion
			// error is the one worth retrying on.
			log.Error(statusErr, "Failed to record deletion failure")
		}

		return ctrl.Result{}, err
	}

	log.Info("Finished with Temporal namespace", "namespace", name)

	return ctrl.Result{}, r.removeFinalizer(ctx, ns)
}

// deleteTemporalNamespace removes the Temporal namespace this resource owns,
// but only once Temporal's own metadata confirms the namespace is this
// resource's to remove. It returns the reason to report when it refuses or
// fails.
//
// status.ownership is what gets us here; it is not on its own enough to destroy
// anything. The marker is checked immediately before the delete so that a
// namespace which has been replaced, re-registered by someone else, or stripped
// of its metadata since the resource was created is never removed by mistake.
//
// A namespace that has already gone counts as success, so deletion is
// idempotent across retries.
func (r *NamespaceReconciler) deleteTemporalNamespace(
	ctx context.Context,
	ns *temporalv1alpha1.Namespace,
) (string, error) {
	log := logf.FromContext(ctx)

	// Deletion needs a working Connection. Readiness is deliberately not
	// required here: it is a cached judgement that may be stale, and giving up
	// on an owned namespace because of a stale status would orphan it.
	conn, reason, err := r.getConnection(ctx, ns)
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

	name := ns.TemporalName()

	actual, err := temporalClient.DescribeNamespace(ctx, name)
	if err != nil {
		if errors.Is(err, temporal.ErrNamespaceNotFound) {
			// Nothing left to remove - possibly deleted by an earlier attempt
			// that failed before releasing the finalizer, possibly never
			// registered at all. This is the one place a missing namespace
			// counts as success.
			log.Info("Temporal namespace is already gone", "namespace", name)

			return "", nil
		}

		return ReasonDescribeFailed, err
	}

	switch owner, marker := ownerOf(ns, actual); owner {
	case ownerOther:
		return ReasonOwnershipConflict, fmt.Errorf(
			"refusing to delete temporal namespace %q: it belongs to Namespace UID %s, not %s",
			name, marker, ns.UID,
		)
	case ownerUnmarked:
		return ReasonOwnershipUnverified, fmt.Errorf(
			"refusing to delete temporal namespace %q: it carries no %s marker, so it cannot be confirmed "+
				"as this resource's to remove; resolve it by hand, then remove the %s finalizer",
			name, namespaceOwnerKey, temporalv1alpha1.NamespaceFinalizer,
		)
	case ownerSelf:
	}

	if err := temporalClient.DeleteNamespace(ctx, name); err != nil {
		// A not-found here is *not* treated as success. The describe above
		// proved the namespace is there, and the Service will briefly fail to
		// resolve a namespace registered moments earlier - it answers describe
		// from storage but delete from its namespace registry, which lags.
		// Releasing the finalizer on that would orphan a live namespace, so let
		// it retry instead; the registry catches up within seconds.
		return ReasonDeleteFailed, err
	}

	return "", nil
}

// ensureFinalizer adds the finalizer if it is missing.
func (r *NamespaceReconciler) ensureFinalizer(ctx context.Context, ns *temporalv1alpha1.Namespace) error {
	patch := finalizerPatch(ns)

	if !controllerutil.AddFinalizer(ns, temporalv1alpha1.NamespaceFinalizer) {
		return nil
	}

	return r.Patch(ctx, ns, patch)
}

// removeFinalizer releases the finalizer, allowing Kubernetes to remove the
// resource.
func (r *NamespaceReconciler) removeFinalizer(ctx context.Context, ns *temporalv1alpha1.Namespace) error {
	patch := finalizerPatch(ns)

	if !controllerutil.RemoveFinalizer(ns, temporalv1alpha1.NamespaceFinalizer) {
		return nil
	}

	return r.Patch(ctx, ns, patch)
}

// finalizerPatch captures the resource as it stands, so that only the finalizer
// change is sent.
//
// A full update would round-trip the spec, and marshalling it rewrites
// metav1.Duration into its canonical form - "36h" becomes "36h0m0s". The API
// server sees that as a spec change, bumps the generation, and wakes the
// controller up again for no reason. Optimistic locking makes a concurrent
// finalizer write a retryable conflict rather than a silent clobber.
func finalizerPatch(ns *temporalv1alpha1.Namespace) client.Patch {
	return client.MergeFromWithOptions(ns.DeepCopy(), client.MergeFromWithOptimisticLock{})
}

// getConnection fetches the referenced Connection without judging its
// readiness.
func (r *NamespaceReconciler) getConnection(
	ctx context.Context,
	ns *temporalv1alpha1.Namespace,
) (*temporalv1alpha1.Connection, string, error) {
	conn := &temporalv1alpha1.Connection{}
	key := types.NamespacedName{Namespace: ns.Namespace, Name: ns.Spec.ConnectionRef.Name}

	if err := r.Get(ctx, key, conn); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, ReasonConnectionNotFound, fmt.Errorf("connection %s not found: %w", key, err)
		}

		// The Connection may well be fine; we just could not read it. That is
		// still "readiness not established", so it is retried the same way.
		return nil, ReasonConnectionNotReady, fmt.Errorf("getting connection %s: %w", key, err)
	}

	return conn, "", nil
}

// readyConnection fetches the referenced Connection and checks that it has
// reported itself Ready.
//
// Readiness is a cheap gate that keeps the operator from dialling a Temporal
// Service already known to be broken. It is not a guarantee: a Connection can
// be stale, so Temporal calls made afterwards still report their own errors.
func (r *NamespaceReconciler) readyConnection(
	ctx context.Context,
	ns *temporalv1alpha1.Namespace,
) (*temporalv1alpha1.Connection, string, error) {
	conn, reason, err := r.getConnection(ctx, ns)
	if err != nil {
		return nil, reason, err
	}

	key := types.NamespacedName{Namespace: conn.Namespace, Name: conn.Name}

	ready := meta.FindStatusCondition(conn.Status.Conditions, temporalv1alpha1.ConditionTypeReady)
	switch {
	case ready == nil:
		return nil, ReasonConnectionNotReady, fmt.Errorf("connection %s has not been validated yet", key)
	case ready.Status != metav1.ConditionTrue:
		return nil, ReasonConnectionNotReady, fmt.Errorf("connection %s is not ready: %s", key, ready.Message)
	}

	return conn, "", nil
}

// temporalClient resolves a Connection and dials the Temporal Service it
// describes.
func (r *NamespaceReconciler) temporalClient(
	ctx context.Context,
	conn *temporalv1alpha1.Connection,
) (TemporalNamespaceClient, string, error) {
	opts, err := r.Resolver.ResolveConnection(ctx, conn)
	if err != nil {
		return nil, ReasonInvalidConfiguration, err
	}

	temporalClient, err := r.connectNamespace(ctx, opts)
	if err != nil {
		return nil, ReasonConnectionFailed, err
	}

	return temporalClient, "", nil
}

// connectNamespace dials a Temporal Service, using the injected dialler if one
// is set.
func (r *NamespaceReconciler) connectNamespace(
	ctx context.Context,
	opts *sdkclient.Options,
) (TemporalNamespaceClient, error) {
	if r.Connect != nil {
		return r.Connect(ctx, opts)
	}

	return temporal.New(ctx, opts)
}

// persistOwnership writes an ownership transition straight to the API server.
//
// Unlike the Ready condition, this cannot wait until the end of the reconcile:
// the value has to be durable before the Temporal call it describes is made.
func (r *NamespaceReconciler) persistOwnership(
	ctx context.Context,
	ns *temporalv1alpha1.Namespace,
	ownership temporalv1alpha1.NamespaceOwnership,
) error {
	if ns.Status.Ownership == ownership {
		return nil
	}

	ns.Status.Ownership = ownership

	return r.Status().Update(ctx, ns)
}

// updateNamespaceStatus writes the condition to the Namespace, doing nothing if
// the stored status already says the same thing. Skipping the no-op write keeps
// the controller from waking itself up over and over.
//
// observed is the status as it was read at the start of the reconcile, so
// changes made along the way are still detected.
func (r *NamespaceReconciler) updateNamespaceStatus(
	ctx context.Context,
	ns *temporalv1alpha1.Namespace,
	observed *temporalv1alpha1.NamespaceStatus,
	condition *metav1.Condition,
) error {
	meta.SetStatusCondition(&ns.Status.Conditions, *condition)
	ns.Status.ObservedGeneration = ns.Generation

	if equality.Semantic.DeepEqual(observed, &ns.Status) {
		return nil
	}

	return r.Status().Update(ctx, ns)
}

// indexNamespaceByConnection extracts the Connection a Namespace references, so
// that dependants can be looked up by it.
//
// It is called for every Namespace the cache holds, including ones that are
// half-built or of the wrong type, so it never assumes anything about what it
// is handed.
func indexNamespaceByConnection(obj client.Object) []string {
	ns, ok := obj.(*temporalv1alpha1.Namespace)
	if !ok || ns == nil {
		return nil
	}

	name := ns.Spec.ConnectionRef.Name
	if name == "" {
		// An unreferenced Namespace is simply absent from the index rather than
		// indexed under the empty string, where a malformed lookup could find
		// it.
		return nil
	}

	return []string{name}
}

// namespacesForConnection maps a Connection event onto the Namespaces that
// depend on it.
//
// Both kinds are namespaced, and a Connection only ever serves Namespaces
// beside it, so the list is confined to the Connection's own Kubernetes
// namespace as well as being filtered by the index. Two Namespaces in different
// Kubernetes namespaces may reference the same Connection name and have nothing
// to do with each other.
func (r *NamespaceReconciler) namespacesForConnection(ctx context.Context, conn client.Object) []ctrl.Request {
	log := logf.FromContext(ctx)

	namespaces := &temporalv1alpha1.NamespaceList{}
	if err := r.List(
		ctx, namespaces,
		client.InNamespace(conn.GetNamespace()),
		client.MatchingFields{namespaceConnectionRefIndex: conn.GetName()},
	); err != nil {
		// A map function has nowhere to return an error to, and taking the
		// controller down over a failed list would be worse than missing the
		// wake-up. The dependency requeue picks these up instead.
		log.Error(err, "Failed to find Namespaces depending on Connection",
			"connection", client.ObjectKeyFromObject(conn))

		return nil
	}

	requests := make([]ctrl.Request, 0, len(namespaces.Items))
	for i := range namespaces.Items {
		requests = append(requests, ctrl.Request{
			NamespacedName: client.ObjectKeyFromObject(&namespaces.Items[i]),
		})
	}

	return requests
}

// SetupWithManager sets up the controller with the Manager.
func (r *NamespaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Resolver == nil {
		// Read through the API reader rather than the cache: caching Secrets
		// would mean watching every Secret in the cluster, and the operator
		// only ever needs to get the ones a Connection names.
		r.Resolver = connection.NewResolver(mgr.GetAPIReader())
	}

	// The cache has not started yet, so this only registers the indexer and
	// returns; there is nothing for a caller's context to cancel.
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&temporalv1alpha1.Namespace{},
		namespaceConnectionRefIndex,
		indexNamespaceByConnection,
	); err != nil {
		return fmt.Errorf("indexing namespaces by %s: %w", namespaceConnectionRefIndex, err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		// Only the spec matters here, so ignore the status writes this
		// controller makes itself. Kubernetes bumps the generation when it
		// stamps a deletion timestamp, so the finalizer flow still runs.
		For(&temporalv1alpha1.Namespace{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		// Wake dependants as soon as their Connection changes, rather than
		// leaving them to notice on their next retry.
		//
		// Deliberately unfiltered. A Connection's readiness lives in its
		// status, so the generation predicate guarding the primary resource
		// above would discard precisely the events that matter here - and
		// predicates passed to For() apply only to For(), so it does not reach
		// this watch of its own accord.
		Watches(
			&temporalv1alpha1.Connection{},
			handler.EnqueueRequestsFromMapFunc(r.namespacesForConnection),
		).
		Named("namespace").
		Complete(r)
}
