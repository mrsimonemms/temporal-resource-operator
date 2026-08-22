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
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	temporalv1alpha1 "github.com/mrsimonemms/temporal-resource-operator/api/v1alpha1"
	"github.com/mrsimonemms/temporal-resource-operator/internal/connection"
	"github.com/mrsimonemms/temporal-resource-operator/internal/temporal"
)

// Reasons reported on a Namespace's Ready condition.
const (
	// ReasonCreated means the Temporal namespace was registered by this
	// reconcile.
	ReasonCreated = "Created"

	// ReasonReconciled means the Temporal namespace already existed and needed
	// no work.
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
)

const (
	// namespaceResyncInterval is how often a reconciled Namespace is checked
	// again. The Temporal Service is outside Kubernetes, so nothing will tell
	// us if the namespace disappears.
	namespaceResyncInterval = 5 * time.Minute

	// dependencyRetryInterval is how soon to look again when the referenced
	// Connection is missing or not yet Ready. This is an expected transient
	// state rather than a failure, so it is retried on a fixed interval rather
	// than through error backoff.
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
	CreateNamespace(ctx context.Context, name string, retention time.Duration) error
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

// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=namespaces/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=connections,verbs=get;list;watch

// Reconcile ensures the Temporal namespace named by this resource exists on the
// referenced Connection's Temporal Service, and reports the outcome on the
// Ready condition.
//
// Deleting a Namespace stops the operator managing that Temporal namespace; the
// namespace itself is left alone, so there is nothing to clean up and no
// finalizer is registered.
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

	if !ns.GetDeletionTimestamp().IsZero() {
		log.Info("Namespace is being deleted")
		return ctrl.Result{}, nil
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

	if err := r.updateNamespaceStatus(ctx, ns, condition); err != nil {
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

// reconcileNamespace ensures the Temporal namespace exists, returning the
// reason and, on success, the message to report. A non-nil error means the
// Namespace is not Ready.
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

	opts, err := r.Resolver.ResolveConnection(ctx, conn)
	if err != nil {
		return ReasonInvalidConfiguration, "", err
	}

	temporalClient, err := r.connectNamespace(ctx, opts)
	if err != nil {
		return ReasonConnectionFailed, "", err
	}
	defer temporalClient.Close()

	name := ns.TemporalName()

	switch _, err := temporalClient.DescribeNamespace(ctx, name); {
	case err == nil:
		// Already registered. Retention and every other setting are left as
		// they are - drift reconciliation is a separate concern.
		return ReasonReconciled, fmt.Sprintf("Temporal namespace %q exists", name), nil
	case !errors.Is(err, temporal.ErrNamespaceNotFound):
		return ReasonDescribeFailed, "", err
	}

	retention := ns.Spec.RetentionDuration()
	if err := temporalClient.CreateNamespace(ctx, name, retention); err != nil {
		return ReasonCreateFailed, "", err
	}

	return ReasonCreated, fmt.Sprintf("Registered Temporal namespace %q with retention %s", name, retention), nil
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

	ready := meta.FindStatusCondition(conn.Status.Conditions, temporalv1alpha1.ConditionTypeReady)
	switch {
	case ready == nil:
		return nil, ReasonConnectionNotReady, fmt.Errorf("connection %s has not been validated yet", key)
	case ready.Status != metav1.ConditionTrue:
		return nil, ReasonConnectionNotReady, fmt.Errorf("connection %s is not ready: %s", key, ready.Message)
	}

	return conn, "", nil
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

// updateNamespaceStatus writes the condition to the Namespace, doing nothing if
// the status is already correct. Skipping the no-op write keeps the controller
// from waking itself up over and over.
func (r *NamespaceReconciler) updateNamespaceStatus(
	ctx context.Context,
	ns *temporalv1alpha1.Namespace,
	condition *metav1.Condition,
) error {
	before := ns.Status.DeepCopy()

	meta.SetStatusCondition(&ns.Status.Conditions, *condition)
	ns.Status.ObservedGeneration = ns.Generation

	if equality.Semantic.DeepEqual(before, &ns.Status) {
		return nil
	}

	return r.Status().Update(ctx, ns)
}

// SetupWithManager sets up the controller with the Manager.
func (r *NamespaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Resolver == nil {
		// Read through the API reader rather than the cache: caching Secrets
		// would mean watching every Secret in the cluster, and the operator
		// only ever needs to get the ones a Connection names.
		r.Resolver = connection.NewResolver(mgr.GetAPIReader())
	}

	return ctrl.NewControllerManagedBy(mgr).
		// Only the spec matters here, so ignore the status writes this
		// controller makes itself.
		For(&temporalv1alpha1.Namespace{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("namespace").
		Complete(r)
}
