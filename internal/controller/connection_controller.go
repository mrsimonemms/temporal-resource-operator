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
	"time"

	sdkclient "go.temporal.io/sdk/client"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	temporalv1alpha1 "github.com/mrsimonemms/temporal-resource-operator/api/v1alpha1"
	"github.com/mrsimonemms/temporal-resource-operator/internal/connection"
	"github.com/mrsimonemms/temporal-resource-operator/internal/temporal"
)

// Reasons reported on the Ready condition.
const (
	// ReasonConnected means the Temporal Service was reached and is healthy.
	ReasonConnected = "Connected"

	// ReasonInvalidConfiguration means the Connection could not be turned into
	// a usable set of Temporal client options - bad credentials, a missing
	// Secret, an unparseable certificate and so on.
	ReasonInvalidConfiguration = "InvalidConfiguration"

	// ReasonConnectionFailed means the Temporal Service could not be dialled.
	ReasonConnectionFailed = "ConnectionFailed"

	// ReasonHealthCheckFailed means the Temporal Service was dialled but did
	// not report itself healthy.
	ReasonHealthCheckFailed = "HealthCheckFailed"
)

const (
	// revalidateInterval is how often a healthy Connection is re-checked. The
	// Temporal Service lives outside Kubernetes, so nothing will tell us when
	// it stops being reachable.
	revalidateInterval = 5 * time.Minute

	// validateTimeout bounds a single validation attempt, so that an
	// unreachable Temporal Service cannot wedge a reconcile worker.
	validateTimeout = 30 * time.Second
)

// TemporalClient is the part of the Temporal client wrapper the reconciler
// uses. It exists so that connection validation can be substituted in tests.
type TemporalClient interface {
	CheckHealth(ctx context.Context) error
	Close()
}

// ConnectionReconciler reconciles a Connection object
type ConnectionReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Resolver turns a Connection into Temporal client options.
	Resolver *connection.Resolver

	// Connect dials a Temporal Service. Defaults to the internal/temporal
	// package when unset.
	Connect func(ctx context.Context, opts *sdkclient.Options) (TemporalClient, error)
}

// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=connections,verbs=get;list;watch
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=connections/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get

// Reconcile validates that the Temporal Service described by the Connection is
// reachable and healthy, and reports the outcome on the Ready condition.
//
// Deleting a Connection does not touch Temporal, so there is nothing to clean
// up and no finalizer is registered.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *ConnectionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	conn := &temporalv1alpha1.Connection{}
	if err := r.Get(ctx, req.NamespacedName, conn); err != nil {
		if apierrors.IsNotFound(err) {
			// Deleted, or the cache is behind. Either way there is nothing to
			// reconcile - importantly, do not carry on with a zero-value
			// Connection.
			log.Info("Connection no longer exists")
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	if !conn.GetDeletionTimestamp().IsZero() {
		log.Info("Connection is being deleted")
		return ctrl.Result{}, nil
	}

	reason, validationErr := r.validate(ctx, conn)

	condition := &metav1.Condition{
		Type:               temporalv1alpha1.ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            "Temporal Service is reachable and healthy",
		ObservedGeneration: conn.Generation,
	}
	if validationErr != nil {
		log.Error(validationErr, "Failed to validate Connection", "reason", reason)

		condition.Status = metav1.ConditionFalse
		condition.Message = validationErr.Error()
	}

	if err := r.updateStatus(ctx, conn, condition); err != nil {
		return ctrl.Result{}, err
	}

	if validationErr != nil {
		// Returning the error requeues with the controller's backoff.
		return ctrl.Result{}, validationErr
	}

	return ctrl.Result{RequeueAfter: revalidateInterval}, nil
}

// validate resolves the Connection, dials the Temporal Service and health
// checks it, returning the reason describing which step failed.
func (r *ConnectionReconciler) validate(
	ctx context.Context,
	conn *temporalv1alpha1.Connection,
) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, validateTimeout)
	defer cancel()

	opts, err := r.Resolver.ResolveConnection(ctx, conn)
	if err != nil {
		return ReasonInvalidConfiguration, err
	}

	temporalClient, err := r.connect(ctx, opts)
	if err != nil {
		return ReasonConnectionFailed, err
	}
	defer temporalClient.Close()

	if err := temporalClient.CheckHealth(ctx); err != nil {
		return ReasonHealthCheckFailed, err
	}

	return ReasonConnected, nil
}

// connect dials a Temporal Service, using the injected dialler if one is set.
func (r *ConnectionReconciler) connect(
	ctx context.Context,
	opts *sdkclient.Options,
) (TemporalClient, error) {
	if r.Connect != nil {
		return r.Connect(ctx, opts)
	}

	return temporal.New(ctx, opts)
}

// updateStatus writes the condition to the Connection, doing nothing if the
// status is already correct. Skipping the no-op write keeps the controller from
// waking itself up over and over.
func (r *ConnectionReconciler) updateStatus(
	ctx context.Context,
	conn *temporalv1alpha1.Connection,
	condition *metav1.Condition,
) error {
	before := conn.Status.DeepCopy()

	meta.SetStatusCondition(&conn.Status.Conditions, *condition)
	conn.Status.ObservedGeneration = conn.Generation

	if equality.Semantic.DeepEqual(before, &conn.Status) {
		return nil
	}

	return r.Status().Update(ctx, conn)
}

// SetupWithManager sets up the controller with the Manager.
func (r *ConnectionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Resolver == nil {
		// Read through the API reader rather than the cache: caching Secrets
		// would mean watching every Secret in the cluster, and the operator
		// only ever needs to get the ones a Connection names.
		r.Resolver = connection.NewResolver(mgr.GetAPIReader())
	}

	return ctrl.NewControllerManagedBy(mgr).
		// Only the spec matters here, so ignore the status writes this
		// controller makes itself.
		For(&temporalv1alpha1.Connection{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("connection").
		Complete(r)
}
