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
	"strings"
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

	temporalv1beta1 "github.com/mrsimonemms/temporal-resource-operator/api/v1beta1"
	"github.com/mrsimonemms/temporal-resource-operator/internal/connection"
	"github.com/mrsimonemms/temporal-resource-operator/internal/temporal"
)

// Reasons reported on a Schedule's Ready condition that no other resource
// needs. Everything else - Created, Adopted, Updated, Reconciled, the
// dependency reasons, the failure reasons and Conflict - is the shared
// vocabulary declared by the sibling controllers, and is used here unchanged.
const (
	// ReasonInvalidSchedule means the spec describes a schedule Temporal would
	// not accept: a cron expression that does not parse, an unknown time zone, a
	// negative jitter. The CRD rejects most of this on the way in, so it is
	// reported for an object stored before a rule existed, or written by
	// something that bypassed admission.
	ReasonInvalidSchedule = "InvalidSchedule"

	// ReasonSchedulesNotAllowed means the Temporal Service has schedules
	// switched off for this namespace.
	ReasonSchedulesNotAllowed = "SchedulesNotAllowed"
)

// Indexes letting a Connection or Namespace event find the Schedules that
// depend on it with one lookup instead of listing and scanning every Schedule.
const (
	scheduleConnectionRefIndex = ".spec.connectionRef.name"
	scheduleNamespaceRefIndex  = ".spec.namespaceRef.name"
)

// scheduleTimeout bounds the Temporal calls made by a single reconcile, so an
// unresponsive Service cannot wedge a reconcile worker.
const scheduleTimeout = 30 * time.Second

// scheduleConflictRetries is how many times one reconcile will re-read and try
// again when Temporal reports the schedule changed underneath it.
//
// Retrying in place rather than through the controller's backoff matters
// because the conflict is nearly always a person and the operator writing at
// the same moment, which resolves on the very next read. A small bound keeps a
// genuinely contended schedule from spinning: past it, the reconcile gives up
// and lets the ordinary requeue try later.
const scheduleConflictRetries = 3

// TemporalScheduleClient is the part of the Temporal client wrapper the Schedule
// reconciler uses. It exists so that controller tests can run without a Temporal
// Service.
type TemporalScheduleClient interface {
	DescribeSchedule(ctx context.Context, namespace, id string) (*temporal.Schedule, error)
	CreateSchedule(ctx context.Context, namespace string, desired *temporal.ScheduleDesired) error
	UpdateSchedule(
		ctx context.Context,
		namespace string,
		actual *temporal.Schedule,
		desired *temporal.ScheduleDesired,
	) error
	DeleteSchedule(ctx context.Context, namespace, id string) error
	Close()
}

// ScheduleReconciler reconciles a Schedule object
type ScheduleReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Resolver turns a Connection into Temporal client options.
	Resolver *connection.Resolver

	// Connect dials a Temporal Service. Defaults to the internal/temporal
	// package when unset.
	Connect func(ctx context.Context, opts *sdkclient.Options) (TemporalScheduleClient, error)
}

// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=schedules,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=schedules/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=schedules/finalizers,verbs=update
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=connections,verbs=get;list;watch
// +kubebuilder:rbac:groups=temporal.simonemms.com,resources=namespaces,verbs=get;list;watch

// Reconcile ensures the Temporal schedule named by this resource exists on the
// referenced namespace, matches the declared parts of the spec, and is removed
// again if - and only if - this resource is what created it.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *ScheduleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	schedule := &temporalv1beta1.Schedule{}
	if err := r.Get(ctx, req.NamespacedName, schedule); err != nil {
		if apierrors.IsNotFound(err) {
			// Deleted, or the cache is behind. Either way there is nothing to
			// reconcile - importantly, do not carry on with a zero-value
			// Schedule.
			log.Info("Schedule no longer exists")

			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	// Snapshot the status as it is stored, before anything below mutates it in
	// memory. The final write compares against this rather than against its own
	// starting point, so a change made during reconciliation - ownership and the
	// spec hashes in particular - is never mistaken for a no-op.
	observed := schedule.Status.DeepCopy()

	if !schedule.GetDeletionTimestamp().IsZero() {
		return r.finalise(ctx, schedule, observed)
	}

	// The finalizer has to be in place before anything is created in Temporal.
	// Creating first would leave a window in which a deletion races the create
	// and orphans the schedule.
	if err := r.ensureFinalizer(ctx, schedule); err != nil {
		return ctrl.Result{}, err
	}

	reason, message, reconcileErr := r.reconcileSchedule(ctx, schedule)

	condition := &metav1.Condition{
		Type:               temporalv1beta1.ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: schedule.Generation,
	}
	if reconcileErr != nil {
		condition.Status = metav1.ConditionFalse
		condition.Message = reconcileErr.Error()
	}

	if err := r.updateScheduleStatus(ctx, schedule, observed, condition); err != nil {
		return ctrl.Result{}, err
	}

	switch {
	case reconcileErr == nil:
		return ctrl.Result{RequeueAfter: namespaceResyncInterval}, nil
	case reason == ReasonInvalidSchedule:
		// Only a change to the spec can fix this, and that arrives as a fresh
		// event. Retrying on a timer would just repeat the same complaint.
		log.Error(reconcileErr, "Schedule spec is invalid", "reason", reason)

		return ctrl.Result{}, nil
	case reason == ReasonConnectionNotFound, reason == ReasonConnectionNotReady,
		reason == ReasonNamespaceNotFound, reason == ReasonNamespaceNotReady:
		// The dependency will become ready on its own schedule. Waiting for it
		// is not a failure, so retry on a fixed interval rather than burning
		// error backoff on it.
		log.Info("Waiting for a dependency", "reason", reason)

		return ctrl.Result{RequeueAfter: dependencyRetryInterval}, nil
	case reason == ReasonSchedulesNotAllowed:
		// Switching schedules on is Temporal Service configuration, and nothing
		// in Kubernetes will tell us when it happens. Look again on the ordinary
		// resync rather than hammering a Service that has said no.
		log.Error(reconcileErr, "Temporal namespace does not allow schedules", "reason", reason)

		return ctrl.Result{RequeueAfter: namespaceResyncInterval}, nil
	default:
		log.Error(reconcileErr, "Failed to reconcile Schedule", "reason", reason)
		// Returning the error requeues with the controller's backoff.
		return ctrl.Result{}, reconcileErr
	}
}

// reconcileSchedule brings the Temporal schedule into line with the spec,
// returning the reason and, on success, the message to report. A non-nil error
// means the Schedule is not Ready.
func (r *ScheduleReconciler) reconcileSchedule(
	ctx context.Context,
	schedule *temporalv1beta1.Schedule,
) (reason, message string, err error) {
	// Check the spec before reaching for the network. A cron expression that
	// does not parse is not something a retry will fix, and there is no sense
	// dialling Temporal to find that out.
	if err := schedule.Spec.Validate(); err != nil {
		return ReasonInvalidSchedule, "", err
	}

	desired, err := desiredSchedule(schedule)
	if err != nil {
		// Building the request is the last piece of validation: it decodes the
		// search attribute values and encodes the payloads, so anything the spec
		// got wrong that Validate could not see surfaces here rather than as a
		// rejected RPC.
		return ReasonInvalidSchedule, "", err
	}

	conn, reason, err := r.readyScheduleDependencies(ctx, schedule)
	if err != nil {
		return reason, "", err
	}

	ctx, cancel := context.WithTimeout(ctx, scheduleTimeout)
	defer cancel()

	temporalClient, reason, err := r.scheduleClient(ctx, conn)
	if err != nil {
		return reason, "", err
	}
	defer temporalClient.Close()

	return r.applySchedule(ctx, schedule, temporalClient, desired)
}

// applySchedule creates or reconciles the Temporal schedule, retrying a conflict
// from a fresh read.
//
// A conflict means somebody wrote the schedule between this reconcile's read and
// its write. Starting again from a fresh read is the only correct answer: the
// operator preserves the fields it does not manage by copying them off what it
// read, so retrying with the stale copy would put back values that are no longer
// there.
func (r *ScheduleReconciler) applySchedule(
	ctx context.Context,
	schedule *temporalv1beta1.Schedule,
	temporalClient TemporalScheduleClient,
	desired *temporal.ScheduleDesired,
) (reason, message string, err error) {
	namespace := schedule.TemporalNamespace()
	id := schedule.TemporalName()

	for attempt := range scheduleConflictRetries {
		// Deliberately its own variable rather than the named return: the loop
		// has to be able to look at this attempt's error without the caller
		// seeing it, because a conflict is answered here rather than reported.
		actual, attemptErr := temporalClient.DescribeSchedule(ctx, namespace, id)

		switch {
		case attemptErr == nil:
			reason, message, attemptErr = r.reconcileExisting(ctx, schedule, temporalClient, actual, desired)
		case errors.Is(attemptErr, temporal.ErrScheduleNotFound):
			reason, message, attemptErr = r.createSchedule(ctx, schedule, temporalClient, desired)
		case errors.Is(attemptErr, temporal.ErrSchedulesNotAllowed):
			return ReasonSchedulesNotAllowed, "", attemptErr
		default:
			return ReasonDescribeFailed, "", attemptErr
		}

		if attemptErr == nil || !errors.Is(attemptErr, temporal.ErrScheduleChanged) {
			return reason, message, attemptErr
		}

		logf.FromContext(ctx).Info("Temporal schedule changed while it was being written; reading it again",
			"schedule", id, "attempt", attempt+1)
	}

	// Still contended after every attempt. Report it rather than looping here;
	// the requeue will try again with a fresh reconcile.
	return ReasonConflict, "", fmt.Errorf(
		"%w: temporal schedule %q was modified during each of %d attempts to write it",
		temporal.ErrScheduleChanged, id, scheduleConflictRetries,
	)
}

// createSchedule creates a Temporal schedule that does not exist and, if
// ownership has not been settled yet, takes ownership of it.
//
// Ownership is recorded as Creating *before* the create call. That write is what
// makes the create survivable: if this reconcile dies between creating the
// schedule and recording the result, the next one finds the schedule present
// with ownership already at Creating, and so knows it is most likely its own
// work rather than something to adopt.
//
// An already-settled ownership is left alone. A schedule that was adopted and
// has since been removed behind the operator's back is put back, but that does
// not make the operator its owner - the user's schedule does not become the
// operator's to delete just because it had to be restored.
func (r *ScheduleReconciler) createSchedule(
	ctx context.Context,
	schedule *temporalv1beta1.Schedule,
	temporalClient TemporalScheduleClient,
	desired *temporal.ScheduleDesired,
) (reason, message string, err error) {
	establishing := !schedule.Status.Ownership.IsEstablished()

	if establishing {
		if err := r.persistOwnership(ctx, schedule, temporalv1beta1.ScheduleOwnershipCreating); err != nil {
			return ReasonCreateFailed, "", fmt.Errorf("recording intent to create Temporal schedule: %w", err)
		}
	}

	namespace := schedule.TemporalNamespace()
	id := schedule.TemporalName()

	if err := temporalClient.CreateSchedule(ctx, namespace, desired); err != nil {
		if errors.Is(err, temporal.ErrSchedulesNotAllowed) {
			return ReasonSchedulesNotAllowed, "", err
		}

		return ReasonCreateFailed, "", err
	}

	if establishing {
		// Settled in memory; the caller's status write persists it alongside the
		// Ready condition. Should that write fail, ownership stays at Creating
		// and the next reconcile resolves it to Created.
		schedule.Status.Ownership = temporalv1beta1.ScheduleOwnershipCreated
	}

	// Record what was asked for and what the Service made of it, so the next
	// reconcile can tell an unchanged schedule from a drifted one. Without this
	// a cron schedule would be rewritten on every resync, because Temporal
	// compiles cron away and there would be nothing left to compare.
	if err := r.recordSpecHashes(ctx, schedule, temporalClient, desired); err != nil {
		return ReasonDescribeFailed, "", err
	}

	return ReasonCreated, fmt.Sprintf(
		"Created Temporal schedule %q in namespace %q", id, namespace,
	), nil
}

// reconcileExisting takes ownership of a Temporal schedule that already exists
// and corrects any drift in the parts of it the spec declares.
func (r *ScheduleReconciler) reconcileExisting(
	ctx context.Context,
	schedule *temporalv1beta1.Schedule,
	temporalClient TemporalScheduleClient,
	actual *temporal.Schedule,
	desired *temporal.ScheduleDesired,
) (reason, message string, err error) {
	namespace := schedule.TemporalNamespace()
	id := schedule.TemporalName()

	adopting := establishScheduleOwnership(schedule)

	drift, err := scheduleDrift(schedule, actual, desired)
	if err != nil {
		return ReasonInvalidSchedule, "", err
	}

	if len(drift) == 0 {
		if adopting {
			// Adoption is a status change rather than a Temporal one, but the
			// hashes still have to be recorded: without them the next reconcile
			// could not tell whether the timing had drifted.
			if err := r.recordSpecHashes(ctx, schedule, temporalClient, desired); err != nil {
				return ReasonDescribeFailed, "", err
			}

			return ReasonAdopted, fmt.Sprintf(
				"Adopted existing Temporal schedule %q in namespace %q", id, namespace,
			), nil
		}

		return ReasonReconciled, fmt.Sprintf("Temporal schedule %q matches the spec", id), nil
	}

	if err := temporalClient.UpdateSchedule(ctx, namespace, actual, desired); err != nil {
		if errors.Is(err, temporal.ErrScheduleChanged) {
			// Handed back to applySchedule, which starts again from a fresh
			// read. Retrying here with the stale copy would put back fields that
			// have since changed.
			return ReasonConflict, "", err
		}

		if errors.Is(err, temporal.ErrSchedulesNotAllowed) {
			return ReasonSchedulesNotAllowed, "", err
		}

		return ReasonUpdateFailed, "", err
	}

	if err := r.recordSpecHashes(ctx, schedule, temporalClient, desired); err != nil {
		return ReasonDescribeFailed, "", err
	}

	return ReasonUpdated, fmt.Sprintf(
		"Updated Temporal schedule %q: %s", id, strings.Join(drift, ", "),
	), nil
}

// scheduleDrift lists what differs between the spec and the Temporal schedule.
//
// The timing specification is handled separately from everything else, and has
// to be. Temporal canonicalises a spec on the way in - cron expressions and the
// legacy calendar form are compiled into structured calendars and the originals
// discarded - so a cron expression cannot be compared against what comes back.
// Two hashes in the status stand in for that comparison: one says whether the
// resource has changed since the operator last wrote it, and the other whether
// anybody has changed the schedule on the Service since.
func scheduleDrift(
	schedule *temporalv1beta1.Schedule,
	actual *temporal.Schedule,
	desired *temporal.ScheduleDesired,
) ([]string, error) {
	drift, err := actual.ScheduleDrift(desired)
	if err != nil {
		return nil, err
	}

	desiredHash := temporal.DesiredTimingHash(&desired.Timing)

	// An unrecorded hash means the operator has never written this schedule and
	// so cannot say whether the timing matches. Writing is the safe answer: the
	// update is a full replacement of what the spec declares, so it is
	// idempotent, and the hashes are recorded straight afterwards.
	switch {
	case schedule.Status.DesiredSpecHash == "" || schedule.Status.AppliedSpecHash == "":
		drift = append(drift, "schedule")
	case schedule.Status.DesiredSpecHash != desiredHash:
		drift = append(drift, "schedule")
	case schedule.Status.AppliedSpecHash != actual.TimingHash():
		drift = append(drift, "schedule")
	}

	return drift, nil
}

// recordSpecHashes re-reads the schedule and records what was asked for
// alongside what the Service made of it.
//
// The read matters as much as the write. Temporal rewrites a timing
// specification into canonical form, so the only way to know what it now holds
// is to ask - and the answer is what the next reconcile compares against to
// decide whether anybody has changed it since.
func (r *ScheduleReconciler) recordSpecHashes(
	ctx context.Context,
	schedule *temporalv1beta1.Schedule,
	temporalClient TemporalScheduleClient,
	desired *temporal.ScheduleDesired,
) error {
	desiredHash := temporal.DesiredTimingHash(&desired.Timing)

	applied, err := temporalClient.DescribeSchedule(ctx, schedule.TemporalNamespace(), schedule.TemporalName())
	if err != nil {
		return fmt.Errorf("reading back temporal schedule %s: %w", schedule.TemporalName(), err)
	}

	// Held in memory; the caller's status write persists both alongside the
	// Ready condition. Should that write fail, the hashes stay as they were and
	// the next reconcile writes the schedule again - wasteful, not wrong,
	// because the update is a full replacement of the declared state.
	schedule.Status.DesiredSpecHash = desiredHash
	schedule.Status.AppliedSpecHash = applied.TimingHash()

	return nil
}

// establishScheduleOwnership settles, once, how this resource came to manage an
// existing Temporal schedule. It reports whether that settled to a fresh
// adoption.
//
// An established ownership is never revisited: whether the schedule exists right
// now says nothing about who created it, so re-deriving ownership from its
// existence would let a Created schedule silently become Adopted.
//
// Unlike a Temporal namespace, a schedule carries no marker the operator can
// check this against. The one durable free-form space on a schedule is its memo,
// and this CRD hands that to the user, so the belief recorded here is all there
// is. Adopting is the conservative reading, because an adopted schedule is never
// deleted.
func establishScheduleOwnership(schedule *temporalv1beta1.Schedule) bool {
	if schedule.Status.Ownership.IsEstablished() {
		return false
	}

	if schedule.Status.Ownership == temporalv1beta1.ScheduleOwnershipCreating {
		// A create that was interrupted between the intent being recorded and
		// its result. What is there is this resource's own work.
		schedule.Status.Ownership = temporalv1beta1.ScheduleOwnershipCreated

		return false
	}

	schedule.Status.Ownership = temporalv1beta1.ScheduleOwnershipAdopted

	return true
}

// finalise runs the deletion flow: remove the Temporal schedule if this resource
// owns it and has been asked to, then release the finalizer.
//
// The sibling controllers' finalise mirrors this step by step on purpose; see
// the note in dependency.go for why the four are not folded together.
//
//nolint:dupl // mirrored by the sibling controllers on purpose
func (r *ScheduleReconciler) finalise(
	ctx context.Context,
	schedule *temporalv1beta1.Schedule,
	observed *temporalv1beta1.ScheduleStatus,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(schedule, temporalv1beta1.ScheduleFinalizer) {
		// Nothing is holding the resource open, so it is already on its way out.
		return ctrl.Result{}, nil
	}

	id := schedule.TemporalName()
	ownership := schedule.Status.Ownership

	// Orphan is checked first, and deliberately before anything that needs a
	// Connection. It is the way out of a deletion that would otherwise be
	// blocked forever - a schedule whose Connection has been deleted, say - so
	// it must not depend on any of the machinery that could be what is broken.
	if policy := schedule.Spec.DeletionPolicyValue(); policy == temporalv1beta1.ScheduleDeletionPolicyOrphan {
		log.Info("Leaving Temporal schedule in place", "schedule", id, "deletionPolicy", string(policy))

		return ctrl.Result{}, r.removeFinalizer(ctx, schedule)
	}

	if !ownership.OwnsSchedule() {
		// Adopted, or never established. Either way this resource did not create
		// the schedule, so it has no business deleting it.
		log.Info("Leaving Temporal schedule in place", "schedule", id, "ownership", string(ownership))

		return ctrl.Result{}, r.removeFinalizer(ctx, schedule)
	}

	if reason, err := r.deleteTemporalSchedule(ctx, schedule); err != nil {
		log.Error(err, "Failed to delete Temporal schedule", "schedule", id, "reason", reason)

		// Hold the finalizer. Releasing it now would orphan a schedule this
		// resource is responsible for, and it would keep running.
		condition := &metav1.Condition{
			Type:               temporalv1beta1.ConditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            err.Error(),
			ObservedGeneration: schedule.Generation,
		}
		if statusErr := r.updateScheduleStatus(ctx, schedule, observed, condition); statusErr != nil {
			// Reporting why deletion is stuck is a convenience; the deletion
			// error is the one worth retrying on.
			log.Error(statusErr, "Failed to record deletion failure")
		}

		return ctrl.Result{}, err
	}

	log.Info("Finished with Temporal schedule", "schedule", id)

	return ctrl.Result{}, r.removeFinalizer(ctx, schedule)
}

// deleteTemporalSchedule removes the Temporal schedule this resource owns. It
// returns the reason to report when it fails.
//
// A schedule that has already gone counts as success, so deletion is idempotent
// across retries.
func (r *ScheduleReconciler) deleteTemporalSchedule(
	ctx context.Context,
	schedule *temporalv1beta1.Schedule,
) (string, error) {
	log := logf.FromContext(ctx)

	// Deletion needs a working Connection. Readiness is deliberately not
	// required here: it is a cached judgement that may be stale, and giving up
	// on an owned schedule because of a stale status would leave it running.
	conn, reason, err := getConnection(ctx, r.Client, schedule.Spec.ConnectionRef.Name, schedule.Namespace)
	if err != nil {
		return reason, err
	}

	ctx, cancel := context.WithTimeout(ctx, scheduleTimeout)
	defer cancel()

	temporalClient, reason, err := r.scheduleClient(ctx, conn)
	if err != nil {
		return reason, err
	}
	defer temporalClient.Close()

	namespace := schedule.TemporalNamespace()
	id := schedule.TemporalName()

	if err := temporalClient.DeleteSchedule(ctx, namespace, id); err != nil {
		if errors.Is(err, temporal.ErrScheduleNotFound) {
			// Nothing left to remove - possibly deleted by an earlier attempt
			// that failed before releasing the finalizer, possibly never created
			// at all. A schedule that is gone cannot run, which is the whole
			// point of deleting it, so this is success.
			//
			// This covers the Temporal namespace having been deleted too: the
			// Service answers NotFound for a schedule in a namespace it no
			// longer has, and a schedule whose namespace is gone is gone.
			log.Info("Temporal schedule is already gone", "schedule", id, "namespace", namespace)

			return "", nil
		}

		return ReasonDeleteFailed, err
	}

	return "", nil
}

// ensureFinalizer adds the finalizer if it is missing.
func (r *ScheduleReconciler) ensureFinalizer(ctx context.Context, schedule *temporalv1beta1.Schedule) error {
	patch := scheduleFinalizerPatch(schedule)

	if !controllerutil.AddFinalizer(schedule, temporalv1beta1.ScheduleFinalizer) {
		return nil
	}

	return r.Patch(ctx, schedule, patch)
}

// removeFinalizer releases the finalizer, allowing Kubernetes to remove the
// resource.
func (r *ScheduleReconciler) removeFinalizer(ctx context.Context, schedule *temporalv1beta1.Schedule) error {
	patch := scheduleFinalizerPatch(schedule)

	if !controllerutil.RemoveFinalizer(schedule, temporalv1beta1.ScheduleFinalizer) {
		return nil
	}

	return r.Patch(ctx, schedule, patch)
}

// scheduleFinalizerPatch captures the resource as it stands, so that only the
// finalizer change is sent.
//
// A full update would round-trip the spec, and marshalling it rewrites a
// Duration into its canonical form - "1d" becomes "24h0m0s". The API server sees
// that as a spec change, bumps the generation, and wakes the controller up again
// for no reason. Optimistic locking makes a concurrent finalizer write a
// retryable conflict rather than a silent clobber.
func scheduleFinalizerPatch(schedule *temporalv1beta1.Schedule) client.Patch {
	return client.MergeFromWithOptions(schedule.DeepCopy(), client.MergeFromWithOptimisticLock{})
}

// readyScheduleDependencies fetches the Connection and Namespace this resource
// needs and checks that both have reported themselves Ready.
//
// The Namespace gate matters here: Temporal refuses to create a schedule in a
// namespace that does not exist.
//
//nolint:dupl // parallel with the sibling controllers on purpose; see dependency.go
func (r *ScheduleReconciler) readyScheduleDependencies(
	ctx context.Context,
	schedule *temporalv1beta1.Schedule,
) (*temporalv1beta1.Connection, string, error) {
	conn, reason, err := getConnection(ctx, r.Client, schedule.Spec.ConnectionRef.Name, schedule.Namespace)
	if err != nil {
		return nil, reason, err
	}

	if reason, err := readyCondition(conn.Status.Conditions, "connection", client.ObjectKeyFromObject(conn),
		ReasonConnectionNotReady); err != nil {
		return nil, reason, err
	}

	namespace := &temporalv1beta1.Namespace{}
	key := types.NamespacedName{Namespace: schedule.Namespace, Name: schedule.Spec.NamespaceRef.Name}

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

// scheduleClient resolves a Connection and dials the Temporal Service it
// describes.
func (r *ScheduleReconciler) scheduleClient(
	ctx context.Context,
	conn *temporalv1beta1.Connection,
) (TemporalScheduleClient, string, error) {
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

// persistOwnership writes an ownership transition straight to the API server.
//
// Unlike the Ready condition, this cannot wait until the end of the reconcile:
// the value has to be durable before the Temporal call it describes is made.
func (r *ScheduleReconciler) persistOwnership(
	ctx context.Context,
	schedule *temporalv1beta1.Schedule,
	ownership temporalv1beta1.ScheduleOwnership,
) error {
	if schedule.Status.Ownership == ownership {
		return nil
	}

	schedule.Status.Ownership = ownership

	return r.Status().Update(ctx, schedule)
}

// updateScheduleStatus writes the condition to the Schedule, doing nothing if
// the stored status already says the same thing. Skipping the no-op write keeps
// the controller from waking itself up over and over.
//
// observed is the status as it was read at the start of the reconcile, so
// changes made along the way are still detected.
func (r *ScheduleReconciler) updateScheduleStatus(
	ctx context.Context,
	schedule *temporalv1beta1.Schedule,
	observed *temporalv1beta1.ScheduleStatus,
	condition *metav1.Condition,
) error {
	meta.SetStatusCondition(&schedule.Status.Conditions, *condition)
	schedule.Status.ObservedGeneration = schedule.Generation

	if equality.Semantic.DeepEqual(observed, &schedule.Status) {
		return nil
	}

	return r.Status().Update(ctx, schedule)
}

// indexScheduleByConnection extracts the Connection a Schedule references, so
// that dependants can be looked up by it.
//
// It is called for every Schedule the cache holds, including ones that are
// half-built or of the wrong type, so it never assumes anything about what it is
// handed.
func indexScheduleByConnection(obj client.Object) []string {
	return indexScheduleRef(obj, func(s *temporalv1beta1.Schedule) string {
		return s.Spec.ConnectionRef.Name
	})
}

// indexScheduleByNamespace extracts the Namespace a Schedule references.
func indexScheduleByNamespace(obj client.Object) []string {
	return indexScheduleRef(obj, func(s *temporalv1beta1.Schedule) string {
		return s.Spec.NamespaceRef.Name
	})
}

// indexScheduleRef is the shared body of the two indexers: read a reference off
// a Schedule, and leave an unreferenced one out of the index entirely rather
// than filing it under the empty string, where a malformed lookup could find it.
func indexScheduleRef(obj client.Object, ref func(*temporalv1beta1.Schedule) string) []string {
	schedule, ok := obj.(*temporalv1beta1.Schedule)
	if !ok || schedule == nil {
		return nil
	}

	name := ref(schedule)
	if name == "" {
		return nil
	}

	return []string{name}
}

// schedulesForDependency maps an event on a Connection or Namespace onto the
// Schedules that depend on it through the given index.
func (r *ScheduleReconciler) schedulesForDependency(
	index string,
) func(context.Context, client.Object) []ctrl.Request {
	return func(ctx context.Context, dependency client.Object) []ctrl.Request {
		return dependants(ctx, r.Client, &temporalv1beta1.ScheduleList{}, index, dependency)
	}
}

// SetupWithManager sets up the controller with the Manager.
//
//nolint:dupl // mirrored by the sibling controllers on purpose
func (r *ScheduleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Resolver == nil {
		// Read through the API reader rather than the cache: caching Secrets
		// would mean watching every Secret in the cluster, and the operator only
		// ever needs to get the ones a Connection names.
		r.Resolver = connection.NewResolver(mgr.GetAPIReader())
	}

	indexes := map[string]client.IndexerFunc{
		scheduleConnectionRefIndex: indexScheduleByConnection,
		scheduleNamespaceRefIndex:  indexScheduleByNamespace,
	}

	for field, extract := range indexes {
		// The cache has not started yet, so this only registers the indexer and
		// returns; there is nothing for a caller's context to cancel.
		if err := mgr.GetFieldIndexer().IndexField(
			context.Background(), &temporalv1beta1.Schedule{}, field, extract,
		); err != nil {
			return fmt.Errorf("indexing schedules by %s: %w", field, err)
		}
	}

	return ctrl.NewControllerManagedBy(mgr).
		// Only the spec matters here, so ignore the status writes this
		// controller makes itself. Kubernetes bumps the generation when it
		// stamps a deletion timestamp, so the finalizer flow still runs.
		For(&temporalv1beta1.Schedule{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		// Wake dependants as soon as a dependency changes. Both watches are
		// deliberately unfiltered: readiness lives in status, so the generation
		// predicate guarding the primary resource above would discard precisely
		// the events that matter.
		Watches(
			&temporalv1beta1.Connection{},
			handler.EnqueueRequestsFromMapFunc(r.schedulesForDependency(scheduleConnectionRefIndex)),
		).
		Watches(
			&temporalv1beta1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(r.schedulesForDependency(scheduleNamespaceRefIndex)),
		).
		Named("schedule").
		Complete(r)
}
