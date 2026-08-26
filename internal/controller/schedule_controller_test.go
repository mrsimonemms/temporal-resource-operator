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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	temporalv1beta1 "github.com/mrsimonemms/temporal-resource-operator/api/v1beta1"
	"github.com/mrsimonemms/temporal-resource-operator/internal/connection"
	"github.com/mrsimonemms/temporal-resource-operator/internal/temporal"
)

// scheduleCall records one write to the fake Service.
type scheduleCall struct {
	namespace string
	id        string
	desired   *temporal.ScheduleDesired
}

// fakeScheduleClient stands in for a Temporal Service.
//
// It models the two things about schedules that shape the controller: the
// Service canonicalises a timing specification on the way in, so what comes back
// is never what was sent; and an update is refused when the schedule has moved
// on since it was read.
type fakeScheduleClient struct {
	// existing holds the schedules the Service knows about, keyed by ID. The
	// value is the desired state it was last written with, plus whatever has
	// happened to it since.
	existing map[string]*fakeSchedule

	describeErr error
	createErr   error
	updateErr   error
	deleteErr   error

	// onDescribe runs after each describe has been answered, letting a spec
	// change the schedule underneath the operator mid-reconcile. It runs after
	// rather than before on purpose: the caller has to come away holding the
	// token from before the change, which is the situation a conflict is.
	onDescribe func(id string)

	described []string
	created   []scheduleCall
	updated   []scheduleCall
	deleted   []string
	closed    bool
}

// fakeSchedule is one schedule as the fake Service holds it.
type fakeSchedule struct {
	desired *temporal.ScheduleDesired

	// generation stands in for the conflict token: every write bumps it, and an
	// update carrying an older one is refused.
	generation int

	// timing stands in for the canonical spec the Service would report. It is
	// deliberately not the desired timing: the point is that it is the Service's
	// own form, which the operator can only compare against itself.
	timing string

	paused bool
	notes  string
}

func (f *fakeScheduleClient) DescribeSchedule(
	_ context.Context,
	namespace, id string,
) (*temporal.Schedule, error) {
	f.described = append(f.described, id)

	if f.describeErr != nil {
		return nil, f.describeErr
	}

	schedule, ok := f.existing[id]
	if !ok {
		return nil, scheduleNotFound(id)
	}

	answer := temporal.NewFakeSchedule(&temporal.FakeScheduleOptions{
		ID:            id,
		Namespace:     namespace,
		Desired:       schedule.desired,
		TimingHash:    schedule.timing,
		Paused:        schedule.paused,
		Notes:         schedule.notes,
		ConflictToken: fmt.Appendf(nil, "%d", schedule.generation),
	})

	if f.onDescribe != nil {
		f.onDescribe(id)
	}

	return answer, nil
}

func (f *fakeScheduleClient) CreateSchedule(
	_ context.Context,
	namespace string,
	desired *temporal.ScheduleDesired,
) error {
	f.created = append(f.created, scheduleCall{namespace: namespace, id: desired.ID, desired: desired})

	if f.createErr != nil {
		return f.createErr
	}

	if _, exists := f.existing[desired.ID]; exists {
		return fmt.Errorf("%w: %s", temporal.ErrScheduleExists, desired.ID)
	}

	f.put(desired)

	return nil
}

func (f *fakeScheduleClient) UpdateSchedule(
	_ context.Context,
	namespace string,
	actual *temporal.Schedule,
	desired *temporal.ScheduleDesired,
) error {
	f.updated = append(f.updated, scheduleCall{namespace: namespace, id: desired.ID, desired: desired})

	if f.updateErr != nil {
		return f.updateErr
	}

	schedule, ok := f.existing[desired.ID]
	if !ok {
		return scheduleNotFound(desired.ID)
	}

	// The conflict token check, which is the whole reason the operator reads
	// before it writes. A write carrying a token from before somebody else's
	// change is refused rather than clobbering it.
	if token := string(temporal.FakeScheduleToken(actual)); token != fmt.Sprintf("%d", schedule.generation) {
		return fmt.Errorf("%w: %s", temporal.ErrScheduleChanged, desired.ID)
	}

	f.put(desired)

	return nil
}

func (f *fakeScheduleClient) DeleteSchedule(_ context.Context, _, id string) error {
	f.deleted = append(f.deleted, id)

	if f.deleteErr != nil {
		return f.deleteErr
	}

	if _, ok := f.existing[id]; !ok {
		return scheduleNotFound(id)
	}

	delete(f.existing, id)

	return nil
}

func (f *fakeScheduleClient) Close() { f.closed = true }

// put writes a schedule, bumping its generation and recomputing the canonical
// timing the Service would report.
func (f *fakeScheduleClient) put(desired *temporal.ScheduleDesired) {
	previous := f.existing[desired.ID]

	generation := 1
	paused, notes := false, ""

	if previous != nil {
		generation = previous.generation + 1
		// State the spec does not declare survives, as it does on the Service.
		paused, notes = previous.paused, previous.notes
	}

	if desired.State.Paused != nil {
		paused = *desired.State.Paused
	}

	if desired.State.Notes != nil {
		notes = *desired.State.Notes
	}

	f.existing[desired.ID] = &fakeSchedule{
		desired:    desired,
		generation: generation,
		timing:     temporal.FakeCanonicalTiming(&desired.Timing),
		paused:     paused,
		notes:      notes,
	}
}

// pauseOutsideTheOperator pauses a schedule the way a person would, without
// touching anything else. It bumps the generation, so an update already in
// flight is refused.
func (f *fakeScheduleClient) pauseOutsideTheOperator(id string) {
	schedule, ok := f.existing[id]
	Expect(ok).To(BeTrue(), "the schedule should exist")

	schedule.paused = true
	schedule.notes = "paused by a human"
	schedule.generation++
}

// retimeOutsideTheOperator changes a schedule's timing on the Service, standing
// in for somebody editing it in Temporal's UI.
func (f *fakeScheduleClient) retimeOutsideTheOperator(id string) {
	schedule, ok := f.existing[id]
	Expect(ok).To(BeTrue(), "the schedule should exist")

	schedule.timing = "edited-elsewhere"
	schedule.generation++
}

// dependant builds a Schedule in the default Kubernetes namespace pointing at
// the named Connection and Namespace, for the dependency watch specs.
func dependant(resource, conn, ns string) *temporalv1beta1.Schedule {
	return &temporalv1beta1.Schedule{
		ObjectMeta: metav1.ObjectMeta{Name: resource, Namespace: "default"},
		Spec: temporalv1beta1.ScheduleSpec{
			ConnectionRef: corev1.LocalObjectReference{Name: conn},
			NamespaceRef:  corev1.LocalObjectReference{Name: ns},
		},
	}
}

// elsewhere moves a dependant into another Kubernetes namespace, which is how
// the specs check that a lookup does not cross between them.
func elsewhere(schedule *temporalv1beta1.Schedule) *temporalv1beta1.Schedule {
	schedule.Namespace = "other"

	return schedule
}

// scheduleNotFound builds the error the real client produces for a missing
// schedule, so the specs exercise the same errors.Is contract the controller
// relies on.
func scheduleNotFound(id string) error {
	return fmt.Errorf("%w: %w", temporal.ErrScheduleNotFound, serviceerror.NewNotFound(id))
}

var _ = Describe("Schedule Controller", func() {
	const (
		namespace  = "default"
		address    = "localhost:7233"
		scheduleID = "PaymentsNightly"
	)

	var (
		reconciler     *ScheduleReconciler
		temporalClient *fakeScheduleClient
		dialErr        error
		dialled        int
		name           string
		connectionName string
		namespaceName  string
		key            types.NamespacedName
	)

	reconcile := func() (ctrl.Result, error) {
		return reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	}

	stored := func() *temporalv1beta1.Schedule {
		schedule := &temporalv1beta1.Schedule{}
		Expect(k8sClient.Get(ctx, key, schedule)).To(Succeed())

		return schedule
	}

	readyCondition := func() *metav1.Condition {
		return meta.FindStatusCondition(stored().Status.Conditions, temporalv1beta1.ConditionTypeReady)
	}

	ownership := func() temporalv1beta1.ScheduleOwnership {
		return stored().Status.Ownership
	}

	hasFinalizer := func() bool {
		return controllerutil.ContainsFinalizer(stored(), temporalv1beta1.ScheduleFinalizer)
	}

	isGone := func() bool {
		return apierrors.IsNotFound(k8sClient.Get(ctx, key, &temporalv1beta1.Schedule{}))
	}

	// createConnection persists a Connection and gives it a Ready condition.
	createConnection := func(status metav1.ConditionStatus) {
		conn := &temporalv1beta1.Connection{
			ObjectMeta: metav1.ObjectMeta{Name: connectionName, Namespace: namespace},
			Spec:       temporalv1beta1.ConnectionSpec{Address: address},
		}
		Expect(k8sClient.Create(ctx, conn)).To(Succeed())

		if status == "" {
			return
		}

		meta.SetStatusCondition(&conn.Status.Conditions, metav1.Condition{
			Type:               temporalv1beta1.ConditionTypeReady,
			Status:             status,
			Reason:             ReasonConnected,
			Message:            connectionReadyMessage,
			ObservedGeneration: conn.Generation,
		})
		Expect(k8sClient.Status().Update(ctx, conn)).To(Succeed())
	}

	// createTemporalNamespace persists a Namespace and gives it a Ready
	// condition. Its metadata.name is the Temporal namespace name.
	createTemporalNamespace := func(status metav1.ConditionStatus) {
		ns := &temporalv1beta1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespaceName, Namespace: namespace},
			Spec: temporalv1beta1.NamespaceSpec{
				ConnectionRef: corev1.LocalObjectReference{Name: connectionName},
			},
		}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())

		if status == "" {
			return
		}

		meta.SetStatusCondition(&ns.Status.Conditions, metav1.Condition{
			Type:               temporalv1beta1.ConditionTypeReady,
			Status:             status,
			Reason:             ReasonCreated,
			ObservedGeneration: ns.Generation,
		})
		Expect(k8sClient.Status().Update(ctx, ns)).To(Succeed())
	}

	// readyDependencies is the ordinary starting point: a Connection and a
	// Namespace that are both Ready.
	readyDependencies := func() {
		createConnection(metav1.ConditionTrue)
		createTemporalNamespace(metav1.ConditionTrue)
	}

	// scheduleSpec is the smallest Schedule a spec can start from.
	scheduleSpec := func() temporalv1beta1.ScheduleSpec {
		return temporalv1beta1.ScheduleSpec{
			ScheduleID:    scheduleID,
			ConnectionRef: corev1.LocalObjectReference{Name: connectionName},
			NamespaceRef:  corev1.LocalObjectReference{Name: namespaceName},
			Schedule: temporalv1beta1.ScheduleTiming{
				Cron: []string{"30 2 * * *"},
			},
			Action: temporalv1beta1.ScheduleAction{
				Workflow: temporalv1beta1.ScheduleWorkflow{
					Type:      "ReconcilePayments",
					TaskQueue: "payments",
				},
			},
		}
	}

	// createSchedule persists a Schedule, optionally adjusted first.
	createSchedule := func(mutate ...func(spec *temporalv1beta1.ScheduleSpec)) *temporalv1beta1.Schedule {
		spec := scheduleSpec()
		for _, m := range mutate {
			m(&spec)
		}

		schedule := &temporalv1beta1.Schedule{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec:       spec,
		}
		Expect(k8sClient.Create(ctx, schedule)).To(Succeed())

		return schedule
	}

	// updateSchedule edits the stored resource.
	updateSchedule := func(mutate func(spec *temporalv1beta1.ScheduleSpec)) {
		schedule := stored()
		mutate(&schedule.Spec)
		Expect(k8sClient.Update(ctx, schedule)).To(Succeed())
	}

	// putTemporalSchedule seeds the fake Service with a schedule the operator
	// did not create, standing in for one that already existed.
	putTemporalSchedule := func(mutate ...func(desired *temporal.ScheduleDesired)) {
		desired := &temporal.ScheduleDesired{
			ID:     scheduleID,
			Timing: temporal.ScheduleTiming{Cron: []string{"30 2 * * *"}},
			Workflow: temporal.ScheduleWorkflow{
				Type:      "ReconcilePayments",
				TaskQueue: "payments",
			},
			Policies: temporal.SchedulePolicies{Overlap: temporal.ScheduleOverlapPolicySkip},
		}
		for _, m := range mutate {
			m(desired)
		}

		temporalClient.put(desired)
	}

	// temporalSchedule returns the schedule the fake Service holds.
	temporalSchedule := func() *fakeSchedule {
		schedule, ok := temporalClient.existing[scheduleID]
		Expect(ok).To(BeTrue(), "the Temporal schedule should exist")

		return schedule
	}

	setOwnership := func(value temporalv1beta1.ScheduleOwnership) {
		schedule := stored()
		schedule.Status.Ownership = value
		Expect(k8sClient.Status().Update(ctx, schedule)).To(Succeed())
	}

	beginDeletion := func() {
		Expect(hasFinalizer()).To(BeTrue(), "the resource needs a finalizer to survive deletion")
		Expect(k8sClient.Delete(ctx, stored())).To(Succeed())
		Expect(stored().GetDeletionTimestamp().IsZero()).To(BeFalse())
	}

	resetTemporalCalls := func() {
		dialled = 0
		temporalClient.described = nil
		temporalClient.created = nil
		temporalClient.updated = nil
		temporalClient.deleted = nil
	}

	expectNoTemporalContact := func() {
		Expect(dialled).To(BeZero(), "Temporal should not have been dialled")
		Expect(temporalClient.described).To(BeEmpty())
		Expect(temporalClient.created).To(BeEmpty())
		Expect(temporalClient.updated).To(BeEmpty())
		Expect(temporalClient.deleted).To(BeEmpty())
	}

	BeforeEach(func() {
		temporalClient = &fakeScheduleClient{existing: map[string]*fakeSchedule{}}
		dialErr = nil
		dialled = 0

		// The prefixes are the Schedule suite's own. Every controller suite
		// derives its names from the spec's line number, so two specs on the
		// same line of different files would otherwise ask the API server for
		// the same Connection - and the second one would get a conflict.
		suffix := GinkgoRandomSeed() + int64(CurrentSpecReport().LineNumber())
		name = fmt.Sprintf("sc-%d", suffix)
		connectionName = fmt.Sprintf("sc-conn-%d", suffix)
		namespaceName = fmt.Sprintf("sc-ns-%d", suffix)
		key = types.NamespacedName{Name: name, Namespace: namespace}

		reconciler = &ScheduleReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Resolver: connection.NewResolver(k8sClient),
			Connect: func(_ context.Context, _ *sdkclient.Options) (TemporalScheduleClient, error) {
				dialled++
				if dialErr != nil {
					return nil, dialErr
				}

				return temporalClient, nil
			},
		}
	})

	AfterEach(func() {
		// Best effort: a spec that deliberately leaves a resource stuck mid
		// deletion has already made its point, and cleaning up after it must not
		// turn into a second failure.
		schedule := &temporalv1beta1.Schedule{}
		if err := k8sClient.Get(ctx, key, schedule); err != nil {
			return
		}

		if controllerutil.RemoveFinalizer(schedule, temporalv1beta1.ScheduleFinalizer) {
			_ = k8sClient.Update(ctx, schedule)
		}

		_ = k8sClient.Delete(ctx, schedule)
	})

	Context("when the Schedule does not exist", func() {
		It("should do nothing at all", func() {
			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(ctrl.Result{}))

			expectNoTemporalContact()
		})
	})

	Context("when the spec is invalid", func() {
		BeforeEach(readyDependencies)

		DescribeTable(
			"should refuse before dialling Temporal",
			func(mutate func(spec *temporalv1beta1.ScheduleSpec)) {
				// The whole point of validating first: none of these is
				// something a retry fixes, and there is no sense dialling
				// Temporal to find that out.
				createSchedule(mutate)

				result, err := reconcile()
				Expect(err).NotTo(HaveOccurred(),
					"only a spec change fixes this, so it must not requeue on a timer")
				Expect(result).To(Equal(ctrl.Result{}))

				expectNoTemporalContact()

				condition := readyCondition()
				Expect(condition.Status).To(Equal(metav1.ConditionFalse))
				Expect(condition.Reason).To(Equal(ReasonInvalidSchedule))
			},
			Entry("a cron expression that does not parse", func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Schedule.Cron = []string{"not a cron expression"}
			}),
			Entry("a time zone that is not in the database", func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Schedule.TimeZone = "Mars/Olympus_Mons"
			}),
			Entry("an interval under a second", func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Schedule.Cron = nil
				spec.Schedule.Intervals = []temporalv1beta1.ScheduleInterval{
					{Every: temporalv1beta1.Duration{Duration: time.Millisecond}},
				}
			}),
			Entry("an offset as long as its interval", func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Schedule.Cron = nil
				spec.Schedule.Intervals = []temporalv1beta1.ScheduleInterval{{
					Every:  temporalv1beta1.Duration{Duration: time.Hour},
					Offset: &temporalv1beta1.Duration{Duration: time.Hour},
				}}
			}),
			Entry("a catch-up window under Temporal's minimum", func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Policies = &temporalv1beta1.SchedulePolicies{
					CatchupWindow: &temporalv1beta1.Duration{Duration: time.Second},
				}
			}),
			Entry("input that is not JSON", func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Action.Workflow.Input = []string{"not json"}
			}),
			Entry("a search attribute value of the wrong type", func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Action.Workflow.SearchAttributes = []temporalv1beta1.ScheduleSearchAttribute{
					{Name: "Attempt", Type: temporalv1beta1.SearchAttributeTypeInt, Value: `"three"`},
				}
			}),
		)

		It("should recover once the spec is fixed", func() {
			createSchedule(func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Schedule.Cron = []string{"nonsense"}
			})
			Expect(reconcile()).Error().NotTo(HaveOccurred())
			Expect(readyCondition().Reason).To(Equal(ReasonInvalidSchedule))

			updateSchedule(func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Schedule.Cron = []string{"@daily"}
			})

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCondition().Reason).To(Equal(ReasonCreated))
		})
	})

	Context("when a dependency is not usable", func() {
		DescribeTable(
			"should wait rather than fail",
			func(setup func(), expectedReason string) {
				setup()
				createSchedule()

				result, err := reconcile()
				Expect(err).NotTo(HaveOccurred(),
					"waiting for a dependency is not a failure")
				Expect(result.RequeueAfter).To(Equal(dependencyRetryInterval))

				Expect(dialled).To(BeZero(), "Temporal should not have been dialled")

				condition := readyCondition()
				Expect(condition.Status).To(Equal(metav1.ConditionFalse))
				Expect(condition.Reason).To(Equal(expectedReason))
			},
			Entry("no Connection", func() {}, ReasonConnectionNotFound),
			Entry("a Connection that has not been validated", func() {
				createConnection("")
			}, ReasonConnectionNotReady),
			Entry("a Connection reporting not ready", func() {
				createConnection(metav1.ConditionFalse)
			}, ReasonConnectionNotReady),
			Entry("no Namespace", func() {
				createConnection(metav1.ConditionTrue)
			}, ReasonNamespaceNotFound),
			Entry("a Namespace that has not been validated", func() {
				createConnection(metav1.ConditionTrue)
				createTemporalNamespace("")
			}, ReasonNamespaceNotReady),
			Entry("a Namespace reporting not ready", func() {
				createConnection(metav1.ConditionTrue)
				createTemporalNamespace(metav1.ConditionFalse)
			}, ReasonNamespaceNotReady),
		)

		It("should proceed once both become ready", func() {
			createConnection(metav1.ConditionTrue)
			createSchedule()

			Expect(reconcile()).Error().NotTo(HaveOccurred())
			Expect(readyCondition().Reason).To(Equal(ReasonNamespaceNotFound))

			createTemporalNamespace(metav1.ConditionTrue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
		})
	})

	Context("when creating a schedule", func() {
		BeforeEach(readyDependencies)

		It("should create it and record ownership", func() {
			createSchedule()

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(namespaceResyncInterval))

			Expect(temporalClient.created).To(HaveLen(1))
			Expect(temporalClient.created[0].id).To(Equal(scheduleID))
			Expect(temporalClient.created[0].namespace).To(Equal(namespaceName))

			Expect(ownership()).To(Equal(temporalv1beta1.ScheduleOwnershipCreated))
			Expect(hasFinalizer()).To(BeTrue())

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(ReasonCreated))
			Expect(condition.Message).To(ContainSubstring(scheduleID))
		})

		It("should record the intent to create before creating", func() {
			// The write that makes an interrupted create survivable: a reconcile
			// that dies between creating the schedule and recording the result
			// still leaves Creating behind, so the next one knows the schedule
			// is its own work rather than something to adopt.
			var ownershipAtCreate temporalv1beta1.ScheduleOwnership

			temporalClient.createErr = errors.New("service unavailable")
			createSchedule()

			Expect(reconcile()).Error().To(HaveOccurred())
			ownershipAtCreate = ownership()

			Expect(ownershipAtCreate).To(Equal(temporalv1beta1.ScheduleOwnershipCreating))
			Expect(ownershipAtCreate.OwnsSchedule()).To(BeTrue())
		})

		It("should settle an interrupted create to Created", func() {
			createSchedule()
			setOwnership(temporalv1beta1.ScheduleOwnershipCreating)
			putTemporalSchedule()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(ownership()).To(Equal(temporalv1beta1.ScheduleOwnershipCreated),
				"a schedule left behind by an interrupted create is the operator's own")
			Expect(readyCondition().Reason).NotTo(Equal(ReasonAdopted))
		})

		DescribeTable(
			"should send what the spec declares",
			func(mutate func(spec *temporalv1beta1.ScheduleSpec), check func(desired *temporal.ScheduleDesired)) {
				createSchedule(mutate)

				_, err := reconcile()
				Expect(err).NotTo(HaveOccurred())

				Expect(temporalClient.created).To(HaveLen(1))
				check(temporalClient.created[0].desired)
			},
			Entry("a cron expression",
				func(spec *temporalv1beta1.ScheduleSpec) {
					spec.Schedule.Cron = []string{"@hourly"}
				},
				func(desired *temporal.ScheduleDesired) {
					Expect(desired.Timing.Cron).To(Equal([]string{"@hourly"}))
				}),
			Entry("an interval",
				func(spec *temporalv1beta1.ScheduleSpec) {
					spec.Schedule.Cron = nil
					spec.Schedule.Intervals = []temporalv1beta1.ScheduleInterval{{
						Every:  temporalv1beta1.Duration{Duration: 6 * time.Hour},
						Offset: &temporalv1beta1.Duration{Duration: 5 * time.Hour},
					}}
				},
				func(desired *temporal.ScheduleDesired) {
					Expect(desired.Timing.Intervals).To(Equal([]temporal.ScheduleInterval{
						{Every: 6 * time.Hour, Offset: 5 * time.Hour},
					}))
				}),
			Entry("a calendar, with the API's optional end and step resolved",
				func(spec *temporalv1beta1.ScheduleSpec) {
					spec.Schedule.Cron = nil
					spec.Schedule.Calendars = []temporalv1beta1.ScheduleCalendar{{
						Hour: []temporalv1beta1.ScheduleRange{{Start: 9}},
					}}
				},
				func(desired *temporal.ScheduleDesired) {
					// An omitted end means "start alone" and an omitted step
					// means 1, resolved here so that writing it either way
					// produces the same request.
					Expect(desired.Timing.Calendars).To(Equal([]temporal.ScheduleCalendar{{
						Hour: []temporal.ScheduleRange{{Start: 9, End: 9, Step: 1}},
					}}))
				}),
			Entry("a mixed specification",
				func(spec *temporalv1beta1.ScheduleSpec) {
					spec.Schedule.Intervals = []temporalv1beta1.ScheduleInterval{
						{Every: temporalv1beta1.Duration{Duration: 6 * time.Hour}},
					}
					spec.Schedule.Calendars = []temporalv1beta1.ScheduleCalendar{{
						Hour: []temporalv1beta1.ScheduleRange{{Start: 9}},
					}}
				},
				func(desired *temporal.ScheduleDesired) {
					// Temporal takes the union, so all three are sent.
					Expect(desired.Timing.Cron).To(HaveLen(1))
					Expect(desired.Timing.Intervals).To(HaveLen(1))
					Expect(desired.Timing.Calendars).To(HaveLen(1))
				}),
			Entry("workflow input",
				func(spec *temporalv1beta1.ScheduleSpec) {
					spec.Action.Workflow.Input = []string{`{"mode":"nightly"}`}
				},
				func(desired *temporal.ScheduleDesired) {
					Expect(desired.Workflow.Input).To(Equal([]string{`{"mode":"nightly"}`}))
				}),
			Entry("policies",
				func(spec *temporalv1beta1.ScheduleSpec) {
					spec.Policies = &temporalv1beta1.SchedulePolicies{
						Overlap:        temporalv1beta1.ScheduleOverlapPolicyBufferAll,
						CatchupWindow:  &temporalv1beta1.Duration{Duration: time.Hour},
						PauseOnFailure: true,
					}
				},
				func(desired *temporal.ScheduleDesired) {
					Expect(desired.Policies.Overlap).To(Equal(temporal.ScheduleOverlapPolicyBufferAll))
					Expect(*desired.Policies.CatchupWindow).To(Equal(time.Hour))
					Expect(desired.Policies.PauseOnFailure).To(BeTrue())
				}),
			Entry("a paused state",
				func(spec *temporalv1beta1.ScheduleSpec) {
					paused := true
					spec.State = &temporalv1beta1.ScheduleStateSpec{Paused: &paused}
				},
				func(desired *temporal.ScheduleDesired) {
					Expect(desired.State.Paused).NotTo(BeNil())
					Expect(*desired.State.Paused).To(BeTrue())
				}),
			Entry("typed search attributes",
				func(spec *temporalv1beta1.ScheduleSpec) {
					spec.Action.Workflow.SearchAttributes = []temporalv1beta1.ScheduleSearchAttribute{
						{Name: "Attempt", Type: temporalv1beta1.SearchAttributeTypeInt, Value: "3"},
					}
				},
				func(desired *temporal.ScheduleDesired) {
					// Decoded into the Go type the Temporal type calls for, not
					// left as the string it was written as.
					Expect(desired.Workflow.SearchAttributes).To(HaveLen(1))
					Expect(desired.Workflow.SearchAttributes[0].Value).To(Equal(int64(3)))
				}),
		)

		It("should default the overlap policy the way Temporal does", func() {
			createSchedule()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created[0].desired.Policies.Overlap).
				To(Equal(temporal.ScheduleOverlapPolicySkip))
		})

		It("should leave undeclared state unmanaged", func() {
			createSchedule()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			desired := temporalClient.created[0].desired
			Expect(desired.State.Paused).To(BeNil())
			Expect(desired.State.Notes).To(BeNil())
			Expect(desired.State.LimitedActions).To(BeNil())
			Expect(desired.Memo).To(BeNil())
			Expect(desired.SearchAttributes).To(BeNil())
		})

		It("should record both spec hashes so the next reconcile can compare", func() {
			createSchedule()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			schedule := stored()
			Expect(schedule.Status.DesiredSpecHash).NotTo(BeEmpty())
			Expect(schedule.Status.AppliedSpecHash).NotTo(BeEmpty())
		})

		It("should report CreateFailed when Temporal refuses", func() {
			temporalClient.createErr = errors.New("service unavailable")
			createSchedule()

			_, err := reconcile()
			Expect(err).To(MatchError(temporalClient.createErr))

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonCreateFailed))
		})

		It("should report a namespace with schedules switched off", func() {
			temporalClient.createErr = fmt.Errorf("%w: disabled", temporal.ErrSchedulesNotAllowed)
			createSchedule()

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred(),
				"switching schedules on is Service configuration, not something a retry fixes")
			Expect(result.RequeueAfter).To(Equal(namespaceResyncInterval))

			Expect(readyCondition().Reason).To(Equal(ReasonSchedulesNotAllowed))
		})
	})

	Context("when the schedule already exists", func() {
		BeforeEach(readyDependencies)

		It("should adopt it rather than claiming it", func() {
			putTemporalSchedule()
			createSchedule()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(BeEmpty(),
				"a schedule that already exists must never be created again")
			Expect(ownership()).To(Equal(temporalv1beta1.ScheduleOwnershipAdopted))
			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
		})

		It("should apply the declared state on the reconcile that adopts", func() {
			// Adopting writes, and has to. The operator has no recorded hash for
			// a schedule it has never written, and Temporal reports a
			// canonicalised timing rather than the one it was given, so there is
			// nothing to compare a cron expression against. Writing is the only
			// answer that leaves the schedule matching the spec - and it is safe,
			// because the update preserves every field the operator does not
			// manage.
			putTemporalSchedule()
			createSchedule()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.updated).To(HaveLen(1))
			Expect(readyCondition().Reason).To(Equal(ReasonUpdated))
		})

		It("should settle down and stop writing after it has adopted", func() {
			// The write above happens once. Once the hashes are recorded the
			// operator can compare, and an unchanged schedule is left alone.
			putTemporalSchedule()
			createSchedule()
			Expect(reconcile()).Error().NotTo(HaveOccurred())
			resetTemporalCalls()

			for range 3 {
				_, err := reconcile()
				Expect(err).NotTo(HaveOccurred())
			}

			Expect(temporalClient.updated).To(BeEmpty())
			Expect(readyCondition().Reason).To(Equal(ReasonReconciled))
			Expect(ownership()).To(Equal(temporalv1beta1.ScheduleOwnershipAdopted))
		})

		It("should record the hashes when it adopts, so it can compare later", func() {
			putTemporalSchedule()
			createSchedule()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(stored().Status.DesiredSpecHash).NotTo(BeEmpty())
			Expect(stored().Status.AppliedSpecHash).NotTo(BeEmpty())
		})

		It("should reconcile the declared fields of an adopted schedule", func() {
			// Adoption decides deletion rights, not whether declared fields are
			// managed. A schedule the operator did not create still gets the
			// configuration the spec asks for.
			putTemporalSchedule(func(desired *temporal.ScheduleDesired) {
				desired.Workflow.TaskQueue = "somewhere-else"
			})
			createSchedule()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.updated).To(HaveLen(1))
			Expect(temporalSchedule().desired.Workflow.TaskQueue).To(Equal("payments"))

			Expect(ownership()).To(Equal(temporalv1beta1.ScheduleOwnershipAdopted),
				"configuring a schedule does not make it the operator's to delete")
			Expect(readyCondition().Reason).To(Equal(ReasonUpdated))
		})

		It("should never turn an adopted schedule into a created one", func() {
			putTemporalSchedule()
			createSchedule()
			Expect(reconcile()).Error().NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.ScheduleOwnershipAdopted))

			// Change something so the next reconcile actually writes.
			updateSchedule(func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Action.Workflow.TaskQueue = "payments-v2"
			})

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.ScheduleOwnershipAdopted))
		})

		It("should put back a schedule that was adopted and then removed", func() {
			putTemporalSchedule()
			createSchedule()
			Expect(reconcile()).Error().NotTo(HaveOccurred())

			delete(temporalClient.existing, scheduleID)
			resetTemporalCalls()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(HaveLen(1))
			Expect(ownership()).To(Equal(temporalv1beta1.ScheduleOwnershipAdopted),
				"restoring a schedule does not make it the operator's to delete")
		})
	})

	Context("when the schedule has drifted", func() {
		BeforeEach(func() {
			readyDependencies()
			createSchedule()
			Expect(reconcile()).Error().NotTo(HaveOccurred())
			resetTemporalCalls()
		})

		It("should leave a schedule that already matches alone", func() {
			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.updated).To(BeEmpty(), "a matching schedule needs no update")
			Expect(readyCondition().Reason).To(Equal(ReasonReconciled))
		})

		It("should not rewrite a cron schedule on every resync", func() {
			// The failure this whole hash mechanism exists to prevent. Temporal
			// compiles cron into a calendar and never reports it back, so
			// comparing the spec against what the Service holds would see
			// permanent drift and write for ever.
			for range 5 {
				_, err := reconcile()
				Expect(err).NotTo(HaveOccurred())
			}

			Expect(temporalClient.updated).To(BeEmpty())
			Expect(readyCondition().Reason).To(Equal(ReasonReconciled))
		})

		It("should correct the timing when the spec changes", func() {
			updateSchedule(func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Schedule.Cron = []string{"@hourly"}
			})

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.updated).To(HaveLen(1))
			Expect(temporalClient.updated[0].desired.Timing.Cron).To(Equal([]string{"@hourly"}))
			Expect(readyCondition().Reason).To(Equal(ReasonUpdated))
			Expect(readyCondition().Message).To(ContainSubstring("schedule"))
		})

		It("should correct the timing when somebody changes it on the Service", func() {
			// The other half of drift detection: the recorded hash of what
			// Temporal held no longer matches what it holds now.
			temporalClient.retimeOutsideTheOperator(scheduleID)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.updated).To(HaveLen(1))
			Expect(readyCondition().Reason).To(Equal(ReasonUpdated))
		})

		It("should correct the workflow action", func() {
			updateSchedule(func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Action.Workflow.TaskQueue = "payments-v2"
			})

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.updated).To(HaveLen(1))
			Expect(temporalSchedule().desired.Workflow.TaskQueue).To(Equal("payments-v2"))
			Expect(readyCondition().Message).To(ContainSubstring("action"))
		})

		It("should correct the policies", func() {
			updateSchedule(func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Policies = &temporalv1beta1.SchedulePolicies{
					Overlap: temporalv1beta1.ScheduleOverlapPolicyAllowAll,
				}
			})

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalSchedule().desired.Policies.Overlap).
				To(Equal(temporal.ScheduleOverlapPolicyAllowAll))
			Expect(readyCondition().Message).To(ContainSubstring("policies"))
		})

		It("should ignore a pause the spec does not manage", func() {
			// The reason paused is a pointer. Somebody stopped this schedule to
			// investigate something; the operator has no business resuming it.
			temporalClient.pauseOutsideTheOperator(scheduleID)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.updated).To(BeEmpty())
			Expect(temporalSchedule().paused).To(BeTrue())
			Expect(readyCondition().Reason).To(Equal(ReasonReconciled))
		})

		It("should correct a pause the spec does manage", func() {
			updateSchedule(func(spec *temporalv1beta1.ScheduleSpec) {
				paused := false
				spec.State = &temporalv1beta1.ScheduleStateSpec{Paused: &paused}
			})
			Expect(reconcile()).Error().NotTo(HaveOccurred())

			temporalClient.pauseOutsideTheOperator(scheduleID)
			resetTemporalCalls()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.updated).To(HaveLen(1))
			Expect(temporalSchedule().paused).To(BeFalse())
			Expect(readyCondition().Message).To(ContainSubstring("state.paused"))
		})

		It("should report every managed field that drifted", func() {
			updateSchedule(func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Schedule.Cron = []string{"@hourly"}
				spec.Action.Workflow.TaskQueue = "payments-v2"
				spec.Policies = &temporalv1beta1.SchedulePolicies{
					Overlap: temporalv1beta1.ScheduleOverlapPolicyAllowAll,
				}
			})

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			message := readyCondition().Message
			Expect(message).To(ContainSubstring("action"))
			Expect(message).To(ContainSubstring("policies"))
			Expect(message).To(ContainSubstring("schedule"))
		})

		It("should report UpdateFailed when Temporal refuses", func() {
			temporalClient.updateErr = errors.New("service unavailable")
			updateSchedule(func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Schedule.Cron = []string{"@hourly"}
			})

			_, err := reconcile()
			Expect(err).To(MatchError(temporalClient.updateErr))
			Expect(readyCondition().Reason).To(Equal(ReasonUpdateFailed))
		})

		It("should write again when the hashes are missing", func() {
			// An unrecorded hash means the operator cannot say whether the
			// timing matches. Writing is the safe answer, because the update is
			// a full replacement of the declared state and so idempotent.
			schedule := stored()
			schedule.Status.DesiredSpecHash = ""
			schedule.Status.AppliedSpecHash = ""
			Expect(k8sClient.Status().Update(ctx, schedule)).To(Succeed())

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.updated).To(HaveLen(1))
			Expect(stored().Status.DesiredSpecHash).NotTo(BeEmpty())
		})
	})

	Context("when a write races somebody else", func() {
		BeforeEach(func() {
			readyDependencies()
			createSchedule()
			Expect(reconcile()).Error().NotTo(HaveOccurred())
			resetTemporalCalls()
		})

		It("should re-read and succeed when the schedule moved underneath it", func() {
			// The conflict token doing its job: the first write is refused
			// because somebody changed the schedule between the read and the
			// write, and the retry starts again from a fresh read rather than
			// putting stale fields back.
			first := true
			temporalClient.onDescribe = func(id string) {
				if !first {
					return
				}

				first = false
				temporalClient.pauseOutsideTheOperator(id)
			}

			updateSchedule(func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Schedule.Cron = []string{"@hourly"}
			})

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(len(temporalClient.described)).To(BeNumerically(">=", 2),
				"the retry must start from a fresh read")
			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCondition().Reason).To(Equal(ReasonUpdated))

			// The pause somebody else applied survives, because the retry read
			// it rather than putting back the copy it started with.
			Expect(temporalSchedule().paused).To(BeTrue())
		})

		It("should give up and report Conflict when it never settles", func() {
			// Something is writing this schedule constantly. Spinning here would
			// not help, so the reconcile reports it and lets the requeue try
			// later.
			temporalClient.onDescribe = func(id string) {
				if _, ok := temporalClient.existing[id]; ok {
					temporalClient.existing[id].generation++
				}
			}

			updateSchedule(func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Schedule.Cron = []string{"@hourly"}
			})

			_, err := reconcile()
			Expect(err).To(MatchError(temporal.ErrScheduleChanged))

			Expect(temporalClient.updated).To(HaveLen(scheduleConflictRetries))
			Expect(readyCondition().Reason).To(Equal(ReasonConflict))
		})
	})

	Context("when the Schedule is deleted", func() {
		BeforeEach(readyDependencies)

		It("should delete a schedule it created", func() {
			createSchedule()
			Expect(reconcile()).Error().NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.ScheduleOwnershipCreated))

			beginDeletion()
			resetTemporalCalls()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.deleted).To(Equal([]string{scheduleID}))
			Expect(isGone()).To(BeTrue())
		})

		It("should leave a schedule it adopted", func() {
			putTemporalSchedule()
			createSchedule()
			Expect(reconcile()).Error().NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.ScheduleOwnershipAdopted))

			beginDeletion()
			resetTemporalCalls()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.deleted).To(BeEmpty())
			Expect(temporalClient.existing).To(HaveKey(scheduleID))
			Expect(isGone()).To(BeTrue())
		})

		It("should contact Temporal not at all when the policy is Orphan", func() {
			// Orphan is checked before anything that needs a Connection, because
			// it is the way out of a deletion blocked on a broken one.
			createSchedule(func(spec *temporalv1beta1.ScheduleSpec) {
				spec.DeletionPolicy = temporalv1beta1.ScheduleDeletionPolicyOrphan
			})
			Expect(reconcile()).Error().NotTo(HaveOccurred())

			beginDeletion()
			resetTemporalCalls()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			expectNoTemporalContact()
			Expect(temporalClient.existing).To(HaveKey(scheduleID))
			Expect(isGone()).To(BeTrue())
		})

		It("should treat a schedule that is already gone as deleted", func() {
			createSchedule()
			Expect(reconcile()).Error().NotTo(HaveOccurred())

			delete(temporalClient.existing, scheduleID)
			beginDeletion()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(isGone()).To(BeTrue())
		})

		It("should hold the finalizer when the delete fails", func() {
			createSchedule()
			Expect(reconcile()).Error().NotTo(HaveOccurred())

			temporalClient.deleteErr = errors.New("service unavailable")
			beginDeletion()

			_, err := reconcile()
			Expect(err).To(MatchError(temporalClient.deleteErr))

			Expect(isGone()).To(BeFalse())
			Expect(hasFinalizer()).To(BeTrue(),
				"releasing it would orphan a running schedule")
			Expect(readyCondition().Reason).To(Equal(ReasonDeleteFailed))
		})

		It("should finish once the delete succeeds", func() {
			createSchedule()
			Expect(reconcile()).Error().NotTo(HaveOccurred())

			temporalClient.deleteErr = errors.New("service unavailable")
			beginDeletion()
			Expect(reconcile()).Error().To(HaveOccurred())

			temporalClient.deleteErr = nil

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(isGone()).To(BeTrue())
		})

		It("should let Orphan release a deletion blocked on a missing Connection", func() {
			// The documented recovery route, and the reason the policy stays
			// mutable while the resource is already terminating.
			createSchedule()
			Expect(reconcile()).Error().NotTo(HaveOccurred())

			conn := &temporalv1beta1.Connection{}
			connKey := types.NamespacedName{Name: connectionName, Namespace: namespace}
			Expect(k8sClient.Get(ctx, connKey, conn)).To(Succeed())
			Expect(k8sClient.Delete(ctx, conn)).To(Succeed())

			beginDeletion()

			Expect(reconcile()).Error().To(HaveOccurred())
			Expect(isGone()).To(BeFalse())

			schedule := stored()
			schedule.Spec.DeletionPolicy = temporalv1beta1.ScheduleDeletionPolicyOrphan
			Expect(k8sClient.Update(ctx, schedule)).To(Succeed())

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(isGone()).To(BeTrue())
		})

		It("should do nothing when ownership was never established", func() {
			createSchedule()
			// Deleted before it ever reconciled, so nothing is known about it.
			Expect(reconciler.ensureFinalizer(ctx, stored())).To(Succeed())

			beginDeletion()
			resetTemporalCalls()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.deleted).To(BeEmpty())
			Expect(isGone()).To(BeTrue())
		})
	})

	Context("dependency watches", func() {
		Describe("the field indexes", func() {
			DescribeTable(
				"should index a Schedule by what it references",
				func(index func(ctrlclient.Object) []string, obj ctrlclient.Object, expected []string) {
					Expect(index(obj)).To(Equal(expected))
				},
				Entry("by Connection", indexScheduleByConnection,
					dependant("s", "prod", "payments"), []string{"prod"}),
				Entry("by Namespace", indexScheduleByNamespace,
					dependant("s", "prod", "payments"), []string{"payments"}),
				// An unreferenced Schedule is absent from the index rather than
				// filed under the empty string, where a malformed lookup could
				// find it.
				Entry("an empty Connection reference", indexScheduleByConnection,
					dependant("s", "", "payments"), []string(nil)),
				Entry("an empty Namespace reference", indexScheduleByNamespace,
					dependant("s", "prod", ""), []string(nil)),
				Entry("a nil Schedule", indexScheduleByConnection,
					(*temporalv1beta1.Schedule)(nil), []string(nil)),
				Entry("an object of another kind", indexScheduleByConnection,
					&temporalv1beta1.Connection{}, []string(nil)),
			)
		})

		Describe("mapping an event onto its dependants", func() {
			var mapper *ScheduleReconciler

			BeforeEach(func() {
				mapper = &ScheduleReconciler{
					Client: fake.NewClientBuilder().
						WithScheme(k8sClient.Scheme()).
						WithIndex(&temporalv1beta1.Schedule{},
							scheduleConnectionRefIndex, indexScheduleByConnection).
						WithIndex(&temporalv1beta1.Schedule{},
							scheduleNamespaceRefIndex, indexScheduleByNamespace).
						WithObjects(
							dependant("s-a", "prod", "payments"),
							dependant("s-b", "prod", "billing"),
							dependant("s-c", "staging", "payments"),
							elsewhere(dependant("s-d", "prod", "payments")),
						).
						Build(),
				}
			})

			It("should wake the Schedules using a Connection", func() {
				requests := mapper.schedulesForDependency(scheduleConnectionRefIndex)(
					ctx,
					&temporalv1beta1.Connection{
						ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "default"},
					},
				)

				Expect(requests).To(ConsistOf(
					ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "s-a"}},
					ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "s-b"}},
				))
			})

			It("should wake the Schedules using a Namespace", func() {
				requests := mapper.schedulesForDependency(scheduleNamespaceRefIndex)(
					ctx,
					&temporalv1beta1.Namespace{
						ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default"},
					},
				)

				Expect(requests).To(ConsistOf(
					ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "s-a"}},
					ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "s-c"}},
				))
			})

			It("should not cross Kubernetes namespaces", func() {
				// A like-named Connection in another Kubernetes namespace is a
				// different Connection.
				requests := mapper.schedulesForDependency(scheduleConnectionRefIndex)(
					ctx,
					&temporalv1beta1.Connection{
						ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "default"},
					},
				)

				Expect(requests).NotTo(ContainElement(
					ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "other", Name: "s-d"}},
				))
			})

			It("should return nothing when nothing depends on it", func() {
				requests := mapper.schedulesForDependency(scheduleConnectionRefIndex)(
					ctx,
					&temporalv1beta1.Connection{
						ObjectMeta: metav1.ObjectMeta{Name: "unused", Namespace: "default"},
					},
				)

				Expect(requests).To(BeEmpty())
			})
		})

		It("should register both indexes and both watches", func() {
			mgr, err := ctrl.NewManager(cfg, ctrl.Options{
				Scheme:                 k8sClient.Scheme(),
				Metrics:                metricsserver.Options{BindAddress: "0"},
				HealthProbeBindAddress: "0",
			})
			Expect(err).NotTo(HaveOccurred())

			// This fails if an index cannot be registered - a duplicate name,
			// the wrong object type - or if a watch cannot be built.
			Expect((&ScheduleReconciler{
				Client: mgr.GetClient(),
				Scheme: mgr.GetScheme(),
			}).SetupWithManager(mgr)).To(Succeed())
		})
	})

	Context("in general", func() {
		BeforeEach(readyDependencies)

		It("should close the Temporal client it opened", func() {
			createSchedule()

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.closed).To(BeTrue())
		})

		It("should keep observedGeneration current on success and on failure", func() {
			createSchedule()
			Expect(reconcile()).Error().NotTo(HaveOccurred())

			schedule := stored()
			Expect(schedule.Status.ObservedGeneration).To(Equal(schedule.Generation))

			updateSchedule(func(spec *temporalv1beta1.ScheduleSpec) {
				spec.Schedule.Cron = []string{"nonsense"}
			})
			Expect(reconcile()).Error().NotTo(HaveOccurred())

			schedule = stored()
			Expect(schedule.Status.ObservedGeneration).To(Equal(schedule.Generation))
			Expect(readyCondition().ObservedGeneration).To(Equal(schedule.Generation))
		})

		It("should report ConnectionFailed when Temporal cannot be dialled", func() {
			dialErr = errors.New("connection refused")
			createSchedule()

			_, err := reconcile()
			Expect(err).To(MatchError(dialErr))
			Expect(readyCondition().Reason).To(Equal(ReasonConnectionFailed))
		})

		It("should report DescribeFailed when the lookup fails", func() {
			temporalClient.describeErr = errors.New("service unavailable")
			createSchedule()

			_, err := reconcile()
			Expect(err).To(MatchError(temporalClient.describeErr))
			Expect(readyCondition().Reason).To(Equal(ReasonDescribeFailed))
		})
	})
})
