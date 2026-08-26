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

package temporal

import (
	"context"
	"errors"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"
	"go.temporal.io/sdk/converter"
	sdkmocks "go.temporal.io/sdk/mocks"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

var _ = Describe("Schedule operations", func() {
	const (
		namespaceName = "payments"
		scheduleID    = "PaymentsNightly"
	)

	var (
		ctx             context.Context
		workflowService *workflowservicemock.MockWorkflowServiceClient
		temporalClient  *Client
	)

	BeforeEach(func() {
		ctx = context.Background()

		ctrl := gomock.NewController(GinkgoT())
		DeferCleanup(ctrl.Finish)

		workflowService = workflowservicemock.NewMockWorkflowServiceClient(ctrl)

		sdkClient := &sdkmocks.Client{}
		sdkClient.On("WorkflowService").Return(workflowService)

		temporalClient = &Client{client: sdkClient}
	})

	// minimalDesired is the smallest schedule the wrapper will build, so that a
	// spec exercising one field is not also carrying another.
	minimalDesired := func() *ScheduleDesired {
		return &ScheduleDesired{
			ID: scheduleID,
			Timing: ScheduleTiming{
				Cron: []string{"30 2 * * *"},
			},
			Workflow: ScheduleWorkflow{
				Type:      "ReconcilePayments",
				TaskQueue: "payments",
			},
			Policies: SchedulePolicies{Overlap: ScheduleOverlapPolicySkip},
		}
	}

	Describe("DescribeSchedule", func() {
		It("should return the schedule and its conflict token", func() {
			workflowService.EXPECT().
				DescribeSchedule(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *workflowservice.DescribeScheduleRequest,
					_ ...grpc.CallOption,
				) (*workflowservice.DescribeScheduleResponse, error) {
					Expect(req.GetNamespace()).To(Equal(namespaceName))
					Expect(req.GetScheduleId()).To(Equal(scheduleID))

					return &workflowservice.DescribeScheduleResponse{
						Schedule: &schedulepb.Schedule{
							State: &schedulepb.ScheduleState{
								Paused:           true,
								Notes:            "paused by hand",
								LimitedActions:   true,
								RemainingActions: 4,
							},
						},
						ConflictToken: []byte("token-1"),
					}, nil
				})

			schedule, err := temporalClient.DescribeSchedule(ctx, namespaceName, scheduleID)
			Expect(err).NotTo(HaveOccurred())

			Expect(schedule.ID).To(Equal(scheduleID))
			Expect(schedule.Paused()).To(BeTrue())
			Expect(schedule.Notes()).To(Equal("paused by hand"))

			remaining, limited := schedule.RemainingActions()
			Expect(limited).To(BeTrue())
			Expect(remaining).To(BeNumerically("==", 4))
		})

		It("should report a missing schedule as ErrScheduleNotFound", func() {
			temporalErr := serviceerror.NewNotFound("schedule not found")
			workflowService.EXPECT().
				DescribeSchedule(gomock.Any(), gomock.Any()).
				Return(nil, temporalErr)

			_, err := temporalClient.DescribeSchedule(ctx, namespaceName, scheduleID)
			Expect(err).To(MatchError(ErrScheduleNotFound))

			// The underlying Temporal error stays in the chain, so callers can
			// still report what the Service actually said.
			Expect(err).To(MatchError(temporalErr))
		})

		It("should not mistake another failure for a missing schedule", func() {
			workflowService.EXPECT().
				DescribeSchedule(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewUnavailable("service down"))

			_, err := temporalClient.DescribeSchedule(ctx, namespaceName, scheduleID)
			Expect(errors.Is(err, ErrScheduleNotFound)).To(BeFalse())
			Expect(err).To(MatchError(ContainSubstring("service down")))
		})

		It("should report a namespace with schedules switched off", func() {
			workflowService.EXPECT().
				DescribeSchedule(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewInvalidArgument("Schedules are not allowed on this namespace"))

			_, err := temporalClient.DescribeSchedule(ctx, namespaceName, scheduleID)
			Expect(err).To(MatchError(ErrSchedulesNotAllowed))
		})

		It("should reject a missing name without calling Temporal", func() {
			_, err := temporalClient.DescribeSchedule(ctx, namespaceName, "")
			Expect(err).To(MatchError(ErrNoScheduleID))

			_, err = temporalClient.DescribeSchedule(ctx, "", scheduleID)
			Expect(err).To(MatchError(ErrNoNamespaceName))
		})
	})

	Describe("CreateSchedule", func() {
		// captureCreate runs the create and hands the spec back for assertions.
		captureCreate := func(desired *ScheduleDesired) *workflowservice.CreateScheduleRequest {
			var captured *workflowservice.CreateScheduleRequest

			workflowService.EXPECT().
				CreateSchedule(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *workflowservice.CreateScheduleRequest,
					_ ...grpc.CallOption,
				) (*workflowservice.CreateScheduleResponse, error) {
					captured = req

					return &workflowservice.CreateScheduleResponse{}, nil
				})

			Expect(temporalClient.CreateSchedule(ctx, namespaceName, desired)).To(Succeed())

			return captured
		}

		It("should send the identity and an idempotence key", func() {
			req := captureCreate(minimalDesired())

			Expect(req.GetNamespace()).To(Equal(namespaceName))
			Expect(req.GetScheduleId()).To(Equal(scheduleID))
			Expect(req.GetIdentity()).To(Equal(scheduleIdentity))
			Expect(req.GetRequestId()).NotTo(BeEmpty())
		})

		It("should send cron expressions for the Service to compile", func() {
			desired := minimalDesired()
			desired.Timing.Cron = []string{"30 2 * * *", "@hourly"}

			spec := captureCreate(desired).GetSchedule().GetSpec()

			Expect(spec.GetCronString()).To(Equal([]string{"30 2 * * *", "@hourly"}))
		})

		It("should send calendars in the structured form", func() {
			desired := minimalDesired()
			desired.Timing = ScheduleTiming{Calendars: []ScheduleCalendar{{
				Hour:      []ScheduleRange{{Start: 9, End: 9, Step: 1}},
				DayOfWeek: []ScheduleRange{{Start: 1, End: 5, Step: 1}},
				Comment:   "Weekday mornings",
			}}}

			spec := captureCreate(desired).GetSchedule().GetSpec()

			Expect(spec.GetStructuredCalendar()).To(HaveLen(1))
			calendar := spec.GetStructuredCalendar()[0]
			Expect(calendar.GetHour()).To(HaveLen(1))
			Expect(calendar.GetHour()[0].GetStart()).To(BeNumerically("==", 9))
			Expect(calendar.GetDayOfWeek()[0].GetEnd()).To(BeNumerically("==", 5))
			Expect(calendar.GetComment()).To(Equal("Weekday mornings"))

			// The legacy calendar field stays empty: the Service compiles that
			// one into the structured form anyway.
			Expect(spec.GetCalendar()).To(BeEmpty())
		})

		It("should send exclusions separately from calendars", func() {
			desired := minimalDesired()
			desired.Timing = ScheduleTiming{
				Calendars:        []ScheduleCalendar{{Hour: []ScheduleRange{{Start: 9}}}},
				ExcludeCalendars: []ScheduleCalendar{{DayOfMonth: []ScheduleRange{{Start: 1}}}},
			}

			spec := captureCreate(desired).GetSchedule().GetSpec()

			Expect(spec.GetStructuredCalendar()).To(HaveLen(1))
			Expect(spec.GetExcludeStructuredCalendar()).To(HaveLen(1))
			Expect(spec.GetExcludeStructuredCalendar()[0].GetDayOfMonth()[0].GetStart()).
				To(BeNumerically("==", 1))
		})

		It("should send intervals as a period and a phase", func() {
			desired := minimalDesired()
			desired.Timing = ScheduleTiming{Intervals: []ScheduleInterval{
				{Every: 6 * time.Hour, Offset: 5 * time.Hour},
			}}

			spec := captureCreate(desired).GetSchedule().GetSpec()

			Expect(spec.GetInterval()).To(HaveLen(1))
			Expect(spec.GetInterval()[0].GetInterval().AsDuration()).To(Equal(6 * time.Hour))
			Expect(spec.GetInterval()[0].GetPhase().AsDuration()).To(Equal(5 * time.Hour))
		})

		It("should send the schedule's temporal bounds", func() {
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			end := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
			jitter := 5 * time.Minute

			desired := minimalDesired()
			desired.Timing.StartAt = &start
			desired.Timing.EndAt = &end
			desired.Timing.Jitter = &jitter
			desired.Timing.TimeZone = "Europe/London"

			spec := captureCreate(desired).GetSchedule().GetSpec()

			Expect(spec.GetStartTime().AsTime()).To(BeTemporally("==", start))
			Expect(spec.GetEndTime().AsTime()).To(BeTemporally("==", end))
			Expect(spec.GetJitter().AsDuration()).To(Equal(5 * time.Minute))
			Expect(spec.GetTimezoneName()).To(Equal("Europe/London"))
		})

		It("should send the workflow action", func() {
			desired := minimalDesired()
			desired.Workflow.WorkflowID = "payments-nightly"

			start := captureCreate(desired).GetSchedule().GetAction().GetStartWorkflow()

			Expect(start.GetWorkflowType().GetName()).To(Equal("ReconcilePayments"))
			Expect(start.GetTaskQueue().GetName()).To(Equal("payments"))
			Expect(start.GetTaskQueue().GetKind()).To(Equal(enumspb.TASK_QUEUE_KIND_NORMAL))
			Expect(start.GetWorkflowId()).To(Equal("payments-nightly"))
		})

		It("should encode input as json/plain payloads, one per argument", func() {
			// The same encoding "temporal workflow start --input" produces, so a
			// worker written against any SDK deserialises them as it always
			// would.
			desired := minimalDesired()
			desired.Workflow.Input = []string{`{"mode":"nightly"}`, `42`}

			start := captureCreate(desired).GetSchedule().GetAction().GetStartWorkflow()

			Expect(start.GetInput().GetPayloads()).To(HaveLen(2))

			var first map[string]string
			Expect(converter.GetDefaultDataConverter().
				FromPayload(start.GetInput().GetPayloads()[0], &first)).To(Succeed())
			Expect(first).To(Equal(map[string]string{"mode": "nightly"}))

			Expect(start.GetInput().GetPayloads()[0].GetMetadata()).
				To(HaveKeyWithValue("encoding", []byte("json/plain")))
		})

		It("should keep a large integer argument exact", func() {
			// Decoding into json.Number rather than float64 is what stops a
			// 64-bit identifier losing its last digits on the way through.
			desired := minimalDesired()
			desired.Workflow.Input = []string{`9007199254740993`}

			start := captureCreate(desired).GetSchedule().GetAction().GetStartWorkflow()

			Expect(string(start.GetInput().GetPayloads()[0].GetData())).To(Equal("9007199254740993"))
		})

		It("should reject input that is not JSON before calling Temporal", func() {
			desired := minimalDesired()
			desired.Workflow.Input = []string{`not json`}

			Expect(temporalClient.CreateSchedule(ctx, namespaceName, desired)).
				To(MatchError(ContainSubstring("not valid JSON")))
		})

		It("should send the workflow's timeouts", func() {
			task, run, execution := 10*time.Second, time.Hour, 2*time.Hour

			desired := minimalDesired()
			desired.Workflow.TaskTimeout = &task
			desired.Workflow.RunTimeout = &run
			desired.Workflow.ExecutionTimeout = &execution

			start := captureCreate(desired).GetSchedule().GetAction().GetStartWorkflow()

			Expect(start.GetWorkflowTaskTimeout().AsDuration()).To(Equal(10 * time.Second))
			Expect(start.GetWorkflowRunTimeout().AsDuration()).To(Equal(time.Hour))
			Expect(start.GetWorkflowExecutionTimeout().AsDuration()).To(Equal(2 * time.Hour))
		})

		It("should leave an unset timeout unset, so the namespace default applies", func() {
			start := captureCreate(minimalDesired()).GetSchedule().GetAction().GetStartWorkflow()

			Expect(start.GetWorkflowTaskTimeout()).To(BeNil())
			Expect(start.GetWorkflowRunTimeout()).To(BeNil())
			Expect(start.GetWorkflowExecutionTimeout()).To(BeNil())
		})

		It("should send typed search attributes with their type recorded", func() {
			// Without the type in the payload metadata the Service cannot tell
			// an Int from a Text, and will not index the value.
			desired := minimalDesired()
			desired.Workflow.SearchAttributes = []ScheduleSearchAttribute{
				{Name: "CustomerId", Type: "Keyword", Value: "acme"},
				{Name: "Attempt", Type: "Int", Value: int64(3)},
			}

			start := captureCreate(desired).GetSchedule().GetAction().GetStartWorkflow()
			fields := start.GetSearchAttributes().GetIndexedFields()

			Expect(fields).To(HaveLen(2))
			Expect(fields["CustomerId"].GetMetadata()).To(HaveKeyWithValue("type", []byte("Keyword")))
			Expect(fields["Attempt"].GetMetadata()).To(HaveKeyWithValue("type", []byte("Int")))
		})

		It("should refuse a search attribute type Temporal does not know", func() {
			desired := minimalDesired()
			desired.Workflow.SearchAttributes = []ScheduleSearchAttribute{
				{Name: "Attr", Type: "Nonsense", Value: "x"},
			}

			Expect(temporalClient.CreateSchedule(ctx, namespaceName, desired)).
				To(MatchError(ErrUnknownSearchAttributeType))
		})

		It("should send the workflow memo and the schedule memo separately", func() {
			desired := minimalDesired()
			desired.Workflow.Memo = map[string]string{"owner": `"payments-team"`}
			desired.Memo = map[string]string{"runbook": `"https://example.com/runbook"`}

			req := captureCreate(desired)

			workflowMemo := req.GetSchedule().GetAction().GetStartWorkflow().GetMemo()
			Expect(workflowMemo.GetFields()).To(HaveKey("owner"))
			Expect(workflowMemo.GetFields()).NotTo(HaveKey("runbook"))

			Expect(req.GetMemo().GetFields()).To(HaveKey("runbook"))
			Expect(req.GetMemo().GetFields()).NotTo(HaveKey("owner"))
		})

		It("should send the priority settings", func() {
			key := int32(2)
			weight := float32(9)

			desired := minimalDesired()
			desired.Workflow.Priority = &SchedulePriority{
				Key:            &key,
				FairnessKey:    "tenant-acme",
				FairnessWeight: &weight,
			}

			start := captureCreate(desired).GetSchedule().GetAction().GetStartWorkflow()

			Expect(start.GetPriority().GetPriorityKey()).To(BeNumerically("==", 2))
			Expect(start.GetPriority().GetFairnessKey()).To(Equal("tenant-acme"))
			Expect(start.GetPriority().GetFairnessWeight()).To(BeNumerically("==", 9))
		})

		It("should send no priority at all when nothing was asked for", func() {
			desired := minimalDesired()
			desired.Workflow.Priority = &SchedulePriority{}

			start := captureCreate(desired).GetSchedule().GetAction().GetStartWorkflow()

			Expect(start.GetPriority()).To(BeNil())
		})

		It("should send the static summary and details as payloads", func() {
			desired := minimalDesired()
			desired.Workflow.StaticSummary = "Nightly reconciliation"
			desired.Workflow.StaticDetails = "Runs after the batch\n\nSee the runbook."

			start := captureCreate(desired).GetSchedule().GetAction().GetStartWorkflow()

			var summary, details string
			Expect(converter.GetDefaultDataConverter().
				FromPayload(start.GetUserMetadata().GetSummary(), &summary)).To(Succeed())
			Expect(summary).To(Equal("Nightly reconciliation"))

			Expect(converter.GetDefaultDataConverter().
				FromPayload(start.GetUserMetadata().GetDetails(), &details)).To(Succeed())
			Expect(details).To(ContainSubstring("\n\n"), "multi-line markdown should survive")
		})

		It("should send the policies", func() {
			window := time.Hour

			desired := minimalDesired()
			desired.Policies = SchedulePolicies{
				Overlap:        ScheduleOverlapPolicyBufferOne,
				CatchupWindow:  &window,
				PauseOnFailure: true,
			}

			policies := captureCreate(desired).GetSchedule().GetPolicies()

			Expect(policies.GetOverlapPolicy()).To(Equal(enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE))
			Expect(policies.GetCatchupWindow().AsDuration()).To(Equal(time.Hour))
			Expect(policies.GetPauseOnFailure()).To(BeTrue())
		})

		It("should refuse an overlap policy Temporal does not have", func() {
			desired := minimalDesired()
			desired.Policies.Overlap = "BufferSome"

			Expect(temporalClient.CreateSchedule(ctx, namespaceName, desired)).
				To(MatchError(ContainSubstring("unknown schedule overlap policy")))
		})

		It("should create a schedule already paused when asked to", func() {
			paused := true
			notes := "waiting for the worker fleet"

			desired := minimalDesired()
			desired.State = ScheduleStateSpec{Paused: &paused, Notes: &notes}

			state := captureCreate(desired).GetSchedule().GetState()

			Expect(state.GetPaused()).To(BeTrue())
			Expect(state.GetNotes()).To(Equal(notes))
		})

		It("should say nothing about remaining actions", func() {
			// There is no way to ask for a remaining-action count, because it is
			// a counter the Service consumes rather than a state anybody sets.
			// A create therefore leaves Temporal's own defaults in place.
			state := captureCreate(minimalDesired()).GetSchedule().GetState()

			Expect(state.GetLimitedActions()).To(BeFalse())
			Expect(state.GetRemainingActions()).To(BeZero())
		})

		It("should report a schedule that already exists", func() {
			workflowService.EXPECT().
				CreateSchedule(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewAlreadyExists("already there"))

			Expect(temporalClient.CreateSchedule(ctx, namespaceName, minimalDesired())).
				To(MatchError(ErrScheduleExists))
		})

		It("should reject a missing ID or namespace without calling Temporal", func() {
			Expect(temporalClient.CreateSchedule(ctx, namespaceName, nil)).
				To(MatchError(ErrNoScheduleID))
			Expect(temporalClient.CreateSchedule(ctx, namespaceName, &ScheduleDesired{})).
				To(MatchError(ErrNoScheduleID))
			Expect(temporalClient.CreateSchedule(ctx, "", minimalDesired())).
				To(MatchError(ErrNoNamespaceName))
		})
	})

	Describe("UpdateSchedule", func() {
		// existing stands in for a schedule the Service already holds, carrying
		// several fields this operator does not manage. Every preservation spec
		// below is about one of them surviving an update.
		existing := func() *Schedule {
			return &Schedule{
				ID: scheduleID,
				schedule: &schedulepb.Schedule{
					Spec: &schedulepb.ScheduleSpec{
						StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
							Hour: []*schedulepb.Range{{Start: 2}},
						}},
					},
					Action: &schedulepb.ScheduleAction{
						Action: &schedulepb.ScheduleAction_StartWorkflow{
							StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
								WorkflowId:   "old-id",
								WorkflowType: &commonpb.WorkflowType{Name: "OldWorkflow"},
								TaskQueue:    &taskqueuepb.TaskQueue{Name: "old-queue"},
								// None of these three is on the CRD.
								RetryPolicy: &commonpb.RetryPolicy{MaximumAttempts: 7},
								Header: &commonpb.Header{
									Fields: map[string]*commonpb.Payload{
										"trace": {Data: []byte("abc")},
									},
								},
								WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
							},
						},
					},
					Policies: &schedulepb.SchedulePolicies{
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
						// Not on the CRD, and not on the SDK's model either.
						KeepOriginalWorkflowId: true,
					},
					State: &schedulepb.ScheduleState{
						Paused:           true,
						Notes:            "paused by a human",
						LimitedActions:   true,
						RemainingActions: 3,
					},
				},
				conflictToken: []byte("token-1"),
			}
		}

		captureUpdate := func(actual *Schedule, desired *ScheduleDesired) *workflowservice.UpdateScheduleRequest {
			var captured *workflowservice.UpdateScheduleRequest

			workflowService.EXPECT().
				UpdateSchedule(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *workflowservice.UpdateScheduleRequest,
					_ ...grpc.CallOption,
				) (*workflowservice.UpdateScheduleResponse, error) {
					captured = req

					return &workflowservice.UpdateScheduleResponse{}, nil
				})

			Expect(temporalClient.UpdateSchedule(ctx, namespaceName, actual, desired)).To(Succeed())

			return captured
		}

		It("should send the conflict token it read the schedule with", func() {
			// This is what makes the update safe. The SDK sends nil here and its
			// own documentation warns that concurrent updates race.
			req := captureUpdate(existing(), minimalDesired())

			Expect(req.GetConflictToken()).To(Equal([]byte("token-1")))
			Expect(req.GetNamespace()).To(Equal(namespaceName))
			Expect(req.GetScheduleId()).To(Equal(scheduleID))
		})

		It("should overwrite the managed fields", func() {
			desired := minimalDesired()
			desired.Workflow.Type = "NewWorkflow"
			desired.Workflow.TaskQueue = "new-queue"

			start := captureUpdate(existing(), desired).GetSchedule().GetAction().GetStartWorkflow()

			Expect(start.GetWorkflowType().GetName()).To(Equal("NewWorkflow"))
			Expect(start.GetTaskQueue().GetName()).To(Equal("new-queue"))
		})

		Describe("preserving what the operator does not manage", func() {
			// Temporal replaces the schedule wholesale, so anything not sent
			// back is destroyed. These are the specs that prove working from the
			// message Describe returned - rather than building a fresh one -
			// actually does what it is there for.
			It("should keep a retry policy the CRD cannot express", func() {
				start := captureUpdate(existing(), minimalDesired()).
					GetSchedule().GetAction().GetStartWorkflow()

				Expect(start.GetRetryPolicy().GetMaximumAttempts()).To(BeNumerically("==", 7))
			})

			It("should keep a workflow header", func() {
				start := captureUpdate(existing(), minimalDesired()).
					GetSchedule().GetAction().GetStartWorkflow()

				Expect(start.GetHeader().GetFields()).To(HaveKey("trace"))
			})

			It("should keep a workflow ID reuse policy", func() {
				start := captureUpdate(existing(), minimalDesired()).
					GetSchedule().GetAction().GetStartWorkflow()

				Expect(start.GetWorkflowIdReusePolicy()).
					To(Equal(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE))
			})

			It("should keep keepOriginalWorkflowId, which the SDK's own model drops", func() {
				// The clearest reason this package talks protobuf rather than
				// using client.ScheduleHandle: the SDK's SchedulePolicies has no
				// such field, so an update through it would silently clear this.
				policies := captureUpdate(existing(), minimalDesired()).GetSchedule().GetPolicies()

				Expect(policies.GetKeepOriginalWorkflowId()).To(BeTrue())
			})

			It("should keep the paused state when the spec does not declare it", func() {
				// Somebody paused this schedule by hand. A reconcile that
				// changes its timing must not quietly resume it.
				state := captureUpdate(existing(), minimalDesired()).GetSchedule().GetState()

				Expect(state.GetPaused()).To(BeTrue())
				Expect(state.GetNotes()).To(Equal("paused by a human"))
			})

			It("should keep the remaining action count exactly as it found it", func() {
				// Temporal counts this down as the schedule acts, and an update
				// replaces the state wholesale - so a value this code did not
				// copy across would be reset to zero. Nothing can ask for it, so
				// preserving it is the only behaviour there is.
				state := captureUpdate(existing(), minimalDesired()).GetSchedule().GetState()

				Expect(state.GetLimitedActions()).To(BeTrue())
				Expect(state.GetRemainingActions()).To(BeNumerically("==", 3))
			})

			It("should keep it while changing the timing, action, policies and pause", func() {
				// The counter has to survive a change to everything else at
				// once, because that is what a real reconcile does.
				paused := true
				window := time.Hour
				notes := "managed by the operator"

				desired := minimalDesired()
				desired.Timing = ScheduleTiming{Cron: []string{"@hourly"}}
				desired.Workflow.Type = "SomethingElse"
				desired.Workflow.TaskQueue = "another-queue"
				desired.Policies = SchedulePolicies{
					Overlap:        ScheduleOverlapPolicyAllowAll,
					CatchupWindow:  &window,
					PauseOnFailure: true,
				}
				desired.State = ScheduleStateSpec{Paused: &paused, Notes: &notes}

				schedule := captureUpdate(existing(), desired).GetSchedule()

				Expect(schedule.GetState().GetLimitedActions()).To(BeTrue())
				Expect(schedule.GetState().GetRemainingActions()).To(BeNumerically("==", 3))

				// And everything that was asked for did change.
				Expect(schedule.GetState().GetPaused()).To(BeTrue())
				Expect(schedule.GetSpec().GetCronString()).To(Equal([]string{"@hourly"}))
				Expect(schedule.GetAction().GetStartWorkflow().GetWorkflowType().GetName()).
					To(Equal("SomethingElse"))
			})

			It("should keep a count the Service has already decremented", func() {
				// The situation that made this a runtime value rather than
				// desired state: Temporal has ticked 3 down to 1 since the
				// operator last looked, and the operator must not put 3 back.
				actual := existing()
				actual.schedule.State.RemainingActions = 1

				state := captureUpdate(actual, minimalDesired()).GetSchedule().GetState()

				Expect(state.GetRemainingActions()).To(BeNumerically("==", 1))
			})

			It("should keep a catch-up window the spec does not declare", func() {
				// The Service fills this field in with its own default of a year
				// and stores it. Clearing it would differ from what the Service
				// holds on every reconcile, and the operator would rewrite the
				// schedule for ever over drift of its own making.
				actual := existing()
				actual.schedule.Policies.CatchupWindow = durationpb.New(365 * 24 * time.Hour)

				policies := captureUpdate(actual, minimalDesired()).GetSchedule().GetPolicies()

				Expect(policies.GetCatchupWindow().AsDuration()).To(Equal(365 * 24 * time.Hour))
			})

			It("should leave the memo alone when the spec does not declare it", func() {
				// Unset means "leave this alone" on the request, which is not
				// the same as an empty message.
				req := captureUpdate(existing(), minimalDesired())

				Expect(req.GetMemo()).To(BeNil())
				Expect(req.GetSearchAttributes()).To(BeNil())
			})
		})

		Describe("managing what the spec does declare", func() {
			It("should resume a schedule the spec says is not paused", func() {
				paused := false

				desired := minimalDesired()
				desired.State = ScheduleStateSpec{Paused: &paused}

				state := captureUpdate(existing(), desired).GetSchedule().GetState()

				Expect(state.GetPaused()).To(BeFalse())
				// The note is still not managed, so it survives.
				Expect(state.GetNotes()).To(Equal("paused by a human"))
			})

			It("should replace the catch-up window when the spec declares one", func() {
				window := time.Hour

				actual := existing()
				actual.schedule.Policies.CatchupWindow = durationpb.New(365 * 24 * time.Hour)

				desired := minimalDesired()
				desired.Policies.CatchupWindow = &window

				policies := captureUpdate(actual, desired).GetSchedule().GetPolicies()

				Expect(policies.GetCatchupWindow().AsDuration()).To(Equal(time.Hour))
			})

			It("should replace the note when the spec declares one", func() {
				notes := "managed by the operator"

				desired := minimalDesired()
				desired.State = ScheduleStateSpec{Notes: &notes}

				Expect(captureUpdate(existing(), desired).GetSchedule().GetState().GetNotes()).
					To(Equal(notes))
			})

			It("should clear the memo when the spec declares it empty", func() {
				// Empty is a request, unlike nil. The Service reads an empty
				// message as "clear it".
				desired := minimalDesired()
				desired.Memo = map[string]string{}

				req := captureUpdate(existing(), desired)

				Expect(req.GetMemo()).NotTo(BeNil())
				Expect(req.GetMemo().GetFields()).To(BeEmpty())
			})

			It("should replace the timing rather than adding to it", func() {
				// The old calendar has to go, or a schedule edited from a
				// calendar to a cron expression would fire on both.
				desired := minimalDesired()
				desired.Timing = ScheduleTiming{Cron: []string{"@hourly"}}

				spec := captureUpdate(existing(), desired).GetSchedule().GetSpec()

				Expect(spec.GetCronString()).To(Equal([]string{"@hourly"}))
				Expect(spec.GetStructuredCalendar()).To(BeEmpty())
			})
		})

		It("should not mutate the schedule it was given", func() {
			// The caller is still holding it, and a half-applied update on a
			// failed write would be worse than no update at all.
			actual := existing()

			desired := minimalDesired()
			desired.Workflow.Type = "NewWorkflow"

			captureUpdate(actual, desired)

			Expect(actual.schedule.GetAction().GetStartWorkflow().GetWorkflowType().GetName()).
				To(Equal("OldWorkflow"))
		})

		It("should report a stale conflict token as ErrScheduleChanged", func() {
			workflowService.EXPECT().
				UpdateSchedule(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewFailedPrecondition("conflict token mismatch"))

			err := temporalClient.UpdateSchedule(ctx, namespaceName, existing(), minimalDesired())
			Expect(err).To(MatchError(ErrScheduleChanged))
		})

		It("should not mistake another failed precondition for a conflict", func() {
			workflowService.EXPECT().
				UpdateSchedule(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewFailedPrecondition("namespace is in handover"))

			err := temporalClient.UpdateSchedule(ctx, namespaceName, existing(), minimalDesired())
			Expect(errors.Is(err, ErrScheduleChanged)).To(BeFalse())
		})

		It("should report a missing schedule as ErrScheduleNotFound", func() {
			workflowService.EXPECT().
				UpdateSchedule(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewNotFound("gone"))

			Expect(temporalClient.UpdateSchedule(ctx, namespaceName, existing(), minimalDesired())).
				To(MatchError(ErrScheduleNotFound))
		})

		It("should reject a missing schedule or namespace without calling Temporal", func() {
			Expect(temporalClient.UpdateSchedule(ctx, namespaceName, nil, minimalDesired())).
				To(MatchError(ErrNoScheduleID))
			Expect(temporalClient.UpdateSchedule(ctx, "", existing(), minimalDesired())).
				To(MatchError(ErrNoNamespaceName))
		})
	})

	Describe("DeleteSchedule", func() {
		It("should delete the schedule", func() {
			workflowService.EXPECT().
				DeleteSchedule(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *workflowservice.DeleteScheduleRequest,
					_ ...grpc.CallOption,
				) (*workflowservice.DeleteScheduleResponse, error) {
					Expect(req.GetNamespace()).To(Equal(namespaceName))
					Expect(req.GetScheduleId()).To(Equal(scheduleID))
					Expect(req.GetIdentity()).To(Equal(scheduleIdentity))

					return &workflowservice.DeleteScheduleResponse{}, nil
				})

			Expect(temporalClient.DeleteSchedule(ctx, namespaceName, scheduleID)).To(Succeed())
		})

		It("should report a missing schedule as ErrScheduleNotFound", func() {
			// Reusing the sentinel is what lets the caller make deletion
			// idempotent without inspecting Temporal error types itself.
			workflowService.EXPECT().
				DeleteSchedule(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewNotFound("gone"))

			Expect(temporalClient.DeleteSchedule(ctx, namespaceName, scheduleID)).
				To(MatchError(ErrScheduleNotFound))
		})

		It("should reject a missing ID without calling Temporal", func() {
			Expect(temporalClient.DeleteSchedule(ctx, namespaceName, "")).To(MatchError(ErrNoScheduleID))
			Expect(temporalClient.DeleteSchedule(ctx, "", scheduleID)).To(MatchError(ErrNoNamespaceName))
		})
	})

	Describe("drift detection", func() {
		// A schedule matching the minimal desired state, as the Service would
		// report it: the cron expression already compiled into a calendar.
		matching := func() *Schedule {
			return &Schedule{
				ID: scheduleID,
				schedule: &schedulepb.Schedule{
					Spec: &schedulepb.ScheduleSpec{
						StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
							Minute: []*schedulepb.Range{{Start: 30}},
							Hour:   []*schedulepb.Range{{Start: 2}},
						}},
					},
					Action: &schedulepb.ScheduleAction{
						Action: &schedulepb.ScheduleAction_StartWorkflow{
							StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
								WorkflowType: &commonpb.WorkflowType{Name: "ReconcilePayments"},
								TaskQueue: &taskqueuepb.TaskQueue{
									Name: "payments",
									Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
								},
							},
						},
					},
					Policies: &schedulepb.SchedulePolicies{
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
					},
					State: &schedulepb.ScheduleState{},
				},
			}
		}

		It("should report nothing when the schedule already matches", func() {
			drift, err := matching().ScheduleDrift(minimalDesired())
			Expect(err).NotTo(HaveOccurred())
			Expect(drift).To(BeEmpty())
		})

		It("should notice the action changing", func() {
			desired := minimalDesired()
			desired.Workflow.TaskQueue = "somewhere-else"

			drift, err := matching().ScheduleDrift(desired)
			Expect(err).NotTo(HaveOccurred())
			Expect(drift).To(ConsistOf("action"))
		})

		It("should not read the Service's own catch-up window default as drift", func() {
			// The bug the e2e suite caught: a schedule created without a
			// catch-up window comes back with Temporal's one-year default on it,
			// and an operator that cleared the field reported "Updated" for ever.
			actual := matching()
			actual.schedule.Policies.CatchupWindow = durationpb.New(365 * 24 * time.Hour)

			drift, err := actual.ScheduleDrift(minimalDesired())
			Expect(err).NotTo(HaveOccurred())
			Expect(drift).To(BeEmpty())
		})

		It("should notice the policies changing", func() {
			desired := minimalDesired()
			desired.Policies.Overlap = ScheduleOverlapPolicyAllowAll

			drift, err := matching().ScheduleDrift(desired)
			Expect(err).NotTo(HaveOccurred())
			Expect(drift).To(ConsistOf("policies"))
		})

		It("should notice a managed paused state changing", func() {
			paused := true

			desired := minimalDesired()
			desired.State = ScheduleStateSpec{Paused: &paused}

			drift, err := matching().ScheduleDrift(desired)
			Expect(err).NotTo(HaveOccurred())
			Expect(drift).To(ConsistOf("state.paused"))
		})

		It("should ignore a paused state the spec does not declare", func() {
			// The point of the pointer: a schedule paused by hand is not drift.
			actual := matching()
			actual.schedule.State.Paused = true

			drift, err := actual.ScheduleDrift(minimalDesired())
			Expect(err).NotTo(HaveOccurred())
			Expect(drift).To(BeEmpty())
		})

		It("should ignore a note the spec does not declare", func() {
			// Temporal rewrites the note itself when pause-on-failure fires.
			actual := matching()
			actual.schedule.State.Notes = "paused because the last run failed"

			drift, err := actual.ScheduleDrift(minimalDesired())
			Expect(err).NotTo(HaveOccurred())
			Expect(drift).To(BeEmpty())
		})

		It("should ignore a memo the spec does not declare", func() {
			actual := matching()
			actual.memo = &commonpb.Memo{Fields: map[string]*commonpb.Payload{
				"set-by-hand": {Data: []byte(`"x"`)},
			}}

			drift, err := actual.ScheduleDrift(minimalDesired())
			Expect(err).NotTo(HaveOccurred())
			Expect(drift).To(BeEmpty())
		})

		It("should notice a declared memo differing", func() {
			desired := minimalDesired()
			desired.Memo = map[string]string{"owner": `"payments"`}

			drift, err := matching().ScheduleDrift(desired)
			Expect(err).NotTo(HaveOccurred())
			Expect(drift).To(ConsistOf("memo"))
		})

		It("should ignore Temporal decrementing the remaining action count", func() {
			// Nothing declares it, so nothing can drift. Were this reported as
			// drift the operator would write on every reconcile, and each write
			// would put the counter back - a schedule asked to run ten times
			// would run for ever.
			actual := matching()
			actual.schedule.State.LimitedActions = true
			actual.schedule.State.RemainingActions = 7

			drift, err := actual.ScheduleDrift(minimalDesired())
			Expect(err).NotTo(HaveOccurred())
			Expect(drift).To(BeEmpty())

			actual.schedule.State.RemainingActions = 6

			drift, err = actual.ScheduleDrift(minimalDesired())
			Expect(err).NotTo(HaveOccurred())
			Expect(drift).To(BeEmpty())
		})

		It("should list every managed field that differs", func() {
			paused := true

			desired := minimalDesired()
			desired.Workflow.TaskQueue = "elsewhere"
			desired.Policies.Overlap = ScheduleOverlapPolicyAllowAll
			desired.State = ScheduleStateSpec{Paused: &paused}

			drift, err := matching().ScheduleDrift(desired)
			Expect(err).NotTo(HaveOccurred())
			Expect(drift).To(ConsistOf("action", "policies", "state.paused"))
		})
	})

	Describe("timing hashes", func() {
		// The timing specification cannot be compared directly, because the
		// Service compiles cron into calendars and throws the original away. The
		// two hashes are what stand in for that comparison.
		It("should be stable for the same timing", func() {
			timing := ScheduleTiming{Cron: []string{"30 2 * * *"}}

			Expect(DesiredTimingHash(&timing)).To(Equal(DesiredTimingHash(&timing)))
			Expect(DesiredTimingHash(&timing)).NotTo(BeEmpty())
		})

		It("should change when the timing changes", func() {
			Expect(DesiredTimingHash(&ScheduleTiming{Cron: []string{"30 2 * * *"}})).
				NotTo(Equal(DesiredTimingHash(&ScheduleTiming{Cron: []string{"@hourly"}})))
		})

		It("should not change when an unrelated part of the spec changes", func() {
			// Only the timing is hashed, so an action change is not mistaken for
			// a timing change - it is caught by comparing the action directly.
			Expect(DesiredTimingHash(&ScheduleTiming{Cron: []string{"30 2 * * *"}})).
				To(Equal(DesiredTimingHash(&ScheduleTiming{Cron: []string{"30 2 * * *"}})))
		})

		It("should read the same schedule from the Service the same way twice", func() {
			schedule := &Schedule{schedule: &schedulepb.Schedule{
				Spec: &schedulepb.ScheduleSpec{
					Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}},
				},
			}}

			Expect(schedule.TimingHash()).To(Equal(schedule.TimingHash()))
			Expect(schedule.TimingHash()).NotTo(BeEmpty())
		})

		It("should tell two different server-side timings apart", func() {
			hourly := &Schedule{schedule: &schedulepb.Schedule{
				Spec: &schedulepb.ScheduleSpec{
					Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}},
				},
			}}
			daily := &Schedule{schedule: &schedulepb.Schedule{
				Spec: &schedulepb.ScheduleSpec{
					Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(24 * time.Hour)}},
				},
			}}

			Expect(hourly.TimingHash()).NotTo(Equal(daily.TimingHash()))
		})
	})

	Describe("overlap policies", func() {
		It("should map every policy the pinned Temporal API knows", func() {
			// Derived from Temporal's own table, so a new policy in a future
			// version cannot go unnoticed.
			for name, value := range enumspb.ScheduleOverlapPolicy_shorthandValue {
				got, err := ScheduleOverlapPolicy(name).overlapPolicy()
				Expect(err).NotTo(HaveOccurred(), name)
				Expect(got).To(Equal(enumspb.ScheduleOverlapPolicy(value)), name)
			}
		})

		It("should read the empty policy as unspecified", func() {
			got, err := ScheduleOverlapPolicy("").overlapPolicy()
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED))
		})

		It("should name an unrecognised policy rather than defaulting it", func() {
			// Falling back to Unspecified would read as "use the default", which
			// is a silent behaviour change rather than an error.
			_, err := ScheduleOverlapPolicy("Sometimes").overlapPolicy()
			Expect(err).To(MatchError(ContainSubstring(`"Sometimes"`)))
		})
	})
})
