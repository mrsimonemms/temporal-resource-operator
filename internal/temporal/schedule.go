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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Why this file talks protobuf rather than using the SDK's ScheduleClient.
//
// Temporal's UpdateSchedule replaces the schedule wholesale: "The four main
// fields of the schedule (spec, action, policies, state) are replaced completely
// by the values in this message." Anything not sent is not merged - it is gone.
// So an operator that manages part of a schedule has to put the rest back
// itself, and it can only put back what it can see.
//
// The SDK cannot see all of it. client.SchedulePolicies has no
// KeepOriginalWorkflowId, and client.ScheduleWorkflowAction has no Header, so a
// schedule adopted through the SDK and then updated loses both. The SDK also
// sends ConflictToken: nil, and its own documentation warns that two concurrent
// updates race.
//
// Working at the protobuf level fixes all of it at once. An update starts from
// the message Describe returned, overwrites only the fields the operator
// manages, and sends it back with the conflict token. Everything else survives
// because it was never touched - including fields added to Temporal after this
// code was written.
//
// The protobuf types stay inside this package. Callers get the wrapper types
// below and never see a schedulepb.

var (
	// ErrScheduleNotFound reports that no schedule of that ID exists in the
	// namespace. It is the expected outcome of describing one that has yet to be
	// created, so callers can tell it apart from a genuine failure with
	// errors.Is.
	ErrScheduleNotFound = errors.New("temporal schedule not found")

	// ErrScheduleExists reports that the namespace already has a schedule of
	// that ID.
	ErrScheduleExists = errors.New("temporal schedule already exists")

	// ErrScheduleChanged reports that the schedule was modified between being
	// read and being written, so the change was refused. Re-read it and try
	// again.
	ErrScheduleChanged = errors.New("temporal schedule changed since it was read")

	// ErrNoScheduleID is returned when an operation is given no schedule ID.
	ErrNoScheduleID = errors.New("no schedule id given")

	// ErrSchedulesNotAllowed reports that the Temporal Service has schedules
	// switched off for this namespace.
	ErrSchedulesNotAllowed = errors.New("schedules are not enabled on this temporal namespace")
)

// ScheduleOverlapPolicy decides what happens when a schedule would act while an
// earlier action is still running, in the shorthand form Temporal itself uses.
//
// Keeping the shorthand rather than the protobuf enum means callers never have
// to deal with Temporal's generated types, and the strings match what the
// Temporal CLI shows.
type ScheduleOverlapPolicy string

// The overlap policies Temporal offers. The values are its own shorthand, and
// the table below is derived from the API rather than from these, so a policy
// added to a future Temporal version still converts even before it is named
// here.
const (
	// ScheduleOverlapPolicySkip drops the new action. Temporal's default.
	ScheduleOverlapPolicySkip ScheduleOverlapPolicy = "Skip"

	// ScheduleOverlapPolicyBufferOne keeps one action waiting, dropping the rest.
	ScheduleOverlapPolicyBufferOne ScheduleOverlapPolicy = "BufferOne"

	// ScheduleOverlapPolicyBufferAll queues every action to run in turn.
	ScheduleOverlapPolicyBufferAll ScheduleOverlapPolicy = "BufferAll"

	// ScheduleOverlapPolicyCancelOther cancels the running action first.
	ScheduleOverlapPolicyCancelOther ScheduleOverlapPolicy = "CancelOther"

	// ScheduleOverlapPolicyTerminateOther terminates the running action first.
	ScheduleOverlapPolicyTerminateOther ScheduleOverlapPolicy = "TerminateOther"

	// ScheduleOverlapPolicyAllowAll runs actions concurrently.
	ScheduleOverlapPolicyAllowAll ScheduleOverlapPolicy = "AllowAll"
)

// scheduleOverlapPolicies maps the shorthand onto Temporal's enum, derived from
// the API's own table so that it cannot drift from the pinned version.
var scheduleOverlapPolicies = func() map[ScheduleOverlapPolicy]enumspb.ScheduleOverlapPolicy {
	policies := make(
		map[ScheduleOverlapPolicy]enumspb.ScheduleOverlapPolicy,
		len(enumspb.ScheduleOverlapPolicy_shorthandValue),
	)
	for name, value := range enumspb.ScheduleOverlapPolicy_shorthandValue {
		policies[ScheduleOverlapPolicy(name)] = enumspb.ScheduleOverlapPolicy(value)
	}

	return policies
}()

// overlapPolicy converts the shorthand into the enum Temporal expects. An
// unrecognised policy is an error rather than a silent Unspecified, which
// Temporal would read as "use the default".
func (p ScheduleOverlapPolicy) overlapPolicy() (enumspb.ScheduleOverlapPolicy, error) {
	if p == "" {
		return enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, nil
	}

	value, ok := scheduleOverlapPolicies[p]
	if !ok {
		return enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED,
			fmt.Errorf("unknown schedule overlap policy %q", p)
	}

	return value, nil
}

// ScheduleRange is a set of integer values a calendar field matches.
type ScheduleRange struct {
	// Start is the first value in the range, inclusive.
	Start int32

	// End is the last value in the range, inclusive.
	End int32

	// Step is how far apart matched values are.
	Step int32
}

// ScheduleCalendar matches times against the calendar.
type ScheduleCalendar struct {
	Second     []ScheduleRange
	Minute     []ScheduleRange
	Hour       []ScheduleRange
	DayOfMonth []ScheduleRange
	Month      []ScheduleRange
	Year       []ScheduleRange
	DayOfWeek  []ScheduleRange
	Comment    string
}

// ScheduleInterval matches times at a fixed period from the Unix epoch.
type ScheduleInterval struct {
	// Every is the period between matched times.
	Every time.Duration

	// Offset shifts every matched time later by a fixed amount.
	Offset time.Duration
}

// ScheduleTiming is when a schedule acts.
type ScheduleTiming struct {
	Calendars        []ScheduleCalendar
	Intervals        []ScheduleInterval
	Cron             []string
	ExcludeCalendars []ScheduleCalendar
	StartAt          *time.Time
	EndAt            *time.Time
	Jitter           *time.Duration
	TimeZone         string
}

// ScheduleSearchAttribute is one typed search attribute value, already decoded
// into the Go type its Temporal type calls for.
type ScheduleSearchAttribute struct {
	// Name is the search attribute's name as Temporal knows it.
	Name string

	// Type is the Temporal type, in the same shorthand SearchAttribute uses.
	Type SearchAttributeType

	// Value is the value, as the Go type the Temporal type maps to.
	Value any
}

// SchedulePriority controls task ordering for the workflows a schedule starts.
type SchedulePriority struct {
	// Key is the priority, where smaller means sooner. Nil leaves it unset.
	Key *int32

	// FairnessKey groups tasks for fair dispatch.
	FairnessKey string

	// FairnessWeight is the key's share of dispatch. Nil leaves it unset.
	FairnessWeight *float32
}

// ScheduleWorkflow is the workflow a schedule starts.
type ScheduleWorkflow struct {
	Type       string
	TaskQueue  string
	WorkflowID string

	// Input holds the workflow's arguments as raw JSON, one entry per argument.
	// They are encoded into payloads here, so callers never build one.
	Input []string

	// Memo is the workflow's memo, values as raw JSON.
	Memo map[string]string

	// SearchAttributes are the workflow's typed search attributes.
	SearchAttributes []ScheduleSearchAttribute

	TaskTimeout      *time.Duration
	RunTimeout       *time.Duration
	ExecutionTimeout *time.Duration

	Priority *SchedulePriority

	StaticSummary string
	StaticDetails string
}

// SchedulePolicies are a schedule's overlap and failure policies.
type SchedulePolicies struct {
	Overlap        ScheduleOverlapPolicy
	CatchupWindow  *time.Duration
	PauseOnFailure bool
}

// ScheduleStateSpec is the part of a schedule's state a caller declares.
//
// Every field is a pointer because state is operational: a person can pause a
// schedule, and Temporal pauses one itself when pause-on-failure fires. A nil
// field is left exactly as the Service has it.
//
// The remaining-action count is deliberately not here. It is a counter the
// Service consumes rather than a state anybody sets, so there is nothing
// sensible for a caller to declare: writing a number back would undo the
// counting. It is still carried across every update untouched - see
// buildScheduleState - and can be read with Schedule.RemainingActions.
type ScheduleStateSpec struct {
	Paused *bool
	Notes  *string
}

// ScheduleDesired is the schedule a caller wants, holding only the parts the
// operator manages. Everything else on the Service is preserved rather than
// described here.
type ScheduleDesired struct {
	ID       string
	Timing   ScheduleTiming
	Workflow ScheduleWorkflow
	Policies SchedulePolicies
	State    ScheduleStateSpec

	// Memo is the schedule's own memo - not the workflow's - values as raw JSON.
	// Nil leaves whatever the Service holds; an empty map clears it.
	Memo map[string]string

	// SearchAttributes are the schedule's own typed search attributes. Nil
	// leaves whatever the Service holds; an empty slice clears them.
	SearchAttributes []ScheduleSearchAttribute
}

// Schedule is a Temporal schedule as the Service holds it.
//
// The whole server-side message is carried along unread so that an update can
// put back everything the operator does not manage. Temporal's update replaces
// the schedule wholesale, so anything not sent back is lost.
type Schedule struct {
	// ID is the schedule's ID.
	ID string

	// schedule is the message Describe returned. It is unexported because it is
	// the reason this type exists: callers get answers about it through methods
	// rather than reaching into protobuf.
	schedule *schedulepb.Schedule

	// memo and searchAttributes are the schedule's own, which live outside the
	// schedule message and are updated through their own request fields.
	memo             *commonpb.Memo
	searchAttributes *commonpb.SearchAttributes

	// conflictToken is what makes an update safe. Sending it back means the
	// Service refuses the write if anything changed in between.
	conflictToken []byte
}

// Paused reports whether the Service currently has the schedule paused.
func (s *Schedule) Paused() bool {
	return s.schedule.GetState().GetPaused()
}

// Notes returns the note the Service currently holds on the schedule.
func (s *Schedule) Notes() string {
	return s.schedule.GetState().GetNotes()
}

// RemainingActions returns how many more times the Service will act, and whether
// it is counting at all.
//
// This is a read of Temporal's runtime state and nothing more. The operator does
// not manage it: the count is consumed as the schedule acts, so there is no
// stable value to reconcile towards. It is exposed so that a caller can report
// what the Service says, and so that a test can prove an update left it alone.
func (s *Schedule) RemainingActions() (remaining int64, limited bool) {
	state := s.schedule.GetState()

	return state.GetRemainingActions(), state.GetLimitedActions()
}

// TimingHash fingerprints the timing specification the Service holds.
//
// This is what makes drift detection possible at all. Temporal canonicalises a
// spec on the way in - cron expressions and the legacy calendar form are
// compiled into structured calendars, and the originals discarded - so there is
// nothing on the Service to compare a cron expression against. Comparing this
// against the same hash taken right after the operator last wrote tells you
// whether anybody has changed the timing since.
func (s *Schedule) TimingHash() string {
	return hashProto(s.schedule.GetSpec())
}

// DescribeSchedule looks up a single Temporal schedule by ID.
//
// When the schedule does not exist the returned error satisfies
// errors.Is(err, ErrScheduleNotFound); the underlying Temporal error is kept in
// the chain.
func (c *Client) DescribeSchedule(ctx context.Context, namespace, id string) (*Schedule, error) {
	if id == "" {
		return nil, ErrNoScheduleID
	}

	if namespace == "" {
		return nil, ErrNoNamespaceName
	}

	resp, err := c.client.WorkflowService().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  namespace,
		ScheduleId: id,
	})
	if err != nil {
		return nil, mapScheduleError(fmt.Sprintf("describing schedule %s", id), err)
	}

	return &Schedule{
		ID:               id,
		schedule:         resp.GetSchedule(),
		memo:             resp.GetMemo(),
		searchAttributes: resp.GetSearchAttributes(),
		conflictToken:    resp.GetConflictToken(),
	}, nil
}

// CreateSchedule creates a Temporal schedule.
//
// Everything the caller declared is part of the create request, so a schedule
// never exists in a half-configured state - there is no window between it
// appearing and its policies or its paused state being written.
func (c *Client) CreateSchedule(ctx context.Context, namespace string, desired *ScheduleDesired) error {
	if desired == nil || desired.ID == "" {
		return ErrNoScheduleID
	}

	if namespace == "" {
		return ErrNoNamespaceName
	}

	schedule, err := buildSchedule(nil, desired)
	if err != nil {
		return fmt.Errorf("building schedule %s: %w", desired.ID, err)
	}

	memo, err := buildMemo(desired.Memo)
	if err != nil {
		return fmt.Errorf("building schedule %s memo: %w", desired.ID, err)
	}

	searchAttributes, err := buildSearchAttributes(desired.SearchAttributes)
	if err != nil {
		return fmt.Errorf("building schedule %s search attributes: %w", desired.ID, err)
	}

	_, err = c.client.WorkflowService().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:        namespace,
		ScheduleId:       desired.ID,
		Schedule:         schedule,
		Identity:         scheduleIdentity,
		RequestId:        newRequestID(),
		Memo:             memo,
		SearchAttributes: searchAttributes,
	})
	if err != nil {
		return mapScheduleError(fmt.Sprintf("creating schedule %s", desired.ID), err)
	}

	return nil
}

// UpdateSchedule brings an existing Temporal schedule into line with the desired
// state, leaving everything the operator does not manage exactly as it is.
//
// The update starts from the message actual was read with rather than from an
// empty one. Temporal replaces the schedule wholesale, so this is what keeps a
// retry policy, a header, a keepOriginalWorkflowId or anything else on the
// schedule from being wiped by a change to its timing.
//
// The conflict token actual was read with is sent too, so a schedule modified
// between the read and this write is refused rather than clobbered. That
// produces an error satisfying errors.Is(err, ErrScheduleChanged), which callers
// should answer by re-reading and trying again.
func (c *Client) UpdateSchedule(
	ctx context.Context,
	namespace string,
	actual *Schedule,
	desired *ScheduleDesired,
) error {
	if actual == nil || desired == nil || desired.ID == "" {
		return ErrNoScheduleID
	}

	if namespace == "" {
		return ErrNoNamespaceName
	}

	schedule, err := buildSchedule(actual.schedule, desired)
	if err != nil {
		return fmt.Errorf("building schedule %s: %w", desired.ID, err)
	}

	// Memo and search attributes are their own request fields, and unset means
	// "leave them alone" rather than "clear them". A nil desired map therefore
	// sends nothing at all, and an empty one sends an empty message to clear.
	memo, err := buildMemo(desired.Memo)
	if err != nil {
		return fmt.Errorf("building schedule %s memo: %w", desired.ID, err)
	}

	searchAttributes, err := buildSearchAttributes(desired.SearchAttributes)
	if err != nil {
		return fmt.Errorf("building schedule %s search attributes: %w", desired.ID, err)
	}

	_, err = c.client.WorkflowService().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
		Namespace:        namespace,
		ScheduleId:       desired.ID,
		Schedule:         schedule,
		ConflictToken:    actual.conflictToken,
		Identity:         scheduleIdentity,
		RequestId:        newRequestID(),
		Memo:             memo,
		SearchAttributes: searchAttributes,
	})
	if err != nil {
		return mapScheduleError(fmt.Sprintf("updating schedule %s", desired.ID), err)
	}

	return nil
}

// DeleteSchedule removes a Temporal schedule.
//
// A schedule that is already gone produces an error satisfying
// errors.Is(err, ErrScheduleNotFound), leaving it to the caller to decide
// whether that counts as success.
func (c *Client) DeleteSchedule(ctx context.Context, namespace, id string) error {
	if id == "" {
		return ErrNoScheduleID
	}

	if namespace == "" {
		return ErrNoNamespaceName
	}

	_, err := c.client.WorkflowService().DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{
		Namespace:  namespace,
		ScheduleId: id,
		Identity:   scheduleIdentity,
	})
	if err != nil {
		return mapScheduleError(fmt.Sprintf("deleting schedule %s", id), err)
	}

	return nil
}

// DesiredTimingHash fingerprints a desired timing specification.
//
// It hashes the protobuf that would be sent, so it is stable across reconciles
// and changes exactly when the request would. It is deliberately *not*
// comparable with Schedule.TimingHash: this is the spec before the Service
// canonicalises it, and that one is the spec after. Each is compared against its
// own kind.
func DesiredTimingHash(timing *ScheduleTiming) string {
	return hashProto(buildScheduleSpec(timing))
}

// ScheduleDrift lists the managed fields on which a schedule differs from the
// desired state, in the order they appear in the spec. An empty result means the
// Service already agrees.
//
// The timing specification is deliberately absent: it cannot be compared
// directly, because the Service canonicalises it. That comparison is the
// caller's, using DesiredTimingHash and Schedule.TimingHash.
//
// Only what the caller declares is compared. A nil state field, a nil memo and
// a nil search attribute slice are not managed, so they can never drift.
func (s *Schedule) ScheduleDrift(desired *ScheduleDesired) ([]string, error) {
	if desired == nil {
		return nil, nil
	}

	wanted, err := buildSchedule(s.schedule, desired)
	if err != nil {
		return nil, err
	}

	var drift []string

	if !proto.Equal(s.schedule.GetAction(), wanted.GetAction()) {
		drift = append(drift, "action")
	}

	if !proto.Equal(s.schedule.GetPolicies(), wanted.GetPolicies()) {
		drift = append(drift, "policies")
	}

	drift = append(drift, s.stateDrift(&desired.State)...)

	metadata, err := s.metadataDrift(desired)
	if err != nil {
		return nil, err
	}

	return append(drift, metadata...), nil
}

// stateDrift lists the state fields that differ from the ones the caller
// declared.
//
// It compares field by field rather than as a whole message, because only the
// declared fields are managed: a nil field has already been carried across from
// the Service by buildSchedule, so comparing the messages would report drift on
// something nobody asked to manage.
func (s *Schedule) stateDrift(desired *ScheduleStateSpec) []string {
	state := s.schedule.GetState()

	var drift []string

	if desired.Paused != nil && state.GetPaused() != *desired.Paused {
		drift = append(drift, "state.paused")
	}

	if desired.Notes != nil && state.GetNotes() != *desired.Notes {
		drift = append(drift, "notes")
	}

	return drift
}

// metadataDrift lists whether the schedule's own memo or search attributes
// differ from the declared ones.
//
// Both live outside the schedule message and are updated through their own
// request fields, and both are managed only when the caller declares them - a
// nil map or slice can never drift.
func (s *Schedule) metadataDrift(desired *ScheduleDesired) ([]string, error) {
	var drift []string

	if desired.Memo != nil {
		memo, err := buildMemo(desired.Memo)
		if err != nil {
			return nil, err
		}

		if !proto.Equal(s.memo, memo) {
			drift = append(drift, "memo")
		}
	}

	if desired.SearchAttributes != nil {
		searchAttributes, err := buildSearchAttributes(desired.SearchAttributes)
		if err != nil {
			return nil, err
		}

		if !proto.Equal(s.searchAttributes, searchAttributes) {
			drift = append(drift, "searchAttributes")
		}
	}

	return drift, nil
}

// scheduleIdentity is what the operator calls itself on schedule writes. It
// shows up in Temporal's own history of who changed a schedule.
const scheduleIdentity = "temporal-resource-operator"

// newRequestID produces the idempotence key Temporal wants on a schedule write.
//
// The Service uses it to recognise a retried request, so it has to be fresh per
// logical write rather than per attempt - which is what a new one per call
// gives: the gRPC layer retries an attempt under the same ID, while a fresh
// reconcile is a fresh request. The conflict token, not this, is what makes an
// update safe.
func newRequestID() string {
	return uuid.NewString()
}

// buildSchedule produces the message to send, starting from what the Service
// holds so that unmanaged fields survive.
//
// base is the schedule as Describe returned it, or nil when creating. Cloning it
// rather than building from scratch is the whole point: Temporal replaces the
// four main fields wholesale, so anything this function does not overwrite has
// to already be there.
func buildSchedule(base *schedulepb.Schedule, desired *ScheduleDesired) (*schedulepb.Schedule, error) {
	schedule := &schedulepb.Schedule{}
	if base != nil {
		// Clone, never mutate: the caller is holding base, and a half-applied
		// update on a failed write would be worse than no update at all.
		schedule = proto.Clone(base).(*schedulepb.Schedule)
	}

	schedule.Spec = buildScheduleSpec(&desired.Timing)

	action, err := buildScheduleAction(schedule.GetAction(), &desired.Workflow)
	if err != nil {
		return nil, err
	}

	schedule.Action = action

	policies, err := buildSchedulePolicies(schedule.GetPolicies(), &desired.Policies)
	if err != nil {
		return nil, err
	}

	schedule.Policies = policies
	schedule.State = buildScheduleState(schedule.GetState(), &desired.State)

	return schedule, nil
}

// buildScheduleSpec turns the desired timing into the protobuf spec.
//
// Unlike the other three, this one is built from scratch rather than from what
// the Service holds. The timing specification is entirely the operator's to
// declare - there is no part of it a user could set alongside - and the Service
// rewrites it into canonical form anyway, so carrying the old one across would
// only mix a compiled calendar in with a freshly declared cron expression.
func buildScheduleSpec(timing *ScheduleTiming) *schedulepb.ScheduleSpec {
	spec := &schedulepb.ScheduleSpec{
		TimezoneName: timing.TimeZone,
	}

	for i := range timing.Calendars {
		spec.StructuredCalendar = append(
			spec.StructuredCalendar, buildStructuredCalendar(&timing.Calendars[i]),
		)
	}

	for i := range timing.ExcludeCalendars {
		spec.ExcludeStructuredCalendar = append(
			spec.ExcludeStructuredCalendar, buildStructuredCalendar(&timing.ExcludeCalendars[i]),
		)
	}

	for _, interval := range timing.Intervals {
		spec.Interval = append(spec.Interval, &schedulepb.IntervalSpec{
			Interval: durationpb.New(interval.Every),
			Phase:    durationpb.New(interval.Offset),
		})
	}

	if len(timing.Cron) > 0 {
		spec.CronString = slices.Clone(timing.Cron)
	}

	if timing.StartAt != nil {
		spec.StartTime = timestamppb.New(*timing.StartAt)
	}

	if timing.EndAt != nil {
		spec.EndTime = timestamppb.New(*timing.EndAt)
	}

	if timing.Jitter != nil {
		spec.Jitter = durationpb.New(*timing.Jitter)
	}

	return spec
}

// buildStructuredCalendar converts one calendar to protobuf.
//
// The zero-value handling here is Temporal's, not a convenience: the Service's
// own CleanSpec reads an end below start as equal to start and a step of 0 as 1,
// so sending the defaults out is the same as sending them explicitly - and
// keeps the message small, which is what the Service does itself.
func buildStructuredCalendar(calendar *ScheduleCalendar) *schedulepb.StructuredCalendarSpec {
	return &schedulepb.StructuredCalendarSpec{
		Second:     buildRanges(calendar.Second),
		Minute:     buildRanges(calendar.Minute),
		Hour:       buildRanges(calendar.Hour),
		DayOfMonth: buildRanges(calendar.DayOfMonth),
		Month:      buildRanges(calendar.Month),
		Year:       buildRanges(calendar.Year),
		DayOfWeek:  buildRanges(calendar.DayOfWeek),
		Comment:    calendar.Comment,
	}
}

// buildRanges converts a calendar field's ranges to protobuf.
func buildRanges(ranges []ScheduleRange) []*schedulepb.Range {
	if len(ranges) == 0 {
		return nil
	}

	built := make([]*schedulepb.Range, 0, len(ranges))
	for _, r := range ranges {
		built = append(built, &schedulepb.Range{Start: r.Start, End: r.End, Step: r.Step})
	}

	return built
}

// buildScheduleAction produces the action to send, starting from the one the
// Service holds so that fields this operator does not manage - a retry policy,
// a header, a versioning override, a workflow ID reuse policy - survive an
// update that only changes the workflow type.
func buildScheduleAction(
	base *schedulepb.ScheduleAction,
	workflow *ScheduleWorkflow,
) (*schedulepb.ScheduleAction, error) {
	start := &workflowpb.NewWorkflowExecutionInfo{}
	if existing := base.GetStartWorkflow(); existing != nil {
		start = proto.Clone(existing).(*workflowpb.NewWorkflowExecutionInfo)
	}

	start.WorkflowId = workflow.WorkflowID
	start.WorkflowType = &commonpb.WorkflowType{Name: workflow.Type}
	start.TaskQueue = &taskqueuepb.TaskQueue{
		Name: workflow.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	input, err := buildPayloads(workflow.Input)
	if err != nil {
		return nil, fmt.Errorf("input: %w", err)
	}

	start.Input = input

	memo, err := buildMemo(workflow.Memo)
	if err != nil {
		return nil, fmt.Errorf("memo: %w", err)
	}

	start.Memo = memo

	searchAttributes, err := buildSearchAttributes(workflow.SearchAttributes)
	if err != nil {
		return nil, fmt.Errorf("searchAttributes: %w", err)
	}

	start.SearchAttributes = searchAttributes

	start.WorkflowTaskTimeout = optionalDuration(workflow.TaskTimeout)
	start.WorkflowRunTimeout = optionalDuration(workflow.RunTimeout)
	start.WorkflowExecutionTimeout = optionalDuration(workflow.ExecutionTimeout)

	start.Priority = buildPriority(workflow.Priority)

	metadata, err := buildUserMetadata(workflow.StaticSummary, workflow.StaticDetails)
	if err != nil {
		return nil, fmt.Errorf("staticSummary/staticDetails: %w", err)
	}

	start.UserMetadata = metadata

	return &schedulepb.ScheduleAction{
		Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: start},
	}, nil
}

// buildSchedulePolicies produces the policies to send, starting from the ones
// the Service holds so that keepOriginalWorkflowId - which this operator does
// not expose - survives.
func buildSchedulePolicies(
	base *schedulepb.SchedulePolicies,
	policies *SchedulePolicies,
) (*schedulepb.SchedulePolicies, error) {
	built := &schedulepb.SchedulePolicies{}
	if base != nil {
		built = proto.Clone(base).(*schedulepb.SchedulePolicies)
	}

	overlap, err := policies.Overlap.overlapPolicy()
	if err != nil {
		return nil, err
	}

	built.OverlapPolicy = overlap
	built.PauseOnFailure = policies.PauseOnFailure

	// An absent catch-up window is not managed, and must not be sent as an
	// absent one either. The Service fills the field in with its own default of
	// a year and stores it, so clearing it here would differ from what the
	// Service holds on every single reconcile - and the operator would rewrite
	// the schedule for ever, reporting drift that was its own doing.
	if policies.CatchupWindow != nil {
		built.CatchupWindow = durationpb.New(*policies.CatchupWindow)
	}

	return built, nil
}

// buildScheduleState produces the state to send, changing only the fields the
// caller declared.
//
// This is where the pointers in ScheduleStateSpec earn their keep. State is
// operational: a person pauses a schedule from the UI, Temporal pauses one when
// pause-on-failure fires, and the Service counts remaining actions down itself.
// A nil field is carried across untouched, so the operator does not undo any of
// that on its next resync.
func buildScheduleState(
	base *schedulepb.ScheduleState,
	state *ScheduleStateSpec,
) *schedulepb.ScheduleState {
	built := &schedulepb.ScheduleState{}
	if base != nil {
		built = proto.Clone(base).(*schedulepb.ScheduleState)
	}

	if state.Paused != nil {
		built.Paused = *state.Paused
	}

	if state.Notes != nil {
		built.Notes = *state.Notes
	}

	// LimitedActions and RemainingActions are deliberately never assigned. They
	// came off the clone of what the Service holds and stay exactly as they
	// were, which is the whole reason this builds from the Service's own message
	// rather than an empty one: an update replaces the state wholesale, so a
	// counter this function did not copy across would be reset to zero.
	return built
}

// buildPriority converts the priority settings to protobuf, returning nil when
// nothing was asked for so that the Service's own defaults apply.
func buildPriority(priority *SchedulePriority) *commonpb.Priority {
	if priority == nil {
		return nil
	}

	built := &commonpb.Priority{FairnessKey: priority.FairnessKey}

	if priority.Key != nil {
		built.PriorityKey = *priority.Key
	}

	if priority.FairnessWeight != nil {
		built.FairnessWeight = *priority.FairnessWeight
	}

	if built.GetPriorityKey() == 0 && built.GetFairnessKey() == "" && built.GetFairnessWeight() == 0 {
		// Nothing was actually set, so say nothing rather than sending an empty
		// message the Service would have to interpret.
		return nil
	}

	return built
}

// buildUserMetadata encodes the static summary and details.
//
// Temporal wants each as a json/plain payload holding a single JSON string,
// which is what the default data converter produces for a Go string.
func buildUserMetadata(summary, details string) (*sdkpb.UserMetadata, error) {
	if summary == "" && details == "" {
		return nil, nil
	}

	metadata := &sdkpb.UserMetadata{}

	if summary != "" {
		payload, err := converter.GetDefaultDataConverter().ToPayload(summary)
		if err != nil {
			return nil, fmt.Errorf("encoding summary: %w", err)
		}

		metadata.Summary = payload
	}

	if details != "" {
		payload, err := converter.GetDefaultDataConverter().ToPayload(details)
		if err != nil {
			return nil, fmt.Errorf("encoding details: %w", err)
		}

		metadata.Details = payload
	}

	return metadata, nil
}

// buildPayloads encodes workflow arguments written as JSON.
//
// Each argument is decoded from JSON and re-encoded with Temporal's default data
// converter, which is exactly what "temporal workflow start --input" does. That
// matters twice over: a worker written against any SDK deserialises the result
// the way it always would, and the round trip normalises whitespace and key
// order, so reformatting the YAML does not read as a change to the schedule.
//
// Numbers are decoded as json.Number rather than float64, so a large integer
// argument survives the round trip exactly.
func buildPayloads(input []string) (*commonpb.Payloads, error) {
	if len(input) == 0 {
		return nil, nil
	}

	payloads := make([]*commonpb.Payload, 0, len(input))

	for i, raw := range input {
		value, err := decodeJSON(raw)
		if err != nil {
			return nil, fmt.Errorf("argument %d: %w", i, err)
		}

		payload, err := converter.GetDefaultDataConverter().ToPayload(value)
		if err != nil {
			return nil, fmt.Errorf("argument %d: encoding: %w", i, err)
		}

		payloads = append(payloads, payload)
	}

	return &commonpb.Payloads{Payloads: payloads}, nil
}

// buildMemo encodes a memo whose values are written as JSON.
//
// A nil map returns nil, which every request field reads as "leave this alone".
// An empty map returns an empty message, which they read as "clear it" - the
// distinction the caller needs to be able to make.
func buildMemo(values map[string]string) (*commonpb.Memo, error) {
	if values == nil {
		return nil, nil
	}

	fields := make(map[string]*commonpb.Payload, len(values))

	for _, key := range slices.Sorted(maps.Keys(values)) {
		value, err := decodeJSON(values[key])
		if err != nil {
			return nil, fmt.Errorf("%q: %w", key, err)
		}

		payload, err := converter.GetDefaultDataConverter().ToPayload(value)
		if err != nil {
			return nil, fmt.Errorf("%q: encoding: %w", key, err)
		}

		fields[key] = payload
	}

	return &commonpb.Memo{Fields: fields}, nil
}

// buildSearchAttributes encodes typed search attributes.
//
// The encoding is Temporal's: the value as a json/plain payload, with the
// attribute's type recorded in the payload metadata. Without that metadata the
// Service cannot tell an Int from a Text and will not index the value.
//
// A nil slice returns nil, which the request fields read as "leave them alone";
// an empty slice returns an empty message, which clears them.
func buildSearchAttributes(attributes []ScheduleSearchAttribute) (*commonpb.SearchAttributes, error) {
	if attributes == nil {
		return nil, nil
	}

	fields := make(map[string]*commonpb.Payload, len(attributes))

	for _, attribute := range attributes {
		valueType, err := attribute.Type.indexedValueType()
		if err != nil {
			return nil, fmt.Errorf("%q: %w", attribute.Name, err)
		}

		payload, err := converter.GetDefaultDataConverter().ToPayload(attribute.Value)
		if err != nil {
			return nil, fmt.Errorf("%q: encoding: %w", attribute.Name, err)
		}

		if payload.GetData() != nil {
			payload.Metadata["type"] = []byte(valueType.String())
		}

		fields[attribute.Name] = payload
	}

	return &commonpb.SearchAttributes{IndexedFields: fields}, nil
}

// decodeJSON reads a JSON document, keeping numbers as written.
func decodeJSON(raw string) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("%q is not valid JSON: %w", raw, err)
	}

	if decoder.More() {
		return nil, fmt.Errorf("%q has more than one JSON value", raw)
	}

	return value, nil
}

// optionalDuration converts a duration that may not have been given.
func optionalDuration(d *time.Duration) *durationpb.Duration {
	if d == nil {
		return nil
	}

	return durationpb.New(*d)
}

// hashProto fingerprints a protobuf message.
//
// Marshalling is deterministic, so the same message always produces the same
// hash. Note that protobuf's own documentation is clear that deterministic
// marshalling is stable within a build rather than across versions of the
// library - which is fine here, because a changed hash only ever costs one
// redundant update, and the value is recorded again straight afterwards.
func hashProto(message proto.Message) string {
	if message == nil {
		return ""
	}

	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		// Marshalling a message that was just built, or that came off the wire,
		// does not fail. An empty hash reads as "unknown", which makes the
		// caller write rather than skip - the safe way to be wrong.
		return ""
	}

	sum := sha256.Sum256(encoded)

	return hex.EncodeToString(sum[:])
}

// mapScheduleError turns Temporal's schedule errors into this package's own
// sentinels, keeping the original error in the chain so callers can still report
// what the Service said. Everything else is wrapped with context.
func mapScheduleError(action string, err error) error {
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return fmt.Errorf("%w: %w", ErrScheduleNotFound, err)
	}

	var alreadyExists *serviceerror.AlreadyExists
	if errors.As(err, &alreadyExists) {
		return fmt.Errorf("%w: %w", ErrScheduleExists, err)
	}

	// A stale conflict token comes back as FailedPrecondition rather than as a
	// dedicated type, so the message is what identifies it.
	var failedPrecondition *serviceerror.FailedPrecondition
	if errors.As(err, &failedPrecondition) &&
		strings.Contains(strings.ToLower(failedPrecondition.Error()), "conflict") {
		return fmt.Errorf("%w: %w", ErrScheduleChanged, err)
	}

	// Namespaces can have schedules switched off, which is worth telling apart
	// from a schedule that simply could not be written.
	var invalidArgument *serviceerror.InvalidArgument
	if errors.As(err, &invalidArgument) &&
		strings.Contains(strings.ToLower(invalidArgument.Error()), "schedules are not allowed") {
		return fmt.Errorf("%w: %w", ErrSchedulesNotAllowed, err)
	}

	return fmt.Errorf("%s: %w", action, err)
}
