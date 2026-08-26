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

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ScheduleFinalizer holds a Schedule open until the operator has decided what to
// do with the Temporal schedule it manages.
const ScheduleFinalizer = "temporal.simonemms.com/schedule"

// The limits below mirror the pinned Temporal Service rather than being invented
// here. Each names the server constant it comes from, so that a future Temporal
// version changing one is a documented change rather than a mystery.
const (
	// MaxScheduleIDLength is the longest schedule ID Temporal will accept.
	//
	// The Service validates the schedule ID with its workflow ID prefix already
	// attached - "temporal-sys-scheduler:", 23 characters - against
	// limit.maxIDLength, which defaults to 1000. What is left is the room a
	// schedule ID actually has.
	MaxScheduleIDLength = 1000 - len("temporal-sys-scheduler:")

	// MaxWorkflowIDLength is the longest workflow ID a schedule action may ask
	// for.
	//
	// The Service appends a timestamp to the workflow ID of each run it starts,
	// and validates the ID with a representative timestamp already appended -
	// "-2009-11-10T23:00:00Z", 21 characters - against the same limit.
	MaxWorkflowIDLength = 1000 - len("-2009-11-10T23:00:00Z")

	// MaxIDLength is limit.maxIDLength itself, which bounds a workflow type
	// name and a task queue name.
	MaxIDLength = 1000

	// MaxCalendarCommentLength is the Service's maxCommentLen.
	MaxCalendarCommentLength = 200

	// MaxFairnessKeyBytes is the documented fairness key limit. It is a limit on
	// bytes rather than characters, which is why the rule enforcing it converts
	// before measuring.
	MaxFairnessKeyBytes = 64

	// MinCalendarYear and MaxCalendarYear are the Service's minCalendarYear and
	// maxCalendarYear. A calendar spec cannot name a year outside them.
	MinCalendarYear = 2000
	MaxCalendarYear = 2100
)

// ScheduleDeletionPolicy decides what becomes of the Temporal schedule when the
// resource managing it is deleted.
//
// It mirrors the policy the other resources use deliberately - they should all
// behave the same way - but is its own type so that each resource's API
// describes what it actually does.
// +kubebuilder:validation:Enum=Delete;Orphan
type ScheduleDeletionPolicy string

const (
	// ScheduleDeletionPolicyDelete removes the Temporal schedule along with the
	// resource, but only where the resource created it.
	ScheduleDeletionPolicyDelete ScheduleDeletionPolicy = "Delete"

	// ScheduleDeletionPolicyOrphan leaves the Temporal schedule exactly where it
	// is. The operator asks Temporal nothing at all, so deletion succeeds even
	// when the Connection it would have needed is gone.
	ScheduleDeletionPolicyOrphan ScheduleDeletionPolicy = "Orphan"
)

// DefaultScheduleDeletionPolicy is applied when a Schedule does not ask for one.
// It matches the CRD's default, and is applied again in Go so that an object
// built in memory behaves the same as one that has been through admission.
const DefaultScheduleDeletionPolicy = ScheduleDeletionPolicyDelete

// ScheduleOwnership records how a Schedule came to manage its Temporal schedule,
// and therefore whether deleting the resource deletes the schedule.
//
// A Temporal schedule has nowhere the operator can safely record who created it.
// Its only free-form space is the schedule memo, and this CRD hands that to the
// user - writing an operator key into a map the user also declares would mean
// either fighting over the map or letting a spec change delete the marker. So
// this is the operator's own bookkeeping, like SearchAttribute and
// NexusEndpoint, and cannot be checked against Temporal the way a namespace's
// owner marker can.
// +kubebuilder:validation:Enum=Creating;Created;Adopted
type ScheduleOwnership string

const (
	// ScheduleOwnershipCreating records that the operator found the schedule
	// missing and is about to create it. It is written before the create call,
	// so that a reconcile interrupted between creating and recording the result
	// can still tell that the schedule it now sees is most likely its own work
	// rather than something to adopt.
	ScheduleOwnershipCreating ScheduleOwnership = "Creating"

	// ScheduleOwnershipCreated records that this resource caused the schedule to
	// exist. Deleting the resource deletes the schedule.
	ScheduleOwnershipCreated ScheduleOwnership = "Created"

	// ScheduleOwnershipAdopted records that the schedule pre-dated this
	// resource. The operator keeps its declared configuration in step but does
	// not own its lifecycle, so deleting the resource leaves the schedule alone.
	ScheduleOwnershipAdopted ScheduleOwnership = "Adopted"
)

// OwnsSchedule reports whether deleting this resource should delete the Temporal
// schedule.
//
// Creating counts as owned. The marker is only ever written after the operator
// has seen the schedule missing, so what is there now is either this resource's
// own work or nothing at all - and deleting nothing is harmless.
func (o ScheduleOwnership) OwnsSchedule() bool {
	return o == ScheduleOwnershipCreating || o == ScheduleOwnershipCreated
}

// IsEstablished reports whether ownership has been settled one way or the other.
func (o ScheduleOwnership) IsEstablished() bool {
	return o == ScheduleOwnershipCreated || o == ScheduleOwnershipAdopted
}

// ScheduleOverlapPolicy decides what happens when a schedule would start an
// action while an earlier one is still running.
//
// The values are Temporal's own shorthand names, so what goes in the spec is
// what the Temporal CLI and API call it - no translation table to get wrong.
// +kubebuilder:validation:Enum=Skip;BufferOne;BufferAll;CancelOther;TerminateOther;AllowAll
type ScheduleOverlapPolicy string

const (
	// ScheduleOverlapPolicySkip drops the new action. This is Temporal's default.
	ScheduleOverlapPolicySkip ScheduleOverlapPolicy = "Skip"

	// ScheduleOverlapPolicyBufferOne keeps one action waiting and drops the rest.
	ScheduleOverlapPolicyBufferOne ScheduleOverlapPolicy = "BufferOne"

	// ScheduleOverlapPolicyBufferAll queues every action to run in turn.
	ScheduleOverlapPolicyBufferAll ScheduleOverlapPolicy = "BufferAll"

	// ScheduleOverlapPolicyCancelOther cancels the running action, then starts
	// the new one once it has stopped.
	ScheduleOverlapPolicyCancelOther ScheduleOverlapPolicy = "CancelOther"

	// ScheduleOverlapPolicyTerminateOther terminates the running action and
	// starts the new one immediately.
	ScheduleOverlapPolicyTerminateOther ScheduleOverlapPolicy = "TerminateOther"

	// ScheduleOverlapPolicyAllowAll runs actions concurrently.
	ScheduleOverlapPolicyAllowAll ScheduleOverlapPolicy = "AllowAll"
)

// A calendar field matches a set of values, written as ranges: a start, an
// optional inclusive end and an optional step. It is Temporal's own Range, and
// "start" alone matches one value.
//
// There is a type per field rather than one shared one, because each field has
// its own bounds and those bounds belong in the schema. Expressing them as
// plain minimum and maximum - rather than as a CEL rule walking the list - is
// what lets every collection on a Schedule stay unbounded: Kubernetes refuses a
// CRD whose CEL rules could run over an unbounded list, and would have forced
// an arbitrary cap on how many calendars a schedule may have. Temporal imposes
// no such cap, so neither does this.
//
// The types are otherwise identical, and the names they are declared under
// never reach the generated schema - controller-gen inlines them - so they are
// named after the values they hold.

// ScheduleRange0To59 is a range of seconds or minutes.
type ScheduleRange0To59 struct {
	// start is the first value in the range, and is matched itself.
	// +required
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=59
	Start int32 `json:"start"`

	// end is the last value in the range, and is matched itself. Omit it to
	// match start alone.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=59
	End *int32 `json:"end,omitempty"`

	// step is how far apart the matched values are within the range - 2 matches
	// every other value. Omit it for every value.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Step *int32 `json:"step,omitempty"`
}

// ScheduleRange0To23 is a range of hours.
type ScheduleRange0To23 struct {
	// start is the first value in the range, and is matched itself.
	// +required
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=23
	Start int32 `json:"start"`

	// end is the last value in the range, and is matched itself. Omit it to
	// match start alone.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=23
	End *int32 `json:"end,omitempty"`

	// step is how far apart the matched values are within the range. Omit it
	// for every value.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Step *int32 `json:"step,omitempty"`
}

// ScheduleRange1To31 is a range of days of the month.
type ScheduleRange1To31 struct {
	// start is the first value in the range, and is matched itself.
	// +required
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=31
	Start int32 `json:"start"`

	// end is the last value in the range, and is matched itself. Omit it to
	// match start alone.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=31
	End *int32 `json:"end,omitempty"`

	// step is how far apart the matched values are within the range. Omit it
	// for every value.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Step *int32 `json:"step,omitempty"`
}

// ScheduleRange1To12 is a range of months.
type ScheduleRange1To12 struct {
	// start is the first value in the range, and is matched itself.
	// +required
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=12
	Start int32 `json:"start"`

	// end is the last value in the range, and is matched itself. Omit it to
	// match start alone.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=12
	End *int32 `json:"end,omitempty"`

	// step is how far apart the matched values are within the range. Omit it
	// for every value.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Step *int32 `json:"step,omitempty"`
}

// ScheduleRange0To6 is a range of days of the week, where 0 is Sunday.
type ScheduleRange0To6 struct {
	// start is the first value in the range, and is matched itself. 0 is Sunday.
	// +required
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=6
	Start int32 `json:"start"`

	// end is the last value in the range, and is matched itself. Omit it to
	// match start alone.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=6
	End *int32 `json:"end,omitempty"`

	// step is how far apart the matched values are within the range. Omit it
	// for every value.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Step *int32 `json:"step,omitempty"`
}

// ScheduleRange2000To2100 is a range of years, bounded by the Service's own
// minCalendarYear and maxCalendarYear.
type ScheduleRange2000To2100 struct {
	// start is the first value in the range, and is matched itself.
	// +required
	// +kubebuilder:validation:Minimum=2000
	// +kubebuilder:validation:Maximum=2100
	Start int32 `json:"start"`

	// end is the last value in the range, and is matched itself. Omit it to
	// match start alone.
	// +optional
	// +kubebuilder:validation:Minimum=2000
	// +kubebuilder:validation:Maximum=2100
	End *int32 `json:"end,omitempty"`

	// step is how far apart the matched values are within the range. Omit it
	// for every value.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Step *int32 `json:"step,omitempty"`
}

// ScheduleCalendarRange is what every calendar field's range offers, so that the
// code turning a calendar into a Temporal request - and the code checking it -
// can be written once rather than six times.
//
// It is not part of the serialised API, only of the Go one, so it is excluded
// from deepcopy generation.
//
// +kubebuilder:object:generate=false
type ScheduleCalendarRange interface {
	// StartValue is the first value in the range.
	StartValue() int32

	// EndValue is the last value in the range, which is the start when no end
	// was given.
	EndValue() int32

	// StepValue is the step between matched values, which is 1 when none was
	// given.
	StepValue() int32
}

// rangeEnd resolves an optional end against its start.
func rangeEnd(start int32, end *int32) int32 {
	if end == nil {
		return start
	}

	return *end
}

// rangeStep resolves an optional step, which Temporal reads as 1 when unset.
func rangeStep(step *int32) int32 {
	if step == nil {
		return 1
	}

	return *step
}

func (r ScheduleRange0To59) StartValue() int32 { return r.Start }
func (r ScheduleRange0To59) EndValue() int32   { return rangeEnd(r.Start, r.End) }
func (r ScheduleRange0To59) StepValue() int32  { return rangeStep(r.Step) }

func (r ScheduleRange0To23) StartValue() int32 { return r.Start }
func (r ScheduleRange0To23) EndValue() int32   { return rangeEnd(r.Start, r.End) }
func (r ScheduleRange0To23) StepValue() int32  { return rangeStep(r.Step) }

func (r ScheduleRange1To31) StartValue() int32 { return r.Start }
func (r ScheduleRange1To31) EndValue() int32   { return rangeEnd(r.Start, r.End) }
func (r ScheduleRange1To31) StepValue() int32  { return rangeStep(r.Step) }

func (r ScheduleRange1To12) StartValue() int32 { return r.Start }
func (r ScheduleRange1To12) EndValue() int32   { return rangeEnd(r.Start, r.End) }
func (r ScheduleRange1To12) StepValue() int32  { return rangeStep(r.Step) }

func (r ScheduleRange0To6) StartValue() int32 { return r.Start }
func (r ScheduleRange0To6) EndValue() int32   { return rangeEnd(r.Start, r.End) }
func (r ScheduleRange0To6) StepValue() int32  { return rangeStep(r.Step) }

func (r ScheduleRange2000To2100) StartValue() int32 { return r.Start }
func (r ScheduleRange2000To2100) EndValue() int32   { return rangeEnd(r.Start, r.End) }
func (r ScheduleRange2000To2100) StepValue() int32  { return rangeStep(r.Step) }

// ScheduleCalendar matches times against the calendar, the way a cron expression
// does but written out in full.
//
// A time matches when at least one range of every field matches it. The
// defaults are Temporal's, and they are what make a calendar readable: a
// calendar naming only an hour fires at zero seconds past zero minutes past
// that hour, every day, rather than sixty times a minute.
//
// Every field left out therefore means something specific:
//
//   - second, minute, hour: omitted matches 0
//   - dayOfMonth, month, dayOfWeek: omitted matches all
//   - year: omitted matches all
//
// The bounds on each field are Temporal's own, checked by the Service in
// validateStructuredCalendar and carried here by the range type each field
// takes. Nothing limits how many ranges a field may hold, or how many calendars
// a schedule may have, because Temporal limits neither.
type ScheduleCalendar struct {
	// second ranges to match, 0-59. Omitted matches second 0.
	// +optional
	Second []ScheduleRange0To59 `json:"second,omitempty"`

	// minute ranges to match, 0-59. Omitted matches minute 0.
	// +optional
	Minute []ScheduleRange0To59 `json:"minute,omitempty"`

	// hour ranges to match, 0-23. Omitted matches hour 0.
	// +optional
	Hour []ScheduleRange0To23 `json:"hour,omitempty"`

	// dayOfMonth ranges to match, 1-31. Omitted matches every day.
	// +optional
	DayOfMonth []ScheduleRange1To31 `json:"dayOfMonth,omitempty"`

	// month ranges to match, 1-12 where 1 is January. Omitted matches every
	// month.
	// +optional
	Month []ScheduleRange1To12 `json:"month,omitempty"`

	// year ranges to match, 2000-2100. Omitted matches every year, which is
	// almost always what you want.
	//
	// The bounds are the Service's own minCalendarYear and maxCalendarYear.
	// +optional
	Year []ScheduleRange2000To2100 `json:"year,omitempty"`

	// dayOfWeek ranges to match, 0-6 where **0 is Sunday**. Omitted matches
	// every day.
	//
	// Sunday being 0 is Temporal's numbering, taken from Go's time.Weekday, and
	// is what the Service matches against.
	// +optional
	DayOfWeek []ScheduleRange0To6 `json:"dayOfWeek,omitempty"`

	// comment describes what this calendar is for. Temporal stores it on the
	// calendar and shows it in its UI.
	// +optional
	// +kubebuilder:validation:MaxLength=200
	Comment string `json:"comment,omitempty"`
}

// ScheduleInterval matches times at a fixed period, counted from the Unix epoch
// rather than from when the schedule was created.
//
// The times are every `epoch + (n * every) + offset`. An `every` of 1h matches
// every hour on the hour; the same `every` with an `offset` of 19m matches every
// xx:19:00.
type ScheduleInterval struct {
	// every is the period between matched times.
	//
	// Temporal refuses anything under one second. Written as a duration string,
	// the same syntax spec.retention on a Namespace takes.
	// +required
	Every Duration `json:"every"`

	// offset shifts every matched time later by a fixed amount.
	//
	// Temporal requires an offset shorter than the period - an offset of a whole
	// period is the same as no offset, and the Service refuses it rather than
	// accepting the ambiguity.
	// +optional
	Offset *Duration `json:"offset,omitempty"`
}

// ScheduleTiming says when a schedule should act.
//
// The times are the union of every calendar, interval and cron expression, minus
// the times excludeCalendars match. Any combination is allowed, because Temporal
// allows any combination: cron is there for migrating existing cron workflows,
// and calendars and intervals are there for writing new schedules readably.
//
// At least one of calendars, intervals or cron must be present, or the schedule
// would never act at all.
// +kubebuilder:validation:XValidation:rule="has(self.calendars) || has(self.intervals) || has(self.cron)",message="at least one of calendars, intervals or cron is required"
//
//nolint:lll // kubebuilder markers cannot be wrapped
type ScheduleTiming struct {
	// calendars match times against the calendar, written out in full.
	// +optional
	// +kubebuilder:validation:MinItems=1
	Calendars []ScheduleCalendar `json:"calendars,omitempty"`

	// intervals match times at a fixed period.
	// +optional
	// +kubebuilder:validation:MinItems=1
	Intervals []ScheduleInterval `json:"intervals,omitempty"`

	// cron holds cron expressions, for migrating a cron workflow to a schedule
	// without rewriting its timing.
	//
	// Temporal accepts 5, 6 or 7 space-separated fields:
	//
	//	5: minute hour dayOfMonth month dayOfWeek
	//	6: minute hour dayOfMonth month dayOfWeek year
	//	7: second minute hour dayOfMonth month dayOfWeek year
	//
	// The shorthands @yearly, @monthly, @weekly, @daily and @hourly work, and so
	// does "@every <interval>[/<phase>]", which Temporal compiles into an
	// interval rather than a calendar. A "CRON_TZ=<zone>" or "TZ=<zone>" prefix
	// sets the time zone, in which case leave timeZone empty. A "#" comment may
	// follow the expression.
	//
	// Note that Temporal does *not* implement the special case some cron
	// implementations have of treating dayOfMonth and dayOfWeek as "or" rather
	// than "and" when both are set.
	//
	// **Temporal compiles cron away.** Once a schedule is created the Service
	// stores these as calendars and intervals, and never reports them back as
	// cron. That does not stop the operator noticing drift - see the Schedule
	// documentation - but it does mean the Temporal UI will show a calendar
	// where you wrote cron.
	// +optional
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=1000
	Cron []string `json:"cron,omitempty"`

	// excludeCalendars remove times the rest of the specification would have
	// matched. Every field of an exclusion, seconds included, has to match a
	// time for that time to be skipped.
	// +optional
	// +kubebuilder:validation:MinItems=1
	ExcludeCalendars []ScheduleCalendar `json:"excludeCalendars,omitempty"`

	// startAt drops every matching time before it. Omitted, the schedule reaches
	// back to the beginning of time - which matters only in combination with a
	// catch-up window.
	// +optional
	StartAt *metav1.Time `json:"startAt,omitempty"`

	// endAt drops every matching time after it, retiring the schedule without
	// deleting it.
	// +optional
	EndAt *metav1.Time `json:"endAt,omitempty"`

	// jitter spreads each action out by a random amount between zero and this
	// duration, so that many schedules sharing a time do not all fire at once.
	//
	// Temporal caps the delay at the time until the next action, so jitter
	// longer than the period between actions does not push an action past its
	// successor.
	// +optional
	Jitter *Duration `json:"jitter,omitempty"`

	// timeZone is the IANA time zone the calendars are interpreted in, such as
	// "Europe/London". Defaults to UTC.
	//
	// The Temporal Service loads the zone from its own environment, so a zone
	// this operator can resolve is not automatically one the Service can.
	//
	// Calendar matching is literal, with no special handling of daylight saving:
	// a calendar firing at 02:30 in a zone that observes DST will not fire on
	// the day that has no 02:30, and one firing at 01:30 will fire twice on the
	// day that has two. Use UTC for a schedule that must be entirely
	// self-contained.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	TimeZone string `json:"timeZone,omitempty"`
}

// ScheduleWorkflowTimeouts bounds the workflow runs a schedule starts.
type ScheduleWorkflowTimeouts struct {
	// task is how long a worker has to process one workflow task once it has
	// picked it up.
	// +optional
	Task *Duration `json:"task,omitempty"`

	// run is how long a single run of the workflow may take.
	// +optional
	Run *Duration `json:"run,omitempty"`

	// execution is how long the whole workflow execution may take, retries and
	// continue-as-new included.
	// +optional
	Execution *Duration `json:"execution,omitempty"`
}

// SchedulePriority controls how the workflow's tasks are ordered against others
// on the same task queue.
type SchedulePriority struct {
	// key is the priority, where smaller means sooner.
	//
	// Temporal's range is 1 to a server-configured maximum which defaults to 5,
	// and an absent priority behaves as the middle of that range. The upper
	// bound here is the default maximum; a Service configured with more priority
	// levels than that will accept values this API refuses.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5
	Key *int32 `json:"key,omitempty"`

	// fairnessKey groups this workflow's tasks for fair dispatch - a tenant ID,
	// or a fixed label like "high".
	//
	// Temporal limits it to 64 **bytes**, not characters, which is why the rule
	// below measures the encoded form.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="size(bytes(self)) <= 64",message="fairnessKey must be at most 64 bytes"
	FairnessKey string `json:"fairnessKey,omitempty"`

	// fairnessWeight is this key's share of dispatch relative to other keys.
	// Defaults to 1.
	//
	// Written as a decimal string, because the value is fractional: "9", "0.5",
	// "0.001". Temporal clamps it to between 0.001 and 1000; this API refuses
	// anything outside that instead, so what the spec says is what happens.
	// +optional
	// +kubebuilder:validation:Pattern=`^(0|[1-9][0-9]*)(\.[0-9]+)?$`
	// +kubebuilder:validation:MaxLength=16
	FairnessWeight string `json:"fairnessWeight,omitempty"`
}

// ScheduleSearchAttribute is one typed search attribute value.
//
// The type has to be stated because a search attribute's value cannot be
// interpreted without it: "2026-01-01" is a Datetime or a Keyword depending
// entirely on how the attribute was registered, and sending the wrong one is
// rejected by Temporal.
type ScheduleSearchAttribute struct {
	// name is the search attribute's name, exactly as Temporal knows it -
	// "CustomerId", not "customer-id". It must already be registered on the
	// namespace, which is what a SearchAttribute resource is for.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	Name string `json:"name"`

	// type is the type of the value, using the same names as a SearchAttribute
	// resource and the Temporal CLI.
	// +required
	Type SearchAttributeType `json:"type"`

	// value is the value, written as JSON.
	//
	// What that means follows the type: a Bool takes "true", an Int takes "7", a
	// Double takes "1.5", a Keyword or Text takes a quoted string like
	// "\"gold\"", a Datetime takes a quoted RFC 3339 timestamp, and a
	// KeywordList takes a JSON array of strings.
	// +required
	// +kubebuilder:validation:MinLength=1
	Value string `json:"value"`
}

// ScheduleWorkflow is the workflow a schedule starts each time it acts.
type ScheduleWorkflow struct {
	// type is the workflow type to start, as registered by your worker.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1000
	Type string `json:"type"`

	// taskQueue is the task queue the workflow's tasks are put on. A worker has
	// to be polling it, or the runs will start and sit there.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1000
	TaskQueue string `json:"taskQueue"`

	// workflowId is the workflow ID each run is started with.
	//
	// Temporal appends a timestamp to it so that each run is distinct, which is
	// why the length limit here is short of the usual one. Omit it and Temporal
	// generates a UUID per run - fine, but far less readable in the UI.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=979
	WorkflowID string `json:"workflowId,omitempty"`

	// input holds the workflow's arguments, one entry per argument, each written
	// as JSON.
	//
	// This is the declarative equivalent of "temporal workflow start --input":
	// each entry is encoded with Temporal's default data converter into a
	// json/plain payload, so a worker written against any SDK deserialises them
	// the way it always would. A workflow taking a single struct argument takes
	// one entry.
	//
	// The CLI's file and base64 forms are deliberately absent. A CRD is not a
	// filesystem, and a payload that cannot be read in the spec cannot be
	// reviewed in a pull request.
	// +optional
	// +kubebuilder:validation:items:MinLength=1
	Input []string `json:"input,omitempty"`

	// memo is non-indexed information attached to each run, shown in Temporal's
	// UI. Each value is written as JSON, like input.
	//
	// This is the *workflow's* memo. The schedule has its own, at spec.memo.
	// +optional
	Memo map[string]string `json:"memo,omitempty"`

	// searchAttributes are indexed values attached to each run, so the runs a
	// schedule starts can be found by them.
	//
	// This is the *workflow's* search attributes. The schedule has its own, at
	// spec.searchAttributes.
	// +optional
	SearchAttributes []ScheduleSearchAttribute `json:"searchAttributes,omitempty"`

	// timeouts bound each run. Omitted fields take the namespace's defaults.
	// +optional
	Timeouts *ScheduleWorkflowTimeouts `json:"timeouts,omitempty"`

	// priority controls how this workflow's tasks are ordered against others on
	// the same task queue.
	// +optional
	Priority *SchedulePriority `json:"priority,omitempty"`

	// staticSummary is a one-line description of each run, shown in Temporal's
	// UI. Single-line Temporal Markdown.
	// +optional
	StaticSummary string `json:"staticSummary,omitempty"`

	// staticDetails is a longer description of each run, shown in Temporal's UI.
	// Temporal Markdown, and may span multiple lines.
	// +optional
	StaticDetails string `json:"staticDetails,omitempty"`
}

// ScheduleAction is what a schedule does when it acts.
//
// Temporal models this as a union with exactly one member today - starting a
// workflow - so it is nested rather than flattened, leaving room for whatever
// Temporal adds without the spec having to be rearranged.
//
// Unlike the schedule-level fields, the action is **fully managed**: it
// describes the workflow to start, so omitting input or a memo means starting
// the workflow without them, not leaving whatever was there. Fields this API
// does not model at all - a retry policy, a header, a versioning override - are
// a different matter, and are preserved untouched.
type ScheduleAction struct {
	// workflow starts a workflow execution.
	// +required
	Workflow ScheduleWorkflow `json:"workflow"`
}

// SchedulePolicies control how a schedule behaves around failures and overlaps.
type SchedulePolicies struct {
	// overlap decides what happens when an action would start while an earlier
	// one is still running. Defaults to Skip, which is Temporal's default.
	//
	// This can be changed on a running schedule, though changing it while
	// actions are buffered can produce surprising results: in general the later
	// policy wins.
	// +optional
	// +kubebuilder:default=Skip
	Overlap ScheduleOverlapPolicy `json:"overlap,omitempty"`

	// catchupWindow is how late an action may be taken when the Temporal Service
	// was unavailable at the time it should have run.
	//
	// Omitted, it is not managed: the Service applies its own default of one
	// year, stores it, and reports it back, so the operator leaves whatever is
	// there alone rather than fighting a value it did not set.
	//
	// The minimum is ten seconds. A shorter window is silently raised to ten
	// seconds by the Service, so this API refuses it instead - a spec that does
	// not describe what happens is worse than a rejected one.
	// +optional
	CatchupWindow *Duration `json:"catchupWindow,omitempty"`

	// pauseOnFailure pauses the whole schedule when an action fails or times
	// out, after its retry policy is exhausted.
	//
	// With overlap AllowAll the pause may not stop the next action, because that
	// one may already have started.
	// +optional
	PauseOnFailure bool `json:"pauseOnFailure,omitempty"`
}

// OverlapValue returns the requested overlap policy, falling back to Skip when
// unset.
//
// This is the one place the policy is resolved, so an object built in memory and
// one defaulted by the API server are read the same way.
func (p *SchedulePolicies) OverlapValue() ScheduleOverlapPolicy {
	if p == nil || p.Overlap == "" {
		return ScheduleOverlapPolicySkip
	}

	return p.Overlap
}

// ScheduleStateSpec is the part of a Temporal schedule's state this resource can
// declare.
//
// Temporal treats state as operational rather than declarative: a person can
// pause a schedule from the UI, and pauseOnFailure pauses it without anyone
// asking. So each field here is managed only when it is present, and left alone
// otherwise.
//
// Only one field qualifies. Temporal's remaining-action count is deliberately
// absent, and should not be added: it is a counter the Service consumes, ticking
// 10, 9, 8 as the schedule acts. Reconciling it would mean writing 10 back every
// time the operator looked, so a schedule asked to run ten times would run for
// ever. Pausing is different - it is a state, not a budget, and it stays where
// it is put until somebody moves it, which is exactly what reconciliation is
// for. The counter is still preserved on every update; it is just not something
// this API lets you declare.
type ScheduleStateSpec struct {
	// paused decides whether the schedule is paused.
	//
	// **Omitted, pausing is not managed.** That is the point of the pointer: a
	// person can pause a schedule to stop it while they investigate, and the
	// operator will not undo that on its next resync. Temporal itself pauses
	// schedules too, when pauseOnFailure fires.
	//
	// Set it and pausing becomes managed state like anything else: true keeps
	// the schedule paused, false keeps it running, and a change made behind the
	// operator's back is put back.
	// +optional
	Paused *bool `json:"paused,omitempty"`
}

// ScheduleSpec defines the desired state of Schedule
//
// scheduleId, connectionRef and namespaceRef say which real schedule, on which
// Temporal Service, this resource stands for, and are immutable: changing any of
// them would not move anything, it would point the resource at a different
// schedule and strand whatever it was looking after.
type ScheduleSpec struct {
	// scheduleId is the Temporal schedule's ID, exactly as Temporal knows it -
	// "PaymentsNightly", not "payments-nightly".
	//
	// This is separate from the resource's own metadata.name because Kubernetes
	// only accepts lowercase names for a resource, while schedule IDs are
	// conventionally PascalCase. Nothing is derived from metadata.name.
	//
	// The length limit is Temporal's: the Service validates the ID with its
	// internal "temporal-sys-scheduler:" prefix already attached against a limit
	// that defaults to 1000 characters.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=977
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="scheduleId is immutable"
	ScheduleID string `json:"scheduleId"`

	// connectionRef references the Connection, in the same Kubernetes namespace,
	// describing the Temporal Service holding this schedule.
	// +required
	// +kubebuilder:validation:XValidation:rule="self.name != ''",message="connectionRef.name is required"
	// +kubebuilder:validation:XValidation:rule="self.name == oldSelf.name",message="connectionRef.name is immutable"
	//

	ConnectionRef corev1.LocalObjectReference `json:"connectionRef"`

	// namespaceRef references the Namespace, in the same Kubernetes namespace,
	// whose Temporal namespace holds this schedule.
	//
	// A Namespace's own metadata.name is its Temporal namespace name, so this is
	// also the Temporal namespace the schedule lives in. Finalisation can
	// therefore find the schedule without the Namespace resource still being
	// around to ask.
	// +required
	// +kubebuilder:validation:XValidation:rule="self.name != ''",message="namespaceRef.name is required"
	// +kubebuilder:validation:XValidation:rule="self.name == oldSelf.name",message="namespaceRef.name is immutable"
	//

	NamespaceRef corev1.LocalObjectReference `json:"namespaceRef"`

	// schedule says when the schedule acts.
	// +required
	Schedule ScheduleTiming `json:"schedule"`

	// action says what the schedule does when it acts.
	// +required
	Action ScheduleAction `json:"action"`

	// policies control how the schedule behaves around overlaps and failures.
	// +optional
	Policies *SchedulePolicies `json:"policies,omitempty"`

	// state declares the parts of Temporal's own schedule state this resource
	// manages. Each is managed only when present.
	// +optional
	State *ScheduleStateSpec `json:"state,omitempty"`

	// notes is a human-readable note stored on the schedule, shown in Temporal's
	// UI - why it exists, who owns it, why it is paused.
	//
	// Omitting it leaves whatever note is there alone, which matters because
	// **Temporal overwrites this field itself**: pausing a schedule from the UI
	// or through pauseOnFailure replaces the note with its own explanation.
	// Declaring a note means the operator will put it back, erasing Temporal's
	// explanation of why the schedule paused.
	// +optional
	Notes *string `json:"notes,omitempty"`

	// memo is non-indexed information attached to the schedule itself, shown
	// when listing schedules. Each value is written as JSON.
	//
	// This is the *schedule's* memo, not the workflow's - that one is at
	// spec.action.workflow.memo.
	// +optional
	Memo map[string]string `json:"memo,omitempty"`

	// searchAttributes are indexed values attached to the schedule itself, so
	// schedules can be found by them.
	//
	// This is the *schedule's* search attributes, not the workflow's - those are
	// at spec.action.workflow.searchAttributes.
	// +optional
	SearchAttributes []ScheduleSearchAttribute `json:"searchAttributes,omitempty"`

	// deletionPolicy decides what happens to the Temporal schedule when this
	// resource is deleted. Defaults to Delete.
	//
	// Delete removes the schedule along with the resource, where the operator
	// created it. Orphan leaves it behind and contacts Temporal not at all,
	// which is how a resource is released when the Connection it would have
	// needed no longer exists.
	//
	// This may be changed while the resource is being deleted, which is the
	// supported way out of a deletion blocked on a missing Connection.
	// +optional
	// +kubebuilder:default=Delete
	DeletionPolicy ScheduleDeletionPolicy `json:"deletionPolicy,omitempty"`
}

// TemporalName returns the ID of the Temporal schedule this resource stands for.
//
// This is the one place the mapping lives. It is deliberately not derived from
// the resource's metadata.name, which is Kubernetes identity and nothing more.
func (s *ScheduleSpec) TemporalName() string {
	return s.ScheduleID
}

// TemporalNamespace returns the Temporal namespace the schedule lives in.
//
// This comes from the reference rather than from the Namespace resource itself,
// because a Namespace's Temporal name is its own metadata.name.
func (s *ScheduleSpec) TemporalNamespace() string {
	return s.NamespaceRef.Name
}

// DeletionPolicyValue returns the requested deletion policy, falling back to
// DefaultScheduleDeletionPolicy when it is unset.
//
// Unlike the fields identifying the external schedule, this one is mutable -
// deliberately, because switching a stuck resource to Orphan while it is already
// terminating is how a deletion blocked on a broken dependency is released.
func (s *ScheduleSpec) DeletionPolicyValue() ScheduleDeletionPolicy {
	if s.DeletionPolicy == "" {
		return DefaultScheduleDeletionPolicy
	}

	return s.DeletionPolicy
}

// ScheduleStatus defines the observed state of Schedule.
type ScheduleStatus struct {
	// conditions represent the current state of the Schedule resource.
	//
	// Known condition types:
	// - "Ready": the schedule exists on the referenced Temporal namespace and
	//   its declared configuration matches the spec
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// observedGeneration is the metadata.generation of the Schedule that was
	// last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ownership records whether the operator created the Temporal schedule or
	// adopted one that already existed, and therefore whether deleting this
	// resource removes it.
	//
	// "Creating" is a transient marker written immediately before creating a
	// schedule; it settles to "Created" once creation is confirmed.
	// +optional
	Ownership ScheduleOwnership `json:"ownership,omitempty"`

	// desiredSpecHash fingerprints the timing specification this resource last
	// asked Temporal for.
	//
	// It exists because Temporal does not give back what it was given: cron
	// expressions and the legacy calendar form are compiled into structured
	// calendars on the way in, and the originals are discarded. There is
	// therefore nothing on the Service to compare a cron expression against.
	//
	// Comparing this against the current spec answers "has the resource changed
	// since we last wrote it", which is half of what drift detection needs.
	// +optional
	DesiredSpecHash string `json:"desiredSpecHash,omitempty"`

	// appliedSpecHash fingerprints the timing specification Temporal reported
	// immediately after the operator last wrote it.
	//
	// Comparing this against what Temporal reports now answers the other half:
	// "has anyone changed it on the Service since". Together the two mean the
	// operator writes when something has actually changed, and stays quiet
	// otherwise - rather than rewriting the schedule on every resync because
	// cron cannot be compared.
	// +optional
	AppliedSpecHash string `json:"appliedSpecHash,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=tsc
// +kubebuilder:printcolumn:name="Schedule ID",type=string,JSONPath=".spec.scheduleId"
// +kubebuilder:printcolumn:name="Temporal NS",type=string,JSONPath=".spec.namespaceRef.name"
// +kubebuilder:printcolumn:name="Workflow",type=string,JSONPath=".spec.action.workflow.type"
// +kubebuilder:printcolumn:name="Ownership",type=string,JSONPath=".status.ownership"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Task Queue",type=string,JSONPath=".spec.action.workflow.taskQueue",priority=1
// +kubebuilder:printcolumn:name="Paused",type=boolean,JSONPath=".spec.state.paused",priority=1
// +kubebuilder:printcolumn:name="Time Zone",type=string,JSONPath=".spec.schedule.timeZone",priority=1

// Schedule is the Schema for the schedules API.
//
// One resource manages exactly one Temporal schedule. Which one is
// spec.scheduleId; metadata.name is Kubernetes identity and is never sent to
// Temporal. The two are separate because Kubernetes only accepts lowercase names
// for a resource, and that is not something a CRD can relax, whereas schedule
// IDs are conventionally PascalCase.
//
// The timing specification is deliberately not a printer column. A schedule's
// timing is a set of calendars, intervals and cron expressions, and none of it
// fits usefully in a column.
type Schedule struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Schedule
	// +required
	Spec ScheduleSpec `json:"spec"`

	// status defines the observed state of Schedule
	// +optional
	Status ScheduleStatus `json:"status,omitzero"`
}

// TemporalName returns the ID of the Temporal schedule this resource manages.
// See ScheduleSpec.TemporalName.
func (s *Schedule) TemporalName() string {
	return s.Spec.TemporalName()
}

// TemporalNamespace returns the Temporal namespace the schedule lives in. See
// ScheduleSpec.TemporalNamespace.
func (s *Schedule) TemporalNamespace() string {
	return s.Spec.TemporalNamespace()
}

// +kubebuilder:object:root=true

// ScheduleList contains a list of Schedule
type ScheduleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Schedule `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Schedule{}, &ScheduleList{})
		return nil
	})
}
