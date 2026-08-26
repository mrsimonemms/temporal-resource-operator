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
	"time"

	schedulepb "go.temporal.io/api/schedule/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// FakeDefaultCatchupWindow is the catch-up window a Temporal Service fills in
// when a schedule does not ask for one. It matters to a fake because the
// Service *stores* it: a schedule created without a catch-up window comes back
// with this on it, and an operator that cleared the field would see drift on
// every reconcile.
const FakeDefaultCatchupWindow = 365 * 24 * time.Hour

// Building a Schedule without a Temporal Service.
//
// A Schedule holds the protobuf message Describe returned, unexported, because
// that is the point of the type: callers ask it questions rather than reaching
// into Temporal's generated types. That leaves other packages' tests unable to
// build one, and the Schedule controller's tests need to - they stand a fake
// Service up in place of the real one.
//
// So this is the seam, in the same spirit as the Connect function the
// reconcilers take: a deliberate, documented way in, used by tests and nothing
// else. It lives in its own file so that it is obvious what it is for.

// FakeScheduleOptions describes a schedule for a fake Service to hand back.
type FakeScheduleOptions struct {
	// ID is the schedule's ID.
	ID string

	// Namespace is the Temporal namespace holding it. It is recorded only so
	// that a fake can assert on it.
	Namespace string

	// Desired is the state the schedule was last written with. The action,
	// policies and state are built from it exactly as a create would, so drift
	// detection sees what it would see against a real Service.
	Desired *ScheduleDesired

	// TimingHash stands in for the canonical timing specification the Service
	// holds. Setting it to different values is how a fake reproduces somebody
	// editing the schedule's timing outside the operator.
	TimingHash string

	// Paused and Notes are the state the Service currently holds, which may not
	// be what Desired asked for: a person can pause a schedule, and Temporal
	// rewrites the note itself when pause-on-failure fires.
	Paused bool
	Notes  string

	// ConflictToken is the token a later update has to send back.
	ConflictToken []byte
}

// NewFakeSchedule builds a Schedule as a Temporal Service would have reported
// it. It is for tests in other packages; production code gets one from
// DescribeSchedule.
func NewFakeSchedule(opts *FakeScheduleOptions) *Schedule {
	desired := opts.Desired
	if desired == nil {
		desired = &ScheduleDesired{ID: opts.ID}
	}

	// Ignoring the error is right here: the desired state a test hands over has
	// already been built by the code under test, so it converts. A fake that
	// returned an error would only make every call site noisier.
	schedule, _ := buildSchedule(nil, desired)
	if schedule == nil {
		schedule = &schedulepb.Schedule{}
	}

	// Stand in for the Service having canonicalised the timing. The real one
	// compiles cron expressions and legacy calendars into structured calendars
	// and discards the originals, so what it reports back is never what it was
	// sent - which is exactly why the operator compares hashes rather than
	// specs. Carrying an opaque marker rather than the built spec keeps a fake
	// honest about that.
	schedule.Spec = &schedulepb.ScheduleSpec{
		StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{Comment: opts.TimingHash}},
	}

	// The Service defaults the catch-up window and stores it, so a fake that
	// handed back exactly what it was given would be lying about the one thing
	// hardest to get right.
	if schedule.Policies == nil {
		schedule.Policies = &schedulepb.SchedulePolicies{}
	}

	if schedule.Policies.CatchupWindow == nil {
		schedule.Policies.CatchupWindow = durationpb.New(FakeDefaultCatchupWindow)
	}

	if schedule.State == nil {
		schedule.State = &schedulepb.ScheduleState{}
	}

	schedule.State.Paused = opts.Paused
	schedule.State.Notes = opts.Notes

	memo, _ := buildMemo(desired.Memo)
	searchAttributes, _ := buildSearchAttributes(desired.SearchAttributes)

	return &Schedule{
		ID:               opts.ID,
		schedule:         schedule,
		memo:             memo,
		searchAttributes: searchAttributes,
		conflictToken:    opts.ConflictToken,
	}
}

// FakeScheduleToken returns the conflict token a Schedule was built or read
// with, so that a fake Service can check what an update sent back.
func FakeScheduleToken(schedule *Schedule) []byte {
	if schedule == nil {
		return nil
	}

	return schedule.conflictToken
}

// FakeCanonicalTiming produces a stable stand-in for the canonical timing the
// Service would hold for a desired specification.
//
// It deliberately is not DesiredTimingHash. The two have to differ, because the
// whole reason the operator records two hashes is that what it sends and what
// the Service reports back are not the same thing - and a fake that made them
// equal would let a naive direct comparison pass a test the real Service would
// fail.
func FakeCanonicalTiming(timing *ScheduleTiming) string {
	return "canonical:" + DesiredTimingHash(timing)
}
