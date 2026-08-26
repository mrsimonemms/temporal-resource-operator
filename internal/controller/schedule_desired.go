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
	"fmt"
	"strconv"
	"time"

	temporalv1beta1 "github.com/mrsimonemms/temporal-resource-operator/api/v1beta1"
	"github.com/mrsimonemms/temporal-resource-operator/internal/temporal"
)

// Turning a Schedule resource into the desired state the Temporal wrapper takes.
//
// This is the seam between the two: the API package knows nothing about
// Temporal, and the wrapper knows nothing about Kubernetes, so the translation
// lives here with the controller that needs both. It is deliberately total -
// every field the CRD exposes is mapped, and anything it cannot map is an error
// rather than a silently dropped value.

// desiredSchedule turns a Schedule resource into the desired state to send.
//
// It is the last piece of validation as well as a translation: decoding the
// search attribute values and parsing the fairness weight can fail, and it is
// better to find that out here, before dialling anything, than as a rejected
// RPC.
func desiredSchedule(schedule *temporalv1beta1.Schedule) (*temporal.ScheduleDesired, error) {
	spec := &schedule.Spec

	workflow, err := desiredWorkflow(&spec.Action.Workflow)
	if err != nil {
		return nil, fmt.Errorf("action.workflow: %w", err)
	}

	memo, err := desiredMemo(spec.Memo)
	if err != nil {
		return nil, fmt.Errorf("memo: %w", err)
	}

	searchAttributes, err := desiredSearchAttributes(spec.SearchAttributes)
	if err != nil {
		return nil, fmt.Errorf("searchAttributes: %w", err)
	}

	return &temporal.ScheduleDesired{
		ID:               spec.TemporalName(),
		Timing:           desiredTiming(&spec.Schedule),
		Workflow:         *workflow,
		Policies:         desiredPolicies(spec.Policies),
		State:            desiredState(spec.State, spec.Notes),
		Memo:             memo,
		SearchAttributes: searchAttributes,
	}, nil
}

// desiredTiming maps the timing specification.
func desiredTiming(timing *temporalv1beta1.ScheduleTiming) temporal.ScheduleTiming {
	desired := temporal.ScheduleTiming{
		TimeZone: timing.TimeZone,
	}

	if len(timing.Cron) > 0 {
		desired.Cron = append(desired.Cron, timing.Cron...)
	}

	for i := range timing.Calendars {
		desired.Calendars = append(desired.Calendars, desiredCalendar(&timing.Calendars[i]))
	}

	for i := range timing.ExcludeCalendars {
		desired.ExcludeCalendars = append(desired.ExcludeCalendars, desiredCalendar(&timing.ExcludeCalendars[i]))
	}

	for _, interval := range timing.Intervals {
		desired.Intervals = append(desired.Intervals, temporal.ScheduleInterval{
			Every:  interval.Every.Duration,
			Offset: optionalDurationValue(interval.Offset),
		})
	}

	if timing.StartAt != nil {
		startAt := timing.StartAt.Time
		desired.StartAt = &startAt
	}

	if timing.EndAt != nil {
		endAt := timing.EndAt.Time
		desired.EndAt = &endAt
	}

	if timing.Jitter != nil {
		jitter := timing.Jitter.Duration
		desired.Jitter = &jitter
	}

	return desired
}

// desiredCalendar maps one calendar.
func desiredCalendar(calendar *temporalv1beta1.ScheduleCalendar) temporal.ScheduleCalendar {
	return temporal.ScheduleCalendar{
		Second:     desiredRanges(calendar.Second),
		Minute:     desiredRanges(calendar.Minute),
		Hour:       desiredRanges(calendar.Hour),
		DayOfMonth: desiredRanges(calendar.DayOfMonth),
		Month:      desiredRanges(calendar.Month),
		Year:       desiredRanges(calendar.Year),
		DayOfWeek:  desiredRanges(calendar.DayOfWeek),
		Comment:    calendar.Comment,
	}
}

// desiredRanges maps one calendar field's ranges, resolving the API's optional
// end and step into the values Temporal's own defaults would produce.
//
// Resolving them here rather than passing the absence along keeps the request
// stable: a range written as "start alone" and one written with an explicit
// equal end are the same schedule, and hashing them the same way means editing
// the YAML into the other form is not read as drift.
func desiredRanges[T temporalv1beta1.ScheduleCalendarRange](ranges []T) []temporal.ScheduleRange {
	if len(ranges) == 0 {
		return nil
	}

	desired := make([]temporal.ScheduleRange, 0, len(ranges))
	for _, r := range ranges {
		desired = append(desired, temporal.ScheduleRange{
			Start: r.StartValue(),
			End:   r.EndValue(),
			Step:  r.StepValue(),
		})
	}

	return desired
}

// desiredWorkflow maps the workflow action.
func desiredWorkflow(workflow *temporalv1beta1.ScheduleWorkflow) (*temporal.ScheduleWorkflow, error) {
	memo, err := desiredMemo(workflow.Memo)
	if err != nil {
		return nil, fmt.Errorf("memo: %w", err)
	}

	searchAttributes, err := desiredSearchAttributes(workflow.SearchAttributes)
	if err != nil {
		return nil, fmt.Errorf("searchAttributes: %w", err)
	}

	priority, err := desiredPriority(workflow.Priority)
	if err != nil {
		return nil, fmt.Errorf("priority: %w", err)
	}

	desired := &temporal.ScheduleWorkflow{
		Type:             workflow.Type,
		TaskQueue:        workflow.TaskQueue,
		WorkflowID:       workflow.WorkflowID,
		Memo:             memo,
		SearchAttributes: searchAttributes,
		Priority:         priority,
		StaticSummary:    workflow.StaticSummary,
		StaticDetails:    workflow.StaticDetails,
	}

	if len(workflow.Input) > 0 {
		desired.Input = append(desired.Input, workflow.Input...)
	}

	if timeouts := workflow.Timeouts; timeouts != nil {
		desired.TaskTimeout = optionalDuration(timeouts.Task)
		desired.RunTimeout = optionalDuration(timeouts.Run)
		desired.ExecutionTimeout = optionalDuration(timeouts.Execution)
	}

	return desired, nil
}

// desiredPolicies maps the policies, applying the same defaults the CRD does so
// that an object built in memory behaves like one that has been through
// admission.
func desiredPolicies(policies *temporalv1beta1.SchedulePolicies) temporal.SchedulePolicies {
	desired := temporal.SchedulePolicies{
		Overlap: temporal.ScheduleOverlapPolicy(policies.OverlapValue()),
	}

	if policies == nil {
		return desired
	}

	desired.PauseOnFailure = policies.PauseOnFailure
	desired.CatchupWindow = optionalDuration(policies.CatchupWindow)

	return desired
}

// desiredState maps the declared parts of the schedule's state.
//
// Each field stays a pointer all the way through, because nil is the whole
// point: an undeclared field is left as the Service has it rather than being
// written back to some default.
func desiredState(state *temporalv1beta1.ScheduleStateSpec, notes *string) temporal.ScheduleStateSpec {
	desired := temporal.ScheduleStateSpec{Notes: notes}

	if state == nil {
		return desired
	}

	desired.Paused = state.Paused

	return desired
}

// desiredPriority maps the priority settings, parsing the fairness weight from
// the decimal string the API takes.
//
// The weight is a string in the CRD because Kubernetes APIs avoid floating point
// fields, and Temporal's weight is genuinely fractional - 0.001 to 1000. Parsing
// it here means a value that is not a number is refused before Temporal is
// asked.
func desiredPriority(priority *temporalv1beta1.SchedulePriority) (*temporal.SchedulePriority, error) {
	if priority == nil {
		return nil, nil
	}

	desired := &temporal.SchedulePriority{
		Key:         priority.Key,
		FairnessKey: priority.FairnessKey,
	}

	if priority.FairnessWeight == "" {
		return desired, nil
	}

	weight, err := strconv.ParseFloat(priority.FairnessWeight, 32)
	if err != nil {
		return nil, fmt.Errorf("fairnessWeight %q is not a number: %w", priority.FairnessWeight, err)
	}

	asFloat32 := float32(weight)
	desired.FairnessWeight = &asFloat32

	return desired, nil
}

// desiredMemo maps a memo, keeping the distinction between "not managed" and
// "managed, and empty".
//
// A nil map means the field was omitted, which every Temporal request field
// reads as "leave this alone". An empty map means it was written out as empty,
// which clears it. Copying rather than passing the spec's map along keeps the
// wrapper from holding a reference into an object the informer owns.
func desiredMemo(memo map[string]string) (map[string]string, error) {
	if memo == nil {
		return nil, nil
	}

	desired := make(map[string]string, len(memo))

	for key, value := range memo {
		if key == "" {
			return nil, fmt.Errorf("memo has an empty key")
		}

		desired[key] = value
	}

	return desired, nil
}

// desiredSearchAttributes maps search attributes, decoding each value into the
// Go type its declared Temporal type calls for.
//
// The decoding is what makes the types real. A search attribute's value is JSON
// in the spec, and "7" is an Int or a Text depending entirely on how the
// attribute was registered; decoding it against the declared type here means a
// mistyped value is a clear error rather than an attribute Temporal quietly
// fails to index.
//
// A nil slice means the field was omitted and the attributes are not managed; an
// empty one means they are managed and empty, which clears them.
func desiredSearchAttributes(
	attributes []temporalv1beta1.ScheduleSearchAttribute,
) ([]temporal.ScheduleSearchAttribute, error) {
	if attributes == nil {
		return nil, nil
	}

	desired := make([]temporal.ScheduleSearchAttribute, 0, len(attributes))

	for i, attribute := range attributes {
		value, err := attribute.DecodeValue()
		if err != nil {
			return nil, fmt.Errorf("[%d] (%s): %w", i, attribute.Name, err)
		}

		desired = append(desired, temporal.ScheduleSearchAttribute{
			Name:  attribute.Name,
			Type:  temporal.SearchAttributeType(attribute.Type),
			Value: value,
		})
	}

	return desired, nil
}

// optionalDuration unwraps a duration that may not have been given.
func optionalDuration(d *temporalv1beta1.Duration) *time.Duration {
	if d == nil {
		return nil
	}

	value := d.Duration

	return &value
}

// optionalDurationValue unwraps a duration that may not have been given, using
// zero for absent - which is what Temporal reads an unset interval offset as.
func optionalDurationValue(d *temporalv1beta1.Duration) time.Duration {
	if d == nil {
		return 0
	}

	return d.Duration
}
