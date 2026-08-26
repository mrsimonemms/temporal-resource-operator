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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// This is the second of the three layers a Schedule is checked by. The CRD
// rejects what OpenAPI and CEL can express - structure, integer ranges, enums,
// string lengths, whether a timestamp is after another. This file handles what
// they cannot: durations, whose syntax the CRD checks but whose values it
// cannot compare; cron expressions, which need a parser; and IANA time zones,
// which need the zone database. The Temporal Service remains the third and
// final authority.
//
// None of this runs during decoding, and that is deliberate. A Schedule already
// stored in Kubernetes has to decode even if it is nonsense: a List decodes as
// one response, so one undecodable item would fail the whole call and poison
// the informer behind the controller - the outage that made Duration a custom
// type in the first place. So decoding always succeeds, and the controller asks
// for validation before it acts.

// Temporal's own bounds, from the pinned Service. Each is the value the Service
// enforces rather than a number chosen here.
const (
	// MinScheduleInterval is the shortest interval the Service accepts. Its
	// validateInterval refuses anything under a second.
	MinScheduleInterval = time.Second

	// MinCatchupWindow is the shortest catch-up window the Service honours.
	//
	// The Service does not refuse a shorter one, it silently raises it to this.
	// A spec that does not describe what actually happens is worse than a
	// rejected one, so this is enforced as a minimum here.
	MinCatchupWindow = 10 * time.Second
)

// Fairness weight bounds. Temporal clamps to this range rather than refusing
// values outside it, so, as with the catch-up window, the value is refused here
// instead of being silently changed.
const (
	MinFairnessWeight = 0.001
	MaxFairnessWeight = 1000.0
)

// Errors reported by Schedule validation. They are sentinels so that a caller -
// and a test - can tell one kind of invalid schedule from another without
// matching on message text.
var (
	// ErrInvalidScheduleSpec reports that a Schedule's desired state cannot be
	// sent to Temporal. Every error from Validate wraps it.
	ErrInvalidScheduleSpec = errors.New("invalid schedule spec")

	// ErrNoScheduleRule reports that the timing specification would never match
	// any time.
	ErrNoScheduleRule = errors.New("no calendars, intervals or cron expressions given")

	// ErrInvalidCron reports a cron expression Temporal would not accept.
	ErrInvalidCron = errors.New("invalid cron expression")

	// ErrUnknownTimeZone reports a time zone name that is not in the IANA
	// database.
	ErrUnknownTimeZone = errors.New("unknown time zone")
)

// Validate reports whether this Schedule's desired state can be sent to
// Temporal.
//
// It is deliberately deterministic and offline: nothing here needs a Connection,
// a Temporal Service or the network, so an unusable spec is refused before the
// operator dials anything. Every error wraps ErrInvalidScheduleSpec.
func (s *ScheduleSpec) Validate() error {
	if err := s.Schedule.validate(); err != nil {
		return fmt.Errorf("%w: schedule: %w", ErrInvalidScheduleSpec, err)
	}

	if err := s.Action.Workflow.validate(); err != nil {
		return fmt.Errorf("%w: action.workflow: %w", ErrInvalidScheduleSpec, err)
	}

	if err := s.Policies.validate(); err != nil {
		return fmt.Errorf("%w: policies: %w", ErrInvalidScheduleSpec, err)
	}

	if err := validateJSONMap("memo", s.Memo); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidScheduleSpec, err)
	}

	if err := validateSearchAttributes("searchAttributes", s.SearchAttributes); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidScheduleSpec, err)
	}

	return nil
}

// validate checks the timing specification.
func (t *ScheduleTiming) validate() error {
	if len(t.Calendars) == 0 && len(t.Intervals) == 0 && len(t.Cron) == 0 {
		// The CRD says this too, so reaching it means an object stored before
		// the rule existed, or one written past admission.
		return ErrNoScheduleRule
	}

	if err := t.validateRules(); err != nil {
		return err
	}

	return t.validateBounds()
}

// validateRules checks each timing rule: the intervals, the cron expressions and
// the calendars, exclusions included.
func (t *ScheduleTiming) validateRules() error {
	for i := range t.Intervals {
		if err := t.Intervals[i].validate(); err != nil {
			return fmt.Errorf("intervals[%d]: %w", i, err)
		}
	}

	for i, expression := range t.Cron {
		if err := ValidateCronExpression(expression); err != nil {
			return fmt.Errorf("cron[%d]: %w", i, err)
		}
	}

	for i := range t.Calendars {
		if err := t.Calendars[i].validate(); err != nil {
			return fmt.Errorf("calendars[%d]: %w", i, err)
		}
	}

	for i := range t.ExcludeCalendars {
		if err := t.ExcludeCalendars[i].validate(); err != nil {
			return fmt.Errorf("excludeCalendars[%d]: %w", i, err)
		}
	}

	return nil
}

// validateBounds checks the settings that apply to the specification as a whole
// rather than to any one rule.
func (t *ScheduleTiming) validateBounds() error {
	if t.Jitter != nil && t.Jitter.Duration < 0 {
		return fmt.Errorf("jitter %s is negative", t.Jitter.Duration)
	}

	if t.StartAt != nil && t.EndAt != nil && !t.EndAt.After(t.StartAt.Time) {
		return fmt.Errorf("endAt %s is not after startAt %s",
			t.EndAt.Format(time.RFC3339), t.StartAt.Format(time.RFC3339))
	}

	if t.TimeZone == "" {
		return nil
	}

	// The zone database is why this cannot be a CRD rule. Note that loading it
	// here proves only that *this* process knows the zone: the Temporal Service
	// loads zones from its own environment, and is the authority on whether it
	// can.
	if _, err := time.LoadLocation(t.TimeZone); err != nil {
		return fmt.Errorf("%w %q: %w", ErrUnknownTimeZone, t.TimeZone, err)
	}

	return nil
}

// validate checks one interval against the Service's validateInterval.
func (i *ScheduleInterval) validate() error {
	every := i.Every.Duration

	if every < MinScheduleInterval {
		return fmt.Errorf("every %s is shorter than the minimum of %s", every, MinScheduleInterval)
	}

	if i.Offset == nil {
		return nil
	}

	offset := i.Offset.Duration

	switch {
	case offset < 0:
		return fmt.Errorf("offset %s is negative", offset)
	case offset >= every:
		// Temporal's check is "phase >= interval". An offset of a whole period
		// is the same as no offset at all, and the Service refuses it rather
		// than accepting something ambiguous.
		return fmt.Errorf("offset %s must be shorter than every %s", offset, every)
	}

	return nil
}

// validate checks one calendar's ranges.
//
// The CRD already bounds each field, so this repeats those bounds for an object
// that was stored before the rules existed or written past admission - and adds
// the one thing CEL cannot see: that a step is useless without a range to step
// through.
func (c *ScheduleCalendar) validate() error {
	fields := []struct {
		name     string
		ranges   []ScheduleRange
		min, max int32
	}{
		{"second", c.Second, 0, 59},
		{"minute", c.Minute, 0, 59},
		{"hour", c.Hour, 0, 23},
		{"dayOfMonth", c.DayOfMonth, 1, 31},
		{"month", c.Month, 1, 12},
		{"year", c.Year, MinCalendarYear, MaxCalendarYear},
		{"dayOfWeek", c.DayOfWeek, 0, 6},
	}

	for _, field := range fields {
		for i, r := range field.ranges {
			if err := r.validate(field.min, field.max); err != nil {
				return fmt.Errorf("%s[%d]: %w", field.name, i, err)
			}
		}
	}

	if len(c.Comment) > MaxCalendarCommentLength {
		return fmt.Errorf("comment is longer than %d characters", MaxCalendarCommentLength)
	}

	return nil
}

// validate checks one range against the bounds of the field holding it.
func (r ScheduleRange) validate(minVal, maxVal int32) error {
	if r.Start < minVal || r.Start > maxVal {
		return fmt.Errorf("start %d is not between %d and %d", r.Start, minVal, maxVal)
	}

	end := r.EndValue()
	if end < r.Start || end > maxVal {
		return fmt.Errorf("end %d is not between start %d and %d", end, r.Start, maxVal)
	}

	if step := r.StepValue(); step < 1 {
		return fmt.Errorf("step %d is not at least 1", step)
	}

	return nil
}

// validate checks the workflow action.
func (w *ScheduleWorkflow) validate() error {
	if w.Type == "" {
		return errors.New("type is required")
	}

	if w.TaskQueue == "" {
		return errors.New("taskQueue is required")
	}

	for i, arg := range w.Input {
		if !json.Valid([]byte(arg)) {
			return fmt.Errorf("input[%d] is not valid JSON", i)
		}
	}

	if err := validateJSONMap("memo", w.Memo); err != nil {
		return err
	}

	if err := validateSearchAttributes("searchAttributes", w.SearchAttributes); err != nil {
		return err
	}

	if err := w.Timeouts.validate(); err != nil {
		return fmt.Errorf("timeouts: %w", err)
	}

	if err := w.Priority.validate(); err != nil {
		return fmt.Errorf("priority: %w", err)
	}

	return nil
}

// validate checks the workflow timeouts. Temporal refuses a negative timeout,
// and treats zero as "unset" - which is what omitting the field already means,
// so a zero written out is a mistake worth naming.
func (t *ScheduleWorkflowTimeouts) validate() error {
	if t == nil {
		return nil
	}

	for _, timeout := range []struct {
		name  string
		value *Duration
	}{
		{"task", t.Task},
		{"run", t.Run},
		{"execution", t.Execution},
	} {
		if timeout.value == nil {
			continue
		}

		if timeout.value.Duration <= 0 {
			return fmt.Errorf("%s %s must be positive; omit it to take the namespace default",
				timeout.name, timeout.value.Duration)
		}
	}

	return nil
}

// validate checks the priority settings.
func (p *SchedulePriority) validate() error {
	if p == nil {
		return nil
	}

	if len(p.FairnessKey) > MaxFairnessKeyBytes {
		// The CRD says this too, in bytes. Repeated here for an object that
		// predates the rule.
		return fmt.Errorf("fairnessKey is longer than %d bytes", MaxFairnessKeyBytes)
	}

	if p.FairnessWeight == "" {
		return nil
	}

	weight, err := strconv.ParseFloat(p.FairnessWeight, 32)
	if err != nil {
		return fmt.Errorf("fairnessWeight %q is not a number: %w", p.FairnessWeight, err)
	}

	if weight < MinFairnessWeight || weight > MaxFairnessWeight {
		return fmt.Errorf("fairnessWeight %s is not between %g and %g",
			p.FairnessWeight, MinFairnessWeight, MaxFairnessWeight)
	}

	return nil
}

// validate checks the policies.
func (p *SchedulePolicies) validate() error {
	if p == nil || p.CatchupWindow == nil {
		return nil
	}

	if window := p.CatchupWindow.Duration; window < MinCatchupWindow {
		return fmt.Errorf("catchupWindow %s is shorter than the minimum of %s", window, MinCatchupWindow)
	}

	return nil
}

// validateJSONMap checks that every value in a memo is valid JSON, since each
// one is encoded as a JSON payload.
func validateJSONMap(field string, values map[string]string) error {
	for key, value := range values {
		if key == "" {
			return fmt.Errorf("%s has an empty key", field)
		}

		if !json.Valid([]byte(value)) {
			return fmt.Errorf("%s[%q] is not valid JSON", field, key)
		}
	}

	return nil
}

// validateSearchAttributes checks that every search attribute's value can be
// read as the type it claims to be.
//
// This is where a typed search attribute earns the type in its spec: the value
// is JSON, and "7" is an Int or a Text depending entirely on what the attribute
// was registered as. Decoding it here means a mistyped value is refused before
// Temporal is asked.
func validateSearchAttributes(field string, attributes []ScheduleSearchAttribute) error {
	seen := make(map[string]struct{}, len(attributes))

	for i, attribute := range attributes {
		if attribute.Name == "" {
			return fmt.Errorf("%s[%d]: name is required", field, i)
		}

		if _, duplicate := seen[attribute.Name]; duplicate {
			return fmt.Errorf("%s[%d]: %q appears more than once", field, i, attribute.Name)
		}

		seen[attribute.Name] = struct{}{}

		if _, err := attribute.DecodeValue(); err != nil {
			return fmt.Errorf("%s[%d] (%s): %w", field, i, attribute.Name, err)
		}
	}

	return nil
}

// DecodeValue reads the search attribute's JSON value as the Go type its
// declared Temporal type calls for.
//
// The returned value is what Temporal's search attribute encoding expects for
// that type, so callers do not have to know the mapping. A KeywordList becomes
// a []string, a Datetime becomes a time.Time, and so on.
func (a ScheduleSearchAttribute) DecodeValue() (any, error) {
	switch a.Type {
	case SearchAttributeTypeBool:
		return decodeJSONValue[bool](a.Value)
	case SearchAttributeTypeInt:
		return decodeJSONValue[int64](a.Value)
	case SearchAttributeTypeDouble:
		return decodeJSONValue[float64](a.Value)
	case SearchAttributeTypeKeyword, SearchAttributeTypeText:
		return decodeJSONValue[string](a.Value)
	case SearchAttributeTypeKeywordList:
		return decodeJSONValue[[]string](a.Value)
	case SearchAttributeTypeDatetime:
		return decodeJSONValue[time.Time](a.Value)
	default:
		return nil, fmt.Errorf("unknown search attribute type %q", a.Type)
	}
}

// decodeJSONValue reads a JSON document as one specific Go type, rejecting
// anything that does not fit rather than coercing it.
func decodeJSONValue[T any](raw string) (any, error) {
	var value T

	decoder := json.NewDecoder(strings.NewReader(raw))
	// Refuse "1 2" and other trailing rubbish, which json.Unmarshal on its own
	// would not: a value with something after it is not the value it looks like.
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("%q is not a valid %T: %w", raw, value, err)
	}

	if decoder.More() {
		return nil, fmt.Errorf("%q has more than one JSON value", raw)
	}

	return value, nil
}

// ValidateCronExpression reports whether Temporal would accept a cron
// expression.
//
// It implements the Service's own grammar - parseCronString and the makeRange it
// calls - rather than borrowing a general-purpose cron library, because the
// general-purpose ones differ from Temporal in ways that matter: Temporal takes
// 5, 6 or 7 fields where most take 5; it has its own set of shorthands; "@every"
// compiles to an interval rather than a calendar; and day-of-week accepts 0
// through 7 in text form even though the structured form stops at 6.
//
// It validates without compiling. The Service compiles the expression itself, so
// producing structured calendars here would be duplicated work and a second
// thing to keep in step; what is needed is that an expression the Service will
// refuse is refused before the operator dials it.
func ValidateCronExpression(expression string) error {
	c := strings.TrimSpace(expression)
	if c == "" {
		return fmt.Errorf("%w: expression is empty", ErrInvalidCron)
	}

	// A leading TZ= or CRON_TZ= sets the time zone for the expression.
	if strings.HasPrefix(c, "TZ=") || strings.HasPrefix(c, "CRON_TZ=") {
		zone, rest, found := strings.Cut(c, " ")
		if !found {
			return fmt.Errorf("%w: %q has a time zone but no fields", ErrInvalidCron, expression)
		}

		_, name, _ := strings.Cut(zone, "=")
		if name == "" {
			return fmt.Errorf("%w: %q has an empty time zone", ErrInvalidCron, expression)
		}

		if _, err := time.LoadLocation(name); err != nil {
			return fmt.Errorf("%w: %w %q", ErrInvalidCron, ErrUnknownTimeZone, name)
		}

		c = rest
	}

	// Anything after a "#" is a comment.
	c, _, _ = strings.Cut(c, "#")
	c = strings.TrimSpace(c)

	// "@every <interval>[/<phase>]" becomes an interval rather than a calendar.
	if strings.HasPrefix(c, "@every") {
		return validateCronInterval(expression, c)
	}

	c = expandCronShorthand(c)

	fields := strings.Fields(c)

	// The Service accepts 5, 6 or 7 fields, and reads them right-to-left from a
	// fixed order: the shorter forms leave off second and year.
	var second, minute, hour, dayOfMonth, month, dayOfWeek, year string

	switch len(fields) {
	case 5:
		minute, hour, dayOfMonth, month, dayOfWeek = fields[0], fields[1], fields[2], fields[3], fields[4]
		second, year = "0", "*"
	case 6:
		minute, hour, dayOfMonth, month, dayOfWeek, year =
			fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]
		second = "0"
	case 7:
		second, minute, hour, dayOfMonth, month, dayOfWeek, year =
			fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6]
	default:
		return fmt.Errorf("%w: %q does not have 5-7 fields", ErrInvalidCron, expression)
	}

	// The bounds and the name handling are the Service's, field for field. Note
	// that day-of-week runs to 7 here - "7" is another way of writing Sunday in
	// cron text, which the Service folds into 0 - even though the structured
	// calendar form stops at 6.
	for _, field := range []struct {
		name     string
		value    string
		min, max int
		names    []string
		offset   int
	}{
		{name: "second", value: second, min: 0, max: 59},
		{name: "minute", value: minute, min: 0, max: 59},
		{name: "hour", value: hour, min: 0, max: 23},
		{name: "dayOfMonth", value: dayOfMonth, min: 1, max: 31},
		{name: "month", value: month, min: 1, max: 12, names: cronMonthNames, offset: 1},
		{name: "dayOfWeek", value: dayOfWeek, min: 0, max: 7, names: cronDayNames},
		{name: "year", value: year, min: MinCalendarYear, max: MaxCalendarYear},
	} {
		if err := validateCronField(field.name, field.value, field.min, field.max, field.names, field.offset); err != nil {
			return fmt.Errorf("%w: %q: %w", ErrInvalidCron, expression, err)
		}
	}

	return nil
}

// validateCronInterval checks an "@every" expression.
func validateCronInterval(expression, c string) error {
	_, interval, found := strings.Cut(c, " ")
	if !found || strings.TrimSpace(interval) == "" {
		return fmt.Errorf("%w: %q has no interval after @every", ErrInvalidCron, expression)
	}

	every, phase, hasPhase := strings.Cut(interval, "/")

	everyDuration, err := ParseDuration(strings.TrimSpace(every))
	if err != nil {
		return fmt.Errorf("%w: %q: interval: %w", ErrInvalidCron, expression, err)
	}

	if everyDuration < MinScheduleInterval {
		return fmt.Errorf("%w: %q: interval %s is shorter than the minimum of %s",
			ErrInvalidCron, expression, everyDuration, MinScheduleInterval)
	}

	if !hasPhase {
		return nil
	}

	phaseDuration, err := ParseDuration(strings.TrimSpace(phase))
	if err != nil {
		return fmt.Errorf("%w: %q: phase: %w", ErrInvalidCron, expression, err)
	}

	switch {
	case phaseDuration < 0:
		return fmt.Errorf("%w: %q: phase %s is negative", ErrInvalidCron, expression, phaseDuration)
	case phaseDuration >= everyDuration:
		return fmt.Errorf("%w: %q: phase %s must be shorter than the interval %s",
			ErrInvalidCron, expression, phaseDuration, everyDuration)
	}

	return nil
}

// cronShorthands are the Service's predefined expressions, expanded exactly as
// handlePredefinedCronStrings expands them.
var cronShorthands = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

// expandCronShorthand replaces a shorthand with the expression it stands for,
// leaving anything else alone.
func expandCronShorthand(c string) string {
	if expanded, ok := cronShorthands[c]; ok {
		return expanded
	}

	return c
}

// The name tables the Service matches against. A month is matched on at least
// three letters and a day on at least two, by prefix, case-insensitively.
var (
	cronMonthNames = []string{
		"january", "february", "march", "april", "may", "june",
		"july", "august", "september", "october", "november", "december",
	}

	cronDayNames = []string{
		"sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
	}
)

// minCronNamePrefix is how many letters of a name have to be given before the
// Service will match it: three for a month, two for a day of the week.
func minCronNamePrefix(names []string) int {
	if len(names) == 12 {
		return 3
	}

	return 2
}

// validateCronField checks one field of a cron expression.
//
// The grammar is the Service's makeRange: comma-separated parts, each of which
// is "*", a value, a range "x-z", or either with a "/step" suffix. offset is 1
// for month, whose names are one-based.
func validateCronField(field, value string, minVal, maxVal int, names []string, offset int) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("%s is empty", field)
	}

	for part := range strings.SplitSeq(value, ",") {
		if err := validateCronPart(field, part, minVal, maxVal, names, offset); err != nil {
			return err
		}
	}

	return nil
}

// validateCronPart checks one comma-separated part of a cron field.
func validateCronPart(field, part string, minVal, maxVal int, names []string, offset int) error {
	if strings.Count(part, "/") > 1 {
		return fmt.Errorf("%s has too many slashes", field)
	}

	if before, after, hasStep := strings.Cut(part, "/"); hasStep {
		if after == "" {
			return fmt.Errorf("%s is missing a step value", field)
		}

		step, err := strconv.Atoi(after)
		if err != nil {
			return fmt.Errorf("%s has a non-numeric step %q", field, after)
		}

		if step < 1 {
			return fmt.Errorf("%s has an invalid step %d", field, step)
		}

		part = before
	}

	if part == "*" {
		return nil
	}

	if strings.Contains(part, "-") {
		if strings.Count(part, "-") > 1 {
			// No negative numbers appear in the grammar, so more than one dash
			// is always a mistake rather than a signed value.
			return fmt.Errorf("%s has too many dashes", field)
		}

		start, end, _ := strings.Cut(part, "-")

		startValue, err := parseCronValue(field, start, minVal, maxVal, names, offset)
		if err != nil {
			return err
		}

		endValue, err := parseCronValue(field, end, minVal, maxVal, names, offset)
		if err != nil {
			return err
		}

		if endValue < startValue {
			return fmt.Errorf("%s end %d is before start %d", field, endValue, startValue)
		}

		return nil
	}

	_, err := parseCronValue(field, part, minVal, maxVal, names, offset)

	return err
}

// parseCronValue reads one value of a cron field, which may be a number or a
// month or day name.
func parseCronValue(field, value string, minVal, maxVal int, names []string, offset int) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("%s has an empty value", field)
	}

	if len(names) > 0 && len(value) >= minCronNamePrefix(names) {
		lowered := strings.ToLower(value)

		for i, name := range names {
			if strings.HasPrefix(name, lowered) {
				number := i + offset
				if number < minVal || number > maxVal {
					return 0, fmt.Errorf("%s %q is not between %d and %d", field, value, minVal, maxVal)
				}

				return number, nil
			}
		}
	}

	number, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s value %q is not a number", field, value)
	}

	if number < minVal || number > maxVal {
		return 0, fmt.Errorf("%s %d is not between %d and %d", field, number, minVal, maxVal)
	}

	return number, nil
}
