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
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// Day is what a "d" unit stands for: exactly 24 hours.
//
// It is not a calendar day. This type knows nothing about time zones or
// daylight saving, so "1d" is 24 hours even across a clock change.
const Day = 24 * time.Hour

// DurationPattern is the syntax a Duration accepts, as a regular expression.
//
// It is Go's own duration syntax - a signed sequence of number/unit pairs, or a
// bare "0" - with one addition: the string may open with a whole number of days
// written as "<n>d". So "72h" and "1h30m" mean what they always did, while "7d"
// and "1d12h" now work too.
//
// This constant is the single source of truth. ParseDuration checks it before
// parsing, and the CRD carries it as the field's pattern, so the API server and
// the operator accept exactly the same strings. A value that reaches etcd is
// therefore always one the operator can decode - which matters because a single
// undecodable object breaks the deserialisation of an entire list response, and
// with it the informer that feeds the controller.
//
// The digit counts are deliberate. Kubernetes can only reject a value it can
// describe, and a regular expression cannot count across a whole string, so the
// components are bounded tightly enough that no accepted string can overflow
// the int64 nanoseconds a time.Duration holds. Together with MaxLength that
// caps an accepted value at a little over 2200000h, well inside the roughly
// 2562047h a time.Duration can represent, and far beyond any real retention.
const DurationPattern = `^[+-]?(0|(\d{1,4}d)?((\d{1,6}(\.\d{0,9})?|\.\d{1,9})(ns|us|µs|μs|ms|s|m|h))+|\d{1,4}d)$`

// DurationMaxLength bounds an accepted duration string. See DurationPattern for
// why the bound exists; 20 characters is many times longer than any duration
// worth writing.
const DurationMaxLength = 20

var (
	// durationRE is DurationPattern, compiled.
	durationRE = regexp.MustCompile(DurationPattern)

	// durationDaysRE splits an accepted string into its sign, its day count and
	// whatever follows. It is only ever applied to a string DurationPattern has
	// already accepted, so it does not need to police the syntax itself.
	durationDaysRE = regexp.MustCompile(`^([+-]?)(\d{1,4})d(.*)$`)
)

// Duration is a length of time, written as a string.
//
// It exists because metav1.Duration hands the string straight to
// time.ParseDuration, which has no notion of days: a Namespace asking for "7d"
// stored cleanly and then broke every subsequent list of Namespaces with
// `unknown unit "d"`. This type accepts days as well, and the CRD refuses
// anything it cannot decode.
//
// The JSON and YAML representation is the duration string, so nothing about the
// public API changes.
// +kubebuilder:validation:Type=string
// +kubebuilder:validation:Pattern=`^[+-]?(0|(\d{1,4}d)?((\d{1,6}(\.\d{0,9})?|\.\d{1,9})(ns|us|µs|μs|ms|s|m|h))+|\d{1,4}d)$`
// +kubebuilder:validation:MaxLength=20
type Duration struct {
	time.Duration `json:"-"`
}

// ParseDuration turns a duration string into a time.Duration.
//
// It accepts what DurationPattern describes: Go's duration syntax, optionally
// led by a whole number of days. The day count is converted here and the rest is
// handed to time.ParseDuration, so the units, fractions and sign behave exactly
// as they do elsewhere in Go. The only thing Go would accept and this will not
// is a component with more digits than DurationPattern allows, which exists to
// keep an accepted value inside what a time.Duration can hold.
func ParseDuration(s string) (time.Duration, error) {
	// Both bounds the CRD applies are applied here too, so the API server and
	// the operator accept exactly the same set of strings.
	if len(s) > DurationMaxLength {
		return 0, fmt.Errorf(
			"invalid duration %q: longer than %d characters", s, DurationMaxLength,
		)
	}

	if !durationRE.MatchString(s) {
		return 0, fmt.Errorf(
			"invalid duration %q: expected a duration such as \"7d\", \"1d12h\" or \"72h\"", s,
		)
	}

	days := durationDaysRE.FindStringSubmatch(s)
	if days == nil {
		// No day component, so this is an ordinary Go duration.
		return time.ParseDuration(s)
	}

	sign, count, rest := days[1], days[2], days[3]

	n, err := strconv.ParseInt(count, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", s, err)
	}

	total := time.Duration(n) * Day
	if sign == "-" {
		total = -total
	}

	if rest == "" {
		return total, nil
	}

	// The sign belongs to the whole duration, so "-1d12h" is -36h rather than
	// -24h plus 12h.
	remainder, err := time.ParseDuration(sign + rest)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", s, err)
	}

	sum := total + remainder
	if (remainder > 0 && sum < total) || (remainder < 0 && sum > total) {
		// DurationPattern is bounded so that this cannot happen, but a wrong
		// answer is worse than an error if that bound is ever loosened.
		return 0, fmt.Errorf("invalid duration %q: out of range", s)
	}

	return sum, nil
}

// MarshalJSON writes the duration as a string, the way it was always
// represented.
//
// The string is the embedded time.Duration's own, so a value read as "7d" is
// written back as
// "168h0m0s". Nothing writes spec.retention back to the API server - see
// finalizerPatch in the Namespace controller - so this canonical form is only
// ever seen by callers that marshal an object themselves.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON reads a duration string, rejecting anything outside
// DurationPattern.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("duration must be a string: %w", err)
	}

	parsed, err := ParseDuration(s)
	if err != nil {
		return err
	}

	d.Duration = parsed

	return nil
}

// OpenAPISchemaType tells OpenAPI generators that this is a string, matching
// the kubebuilder marker above.
func (Duration) OpenAPISchemaType() []string { return []string{"string"} }

// OpenAPISchemaFormat reports no specific format: the pattern does the work.
func (Duration) OpenAPISchemaFormat() string { return "" }
