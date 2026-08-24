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
	"math"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Duration", func() {
	Describe("ParseDuration", func() {
		DescribeTable(
			"accepted durations",
			func(in string, expected time.Duration) {
				got, err := ParseDuration(in)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(expected))
			},
			// The values the bug report and the docs talk about.
			Entry("72h, the default", "72h", 72*time.Hour),
			Entry("168h", "168h", 168*time.Hour),
			Entry("7d, which broke the informer", "7d", 168*time.Hour),
			Entry("1d12h", "1d12h", 36*time.Hour),
			Entry("7d30m", "7d30m", 168*time.Hour+30*time.Minute),
			Entry("2h30m", "2h30m", 2*time.Hour+30*time.Minute),
			Entry("3d", "3d", 72*time.Hour),

			// A day is exactly 24 hours, and days compose with everything else.
			Entry("1d equals 24h", "1d", 24*time.Hour),
			Entry("0d", "0d", time.Duration(0)),
			Entry("1d1s", "1d1s", 24*time.Hour+time.Second),
			Entry("1d1ms", "1d1ms", 24*time.Hour+time.Millisecond),
			Entry("2d3h4m5s", "2d3h4m5s", 48*time.Hour+3*time.Hour+4*time.Minute+5*time.Second),
			Entry("9999d, the largest day count", "9999d", 9999*Day),

			// Every Go unit still works, with and without a day component.
			Entry("nanoseconds", "1ns", time.Nanosecond),
			Entry("microseconds, us", "1us", time.Microsecond),
			Entry("microseconds, mu sign", "1µs", time.Microsecond),
			Entry("microseconds, greek mu", "1μs", time.Microsecond),
			Entry("milliseconds", "500ms", 500*time.Millisecond),
			Entry("seconds", "30s", 30*time.Second),
			Entry("minutes", "45m", 45*time.Minute),
			Entry("sub-hour compound", "1m30s", 90*time.Second),
			Entry("every unit at once", "1h2m3s4ms5us6ns",
				time.Hour+2*time.Minute+3*time.Second+4*time.Millisecond+5*time.Microsecond+6*time.Nanosecond),

			// Go's own quirks are preserved, because Go does the parsing.
			Entry("fractional hours", "1.5h", 90*time.Minute),
			Entry("leading-dot fraction", ".5h", 30*time.Minute),
			Entry("trailing-dot fraction", "5.h", 5*time.Hour),
			Entry("repeated units add up", "1m1m", 2*time.Minute),

			// Zero is valid and means zero.
			Entry("bare zero", "0", time.Duration(0)),
			Entry("signed zero", "-0", time.Duration(0)),
			Entry("zero seconds", "0s", time.Duration(0)),

			// Negatives are part of Go's duration syntax, so the generic type
			// keeps them. Whether a given field may use one is that field's own
			// business - Namespace retention rejects them, see
			// namespace_test.go.
			Entry("negative hours", "-1h", -time.Hour),
			Entry("explicit positive", "+1h", time.Hour),
			Entry("negative days", "-7d", -168*time.Hour),
			Entry("the sign covers the whole duration", "-1d12h", -36*time.Hour),
			Entry("negative compound", "-1h30m", -90*time.Minute),
		)

		DescribeTable(
			"rejected durations",
			func(in string) {
				_, err := ParseDuration(in)
				Expect(err).To(HaveOccurred(), "%q must not be accepted", in)
			},
			// Malformed day syntax. Every one of these would have been stored
			// happily by the old schema.
			Entry("7days", "7days"),
			Entry("a bare unit", "d"),
			Entry("a doubled unit", "7dd"),
			Entry("repeated day components", "1d2d"),
			Entry("days after another unit", "1h2d"),
			Entry("fractional days", "1.5d"),
			Entry("days with no count, signed", "-d"),
			Entry("day count too long", "99999d"),
			Entry("uppercase D", "7D"),

			// Malformed ordinary Go duration syntax.
			Entry("empty", ""),
			Entry("whitespace", " "),
			Entry("padded", " 7d "),
			Entry("number with no unit", "1"),
			Entry("repeated zero", "00"),
			Entry("trailing number", "1h30"),
			Entry("unknown unit", "1x"),
			Entry("unit with no number", "h"),
			Entry("mid-string sign", "1h-30m"),
			Entry("exponent", "1e3h"),
			Entry("hex", "0x10h"),
			Entry("digit separators", "1_000h"),
			Entry("arbitrary text", "forever"),
			Entry("nearly a duration", "seven days"),
			Entry("a number of days in words", "7 d"),
			Entry("component too long", "9999999h"),
			Entry("longer than MaxLength", "1h1h1h1h1h1h1h1h1h1h1h"),
		)

		It("should agree with the CRD pattern about what it accepts", func() {
			// ParseDuration checks DurationPattern first, so the API server and
			// the operator cannot disagree. This spec states the coupling so
			// that loosening one without the other fails here.
			Expect(durationRE.String()).To(Equal(DurationPattern))
			Expect(DurationMaxLength).To(Equal(20))
		})
	})

	Describe("the bound on accepted values", func() {
		// A duration the pattern accepts but time.Duration cannot hold would
		// put us straight back where we started: stored happily, undecodable
		// later. The pattern's digit counts exist to make that impossible.
		It("should not accept any string that overflows a time.Duration", func() {
			// The densest accepted strings, worked out from the pattern: one
			// four-digit day component plus as many six-digit hour components
			// as MaxLength allows.
			worst := []string{
				"9999d999999h999999h",
				"-9999d999999h999999h",
				"999999h999999h99999h",
				"9999d999999h99999h9h",
				"9999d",
				"999999h",
			}

			for _, s := range worst {
				Expect(len(s)).To(BeNumerically("<=", DurationMaxLength), s)
				Expect(durationRE.MatchString(s)).To(BeTrue(), "%q should match the pattern", s)

				got, err := ParseDuration(s)
				Expect(err).NotTo(HaveOccurred(), "%q matches the pattern so it must parse", s)
				Expect(got.Hours()).To(BeNumerically("<", math.MaxInt64/float64(time.Hour)))
			}
		})

		It("should reject the strings that would overflow", func() {
			for _, s := range []string{
				"9999999999999999999h",
				"99999d",
				"9999999h",
				"9223372036854775808ns",
			} {
				_, err := ParseDuration(s)
				Expect(err).To(HaveOccurred(), "%q must not be accepted", s)
			}
		})

		It("should parse everything the pattern accepts, across a generated corpus", func() {
			// Anything matching the pattern must parse, or an object that
			// passed admission could still break the informer. Build a corpus
			// by combination rather than by hand.
			counts := []string{"0", "1", "9", "30", "168", "999", "9999", "999999", "0.5", ".5", "5."}
			units := []string{"ns", "us", "µs", "μs", "ms", "s", "m", "h"}
			signs := []string{"", "+", "-"}
			days := []string{"", "0d", "1d", "7d", "9999d"}

			checked := 0
			for _, sign := range signs {
				for _, day := range days {
					for _, count := range counts {
						for _, unit := range units {
							for _, tail := range []string{"", count + unit} {
								s := sign + day + count + unit + tail
								if len(s) > DurationMaxLength || !durationRE.MatchString(s) {
									continue
								}

								_, err := ParseDuration(s)
								Expect(err).NotTo(HaveOccurred(),
									"%q matches the pattern so it must parse", s)
								checked++
							}
						}
					}
				}
			}

			Expect(checked).To(BeNumerically(">", 500), "the corpus should be a real test")
		})
	})

	Describe("JSON", func() {
		It("should unmarshal a duration string", func() {
			var d Duration
			Expect(json.Unmarshal([]byte(`"7d"`), &d)).To(Succeed())
			Expect(d.Duration).To(Equal(168 * time.Hour))
		})

		It("should marshal as a duration string", func() {
			out, err := json.Marshal(Duration{Duration: 168 * time.Hour})
			Expect(err).NotTo(HaveOccurred())
			Expect(string(out)).To(Equal(`"168h0m0s"`))
		})

		DescribeTable(
			"round trips",
			func(in string, expected time.Duration) {
				var d Duration
				Expect(json.Unmarshal(fmt.Appendf(nil, "%q", in), &d)).To(Succeed())
				Expect(d.Duration).To(Equal(expected))

				out, err := json.Marshal(d)
				Expect(err).NotTo(HaveOccurred())

				// The canonical form must itself be readable, so a value can
				// survive any number of trips through Go.
				var again Duration
				Expect(json.Unmarshal(out, &again)).To(Succeed())
				Expect(again.Duration).To(Equal(expected))
			},
			Entry("7d", "7d", 168*time.Hour),
			Entry("72h", "72h", 72*time.Hour),
			Entry("1d12h", "1d12h", 36*time.Hour),
			Entry("7d30m", "7d30m", 168*time.Hour+30*time.Minute),
			Entry("500ms", "500ms", 500*time.Millisecond),
			Entry("0s", "0s", time.Duration(0)),
			Entry("-1h", "-1h", -time.Hour),
		)

		It("should reject a malformed duration string", func() {
			var d Duration
			err := json.Unmarshal([]byte(`"7days"`), &d)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(`invalid duration "7days"`))
		})

		It("should reject a value that is not a string", func() {
			var d Duration
			Expect(json.Unmarshal([]byte(`604800`), &d)).NotTo(Succeed())
			Expect(json.Unmarshal([]byte(`{"duration":"7d"}`), &d)).NotTo(Succeed())
			Expect(json.Unmarshal([]byte(`null`), &d)).NotTo(Succeed())
		})

		It("should survive being embedded in a spec", func() {
			// The shape that matters: a Namespace spec, as the API server
			// stores it, decoding into Go.
			var spec NamespaceSpec
			Expect(json.Unmarshal([]byte(
				`{"connectionRef":{"name":"production"},"retention":"7d"}`,
			), &spec)).To(Succeed())

			Expect(spec.Retention).NotTo(BeNil())
			Expect(spec.Retention.Duration).To(Equal(168 * time.Hour))
		})

		It("should refuse a spec carrying the duration that caused the outage", func() {
			// Before this type existed, "7d" decoded with
			// `time: unknown unit "d" in duration "7d"`, which is what poisoned
			// the Namespace informer. Now it decodes; something genuinely
			// malformed is what fails, and the CRD pattern stops that reaching
			// storage in the first place.
			var spec NamespaceSpec
			err := json.Unmarshal([]byte(
				`{"connectionRef":{"name":"production"},"retention":"7 days please"}`,
			), &spec)
			Expect(err).To(HaveOccurred())
		})
	})
})
