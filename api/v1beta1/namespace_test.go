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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// retentionOf builds a spec asking for the given duration string. Going through
// ParseDuration rather than a hand-written time.Duration means these specs test
// what a user would actually write.
func retentionOf(s string) NamespaceSpec {
	d, err := ParseDuration(s)
	Expect(err).NotTo(HaveOccurred(), "%q should be valid syntax", s)

	return NamespaceSpec{Retention: &Duration{Duration: d}}
}

var _ = Describe("Namespace retention", func() {
	It("should bound retention to one and ninety days", func() {
		Expect(MinRetention).To(Equal(24 * time.Hour))
		Expect(MaxRetention).To(Equal(2160 * time.Hour))
		Expect(MaxRetention).To(Equal(90 * Day))
	})

	Describe("RetentionDuration", func() {
		// The range is on the duration, so every notation for the same duration
		// has to be treated the same. These are the forms the documentation
		// promises.
		DescribeTable(
			"accepted retentions",
			func(in string, expected time.Duration) {
				got, err := retentionOf(in).RetentionDuration()
				Expect(err).NotTo(HaveOccurred(), "%q should be in range", in)
				Expect(got).To(Equal(expected))
			},
			// The minimum, written five ways.
			Entry("1d", "1d", 24*time.Hour),
			Entry("24h", "24h", 24*time.Hour),
			Entry("1440m", "1440m", 24*time.Hour),
			Entry("86400s", "86400s", 24*time.Hour),
			Entry("0d24h", "0d24h", 24*time.Hour),

			// In the middle.
			Entry("1d12h", "1d12h", 36*time.Hour),
			Entry("36h", "36h", 36*time.Hour),
			Entry("7d", "7d", 168*time.Hour),
			Entry("72h", "72h", 72*time.Hour),
			Entry("3d", "3d", 72*time.Hour),
			Entry("7d30m", "7d30m", 168*time.Hour+30*time.Minute),

			// The maximum, written three ways.
			Entry("90d", "90d", 2160*time.Hour),
			Entry("2160h", "2160h", 2160*time.Hour),
			Entry("89d24h", "89d24h", 2160*time.Hour),
		)

		DescribeTable(
			"rejected retentions",
			func(in string) {
				_, err := retentionOf(in).RetentionDuration()
				Expect(err).To(HaveOccurred(), "%q should be out of range", in)
				Expect(err.Error()).To(ContainSubstring("out of range"))
			},
			// Below the minimum, in several notations.
			Entry("zero", "0"),
			Entry("zero seconds", "0s"),
			Entry("zero days", "0d"),
			Entry("1h", "1h"),
			Entry("23h59m59s", "23h59m59s"),
			Entry("1439m", "1439m"),
			Entry("86399s", "86399s"),
			Entry("30m", "30m"),
			Entry("500ms", "500ms"),
			Entry("2h30m", "2h30m"),

			// Negative. This is the value that used to be read as "unset".
			Entry("-1h", "-1h"),
			Entry("-1d", "-1d"),
			Entry("-7d", "-7d"),
			Entry("-1d12h", "-1d12h"),

			// Above the maximum, including the smallest possible overshoot.
			Entry("90d1ns", "90d1ns"),
			Entry("90d1s", "90d1s"),
			Entry("2160h1s", "2160h1s"),
			Entry("2160h1ns", "2160h1ns"),
			Entry("2161h", "2161h"),
			Entry("91d", "91d"),
			Entry("129601m", "129601m"),
			Entry("89d100h", "89d100h"),
			Entry("9999d", "9999d"),
		)

		Describe("the boundaries themselves", func() {
			It("should accept exactly the minimum and reject a nanosecond less", func() {
				got, err := (NamespaceSpec{Retention: &Duration{Duration: MinRetention}}).
					RetentionDuration()
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(24 * time.Hour))

				_, err = (NamespaceSpec{Retention: &Duration{Duration: MinRetention - 1}}).
					RetentionDuration()
				Expect(err).To(HaveOccurred())
			})

			It("should accept exactly the maximum and reject a nanosecond more", func() {
				got, err := (NamespaceSpec{Retention: &Duration{Duration: MaxRetention}}).
					RetentionDuration()
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(2160 * time.Hour))

				_, err = (NamespaceSpec{Retention: &Duration{Duration: MaxRetention + 1}}).
					RetentionDuration()
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("omission versus an invalid value", func() {
			// The distinction the API has to keep: saying nothing takes the
			// default, while saying something unusable is an error. The old
			// behaviour conflated the two and quietly turned "0" into 72h.
			It("should default when retention is omitted", func() {
				got, err := (NamespaceSpec{}).RetentionDuration()
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(DefaultRetention))
				Expect(got).To(Equal(72 * time.Hour))
			})

			It("should not default when retention is present but zero", func() {
				got, err := retentionOf("0s").RetentionDuration()
				Expect(err).To(HaveOccurred())
				Expect(got).To(BeZero(), "an error must not come with a usable duration")
			})

			It("should not default when retention is present but negative", func() {
				_, err := retentionOf("-1h").RetentionDuration()
				Expect(err).To(HaveOccurred())
			})

			It("should name the range in the error", func() {
				_, err := retentionOf("1h").RetentionDuration()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("24h0m0s"))
				Expect(err.Error()).To(ContainSubstring("2160h0m0s"))
			})
		})

		// The reason the range lives here and not in UnmarshalJSON. Decoding an
		// object must never fail: a List decodes as one response, so one
		// undecodable item would fail the whole call and break the informer -
		// the exact outage that made this field custom in the first place. An
		// out-of-range value therefore decodes fine and is refused at the point
		// it is asked for.
		It("should still decode an out-of-range retention", func() {
			for _, raw := range []string{"0s", "-1h", "1h", "91d", "9999d"} {
				var spec NamespaceSpec

				data := fmt.Appendf(nil,
					`{"connectionRef":{"name":"production"},"retention":%q}`, raw)
				Expect(json.Unmarshal(data, &spec)).To(Succeed(),
					"%q must decode, whatever the policy says about it", raw)

				Expect(spec.Retention).NotTo(BeNil())

				_, err := spec.RetentionDuration()
				Expect(err).To(HaveOccurred(), "%q should be refused when asked for", raw)
			}
		})

		It("should treat every notation of the same duration identically", func() {
			// Written differently, decided the same. This is the property that a
			// text-based rule - "the day count must be 1 to 90" - would get
			// wrong.
			for _, group := range [][]string{
				{"1d", "24h", "1440m", "86400s", "0d24h"},
				{"90d", "2160h", "89d24h"},
				{"7d", "168h", "6d24h", "10080m"},
			} {
				var first time.Duration

				for i, in := range group {
					got, err := retentionOf(in).RetentionDuration()
					Expect(err).NotTo(HaveOccurred(), in)

					if i == 0 {
						first = got
						continue
					}

					Expect(got).To(Equal(first), "%q should equal %q", in, group[0])
				}
			}
		})
	})
})

var _ = Describe("Namespace archival", func() {
	// Both levels of the block are optional, and "not managed" has to survive
	// each of them being absent. Everything the controller does about Archival
	// hangs off this distinction: nil means leave the Temporal namespace's
	// setting alone, whoever set it.
	Describe("HistoryArchival and VisibilityArchival", func() {
		It("should report nothing managed when archival is omitted", func() {
			spec := NamespaceSpec{}

			Expect(spec.HistoryArchival()).To(BeNil())
			Expect(spec.VisibilityArchival()).To(BeNil())
		})

		It("should report nothing managed when the block is empty", func() {
			spec := NamespaceSpec{Archival: &NamespaceArchival{}}

			Expect(spec.HistoryArchival()).To(BeNil())
			Expect(spec.VisibilityArchival()).To(BeNil())
		})

		It("should keep the two kinds independent", func() {
			spec := NamespaceSpec{Archival: &NamespaceArchival{
				History: &ArchivalConfig{Enabled: true},
			}}

			Expect(spec.HistoryArchival()).To(Equal(&ArchivalConfig{Enabled: true}))
			Expect(spec.VisibilityArchival()).To(BeNil(),
				"configuring one kind says nothing about the other")

			spec = NamespaceSpec{Archival: &NamespaceArchival{
				Visibility: &ArchivalConfig{Enabled: true},
			}}

			Expect(spec.HistoryArchival()).To(BeNil())
			Expect(spec.VisibilityArchival()).To(Equal(&ArchivalConfig{Enabled: true}))
		})

		It("should tell a disabled kind apart from an unmanaged one", func() {
			spec := NamespaceSpec{Archival: &NamespaceArchival{
				History: &ArchivalConfig{Enabled: false},
			}}

			Expect(spec.HistoryArchival()).NotTo(BeNil(),
				"asking for archival off is a request, not an absence")
			Expect(spec.HistoryArchival().Enabled).To(BeFalse())
		})

		It("should carry the URI through", func() {
			spec := NamespaceSpec{Archival: &NamespaceArchival{
				History:    &ArchivalConfig{Enabled: true, URI: "s3://bucket/history"},
				Visibility: &ArchivalConfig{Enabled: true, URI: "s3://bucket/visibility"},
			}}

			Expect(spec.HistoryArchival().URI).To(Equal("s3://bucket/history"))
			Expect(spec.VisibilityArchival().URI).To(Equal("s3://bucket/visibility"))
		})
	})

	Describe("decoding", func() {
		It("should decode the nested shape", func() {
			var spec NamespaceSpec

			data := []byte(`{"connectionRef":{"name":"production"},"retention":"7d",` +
				`"archival":{"history":{"enabled":true},"visibility":{"enabled":false}}}`)
			Expect(json.Unmarshal(data, &spec)).To(Succeed())

			Expect(spec.HistoryArchival()).To(Equal(&ArchivalConfig{Enabled: true}))
			Expect(spec.VisibilityArchival()).To(Equal(&ArchivalConfig{Enabled: false}))
		})

		It("should leave archival unset when the field is absent", func() {
			// A Namespace stored before archival existed decodes with nothing
			// managed, which is what keeps its behaviour unchanged.
			var spec NamespaceSpec

			data := []byte(`{"connectionRef":{"name":"production"},"retention":"7d"}`)
			Expect(json.Unmarshal(data, &spec)).To(Succeed())

			Expect(spec.Archival).To(BeNil())
			Expect(spec.HistoryArchival()).To(BeNil())
			Expect(spec.VisibilityArchival()).To(BeNil())
		})
	})
})
