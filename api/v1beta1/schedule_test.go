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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// duration builds a Duration from the string a user would write, going through
// the same parser the API server's stored value goes through.
func duration(s string) *Duration {
	d, err := ParseDuration(s)
	Expect(err).NotTo(HaveOccurred(), "%q should be valid syntax", s)

	return &Duration{Duration: d}
}

// validScheduleSpec builds the smallest Schedule that Temporal would accept, so
// that a spec exercising one field is not also failing on another.
func validScheduleSpec() ScheduleSpec {
	return ScheduleSpec{
		ScheduleID:    "PaymentsNightly",
		ConnectionRef: corev1.LocalObjectReference{Name: "production"},
		NamespaceRef:  corev1.LocalObjectReference{Name: "payments"},
		Schedule: ScheduleTiming{
			Cron: []string{"30 2 * * *"},
		},
		Action: ScheduleAction{
			Workflow: ScheduleWorkflow{
				Type:      "ReconcilePayments",
				TaskQueue: "payments",
			},
		},
	}
}

var _ = Describe("Schedule", func() {
	Describe("identity and defaults", func() {
		It("should send the schedule ID rather than the resource name", func() {
			// The whole reason the two are separate: Kubernetes will not take a
			// PascalCase resource name, and Temporal schedule IDs conventionally
			// are.
			schedule := &Schedule{
				ObjectMeta: metav1.ObjectMeta{Name: "nightly-payments"},
				Spec:       validScheduleSpec(),
			}

			Expect(schedule.TemporalName()).To(Equal("PaymentsNightly"))
			Expect(schedule.TemporalName()).NotTo(Equal(schedule.Name))
		})

		It("should take the Temporal namespace from the reference", func() {
			// From the reference rather than the Namespace resource, so that
			// finalisation can find the schedule without the Namespace still
			// being around to ask.
			schedule := &Schedule{Spec: validScheduleSpec()}

			Expect(schedule.TemporalNamespace()).To(Equal("payments"))
		})

		It("should default the deletion policy to Delete", func() {
			spec := validScheduleSpec()

			Expect(spec.DeletionPolicyValue()).To(Equal(ScheduleDeletionPolicyDelete))
			Expect(DefaultScheduleDeletionPolicy).To(Equal(ScheduleDeletionPolicyDelete))

			spec.DeletionPolicy = ScheduleDeletionPolicyOrphan
			Expect(spec.DeletionPolicyValue()).To(Equal(ScheduleDeletionPolicyOrphan))
		})

		It("should default the overlap policy to Skip", func() {
			// Skip is Temporal's own default, applied again in Go so that an
			// object built in memory behaves like one that has been through
			// admission.
			Expect((*SchedulePolicies)(nil).OverlapValue()).To(Equal(ScheduleOverlapPolicySkip))
			Expect((&SchedulePolicies{}).OverlapValue()).To(Equal(ScheduleOverlapPolicySkip))
			Expect((&SchedulePolicies{Overlap: ScheduleOverlapPolicyAllowAll}).OverlapValue()).
				To(Equal(ScheduleOverlapPolicyAllowAll))
		})

		It("should mirror Temporal's own ID length limits", func() {
			// Derived from the Service rather than chosen: it validates the ID
			// with its internal prefix attached, and the workflow ID with a
			// timestamp appended.
			Expect(MaxScheduleIDLength).To(Equal(1000 - len("temporal-sys-scheduler:")))
			Expect(MaxScheduleIDLength).To(Equal(977))
			Expect(MaxWorkflowIDLength).To(Equal(1000 - len("-2009-11-10T23:00:00Z")))
			Expect(MaxWorkflowIDLength).To(Equal(979))
		})
	})

	Describe("ownership", func() {
		DescribeTable(
			"should decide whether deleting the resource deletes the schedule",
			func(ownership ScheduleOwnership, owns, established bool) {
				Expect(ownership.OwnsSchedule()).To(Equal(owns))
				Expect(ownership.IsEstablished()).To(Equal(established))
			},
			// Creating counts as owned: the marker is only written after the
			// operator has seen the schedule missing, so what is there is either
			// its own work or nothing.
			Entry("Creating", ScheduleOwnershipCreating, true, false),
			Entry("Created", ScheduleOwnershipCreated, true, true),
			Entry("Adopted", ScheduleOwnershipAdopted, false, true),
			Entry("unset", ScheduleOwnership(""), false, false),
		)
	})

	Describe("ScheduleRange defaults", func() {
		It("should read an absent end as the start", func() {
			Expect(ScheduleRange{Start: 5}.EndValue()).To(BeNumerically("==", 5))
			Expect(ScheduleRange{Start: 5, End: new(int32(9))}.EndValue()).To(BeNumerically("==", 9))
		})

		It("should read an absent step as one", func() {
			Expect(ScheduleRange{Start: 5}.StepValue()).To(BeNumerically("==", 1))
			Expect(ScheduleRange{Start: 5, Step: new(int32(3))}.StepValue()).To(BeNumerically("==", 3))
		})
	})

	Describe("Validate", func() {
		It("should accept the smallest usable schedule", func() {
			spec := validScheduleSpec()

			Expect(spec.Validate()).To(Succeed())
		})

		It("should refuse a timing specification with no rules", func() {
			spec := validScheduleSpec()
			spec.Schedule = ScheduleTiming{}

			err := spec.Validate()
			Expect(err).To(MatchError(ErrInvalidScheduleSpec))
			Expect(err).To(MatchError(ErrNoScheduleRule))
		})

		DescribeTable(
			"should accept any combination Temporal accepts",
			func(timing ScheduleTiming) {
				spec := validScheduleSpec()
				spec.Schedule = timing

				Expect(spec.Validate()).To(Succeed())
			},
			// Temporal takes the union of every rule, so none of these is
			// exclusive with any other. Modelling them as a union would have
			// been wrong.
			Entry("cron alone", ScheduleTiming{Cron: []string{"30 2 * * *"}}),
			Entry("intervals alone", ScheduleTiming{
				Intervals: []ScheduleInterval{{Every: *duration("6h")}},
			}),
			Entry("calendars alone", ScheduleTiming{
				Calendars: []ScheduleCalendar{{Hour: []ScheduleRange{{Start: 9}}}},
			}),
			Entry("cron and intervals", ScheduleTiming{
				Cron:      []string{"30 2 * * *"},
				Intervals: []ScheduleInterval{{Every: *duration("6h")}},
			}),
			Entry("all three, with exclusions", ScheduleTiming{
				Cron:             []string{"@daily"},
				Intervals:        []ScheduleInterval{{Every: *duration("6h"), Offset: duration("5h")}},
				Calendars:        []ScheduleCalendar{{Hour: []ScheduleRange{{Start: 9}}}},
				ExcludeCalendars: []ScheduleCalendar{{DayOfMonth: []ScheduleRange{{Start: 1}}}},
			}),
		)

		Describe("intervals", func() {
			// The bounds are the Service's validateInterval, checked here
			// because the CRD cannot compare two duration strings.
			It("should refuse an interval under a second", func() {
				spec := validScheduleSpec()
				spec.Schedule = ScheduleTiming{Intervals: []ScheduleInterval{{Every: *duration("999ms")}}}

				Expect(spec.Validate()).To(MatchError(ContainSubstring("shorter than the minimum")))
			})

			It("should accept exactly a second", func() {
				spec := validScheduleSpec()
				spec.Schedule = ScheduleTiming{Intervals: []ScheduleInterval{{Every: *duration("1s")}}}

				Expect(spec.Validate()).To(Succeed())
			})

			It("should refuse a negative offset", func() {
				spec := validScheduleSpec()
				spec.Schedule = ScheduleTiming{Intervals: []ScheduleInterval{
					{Every: *duration("6h"), Offset: duration("-1m")},
				}}

				Expect(spec.Validate()).To(MatchError(ContainSubstring("negative")))
			})

			It("should refuse an offset as long as the interval", func() {
				// Temporal's check is "phase >= interval": an offset of a whole
				// period is the same as no offset, and the Service refuses the
				// ambiguity rather than accepting it.
				spec := validScheduleSpec()
				spec.Schedule = ScheduleTiming{Intervals: []ScheduleInterval{
					{Every: *duration("6h"), Offset: duration("6h")},
				}}

				Expect(spec.Validate()).To(MatchError(ContainSubstring("must be shorter than every")))
			})

			It("should accept an offset just under the interval", func() {
				spec := validScheduleSpec()
				spec.Schedule = ScheduleTiming{Intervals: []ScheduleInterval{
					{Every: *duration("6h"), Offset: duration("5h59m59s")},
				}}

				Expect(spec.Validate()).To(Succeed())
			})
		})

		Describe("time zones", func() {
			It("should accept an IANA zone", func() {
				spec := validScheduleSpec()
				spec.Schedule.TimeZone = "Europe/London"

				Expect(spec.Validate()).To(Succeed())
			})

			It("should refuse a zone that is not in the database", func() {
				spec := validScheduleSpec()
				spec.Schedule.TimeZone = "Mars/Olympus_Mons"

				err := spec.Validate()
				Expect(err).To(MatchError(ErrUnknownTimeZone))
				Expect(err).To(MatchError(ContainSubstring("Mars/Olympus_Mons")))
			})

			It("should refuse an abbreviation the database does not define", func() {
				// "BST" is not an IANA zone name, however often people write it.
				spec := validScheduleSpec()
				spec.Schedule.TimeZone = "BST"

				Expect(spec.Validate()).To(MatchError(ErrUnknownTimeZone))
			})
		})

		Describe("start and end", func() {
			start := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
			end := metav1.NewTime(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))

			It("should accept an end after a start", func() {
				spec := validScheduleSpec()
				spec.Schedule.StartAt = &start
				spec.Schedule.EndAt = &end

				Expect(spec.Validate()).To(Succeed())
			})

			It("should refuse an end before a start", func() {
				spec := validScheduleSpec()
				spec.Schedule.StartAt = &end
				spec.Schedule.EndAt = &start

				Expect(spec.Validate()).To(MatchError(ContainSubstring("is not after")))
			})

			It("should refuse an end equal to a start", func() {
				spec := validScheduleSpec()
				spec.Schedule.StartAt = &start
				spec.Schedule.EndAt = &start

				Expect(spec.Validate()).To(MatchError(ContainSubstring("is not after")))
			})

			It("should accept either on its own", func() {
				spec := validScheduleSpec()
				spec.Schedule.StartAt = &start
				Expect(spec.Validate()).To(Succeed())

				spec = validScheduleSpec()
				spec.Schedule.EndAt = &end
				Expect(spec.Validate()).To(Succeed())
			})
		})

		It("should refuse negative jitter", func() {
			spec := validScheduleSpec()
			spec.Schedule.Jitter = duration("-1s")

			Expect(spec.Validate()).To(MatchError(ContainSubstring("jitter")))
		})

		Describe("calendars", func() {
			DescribeTable(
				"should enforce Temporal's own field bounds",
				func(calendar ScheduleCalendar, valid bool) {
					spec := validScheduleSpec()
					spec.Schedule = ScheduleTiming{Calendars: []ScheduleCalendar{calendar}}

					if valid {
						Expect(spec.Validate()).To(Succeed())

						return
					}

					Expect(spec.Validate()).To(HaveOccurred())
				},
				Entry("second at the bounds",
					ScheduleCalendar{Second: []ScheduleRange{{Start: 0, End: new(int32(59))}}}, true),
				Entry("second past the bound",
					ScheduleCalendar{Second: []ScheduleRange{{Start: 60}}}, false),
				Entry("hour at the bound",
					ScheduleCalendar{Hour: []ScheduleRange{{Start: 23}}}, true),
				Entry("hour past the bound",
					ScheduleCalendar{Hour: []ScheduleRange{{Start: 24}}}, false),
				Entry("dayOfMonth starts at one",
					ScheduleCalendar{DayOfMonth: []ScheduleRange{{Start: 1}}}, true),
				Entry("dayOfMonth zero",
					ScheduleCalendar{DayOfMonth: []ScheduleRange{{Start: 0}}}, false),
				Entry("month at the bound",
					ScheduleCalendar{Month: []ScheduleRange{{Start: 12}}}, true),
				Entry("month past the bound",
					ScheduleCalendar{Month: []ScheduleRange{{Start: 13}}}, false),
				// Sunday is 0, from Go's time.Weekday, which is what the Service
				// matches against.
				Entry("dayOfWeek Sunday", ScheduleCalendar{DayOfWeek: []ScheduleRange{{Start: 0}}}, true),
				Entry("dayOfWeek Saturday", ScheduleCalendar{DayOfWeek: []ScheduleRange{{Start: 6}}}, true),
				Entry("dayOfWeek seven",
					ScheduleCalendar{DayOfWeek: []ScheduleRange{{Start: 7}}}, false),
				Entry("year at the bounds",
					ScheduleCalendar{Year: []ScheduleRange{{Start: 2000, End: new(int32(2100))}}}, true),
				Entry("year before the bound",
					ScheduleCalendar{Year: []ScheduleRange{{Start: 1999}}}, false),
				Entry("year past the bound",
					ScheduleCalendar{Year: []ScheduleRange{{Start: 2101}}}, false),
				Entry("end before start",
					ScheduleCalendar{Hour: []ScheduleRange{{Start: 9, End: new(int32(3))}}}, false),
				Entry("step of one",
					ScheduleCalendar{Hour: []ScheduleRange{{Start: 0, End: new(int32(23)), Step: new(int32(1))}}},
					true),
				Entry("step of zero",
					ScheduleCalendar{Hour: []ScheduleRange{{Start: 0, Step: new(int32(0))}}}, false),
			)

			It("should refuse a comment past Temporal's length limit", func() {
				spec := validScheduleSpec()
				spec.Schedule = ScheduleTiming{Calendars: []ScheduleCalendar{
					{Hour: []ScheduleRange{{Start: 9}}, Comment: string(make([]byte, 201))},
				}}

				Expect(spec.Validate()).To(MatchError(ContainSubstring("comment")))
			})

			It("should check exclusions the same way", func() {
				spec := validScheduleSpec()
				spec.Schedule.ExcludeCalendars = []ScheduleCalendar{{Hour: []ScheduleRange{{Start: 24}}}}

				Expect(spec.Validate()).To(MatchError(ContainSubstring("excludeCalendars")))
			})
		})

		Describe("the workflow action", func() {
			It("should refuse input that is not JSON", func() {
				spec := validScheduleSpec()
				spec.Action.Workflow.Input = []string{`{"mode":"nightly"}`, `not json`}

				Expect(spec.Validate()).To(MatchError(ContainSubstring("input[1] is not valid JSON")))
			})

			It("should accept any JSON value as an argument", func() {
				spec := validScheduleSpec()
				spec.Action.Workflow.Input = []string{`{"mode":"nightly"}`, `42`, `"text"`, `null`, `[1,2]`}

				Expect(spec.Validate()).To(Succeed())
			})

			It("should refuse a memo value that is not JSON", func() {
				spec := validScheduleSpec()
				spec.Action.Workflow.Memo = map[string]string{"owner": "payments"}

				Expect(spec.Validate()).To(MatchError(ContainSubstring("not valid JSON")))
			})

			It("should accept a quoted memo value", func() {
				spec := validScheduleSpec()
				spec.Action.Workflow.Memo = map[string]string{"owner": `"payments"`}

				Expect(spec.Validate()).To(Succeed())
			})

			DescribeTable(
				"should refuse a timeout that is not positive",
				func(timeouts ScheduleWorkflowTimeouts) {
					spec := validScheduleSpec()
					spec.Action.Workflow.Timeouts = &timeouts

					Expect(spec.Validate()).To(MatchError(ContainSubstring("must be positive")))
				},
				Entry("zero task", ScheduleWorkflowTimeouts{Task: duration("0s")}),
				Entry("negative run", ScheduleWorkflowTimeouts{Run: duration("-1h")}),
				Entry("zero execution", ScheduleWorkflowTimeouts{Execution: duration("0s")}),
			)

			It("should accept positive timeouts", func() {
				spec := validScheduleSpec()
				spec.Action.Workflow.Timeouts = &ScheduleWorkflowTimeouts{
					Task:      duration("10s"),
					Run:       duration("1h"),
					Execution: duration("2h"),
				}

				Expect(spec.Validate()).To(Succeed())
			})
		})

		Describe("priority", func() {
			It("should accept a weight inside Temporal's clamp range", func() {
				for _, weight := range []string{"0.001", "1", "9", "1000"} {
					spec := validScheduleSpec()
					spec.Action.Workflow.Priority = &SchedulePriority{FairnessWeight: weight}

					Expect(spec.Validate()).To(Succeed(), weight)
				}
			})

			It("should refuse a weight outside it", func() {
				// Temporal clamps rather than refusing, so a spec saying 5000
				// would quietly behave as 1000. Refusing keeps the spec honest.
				for _, weight := range []string{"0", "0.0001", "1000.1", "5000"} {
					spec := validScheduleSpec()
					spec.Action.Workflow.Priority = &SchedulePriority{FairnessWeight: weight}

					Expect(spec.Validate()).To(MatchError(ContainSubstring("fairnessWeight")), weight)
				}
			})

			It("should refuse a fairness key over 64 bytes", func() {
				spec := validScheduleSpec()
				spec.Action.Workflow.Priority = &SchedulePriority{FairnessKey: string(make([]byte, 65))}

				Expect(spec.Validate()).To(MatchError(ContainSubstring("64 bytes")))
			})

			It("should measure the fairness key in bytes, not characters", func() {
				// Temporal's limit is bytes. Sixty-four characters of a
				// multi-byte script is well over it.
				spec := validScheduleSpec()
				spec.Action.Workflow.Priority = &SchedulePriority{
					FairnessKey: "日本語日本語日本語日本語日本語日本語日本語日本語",
				}

				Expect(len([]rune(spec.Action.Workflow.Priority.FairnessKey))).
					To(BeNumerically("<", 64), "under the limit in characters")
				Expect(len(spec.Action.Workflow.Priority.FairnessKey)).
					To(BeNumerically(">", 64), "but over it in bytes")
				Expect(spec.Validate()).To(MatchError(ContainSubstring("64 bytes")))
			})
		})

		Describe("policies", func() {
			It("should accept exactly Temporal's minimum catch-up window", func() {
				spec := validScheduleSpec()
				spec.Policies = &SchedulePolicies{CatchupWindow: duration("10s")}

				Expect(spec.Validate()).To(Succeed())
				Expect(MinCatchupWindow).To(Equal(10 * time.Second))
			})

			It("should refuse a shorter one", func() {
				// The Service raises anything shorter to ten seconds rather than
				// refusing it, so accepting it would mean storing a spec that
				// does not describe what happens.
				spec := validScheduleSpec()
				spec.Policies = &SchedulePolicies{CatchupWindow: duration("9s")}

				Expect(spec.Validate()).To(MatchError(ContainSubstring("shorter than the minimum")))
			})
		})

		Describe("search attributes", func() {
			DescribeTable(
				"should read a value as the type it claims to be",
				func(attributeType SearchAttributeType, value string, expected any) {
					attribute := ScheduleSearchAttribute{Name: "Attr", Type: attributeType, Value: value}

					decoded, err := attribute.DecodeValue()
					Expect(err).NotTo(HaveOccurred())
					Expect(decoded).To(Equal(expected))
				},
				Entry("Bool", SearchAttributeTypeBool, "true", true),
				Entry("Int", SearchAttributeTypeInt, "7", int64(7)),
				Entry("Double", SearchAttributeTypeDouble, "1.5", 1.5),
				Entry("Keyword", SearchAttributeTypeKeyword, `"gold"`, "gold"),
				Entry("Text", SearchAttributeTypeText, `"some words"`, "some words"),
				Entry("KeywordList", SearchAttributeTypeKeywordList, `["a","b"]`, []string{"a", "b"}),
				Entry("Datetime", SearchAttributeTypeDatetime, `"2026-01-01T00:00:00Z"`,
					time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
			)

			DescribeTable(
				"should refuse a value that does not fit its type",
				func(attributeType SearchAttributeType, value string) {
					attribute := ScheduleSearchAttribute{Name: "Attr", Type: attributeType, Value: value}

					_, err := attribute.DecodeValue()
					Expect(err).To(HaveOccurred())
				},
				// The type in the spec is what makes these tellable apart at
				// all: "7" is an Int or a Text depending only on how the
				// attribute was registered.
				Entry("a string as a Bool", SearchAttributeTypeBool, `"true"`),
				Entry("a float as an Int", SearchAttributeTypeInt, "1.5"),
				Entry("an unquoted string as a Keyword", SearchAttributeTypeKeyword, "gold"),
				Entry("a string as a KeywordList", SearchAttributeTypeKeywordList, `"a"`),
				Entry("a number as a Datetime", SearchAttributeTypeDatetime, "1735689600"),
				Entry("two values", SearchAttributeTypeInt, "1 2"),
				Entry("an unknown type", SearchAttributeType("Nonsense"), "1"),
			)

			It("should refuse the same attribute twice", func() {
				spec := validScheduleSpec()
				spec.SearchAttributes = []ScheduleSearchAttribute{
					{Name: "CustomerId", Type: SearchAttributeTypeKeyword, Value: `"a"`},
					{Name: "CustomerId", Type: SearchAttributeTypeKeyword, Value: `"b"`},
				}

				Expect(spec.Validate()).To(MatchError(ContainSubstring("appears more than once")))
			})

			It("should check the schedule's and the workflow's separately", func() {
				spec := validScheduleSpec()
				spec.SearchAttributes = []ScheduleSearchAttribute{
					{Name: "Team", Type: SearchAttributeTypeKeyword, Value: `"payments"`},
				}
				spec.Action.Workflow.SearchAttributes = []ScheduleSearchAttribute{
					{Name: "Team", Type: SearchAttributeTypeInt, Value: `"not an int"`},
				}

				Expect(spec.Validate()).To(MatchError(ContainSubstring("action.workflow")))
			})
		})
	})

	Describe("decoding", func() {
		// The production invariant: a Schedule already stored in Kubernetes has
		// to decode even if it is nonsense. A List decodes as one response, so
		// one undecodable item would fail the whole call and poison the informer
		// behind the controller.
		It("should still decode a schedule Validate would refuse", func() {
			for _, raw := range []string{
				`{"scheduleId":"S","schedule":{"cron":["not a cron expression"]}}`,
				`{"scheduleId":"S","schedule":{"timeZone":"Mars/Olympus_Mons"}}`,
				`{"scheduleId":"S","schedule":{"intervals":[{"every":"1ms"}]}}`,
				`{"scheduleId":"S","schedule":{}}`,
			} {
				var spec ScheduleSpec

				Expect(json.Unmarshal([]byte(raw), &spec)).To(Succeed(),
					"%s must decode, whatever the policy says about it", raw)
				Expect(spec.Validate()).To(HaveOccurred(),
					"%s should be refused when asked for", raw)
			}
		})

		It("should decode a duration written with the day shorthand", func() {
			var spec ScheduleSpec

			raw := `{"scheduleId":"S","schedule":{"intervals":[{"every":"1d","offset":"6h"}]}}`
			Expect(json.Unmarshal([]byte(raw), &spec)).To(Succeed())

			Expect(spec.Schedule.Intervals[0].Every.Duration).To(Equal(24 * time.Hour))
			Expect(spec.Schedule.Intervals[0].Offset.Duration).To(Equal(6 * time.Hour))
		})

		It("should keep an omitted optional field nil", func() {
			// The distinction the whole state model rests on: omitted is not
			// false, it is unmanaged.
			var spec ScheduleSpec

			raw := `{"scheduleId":"S","schedule":{"cron":["@daily"]}}`
			Expect(json.Unmarshal([]byte(raw), &spec)).To(Succeed())

			Expect(spec.State).To(BeNil())
			Expect(spec.Notes).To(BeNil())
			Expect(spec.Policies).To(BeNil())
			Expect(spec.Memo).To(BeNil())
			Expect(spec.SearchAttributes).To(BeNil())
		})

		It("should tell a declared false apart from an omitted one", func() {
			var spec ScheduleSpec

			raw := `{"scheduleId":"S","schedule":{"cron":["@daily"]},"state":{"paused":false}}`
			Expect(json.Unmarshal([]byte(raw), &spec)).To(Succeed())

			Expect(spec.State).NotTo(BeNil())
			Expect(spec.State.Paused).NotTo(BeNil())
			Expect(*spec.State.Paused).To(BeFalse())
		})
	})
})

var _ = Describe("Cron expressions", func() {
	// The grammar is the Service's own parseCronString and makeRange. These
	// specs exist because a general-purpose cron library would differ from
	// Temporal in ways that matter - field counts, shorthands, "@every" - and
	// wrongly rejecting a schedule Temporal would accept is worse than not
	// checking at all.
	DescribeTable(
		"should accept what Temporal accepts",
		func(expression string) {
			Expect(ValidateCronExpression(expression)).To(Succeed(), expression)
		},
		Entry("five fields", "30 2 * * *"),
		Entry("six fields with a year", "30 2 * * * 2026"),
		Entry("seven fields with seconds", "0 30 2 * * * 2026"),
		Entry("a range", "0 12 * * MON-WED"),
		Entry("a list", "0 9,12,17 * * *"),
		Entry("a step", "*/15 * * * *"),
		Entry("a range with a step", "0-30/5 * * * *"),
		Entry("a list of ranges and steps", "1-5/2,8-16/3,2 * * * *"),
		Entry("month names", "0 0 1 JAN,JUL *"),
		Entry("full month names", "0 0 1 january *"),
		Entry("day names", "0 12 * * MON-FRI"),
		Entry("day names, two letters", "0 12 * * SU"),
		// Sunday is both 0 and 7 in cron text, which the Service folds together.
		Entry("Sunday as zero", "0 12 * * 0"),
		Entry("Sunday as seven", "0 12 * * 7"),
		Entry("shorthand @daily", "@daily"),
		Entry("shorthand @hourly", "@hourly"),
		Entry("shorthand @weekly", "@weekly"),
		Entry("shorthand @monthly", "@monthly"),
		Entry("shorthand @yearly", "@yearly"),
		Entry("shorthand @annually", "@annually"),
		Entry("shorthand @midnight", "@midnight"),
		Entry("@every with a duration", "@every 1h"),
		Entry("@every with a phase", "@every 14h/3h"),
		Entry("a time zone prefix", "CRON_TZ=Europe/London 30 2 * * *"),
		Entry("the short time zone prefix", "TZ=UTC 30 2 * * *"),
		Entry("a trailing comment", "30 2 * * * # nightly reconciliation"),
		Entry("surrounding whitespace", "  30 2 * * *  "),
	)

	DescribeTable(
		"should refuse what Temporal refuses",
		func(expression string) {
			Expect(ValidateCronExpression(expression)).To(MatchError(ErrInvalidCron), expression)
		},
		Entry("empty", ""),
		Entry("only whitespace", "   "),
		Entry("four fields", "30 2 * *"),
		Entry("eight fields", "0 30 2 * * * 2026 extra"),
		Entry("a minute past the bound", "60 2 * * *"),
		Entry("an hour past the bound", "0 24 * * *"),
		Entry("a day of month of zero", "0 0 0 * *"),
		Entry("a month past the bound", "0 0 1 13 *"),
		Entry("a day of week past the bound", "0 12 * * 8"),
		Entry("a year before the bound", "30 2 * * * 1999"),
		Entry("a year past the bound", "30 2 * * * 2101"),
		Entry("two slashes", "3/5/7 * * * *"),
		Entry("a missing step", "5/ * * * *"),
		Entry("a zero step", "*/0 * * * *"),
		Entry("a negative step", "*/-1 * * * *"),
		Entry("a non-numeric step", "*/x * * * *"),
		Entry("two dashes", "1-5-7 * * * *"),
		Entry("a backwards range", "30-10 2 * * *"),
		Entry("a word where a number belongs", "abc 2 * * *"),
		Entry("a month name in the hour field", "0 JAN * * *"),
		Entry("an unknown shorthand", "@fortnightly"),
		Entry("@every with no interval", "@every"),
		Entry("@every with rubbish", "@every soon"),
		Entry("@every under a second", "@every 500ms"),
		Entry("@every with a phase as long as the interval", "@every 1h/1h"),
		Entry("@every with a negative phase", "@every 1h/-1m"),
		Entry("a time zone with no fields", "CRON_TZ=Europe/London"),
		Entry("an unknown time zone", "CRON_TZ=Mars/Olympus_Mons 30 2 * * *"),
		Entry("an empty time zone", "TZ= 30 2 * * *"),
	)

	It("should read the shorthands the way Temporal expands them", func() {
		// Each shorthand is the expression the Service substitutes, so the
		// tables cannot drift apart without one of these failing.
		Expect(expandCronShorthand("@yearly")).To(Equal("0 0 1 1 *"))
		Expect(expandCronShorthand("@annually")).To(Equal("0 0 1 1 *"))
		Expect(expandCronShorthand("@monthly")).To(Equal("0 0 1 * *"))
		Expect(expandCronShorthand("@weekly")).To(Equal("0 0 * * 0"))
		Expect(expandCronShorthand("@daily")).To(Equal("0 0 * * *"))
		Expect(expandCronShorthand("@midnight")).To(Equal("0 0 * * *"))
		Expect(expandCronShorthand("@hourly")).To(Equal("0 * * * *"))
		Expect(expandCronShorthand("30 2 * * *")).To(Equal("30 2 * * *"))
	})

	It("should read seconds only in the seven-field form", func() {
		// A five-field expression has no seconds field, so a value that would
		// be a valid second is read as a minute - and 60 is not one.
		Expect(ValidateCronExpression("0 30 2 * * * 2026")).To(Succeed())
		Expect(ValidateCronExpression("60 30 2 * * * 2026")).To(MatchError(ErrInvalidCron))
	})
})
