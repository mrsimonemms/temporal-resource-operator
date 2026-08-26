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
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/util/yaml"
)

// crd is the slice of a CustomResourceDefinition this suite cares about. The
// full schema is controller-gen's business; what matters here is that the
// operator ships exactly one API version, and that it is the one this package
// declares.
type crd struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		Group string `json:"group"`
		Scope string `json:"scope"`
		Names struct {
			Plural     string   `json:"plural"`
			Kind       string   `json:"kind"`
			ShortNames []string `json:"shortNames"`
		} `json:"names"`
		Conversion *struct {
			Strategy string `json:"strategy"`
		} `json:"conversion"`
		Versions []struct {
			Name    string `json:"name"`
			Served  bool   `json:"served"`
			Storage bool   `json:"storage"`
			Schema  struct {
				OpenAPIV3Schema struct {
					Properties struct {
						Spec struct {
							Properties map[string]struct {
								Type         string `json:"type"`
								Pattern      string `json:"pattern"`
								MaxLength    *int64 `json:"maxLength"`
								Default      any    `json:"default"`
								XValidations []struct {
									Rule    string `json:"rule"`
									Message string `json:"message"`
								} `json:"x-kubernetes-validations"`
							} `json:"properties"`
						} `json:"spec"`
					} `json:"properties"`
				} `json:"openAPIV3Schema"`
			} `json:"schema"`
		} `json:"versions"`
	} `json:"spec"`
}

// crdBases is where "make manifests" writes the generated definitions.
const crdBases = "../../config/crd/bases"

// expected maps each generated CRD file to the short names it must keep.
// Connection deliberately has none.
var expected = map[string][]string{
	"temporal.simonemms.com_connections.yaml":      nil,
	"temporal.simonemms.com_namespaces.yaml":       {"tns"},
	"temporal.simonemms.com_searchattributes.yaml": {"tsa"},
	"temporal.simonemms.com_nexusendpoints.yaml":   {"tnx"},
	"temporal.simonemms.com_schedules.yaml":        {"tsc"},
}

var _ = Describe("Generated CRDs", func() {
	It("should generate one file per resource and no others", func() {
		entries, err := os.ReadDir(crdBases)
		Expect(err).NotTo(HaveOccurred())

		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}

		Expect(names).To(HaveLen(len(expected)))
		for name := range expected {
			Expect(names).To(ContainElement(name))
		}
	})

	for file, shortNames := range expected {
		Context(file, func() {
			var (
				raw    []byte
				parsed crd
			)

			BeforeEach(func() {
				var err error
				raw, err = os.ReadFile(filepath.Join(crdBases, file))
				Expect(err).NotTo(HaveOccurred())
				Expect(yaml.Unmarshal(raw, &parsed)).To(Succeed())
			})

			It("should serve exactly one version, and store it", func() {
				Expect(parsed.Spec.Versions).To(HaveLen(1))
				Expect(parsed.Spec.Versions[0].Name).To(Equal(GroupVersion.Version))
				Expect(parsed.Spec.Versions[0].Name).To(Equal("v1beta1"))
				Expect(parsed.Spec.Versions[0].Served).To(BeTrue())
				Expect(parsed.Spec.Versions[0].Storage).To(BeTrue())
			})

			It("should mention no other API version anywhere", func() {
				Expect(string(raw)).NotTo(ContainSubstring("v1alpha1"))
			})

			It("should need no conversion, there being one version", func() {
				if parsed.Spec.Conversion != nil {
					Expect(parsed.Spec.Conversion.Strategy).To(Equal("None"))
				}
			})

			It("should keep its group, scope and short names", func() {
				Expect(parsed.Spec.Group).To(Equal(GroupVersion.Group))
				Expect(parsed.Spec.Scope).To(Equal("Namespaced"))
				Expect(parsed.Spec.Names.ShortNames).To(Equal(shortNames))
			})
		})
	}
})

// archivalSchema is the slice of the Namespace CRD describing spec.archival. It
// is parsed separately from crd because it is the only part of the schema with
// nested objects and required fields worth asserting on.
type archivalSchema struct {
	Spec struct {
		Versions []struct {
			Schema struct {
				OpenAPIV3Schema struct {
					Properties struct {
						Spec struct {
							Properties struct {
								Archival struct {
									Type       string   `json:"type"`
									Required   []string `json:"required"`
									Properties map[string]struct {
										Type       string   `json:"type"`
										Required   []string `json:"required"`
										Properties map[string]struct {
											Type      string `json:"type"`
											MinLength *int64 `json:"minLength"`
											Default   any    `json:"default"`
										} `json:"properties"`
									} `json:"properties"`
								} `json:"archival"`
							} `json:"properties"`
						} `json:"spec"`
					} `json:"properties"`
				} `json:"openAPIV3Schema"`
			} `json:"schema"`
		} `json:"versions"`
	} `json:"spec"`
}

var _ = Describe("The generated Namespace CRD", func() {
	var parsed crd

	BeforeEach(func() {
		raw, err := os.ReadFile(filepath.Join(crdBases, "temporal.simonemms.com_namespaces.yaml"))
		Expect(err).NotTo(HaveOccurred())
		Expect(yaml.Unmarshal(raw, &parsed)).To(Succeed())
	})

	// Everything the operator does about Archival rests on being able to tell
	// "not managed" from "managed, and off". That distinction is the schema's to
	// keep: a block admitted without `enabled` would decode to the zero value
	// and read as an explicit request to disable, which is the one mistake the
	// API must not let a user make by accident.
	Describe("spec.archival", func() {
		var archival archivalSchema

		BeforeEach(func() {
			raw, err := os.ReadFile(filepath.Join(crdBases, "temporal.simonemms.com_namespaces.yaml"))
			Expect(err).NotTo(HaveOccurred())
			Expect(yaml.Unmarshal(raw, &archival)).To(Succeed())
			Expect(archival.Spec.Versions).To(HaveLen(1))
		})

		// block is one of the two kinds, which have to stay identical to each
		// other: history and visibility are the same shape applied to different
		// data.
		block := func(kind string) struct {
			Type       string   `json:"type"`
			Required   []string `json:"required"`
			Properties map[string]struct {
				Type      string `json:"type"`
				MinLength *int64 `json:"minLength"`
				Default   any    `json:"default"`
			} `json:"properties"`
		} {
			props := archival.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties.Spec.Properties.Archival.Properties
			Expect(props).To(HaveKey(kind))

			return props[kind]
		}

		It("should be an optional object, so archival can go unmanaged", func() {
			schema := archival.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties.Spec.Properties.Archival

			Expect(schema.Type).To(Equal("object"))
			Expect(schema.Required).To(BeEmpty(),
				"neither kind may be required, or every Namespace would have to manage archival")
		})

		It("should carry both kinds, and nothing else", func() {
			props := archival.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties.Spec.Properties.Archival.Properties

			Expect(props).To(HaveLen(2))
			Expect(props).To(HaveKey("history"))
			Expect(props).To(HaveKey("visibility"))
		})

		for _, kind := range []string{"history", "visibility"} {
			Context(kind, func() {
				It("should require enabled once the block is present", func() {
					Expect(block(kind).Type).To(Equal("object"))
					Expect(block(kind).Required).To(Equal([]string{"enabled"}))
				})

				It("should keep enabled a boolean with no default", func() {
					enabled := block(kind).Properties["enabled"]

					Expect(enabled.Type).To(Equal("boolean"))
					Expect(enabled.Default).To(BeNil(),
						"a default would make archival managed by accident")
				})

				It("should keep uri an optional non-empty string", func() {
					uri := block(kind).Properties["uri"]

					Expect(uri.Type).To(Equal("string"))
					Expect(block(kind).Required).NotTo(ContainElement("uri"),
						"Temporal supplies a URI when a namespace enables archival without one")
					Expect(uri.MinLength).NotTo(BeNil())
					Expect(*uri.MinLength).To(BeNumerically("==", 1))
					Expect(uri.Default).To(BeNil(),
						"the Service's own default is the only default there is")
				})

				It("should put no scheme restriction on uri", func() {
					// Which URI schemes work depends entirely on the archivers
					// the Temporal Service is configured with, so the schema
					// must not have an opinion. It would only ever be wrong for
					// somebody.
					uri := block(kind).Properties["uri"]

					Expect(uri.Type).To(Equal("string"))
					Expect(block(kind).Properties).To(HaveLen(2))
				})
			})
		}
	})

	// The retention field is the reason Duration exists. If the schema stops
	// carrying the pattern, or carries a different one, the API server and the
	// operator can disagree again about what a duration is - and a value only
	// the API server accepts is one that breaks the Namespace informer.
	Describe("spec.retention", func() {
		var retention struct {
			Type         string `json:"type"`
			Pattern      string `json:"pattern"`
			MaxLength    *int64 `json:"maxLength"`
			Default      any    `json:"default"`
			XValidations []struct {
				Rule    string `json:"rule"`
				Message string `json:"message"`
			} `json:"x-kubernetes-validations"`
		}

		BeforeEach(func() {
			Expect(parsed.Spec.Versions).To(HaveLen(1))

			props := parsed.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties.Spec.Properties
			Expect(props).To(HaveKey("retention"))
			retention = props["retention"]
		})

		It("should still be a plain string in the public API", func() {
			Expect(retention.Type).To(Equal("string"))
		})

		It("should carry exactly the pattern the Go parser enforces", func() {
			Expect(retention.Pattern).To(Equal(DurationPattern))
		})

		It("should carry the same length bound the Go parser enforces", func() {
			Expect(retention.MaxLength).NotTo(BeNil())
			Expect(*retention.MaxLength).To(BeNumerically("==", DurationMaxLength))
		})

		It("should keep defaulting to 72h", func() {
			Expect(retention.Default).To(Equal("72h"))
		})

		It("should accept the documented durations and reject the malformed ones", func() {
			// The schema is only as good as the regular expression in it, so
			// check that expression directly - the same one Kubernetes will
			// apply - rather than trusting that it was generated from the right
			// constant.
			schemaRE := regexp.MustCompile(retention.Pattern)

			for _, good := range []string{"7d", "3d", "1d12h", "7d30m", "72h", "168h", "2h30m", "0s"} {
				Expect(schemaRE.MatchString(good)).To(BeTrue(), "%q should be accepted", good)

				_, err := ParseDuration(good)
				Expect(err).NotTo(HaveOccurred(), "%q should also parse", good)
			}

			for _, bad := range []string{"7days", "d", "7dd", "1d2d", "1.5d", "forever", "", "1h30"} {
				Expect(schemaRE.MatchString(bad)).To(BeFalse(), "%q should be rejected", bad)

				_, err := ParseDuration(bad)
				Expect(err).To(HaveOccurred(), "%q should also fail to parse", bad)
			}
		})

		// The 1..90 day range cannot be written as a regular expression, because
		// the same duration has many spellings. It is carried as CEL instead,
		// and these specs make sure the schema still has both halves of it -
		// admission is where an out-of-range value is meant to be stopped.
		Describe("the range rules", func() {
			It("should carry one rule for each bound", func() {
				Expect(retention.XValidations).To(HaveLen(2))
				Expect(retention.XValidations[0].Message).To(ContainSubstring("at least 1 day"))
				Expect(retention.XValidations[1].Message).To(ContainSubstring("at most 90 days"))
			})

			It("should express the bounds in whole seconds", func() {
				// 86400s is MinRetention and 7776000s is MaxRetention. CEL has
				// no way to build a duration from an integer day count, so the
				// rules compare seconds; if these numbers drift from the Go
				// constants, admission and the operator would disagree.
				Expect(retention.XValidations[0].Rule).
					To(ContainSubstring(fmt.Sprintf("%d", int64(MinRetention.Seconds()))))
				Expect(retention.XValidations[1].Rule).
					To(ContainSubstring(fmt.Sprintf("%d", int64(MaxRetention.Seconds()))))
			})

			It("should scale the day count by a day's worth of seconds", func() {
				Expect(retention.XValidations[0].Rule).
					To(ContainSubstring(fmt.Sprintf("*%d", int64(Day.Seconds()))))
			})
		})
	})
})

// scheduleSchema is the slice of the Schedule CRD these specs assert on. The
// full schema is controller-gen's business; what matters here is that the rules
// mirroring Temporal's own constraints are actually in the file the API server
// will enforce, rather than only in the Go comments that produced it.
type scheduleSchema struct {
	Spec struct {
		Versions []struct {
			Schema struct {
				OpenAPIV3Schema struct {
					Properties struct {
						Spec struct {
							Required   []string `json:"required"`
							Properties map[string]struct {
								Type         string   `json:"type"`
								MinLength    *int64   `json:"minLength"`
								MaxLength    *int64   `json:"maxLength"`
								MaxItems     *int64   `json:"maxItems"`
								Default      any      `json:"default"`
								Enum         []string `json:"enum"`
								Required     []string `json:"required"`
								XValidations []struct {
									Rule    string `json:"rule"`
									Message string `json:"message"`
								} `json:"x-kubernetes-validations"`
								Properties map[string]struct {
									Type         string   `json:"type"`
									MinLength    *int64   `json:"minLength"`
									MaxLength    *int64   `json:"maxLength"`
									MaxItems     *int64   `json:"maxItems"`
									Minimum      *float64 `json:"minimum"`
									Maximum      *float64 `json:"maximum"`
									Default      any      `json:"default"`
									Enum         []string `json:"enum"`
									Required     []string `json:"required"`
									XValidations []struct {
										Rule    string `json:"rule"`
										Message string `json:"message"`
									} `json:"x-kubernetes-validations"`
								} `json:"properties"`
							} `json:"properties"`
						} `json:"spec"`
					} `json:"properties"`
				} `json:"openAPIV3Schema"`
			} `json:"schema"`
		} `json:"versions"`
	} `json:"spec"`
}

var _ = Describe("The generated Schedule CRD", func() {
	var parsed scheduleSchema

	BeforeEach(func() {
		raw, err := os.ReadFile(filepath.Join(crdBases, "temporal.simonemms.com_schedules.yaml"))
		Expect(err).NotTo(HaveOccurred())
		Expect(yaml.Unmarshal(raw, &parsed)).To(Succeed())
		Expect(parsed.Spec.Versions).To(HaveLen(1))
	})

	spec := func() struct {
		Required   []string `json:"required"`
		Properties map[string]struct {
			Type         string   `json:"type"`
			MinLength    *int64   `json:"minLength"`
			MaxLength    *int64   `json:"maxLength"`
			MaxItems     *int64   `json:"maxItems"`
			Default      any      `json:"default"`
			Enum         []string `json:"enum"`
			Required     []string `json:"required"`
			XValidations []struct {
				Rule    string `json:"rule"`
				Message string `json:"message"`
			} `json:"x-kubernetes-validations"`
			Properties map[string]struct {
				Type         string   `json:"type"`
				MinLength    *int64   `json:"minLength"`
				MaxLength    *int64   `json:"maxLength"`
				MaxItems     *int64   `json:"maxItems"`
				Minimum      *float64 `json:"minimum"`
				Maximum      *float64 `json:"maximum"`
				Default      any      `json:"default"`
				Enum         []string `json:"enum"`
				Required     []string `json:"required"`
				XValidations []struct {
					Rule    string `json:"rule"`
					Message string `json:"message"`
				} `json:"x-kubernetes-validations"`
			} `json:"properties"`
		} `json:"properties"`
	} {
		return parsed.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties.Spec
	}

	It("should require the fields that identify the schedule and what it does", func() {
		Expect(spec().Required).To(ConsistOf(
			"scheduleId", "connectionRef", "namespaceRef", "schedule", "action",
		))
	})

	Describe("spec.scheduleId", func() {
		It("should be a non-empty string bounded by Temporal's own ID limit", func() {
			scheduleID := spec().Properties["scheduleId"]

			Expect(scheduleID.Type).To(Equal("string"))
			Expect(scheduleID.MinLength).NotTo(BeNil())
			Expect(*scheduleID.MinLength).To(BeNumerically("==", 1))
			Expect(scheduleID.MaxLength).NotTo(BeNil())
			Expect(*scheduleID.MaxLength).To(BeNumerically("==", MaxScheduleIDLength))
		})

		It("should be immutable", func() {
			// Changing it would not rename anything, it would point the resource
			// at a different schedule and strand the original.
			rules := spec().Properties["scheduleId"].XValidations
			Expect(rules).NotTo(BeEmpty())
			Expect(rules[0].Rule).To(Equal("self == oldSelf"))
			Expect(rules[0].Message).To(ContainSubstring("immutable"))
		})
	})

	DescribeTable(
		"should make the dependency references required and immutable",
		func(field string) {
			rules := spec().Properties[field].XValidations
			Expect(rules).To(HaveLen(2))

			var messages []string
			for _, rule := range rules {
				messages = append(messages, rule.Message)
			}

			Expect(messages).To(ContainElement(ContainSubstring("required")))
			Expect(messages).To(ContainElement(ContainSubstring("immutable")))
		},
		Entry("connectionRef", "connectionRef"),
		Entry("namespaceRef", "namespaceRef"),
	)

	Describe("spec.schedule", func() {
		It("should insist on at least one rule", func() {
			// A timing specification with no calendars, intervals or cron would
			// never act at all, so it is a mistake rather than a schedule.
			rules := spec().Properties["schedule"].XValidations
			Expect(rules).To(HaveLen(1))
			Expect(rules[0].Rule).To(ContainSubstring("has(self.calendars)"))
			Expect(rules[0].Rule).To(ContainSubstring("has(self.intervals)"))
			Expect(rules[0].Rule).To(ContainSubstring("has(self.cron)"))
			Expect(rules[0].Message).To(ContainSubstring("at least one"))
		})

		DescribeTable(
			"should bound each collection",
			func(field string) {
				property := spec().Properties["schedule"].Properties[field]

				Expect(property.Type).To(Equal("array"))
				Expect(property.MaxItems).NotTo(BeNil(), field)
				Expect(*property.MaxItems).To(BeNumerically("==", MaxScheduleSpecItems))
			},
			Entry("calendars", "calendars"),
			Entry("intervals", "intervals"),
			Entry("cron", "cron"),
			Entry("excludeCalendars", "excludeCalendars"),
		)
	})

	Describe("spec.policies", func() {
		It("should offer exactly Temporal's overlap policies", func() {
			overlap := spec().Properties["policies"].Properties["overlap"]

			Expect(overlap.Enum).To(ConsistOf(
				"Skip", "BufferOne", "BufferAll", "CancelOther", "TerminateOther", "AllowAll",
			))
		})

		It("should default the overlap policy to Temporal's own default", func() {
			Expect(spec().Properties["policies"].Properties["overlap"].Default).To(Equal("Skip"))
		})
	})

	Describe("spec.state", func() {
		It("should default nothing, so an omitted field stays unmanaged", func() {
			// The whole state model rests on this: a default would turn
			// "somebody may pause this by hand" into "the operator resumes it on
			// the next resync".
			state := spec().Properties["state"]

			Expect(state.Required).To(BeEmpty())
			Expect(state.Properties["paused"].Type).To(Equal("boolean"))
			Expect(state.Properties["paused"].Default).To(BeNil())
			Expect(state.Properties["limitedActions"].Default).To(BeNil())
		})

		It("should refuse a limited action count below one", func() {
			limited := spec().Properties["state"].Properties["limitedActions"]

			Expect(limited.Minimum).NotTo(BeNil())
			Expect(*limited.Minimum).To(BeNumerically("==", 1))
		})
	})

	It("should default the deletion policy to Delete", func() {
		deletionPolicy := spec().Properties["deletionPolicy"]

		Expect(deletionPolicy.Enum).To(ConsistOf("Delete", "Orphan"))
		Expect(deletionPolicy.Default).To(Equal("Delete"))
	})

	It("should bound the schedule's own search attributes", func() {
		searchAttributes := spec().Properties["searchAttributes"]

		Expect(searchAttributes.Type).To(Equal("array"))
		Expect(searchAttributes.MaxItems).NotTo(BeNil())
	})

	Describe("the rules that mirror Temporal's numeric limits", func() {
		// These live deep in the schema, so rather than modelling every level
		// they are asserted against the raw document. What matters is that the
		// rule is present and says the right number - a rule that silently
		// stopped being generated would be invisible otherwise.
		var raw string

		BeforeEach(func() {
			content, err := os.ReadFile(filepath.Join(crdBases, "temporal.simonemms.com_schedules.yaml"))
			Expect(err).NotTo(HaveOccurred())
			raw = string(content)
		})

		DescribeTable(
			"should carry the calendar bounds the Service enforces",
			func(rule string) {
				Expect(raw).To(ContainSubstring(rule))
			},
			Entry("second", "r.start >= 0 && r.start <= 59"),
			Entry("hour", "r.start >= 0 && r.start <= 23"),
			Entry("dayOfMonth", "r.start >= 1 && r.start <= 31"),
			Entry("month", "r.start >= 1 && r.start <= 12"),
			Entry("dayOfWeek", "r.start >= 0 && r.start <= 6"),
			Entry("year", "r.start >= 2000 && r.start <= 2100"),
		)

		It("should measure the fairness key in bytes", func() {
			// Temporal's limit is 64 bytes, not 64 characters, so the rule has
			// to convert before measuring - size(self) would be wrong.
			Expect(raw).To(ContainSubstring("size(bytes(self)) <= 64"))
		})

		It("should say that day of week counts Sunday as zero", func() {
			// Getting this backwards is the easiest mistake to make with a
			// calendar spec, so the rejection message says which way round it
			// is. The assertion allows for YAML folding the long message across
			// lines, which is why it looks for the phrase rather than the whole
			// sentence.
			Expect(raw).To(ContainSubstring("0 is Sunday"))
		})
	})
})
