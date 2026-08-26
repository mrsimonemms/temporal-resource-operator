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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	temporalv1beta1 "github.com/mrsimonemms/temporal-resource-operator/api/v1beta1"
)

// These specs talk to the envtest API server with the generated CRD loaded, so
// they exercise real admission against the real schema rather than the Go type
// alone. Everything goes through unstructured objects on purpose: sending a
// malformed Schedule through the typed client would fail locally and prove
// nothing about what Kubernetes will store.
//
// The philosophy they encode is the one the Temporal UI already follows - if a
// schedule is obviously wrong, say so at the point somebody writes it, not four
// layers later when the Service refuses it.
var _ = Describe("Schedule admission", func() {
	const namespace = "default"

	var (
		name           string
		connectionName string
	)

	BeforeEach(func() {
		// Unique names per spec keep the specs independent, and the prefixes are
		// this suite's own so that a spec sharing a line number with one in
		// another file cannot collide with it.
		suffix := GinkgoRandomSeed() + int64(CurrentSpecReport().LineNumber())
		name = fmt.Sprintf("sa-%d", suffix)
		connectionName = fmt.Sprintf("sa-conn-%d", suffix)
	})

	// validSpec is the smallest Schedule the API server accepts. Each spec below
	// starts from it and breaks exactly one thing, so a rejection can only be
	// about the thing under test.
	validSpec := func() map[string]any {
		return map[string]any{
			"scheduleId":    "PaymentsNightly",
			"connectionRef": map[string]any{"name": connectionName},
			"namespaceRef":  map[string]any{"name": "payments"},
			"schedule": map[string]any{
				"cron": []any{"30 2 * * *"},
			},
			"action": map[string]any{
				"workflow": map[string]any{
					"type":      "ReconcilePayments",
					"taskQueue": "payments",
				},
			},
		}
	}

	offer := func(spec map[string]any) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "temporal.simonemms.com/v1beta1",
			"kind":       "Schedule",
			"metadata": map[string]any{
				"name":      name,
				"namespace": namespace,
			},
			"spec": spec,
		}}
	}

	// withSpec builds a Schedule from the valid spec with one part replaced.
	withSpec := func(mutate func(spec map[string]any)) *unstructured.Unstructured {
		spec := validSpec()
		mutate(spec)

		return offer(spec)
	}

	stored := func() *temporalv1beta1.Schedule {
		schedule := &temporalv1beta1.Schedule{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, schedule)).
			To(Succeed())

		return schedule
	}

	AfterEach(func() {
		// Schedules created here carry no finalizer, because no reconciler runs
		// against them.
		_ = k8sClient.Delete(ctx, offer(validSpec()))
	})

	Describe("what Kubernetes stores", func() {
		It("should admit the smallest usable schedule", func() {
			Expect(k8sClient.Create(ctx, offer(validSpec()))).To(Succeed())

			schedule := stored()
			Expect(schedule.Spec.TemporalName()).To(Equal("PaymentsNightly"))
			Expect(schedule.Spec.Schedule.Cron).To(Equal([]string{"30 2 * * *"}))
		})

		It("should apply the documented defaults", func() {
			Expect(k8sClient.Create(ctx, offer(validSpec()))).To(Succeed())

			// deletionPolicy defaults; the state fields deliberately do not, so
			// that omitting one leaves it unmanaged.
			schedule := stored()
			Expect(schedule.Spec.DeletionPolicy).To(Equal(temporalv1beta1.ScheduleDeletionPolicyDelete))
			Expect(schedule.Spec.State).To(BeNil())
			Expect(schedule.Spec.Notes).To(BeNil())
			Expect(schedule.Spec.Policies).To(BeNil())
		})

		It("should default overlap only once a policies block exists", func() {
			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["policies"] = map[string]any{"pauseOnFailure": true}
			}))).To(Succeed())

			schedule := stored()
			Expect(schedule.Spec.Policies).NotTo(BeNil())
			Expect(schedule.Spec.Policies.Overlap).To(Equal(temporalv1beta1.ScheduleOverlapPolicySkip))
		})

		It("should keep an explicitly declared false rather than dropping it", func() {
			// The distinction the state model depends on: paused false is a
			// request to keep the schedule running, and has to survive a
			// round trip through the API server as something other than absent.
			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["state"] = map[string]any{"paused": false}
			}))).To(Succeed())

			schedule := stored()
			Expect(schedule.Spec.State).NotTo(BeNil())
			Expect(schedule.Spec.State.Paused).NotTo(BeNil())
			Expect(*schedule.Spec.State.Paused).To(BeFalse())
		})

		It("should admit every combination of timing rules", func() {
			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["schedule"] = map[string]any{
					"cron":      []any{"@daily"},
					"intervals": []any{map[string]any{"every": "6h", "offset": "5h"}},
					"calendars": []any{map[string]any{
						"hour":      []any{map[string]any{"start": int64(9)}},
						"dayOfWeek": []any{map[string]any{"start": int64(1), "end": int64(5)}},
					}},
					"excludeCalendars": []any{map[string]any{
						"dayOfMonth": []any{map[string]any{"start": int64(1)}},
					}},
					"timeZone": "Europe/London",
					"jitter":   "5m",
				}
			}))).To(Succeed())

			schedule := stored()
			Expect(schedule.Spec.Schedule.Cron).To(HaveLen(1))
			Expect(schedule.Spec.Schedule.Intervals).To(HaveLen(1))
			Expect(schedule.Spec.Schedule.Calendars).To(HaveLen(1))
			Expect(schedule.Spec.Schedule.ExcludeCalendars).To(HaveLen(1))
		})

		It("should decode a duration written with the day shorthand", func() {
			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["schedule"] = map[string]any{
					"intervals": []any{map[string]any{"every": "1d"}},
				}
			}))).To(Succeed())

			// The lesson from the Namespace retention outage: a duration the API
			// server stores has to be one the typed client can decode, or the
			// informer behind the controller dies.
			schedule := stored()
			Expect(schedule.Spec.Schedule.Intervals[0].Every.Duration.Hours()).To(BeNumerically("==", 24))
		})
	})

	Describe("what Kubernetes refuses", func() {
		DescribeTable(
			"missing required fields",
			func(mutate func(spec map[string]any)) {
				Expect(k8sClient.Create(ctx, withSpec(mutate))).NotTo(Succeed())
			},
			Entry("no scheduleId", func(spec map[string]any) { delete(spec, "scheduleId") }),
			Entry("an empty scheduleId", func(spec map[string]any) { spec["scheduleId"] = "" }),
			Entry("no connectionRef", func(spec map[string]any) { delete(spec, "connectionRef") }),
			Entry("an empty connectionRef name", func(spec map[string]any) {
				spec["connectionRef"] = map[string]any{"name": ""}
			}),
			Entry("no namespaceRef", func(spec map[string]any) { delete(spec, "namespaceRef") }),
			Entry("an empty namespaceRef name", func(spec map[string]any) {
				spec["namespaceRef"] = map[string]any{"name": ""}
			}),
			Entry("no schedule", func(spec map[string]any) { delete(spec, "schedule") }),
			Entry("no action", func(spec map[string]any) { delete(spec, "action") }),
			Entry("an action with no workflow", func(spec map[string]any) {
				spec["action"] = map[string]any{}
			}),
			Entry("a workflow with no type", func(spec map[string]any) {
				spec["action"] = map[string]any{"workflow": map[string]any{"taskQueue": "payments"}}
			}),
			Entry("a workflow with no task queue", func(spec map[string]any) {
				spec["action"] = map[string]any{"workflow": map[string]any{"type": "Reconcile"}}
			}),
			Entry("an empty workflow type", func(spec map[string]any) {
				spec["action"] = map[string]any{
					"workflow": map[string]any{"type": "", "taskQueue": "payments"},
				}
			}),
			Entry("an empty task queue", func(spec map[string]any) {
				spec["action"] = map[string]any{
					"workflow": map[string]any{"type": "Reconcile", "taskQueue": ""},
				}
			}),
		)

		It("should refuse a timing specification with no rules", func() {
			// The CEL rule that stops a schedule which would never act. Its
			// message has to say what to do about it.
			err := k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["schedule"] = map[string]any{"timeZone": "UTC"}
			}))

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("at least one of calendars, intervals or cron"))
		})

		It("should refuse an empty timing specification", func() {
			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["schedule"] = map[string]any{}
			}))).NotTo(Succeed())
		})

		DescribeTable(
			"empty collections, which say nothing rather than something",
			func(field string) {
				Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
					spec["schedule"] = map[string]any{field: []any{}}
				}))).NotTo(Succeed())
			},
			Entry("cron", "cron"),
			Entry("intervals", "intervals"),
			Entry("calendars", "calendars"),
		)

		It("should refuse an empty cron expression", func() {
			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["schedule"] = map[string]any{"cron": []any{""}}
			}))).NotTo(Succeed())
		})

		DescribeTable(
			"calendar values outside Temporal's own bounds",
			func(field string, value int64) {
				err := k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
					spec["schedule"] = map[string]any{
						"calendars": []any{map[string]any{
							field: []any{map[string]any{"start": value}},
						}},
					}
				}))

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(field))
			},
			Entry("second above 59", "second", int64(60)),
			Entry("second below zero", "second", int64(-1)),
			Entry("minute above 59", "minute", int64(60)),
			Entry("hour above 23", "hour", int64(24)),
			Entry("dayOfMonth of zero", "dayOfMonth", int64(0)),
			Entry("dayOfMonth above 31", "dayOfMonth", int64(32)),
			Entry("month of zero", "month", int64(0)),
			Entry("month above 12", "month", int64(13)),
			Entry("dayOfWeek above 6", "dayOfWeek", int64(7)),
			Entry("year before 2000", "year", int64(1999)),
			Entry("year after 2100", "year", int64(2101)),
		)

		DescribeTable(
			"calendar values at the bounds, which are accepted",
			func(field string, value int64) {
				Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
					spec["schedule"] = map[string]any{
						"calendars": []any{map[string]any{
							field: []any{map[string]any{"start": value}},
						}},
					}
				}))).To(Succeed())
			},
			Entry("second 0", "second", int64(0)),
			Entry("second 59", "second", int64(59)),
			Entry("hour 23", "hour", int64(23)),
			Entry("dayOfMonth 1", "dayOfMonth", int64(1)),
			Entry("dayOfMonth 31", "dayOfMonth", int64(31)),
			Entry("month 12", "month", int64(12)),
			// Sunday is 0 and Saturday is 6, Temporal's numbering.
			Entry("dayOfWeek 0", "dayOfWeek", int64(0)),
			Entry("dayOfWeek 6", "dayOfWeek", int64(6)),
			Entry("year 2000", "year", int64(2000)),
			Entry("year 2100", "year", int64(2100)),
		)

		It("should admit a range whose end is before its start, and leave it to the operator", func() {
			// The one calendar rule that is not in the schema. Comparing two
			// fields of the same object needs CEL, and a CEL rule inside these
			// lists would make Kubernetes demand a maxItems on every collection
			// containing them - which Temporal does not cap. So this is caught
			// by the operator's own validation instead, and reported as
			// InvalidSchedule.
			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["schedule"] = map[string]any{
					"calendars": []any{map[string]any{
						"hour": []any{map[string]any{"start": int64(9), "end": int64(3)}},
					}},
				}
			}))).To(Succeed())

			Expect(stored().Spec.Validate()).To(
				MatchError(ContainSubstring("end 3 is not between start 9")),
			)
		})

		It("should refuse a step below one", func() {
			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["schedule"] = map[string]any{
					"calendars": []any{map[string]any{
						"hour": []any{map[string]any{"start": int64(0), "step": int64(0)}},
					}},
				}
			}))).NotTo(Succeed())
		})

		It("should refuse a calendar comment past Temporal's limit", func() {
			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["schedule"] = map[string]any{
					"calendars": []any{map[string]any{
						"hour":    []any{map[string]any{"start": int64(9)}},
						"comment": string(make([]byte, 201)),
					}},
				}
			}))).NotTo(Succeed())
		})

		It("should refuse an overlap policy Temporal does not have", func() {
			err := k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["policies"] = map[string]any{"overlap": "BufferSome"}
			}))

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Unsupported value"))
		})

		DescribeTable(
			"every overlap policy Temporal does have",
			func(policy string) {
				Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
					spec["policies"] = map[string]any{"overlap": policy}
				}))).To(Succeed())
			},
			Entry("Skip", "Skip"),
			Entry("BufferOne", "BufferOne"),
			Entry("BufferAll", "BufferAll"),
			Entry("CancelOther", "CancelOther"),
			Entry("TerminateOther", "TerminateOther"),
			Entry("AllowAll", "AllowAll"),
		)

		It("should refuse a priority key outside the default range", func() {
			for _, key := range []int64{0, 6} {
				Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
					spec["action"] = map[string]any{"workflow": map[string]any{
						"type":      "Reconcile",
						"taskQueue": "payments",
						"priority":  map[string]any{"key": key},
					}}
				}))).NotTo(Succeed(), "key %d", key)
			}
		})

		It("should refuse a fairness key over 64 bytes", func() {
			// The rule measures bytes, because Temporal's limit is bytes. A
			// 24-character multi-byte key is under the limit in characters and
			// well over it in bytes, which is exactly the case a naive
			// size(self) rule would let through.
			err := k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["action"] = map[string]any{"workflow": map[string]any{
					"type":      "Reconcile",
					"taskQueue": "payments",
					"priority": map[string]any{
						"fairnessKey": "日本語日本語日本語日本語日本語日本語日本語日本語",
					},
				}}
			}))

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("64 bytes"))
		})

		It("should accept a fairness key that fits in 64 bytes", func() {
			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["action"] = map[string]any{"workflow": map[string]any{
					"type":      "Reconcile",
					"taskQueue": "payments",
					"priority":  map[string]any{"fairnessKey": "tenant-acme"},
				}}
			}))).To(Succeed())
		})

		DescribeTable(
			"a fairness weight that is not a plain decimal",
			func(weight string) {
				Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
					spec["action"] = map[string]any{"workflow": map[string]any{
						"type":      "Reconcile",
						"taskQueue": "payments",
						"priority":  map[string]any{"fairnessWeight": weight},
					}}
				}))).NotTo(Succeed(), weight)
			},
			Entry("a word", "heavy"),
			Entry("negative", "-1"),
			Entry("exponent notation", "1e3"),
			Entry("a leading dot", ".5"),
		)

		It("should refuse a schedule ID past Temporal's limit", func() {
			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["scheduleId"] = string(make([]byte, temporalv1beta1.MaxScheduleIDLength+1))
			}))).NotTo(Succeed())
		})

		It("should refuse a workflow ID past Temporal's limit", func() {
			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["action"] = map[string]any{"workflow": map[string]any{
					"type":       "Reconcile",
					"taskQueue":  "payments",
					"workflowId": string(make([]byte, temporalv1beta1.MaxWorkflowIDLength+1)),
				}}
			}))).NotTo(Succeed())
		})

		It("should refuse a duration the operator could not decode", func() {
			// The Namespace retention lesson applied to every duration on a
			// Schedule: a value only the API server accepts is one that breaks
			// the informer.
			for _, every := range []string{"7days", "1d2d", "soon", "1.5d"} {
				Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
					spec["schedule"] = map[string]any{
						"intervals": []any{map[string]any{"every": every}},
					}
				}))).NotTo(Succeed(), every)
			}
		})
	})

	Describe("collections Temporal does not limit", func() {
		// The pinned Service imposes no count limit on calendars, intervals or
		// cron strings. An operator cap would therefore refuse a schedule
		// Temporal would have accepted, which is the operator inventing policy.
		// Fifty-one entries is one past the cap that used to be here.
		const beyondTheOldCap = 51

		It("should admit fifty-one cron expressions", func() {
			cron := make([]any, beyondTheOldCap)
			for i := range cron {
				cron[i] = fmt.Sprintf("%d 2 * * *", i)
			}

			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["schedule"] = map[string]any{"cron": cron}
			}))).To(Succeed())

			Expect(stored().Spec.Schedule.Cron).To(HaveLen(beyondTheOldCap))
		})

		It("should admit fifty-one calendars", func() {
			calendars := make([]any, beyondTheOldCap)
			for i := range calendars {
				calendars[i] = map[string]any{
					"minute": []any{map[string]any{"start": int64(i % 60)}},
				}
			}

			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["schedule"] = map[string]any{"calendars": calendars}
			}))).To(Succeed())

			Expect(stored().Spec.Schedule.Calendars).To(HaveLen(beyondTheOldCap))
		})

		It("should admit fifty-one intervals", func() {
			intervals := make([]any, beyondTheOldCap)
			for i := range intervals {
				intervals[i] = map[string]any{"every": fmt.Sprintf("%dm", i+1)}
			}

			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["schedule"] = map[string]any{"intervals": intervals}
			}))).To(Succeed())

			Expect(stored().Spec.Schedule.Intervals).To(HaveLen(beyondTheOldCap))
		})

		It("should admit fifty-one exclusions, inputs and search attributes", func() {
			excludeCalendars := make([]any, beyondTheOldCap)
			input := make([]any, beyondTheOldCap)
			searchAttributes := make([]any, beyondTheOldCap)

			for i := range beyondTheOldCap {
				excludeCalendars[i] = map[string]any{
					"minute": []any{map[string]any{"start": int64(i % 60)}},
				}
				input[i] = fmt.Sprintf("%d", i)
				searchAttributes[i] = map[string]any{
					"name":  fmt.Sprintf("Attr%d", i),
					"type":  "Int",
					"value": fmt.Sprintf("%d", i),
				}
			}

			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["schedule"] = map[string]any{
					"cron":             []any{"@daily"},
					"excludeCalendars": excludeCalendars,
				}
				spec["searchAttributes"] = searchAttributes
				spec["action"] = map[string]any{"workflow": map[string]any{
					"type":             "ReconcilePayments",
					"taskQueue":        "payments",
					"input":            input,
					"searchAttributes": searchAttributes,
				}}
			}))).To(Succeed())

			schedule := stored()
			Expect(schedule.Spec.Schedule.ExcludeCalendars).To(HaveLen(beyondTheOldCap))
			Expect(schedule.Spec.Action.Workflow.Input).To(HaveLen(beyondTheOldCap))
			Expect(schedule.Spec.SearchAttributes).To(HaveLen(beyondTheOldCap))
		})

		It("should still refuse an invalid entry among fifty-one valid ones", func() {
			// Removing the count limit removes nothing else. Each entry is
			// checked exactly as it was.
			calendars := make([]any, beyondTheOldCap)
			for i := range calendars {
				calendars[i] = map[string]any{
					"minute": []any{map[string]any{"start": int64(i % 60)}},
				}
			}

			calendars[beyondTheOldCap-1] = map[string]any{
				"hour": []any{map[string]any{"start": int64(24)}},
			}

			err := k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["schedule"] = map[string]any{"calendars": calendars}
			}))

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("should be less than or equal to 23"))
		})

		It("should still insist on at least one rule, however many there could be", func() {
			err := k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["schedule"] = map[string]any{"timeZone": "UTC"}
			}))

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("at least one of calendars, intervals or cron"))
		})
	})

	Describe("what the API no longer offers", func() {
		It("should prune a remaining-action count rather than storing it", func() {
			// Structural schema pruning is what proves the field is gone: an
			// unknown key is dropped on the way in rather than kept, so nothing
			// can go on depending on it.
			Expect(k8sClient.Create(ctx, withSpec(func(spec map[string]any) {
				spec["state"] = map[string]any{"paused": false, "limitedActions": int64(10)}
			}))).To(Succeed())

			schedule := stored()
			Expect(schedule.Spec.State).NotTo(BeNil())
			Expect(schedule.Spec.State.Paused).NotTo(BeNil())
			Expect(*schedule.Spec.State.Paused).To(BeFalse())

			// And the API server did not keep it.
			raw := &unstructured.Unstructured{}
			raw.SetGroupVersionKind(offer(validSpec()).GroupVersionKind())
			Expect(k8sClient.Get(ctx,
				types.NamespacedName{Name: name, Namespace: namespace}, raw)).To(Succeed())

			state, found, err := unstructured.NestedMap(raw.Object, "spec", "state")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(state).To(HaveKey("paused"))
			Expect(state).NotTo(HaveKey("limitedActions"))
			Expect(state).NotTo(HaveKey("remainingActions"))
		})
	})

	Describe("what cannot be changed after creation", func() {
		// All three identify the external schedule. Changing one would not move
		// anything; it would point the resource at a different schedule and
		// strand whatever it was looking after.
		DescribeTable(
			"immutable fields",
			func(mutate func(spec map[string]any), expected string) {
				Expect(k8sClient.Create(ctx, offer(validSpec()))).To(Succeed())

				updated := stored()

				patch := offer(validSpec())
				patch.SetResourceVersion(updated.ResourceVersion)

				spec, _, err := unstructured.NestedMap(patch.Object, "spec")
				Expect(err).NotTo(HaveOccurred())
				mutate(spec)
				Expect(unstructured.SetNestedMap(patch.Object, spec, "spec")).To(Succeed())

				err = k8sClient.Update(ctx, patch)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(expected))
			},
			Entry("scheduleId", func(spec map[string]any) {
				spec["scheduleId"] = "SomethingElse"
			}, "scheduleId is immutable"),
			Entry("connectionRef", func(spec map[string]any) {
				spec["connectionRef"] = map[string]any{"name": "staging"}
			}, "connectionRef.name is immutable"),
			Entry("namespaceRef", func(spec map[string]any) {
				spec["namespaceRef"] = map[string]any{"name": "elsewhere"}
			}, "namespaceRef.name is immutable"),
		)

		It("should still allow the deletion policy to change", func() {
			// Deliberately mutable: switching a stuck resource to Orphan while
			// it is already terminating is how a deletion blocked on a broken
			// dependency is released.
			Expect(k8sClient.Create(ctx, offer(validSpec()))).To(Succeed())

			schedule := stored()
			schedule.Spec.DeletionPolicy = temporalv1beta1.ScheduleDeletionPolicyOrphan
			Expect(k8sClient.Update(ctx, schedule)).To(Succeed())

			Expect(stored().Spec.DeletionPolicy).To(Equal(temporalv1beta1.ScheduleDeletionPolicyOrphan))
		})

		It("should allow the timing and the action to change", func() {
			Expect(k8sClient.Create(ctx, offer(validSpec()))).To(Succeed())

			schedule := stored()
			schedule.Spec.Schedule.Cron = []string{"@hourly"}
			schedule.Spec.Action.Workflow.TaskQueue = "payments-v2"
			Expect(k8sClient.Update(ctx, schedule)).To(Succeed())

			Expect(stored().Spec.Schedule.Cron).To(Equal([]string{"@hourly"}))
			Expect(stored().Spec.Action.Workflow.TaskQueue).To(Equal("payments-v2"))
		})
	})
})
