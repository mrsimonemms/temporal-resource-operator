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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	temporalv1beta1 "github.com/mrsimonemms/temporal-resource-operator/api/v1beta1"
)

// These specs talk to the envtest API server with the generated CRD loaded, so
// they exercise real admission against the real schema rather than the Go type
// alone. They exist because of a production failure: a Namespace asking for
// `retention: 7d` was stored happily by a schema that only said `type: string`,
// and from then on every attempt to list Namespaces failed with
//
//	failed to list *v1beta1.Namespace: time: unknown unit "d" in duration "7d"
//
// which poisoned the informer behind the Namespace controller, so updates and
// deletions stopped reconciling. Two things had to change: "7d" has to be
// understood, and a duration the operator cannot decode must never reach
// storage in the first place.
var _ = Describe("Namespace retention admission", func() {
	const namespace = "default"

	var (
		name           string
		connectionName string
	)

	BeforeEach(func() {
		// Unique names per spec keep the specs independent. Namespace names are
		// DNS-1123 labels, so they cannot start with a digit.
		suffix := GinkgoRandomSeed() + int64(CurrentSpecReport().LineNumber())
		name = fmt.Sprintf("ret-%d", suffix)
		connectionName = fmt.Sprintf("conn-%d", suffix)
	})

	// offer builds a Namespace as unstructured JSON, so that a retention string
	// the Go type would refuse can still be put in front of the API server.
	// Sending it through the typed client would fail locally and prove nothing
	// about what Kubernetes will store.
	offer := func(retention string) *unstructured.Unstructured {
		spec := map[string]any{
			"connectionRef": map[string]any{"name": connectionName},
		}
		if retention != "" {
			spec["retention"] = retention
		}

		u := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "temporal.simonemms.com/v1beta1",
			"kind":       "Namespace",
			"metadata": map[string]any{
				"name":      name,
				"namespace": namespace,
			},
			"spec": spec,
		}}

		return u
	}

	// stored reads the resource back through the typed client, which is the
	// decode path the controller and its informer use.
	stored := func() *temporalv1beta1.Namespace {
		ns := &temporalv1beta1.Namespace{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, ns)).
			To(Succeed())

		return ns
	}

	AfterEach(func() {
		// Namespaces created here carry no finalizer, because no reconciler
		// runs against them.
		_ = k8sClient.Delete(ctx, offer(""))
	})

	Describe("values Kubernetes accepts", func() {
		DescribeTable(
			"should admit and decode",
			func(retention string, expected time.Duration) {
				Expect(k8sClient.Create(ctx, offer(retention))).To(Succeed())

				ns := stored()
				Expect(ns.Spec.Retention).NotTo(BeNil())
				Expect(ns.Spec.Retention.Duration).To(Equal(expected))

				effective, err := ns.Spec.RetentionDuration()
				Expect(err).NotTo(HaveOccurred())
				Expect(effective).To(Equal(expected))
			},
			// The value from the bug report.
			Entry("7d", "7d", 168*time.Hour),
			Entry("3d", "3d", 72*time.Hour),
			Entry("1d12h", "1d12h", 36*time.Hour),
			Entry("7d30m", "7d30m", 168*time.Hour+30*time.Minute),

			// Values that worked before the day syntax arrived.
			Entry("72h", "72h", 72*time.Hour),
			Entry("168h", "168h", 168*time.Hour),

			// The lower bound, written every way the grammar allows. The range
			// is on the duration, not on the notation.
			Entry("1d", "1d", 24*time.Hour),
			Entry("24h", "24h", 24*time.Hour),
			Entry("1440m", "1440m", 24*time.Hour),
			Entry("86400s", "86400s", 24*time.Hour),
			Entry("36h", "36h", 36*time.Hour),

			// The upper bound, both ways.
			Entry("90d", "90d", 2160*time.Hour),
			Entry("2160h", "2160h", 2160*time.Hour),

			// Just inside each bound.
			Entry("24h1s", "24h1s", 24*time.Hour+time.Second),
			Entry("1d1ns", "1d1ns", 24*time.Hour+time.Nanosecond),
			Entry("2159h59m59s", "2159h59m59s", 2160*time.Hour-time.Second),
			Entry("89d23h", "89d23h", 89*24*time.Hour+23*time.Hour),
		)

		It("should keep the string the user wrote", func() {
			// Kubernetes stores the JSON it was given; the operator's canonical
			// form is not written back. So "7d" stays "7d" in etcd, in
			// `kubectl get -o yaml`, and in the RETENTION print column.
			Expect(k8sClient.Create(ctx, offer("7d"))).To(Succeed())

			raw := offer("")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, raw)).
				To(Succeed())

			retention, found, err := unstructured.NestedString(raw.Object, "spec", "retention")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(retention).To(Equal("7d"))
		})

		It("should default an omitted retention to 72h", func() {
			Expect(k8sClient.Create(ctx, offer(""))).To(Succeed())

			ns := stored()
			Expect(ns.Spec.Retention).NotTo(BeNil())
			Expect(ns.Spec.Retention.Duration).To(Equal(72 * time.Hour))

			retention, err := ns.Spec.RetentionDuration()
			Expect(err).NotTo(HaveOccurred())
			Expect(retention).To(Equal(temporalv1beta1.DefaultRetention))
		})

		It("should admit a signed retention that is in range", func() {
			// The generic duration syntax allows an explicit sign, and the
			// range check works on the duration rather than the text, so "+7d"
			// is simply 168h.
			Expect(k8sClient.Create(ctx, offer("+7d"))).To(Succeed())

			ns := stored()
			Expect(ns.Spec.Retention.Duration).To(Equal(168 * time.Hour))

			retention, err := ns.Spec.RetentionDuration()
			Expect(err).NotTo(HaveOccurred())
			Expect(retention).To(Equal(168 * time.Hour))
		})
	})

	Describe("values Kubernetes rejects", func() {
		DescribeTable(
			"should refuse to store",
			func(retention string) {
				err := k8sClient.Create(ctx, offer(retention))
				Expect(err).To(HaveOccurred(), "%q must not be storable", retention)
				Expect(err.Error()).To(
					Or(ContainSubstring("should match"), ContainSubstring("Too long")),
					"%q should be rejected by the schema, not by something else", retention,
				)
			},
			// The malformed day syntax the new grammar has to exclude.
			Entry("7days", "7days"),
			Entry("a bare unit", "d"),
			Entry("a doubled unit", "7dd"),
			Entry("repeated day components", "1d2d"),
			Entry("days after another unit", "1h2d"),
			Entry("fractional days", "1.5d"),
			Entry("uppercase D", "7D"),

			// Malformed ordinary Go duration syntax.
			Entry("arbitrary text", "forever"),
			Entry("words", "seven days"),
			Entry("number with no unit", "7"),
			Entry("trailing number", "1h30"),
			Entry("unknown unit", "1x"),
			Entry("mid-string sign", "1h-30m"),

			// Magnitudes that a time.Duration could not hold. The syntax
			// bounds are what keep an accepted value inside what a
			// time.Duration can represent.
			Entry("day count too long", "99999d"),
			Entry("component too long", "9999999h"),
			Entry("absurdly long", "99999999999999999999h"),
		)

		// The range is enforced by CEL on the effective duration, so it holds
		// whichever notation the value is written in. Every one of these is
		// syntactically valid and would decode cleanly - it is the policy, not
		// the grammar, that refuses them.
		DescribeTable(
			"should refuse to store a duration outside 1..90 days",
			func(retention string) {
				err := k8sClient.Create(ctx, offer(retention))
				Expect(err).To(HaveOccurred(), "%q must not be storable", retention)
				Expect(err.Error()).To(
					Or(
						ContainSubstring("at least 1 day"),
						ContainSubstring("at most 90 days"),
					),
					"%q should be rejected by the range rules: %v", retention, err,
				)
			},
			// Below the minimum.
			Entry("zero", "0"),
			Entry("zero seconds", "0s"),
			Entry("zero days", "0d"),
			Entry("1h", "1h"),
			Entry("23h59m59s", "23h59m59s"),
			Entry("just under a day", "1439m"),
			Entry("sub-second", "500ms"),
			Entry("half an hour", "30m"),
			Entry("2h30m", "2h30m"),

			// Negative, which is below the minimum by definition. This is the
			// behaviour that used to be silently treated as "unset".
			Entry("-1h", "-1h"),
			Entry("-1d", "-1d"),
			Entry("negative zero", "-0"),
			Entry("-7d", "-7d"),

			// Above the maximum, including the tightest possible overshoot.
			Entry("90d1ns", "90d1ns"),
			Entry("90d1s", "90d1s"),
			Entry("2160h1s", "2160h1s"),
			Entry("2161h", "2161h"),
			Entry("91d", "91d"),
			Entry("just over in minutes", "129601m"),
		)

		It("should refuse an empty retention", func() {
			// The helper leaves an empty retention out altogether, so that the
			// default applies; an explicitly empty string is a different thing
			// and must be rejected rather than silently defaulted.
			u := offer("")
			Expect(unstructured.SetNestedField(u.Object, "", "spec", "retention")).To(Succeed())

			err := k8sClient.Create(ctx, u)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("should match"))
		})
	})

	// This is the regression guarantee for the original outage. The failure was
	// not that "7d" was rejected - it was that "7d" was ACCEPTED by a schema
	// with no pattern, and then broke the deserialisation of every subsequent
	// list. A single bad object is enough, because a List decodes as one
	// response: one undecodable item fails the whole call, and the informer
	// that call feeds never recovers.
	//
	// So the guarantee is proved at admission: an undecodable retention cannot
	// be created, therefore it cannot be in a list, therefore it cannot poison
	// the cache.
	Describe("informer poisoning", func() {
		It("should not let an undecodable retention into a list of Namespaces", func() {
			By("storing a Namespace with the retention that caused the outage")
			Expect(k8sClient.Create(ctx, offer("7d"))).To(Succeed())

			By("rejecting every retention the operator could not decode")
			for _, bad := range []string{"7days", "7dd", "1d2d", "d", "forever", "99999d"} {
				u := offer(bad)
				u.SetName(fmt.Sprintf("%s-%s", name, "poison"))

				err := k8sClient.Create(ctx, u)
				Expect(err).To(HaveOccurred(), "%q must never reach storage", bad)
			}

			By("listing Namespaces through the typed client, as the informer does")
			// This is the exact operation that failed in production with
			// `failed to list *v1beta1.Namespace: time: unknown unit "d"`.
			list := &temporalv1beta1.NamespaceList{}
			Expect(k8sClient.List(ctx, list)).To(Succeed())

			By("confirming the day-based value decoded")
			var found bool
			for i := range list.Items {
				if list.Items[i].Name == name {
					found = true
					Expect(list.Items[i].Spec.Retention.Duration).To(Equal(168 * time.Hour))
				}
			}
			Expect(found).To(BeTrue(), "the Namespace should be in the list")
		})
	})
})
