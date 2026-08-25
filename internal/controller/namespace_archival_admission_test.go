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
// they exercise real admission against the real schema. What matters about
// spec.archival is which shapes Kubernetes will store: the controller's
// behaviour depends on being able to tell "not managed" from "managed, and off",
// and a schema that admitted a half-written block would blur the two.
var _ = Describe("Namespace archival admission", func() {
	const namespace = "default"

	var (
		name           string
		connectionName string
	)

	BeforeEach(func() {
		// Unique names per spec keep the specs independent. Namespace names are
		// DNS-1123 labels, so they cannot start with a digit.
		suffix := GinkgoRandomSeed() + int64(CurrentSpecReport().LineNumber())
		name = fmt.Sprintf("arc-%d", suffix)
		connectionName = fmt.Sprintf("conn-%d", suffix)
	})

	// offer builds a Namespace as unstructured JSON, so that an archival block
	// the Go type could not express - a missing enabled, an empty uri - can
	// still be put in front of the API server.
	offer := func(archival map[string]any) *unstructured.Unstructured {
		spec := map[string]any{
			"connectionRef": map[string]any{"name": connectionName},
		}
		if archival != nil {
			spec["archival"] = archival
		}

		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "temporal.simonemms.com/v1beta1",
			"kind":       "Namespace",
			"metadata": map[string]any{
				"name":      name,
				"namespace": namespace,
			},
			"spec": spec,
		}}
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
		_ = k8sClient.Delete(ctx, offer(nil))
	})

	Describe("shapes Kubernetes accepts", func() {
		It("should admit both kinds and decode them", func() {
			Expect(k8sClient.Create(ctx, offer(map[string]any{
				"history":    map[string]any{"enabled": true},
				"visibility": map[string]any{"enabled": false},
			}))).To(Succeed())

			ns := stored()
			Expect(ns.Spec.HistoryArchival()).
				To(Equal(&temporalv1beta1.ArchivalConfig{Enabled: true}))
			Expect(ns.Spec.VisibilityArchival()).
				To(Equal(&temporalv1beta1.ArchivalConfig{Enabled: false}))
		})

		It("should admit one kind without the other", func() {
			Expect(k8sClient.Create(ctx, offer(map[string]any{
				"history": map[string]any{"enabled": true, "uri": "s3://bucket/history"},
			}))).To(Succeed())

			ns := stored()
			Expect(ns.Spec.HistoryArchival()).To(Equal(&temporalv1beta1.ArchivalConfig{
				Enabled: true,
				URI:     "s3://bucket/history",
			}))
			Expect(ns.Spec.VisibilityArchival()).To(BeNil())
		})

		It("should admit an empty block as managing nothing", func() {
			Expect(k8sClient.Create(ctx, offer(map[string]any{}))).To(Succeed())

			ns := stored()
			Expect(ns.Spec.Archival).NotTo(BeNil())
			Expect(ns.Spec.HistoryArchival()).To(BeNil())
			Expect(ns.Spec.VisibilityArchival()).To(BeNil())
		})

		It("should admit no archival at all, as every earlier Namespace has", func() {
			Expect(k8sClient.Create(ctx, offer(nil))).To(Succeed())

			ns := stored()
			Expect(ns.Spec.Archival).To(BeNil())
			Expect(ns.Spec.HistoryArchival()).To(BeNil())
			Expect(ns.Spec.VisibilityArchival()).To(BeNil())
		})
	})

	Describe("shapes Kubernetes refuses", func() {
		DescribeTable(
			"should reject",
			func(archival map[string]any, because string) {
				err := k8sClient.Create(ctx, offer(archival))
				Expect(err).To(HaveOccurred(), because)
			},
			// Without enabled there is no request, just an empty object that
			// would silently read as "disable it".
			Entry("history with no enabled",
				map[string]any{"history": map[string]any{}},
				"enabled is required once a kind is present"),
			Entry("visibility with no enabled",
				map[string]any{"visibility": map[string]any{}},
				"enabled is required once a kind is present"),
			Entry("history with only a uri",
				map[string]any{"history": map[string]any{"uri": "s3://bucket/history"}},
				"a destination is not on its own a request to archive"),

			// An empty uri is not the same as no uri, and only one of them
			// means "take the Service's default".
			Entry("an empty uri",
				map[string]any{"history": map[string]any{"enabled": true, "uri": ""}},
				"an empty uri is meaningless; omit the field instead"),

			// The types have to hold, or the controller cannot trust what it
			// decodes.
			Entry("enabled as a string",
				map[string]any{"history": map[string]any{"enabled": "true"}},
				"enabled is a boolean"),
			Entry("a kind as a boolean",
				map[string]any{"history": true},
				"a kind is an object"),
		)
	})
})
