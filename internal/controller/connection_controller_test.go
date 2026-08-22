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
	"context"
	"errors"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	sdkclient "go.temporal.io/sdk/client"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	temporalv1alpha1 "github.com/mrsimonemms/temporal-resource-operator/api/v1alpha1"
	"github.com/mrsimonemms/temporal-resource-operator/internal/connection"
)

// fakeTemporalClient records how it was used and fails the health check on
// demand.
type fakeTemporalClient struct {
	healthErr error
	closed    bool
}

func (f *fakeTemporalClient) CheckHealth(context.Context) error { return f.healthErr }

func (f *fakeTemporalClient) Close() { f.closed = true }

var _ = Describe("Connection Controller", func() {
	const (
		namespace = "default"
		address   = "localhost:7233"
	)

	var (
		reconciler *ConnectionReconciler
		temporal   *fakeTemporalClient
		dialErr    error
		dialledFor *sdkclient.Options
		name       string
		key        types.NamespacedName
	)

	// reconcile runs a single reconcile for the resource under test.
	reconcile := func() (ctrl.Result, error) {
		return reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	}

	// readyCondition returns the Ready condition on the resource under test.
	readyCondition := func() *metav1.Condition {
		conn := &temporalv1alpha1.Connection{}
		Expect(k8sClient.Get(ctx, key, conn)).To(Succeed())

		return meta.FindStatusCondition(conn.Status.Conditions, temporalv1alpha1.ConditionTypeReady)
	}

	// createConnection persists a Connection with the given spec.
	createConnection := func(spec temporalv1alpha1.ConnectionSpec) *temporalv1alpha1.Connection {
		conn := &temporalv1alpha1.Connection{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec:       spec,
		}
		Expect(k8sClient.Create(ctx, conn)).To(Succeed())

		return conn
	}

	BeforeEach(func() {
		temporal = &fakeTemporalClient{}
		dialErr = nil
		dialledFor = nil

		// A unique name per spec keeps the specs independent without having to
		// tear the resource down.
		name = fmt.Sprintf("connection-%d", GinkgoRandomSeed()+int64(CurrentSpecReport().LineNumber()))
		key = types.NamespacedName{Name: name, Namespace: namespace}

		reconciler = &ConnectionReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Resolver: connection.NewResolver(k8sClient),
			Connect: func(_ context.Context, opts *sdkclient.Options) (TemporalClient, error) {
				dialledFor = opts
				if dialErr != nil {
					return nil, dialErr
				}

				return temporal, nil
			},
		}
	})

	AfterEach(func() {
		conn := &temporalv1alpha1.Connection{}
		if err := k8sClient.Get(ctx, key, conn); err == nil {
			Expect(k8sClient.Delete(ctx, conn)).To(Succeed())
		}
	})

	Context("when the Connection does not exist", func() {
		It("should return cleanly without touching Temporal", func() {
			key = types.NamespacedName{Name: "never-created", Namespace: namespace}

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeTrue())
			Expect(dialledFor).To(BeNil())
		})

		It("should return cleanly after the Connection has been deleted", func() {
			createConnection(temporalv1alpha1.ConnectionSpec{Address: address})

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			conn := &temporalv1alpha1.Connection{}
			Expect(k8sClient.Get(ctx, key, conn)).To(Succeed())
			Expect(k8sClient.Delete(ctx, conn)).To(Succeed())

			// No finalizers, so the resource is gone immediately.
			Eventually(func() bool {
				return apierrors.IsNotFound(k8sClient.Get(ctx, key, conn))
			}).Should(BeTrue())

			dialledFor = nil
			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeTrue())
			Expect(dialledFor).To(BeNil())
		})
	})

	Context("when the Temporal Service is healthy", func() {
		It("should report Ready=True", func() {
			conn := createConnection(temporalv1alpha1.ConnectionSpec{Address: address})

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(revalidateInterval))

			Expect(dialledFor).NotTo(BeNil())
			Expect(dialledFor.HostPort).To(Equal(address))
			Expect(temporal.closed).To(BeTrue(), "the Temporal client should be closed")

			condition := readyCondition()
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(ReasonConnected))
			Expect(condition.ObservedGeneration).To(Equal(conn.Generation))

			updated := &temporalv1alpha1.Connection{}
			Expect(k8sClient.Get(ctx, key, updated)).To(Succeed())
			Expect(updated.Status.ObservedGeneration).To(Equal(conn.Generation))
		})

		It("should not rewrite an unchanged status", func() {
			createConnection(temporalv1alpha1.ConnectionSpec{Address: address})

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			before := &temporalv1alpha1.Connection{}
			Expect(k8sClient.Get(ctx, key, before)).To(Succeed())

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			after := &temporalv1alpha1.Connection{}
			Expect(k8sClient.Get(ctx, key, after)).To(Succeed())
			Expect(after.ResourceVersion).To(Equal(before.ResourceVersion),
				"a no-op reconcile should not write the status")
		})
	})

	Context("when validation fails", func() {
		It("should report Ready=False when the Temporal Service cannot be dialled", func() {
			createConnection(temporalv1alpha1.ConnectionSpec{Address: address})
			dialErr = errors.New("connection refused")

			_, err := reconcile()
			Expect(err).To(MatchError(dialErr))

			condition := readyCondition()
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonConnectionFailed))
			Expect(condition.Message).To(ContainSubstring("connection refused"))
		})

		It("should report Ready=False when the health check fails", func() {
			createConnection(temporalv1alpha1.ConnectionSpec{Address: address})
			temporal.healthErr = errors.New("service unhealthy")

			_, err := reconcile()
			Expect(err).To(MatchError(temporal.healthErr))
			Expect(temporal.closed).To(BeTrue(), "the Temporal client should be closed")

			condition := readyCondition()
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonHealthCheckFailed))
			Expect(condition.Message).To(ContainSubstring("service unhealthy"))
		})

		It("should report Ready=False when the credentials Secret is missing", func() {
			createConnection(temporalv1alpha1.ConnectionSpec{
				Address:              address,
				CredentialsSecretRef: &corev1.LocalObjectReference{Name: "missing-secret"},
			})

			_, err := reconcile()
			Expect(err).To(HaveOccurred())
			Expect(dialledFor).To(BeNil(), "Temporal should not be dialled with unresolved credentials")

			condition := readyCondition()
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonInvalidConfiguration))
			Expect(condition.Message).To(ContainSubstring("missing-secret"))
		})

		It("should recover to Ready=True once the failure clears", func() {
			createConnection(temporalv1alpha1.ConnectionSpec{Address: address})
			dialErr = errors.New("connection refused")

			_, err := reconcile()
			Expect(err).To(HaveOccurred())
			Expect(readyCondition().Status).To(Equal(metav1.ConditionFalse))

			dialErr = nil
			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
		})
	})

	Context("when the Connection is invalid", func() {
		It("should be rejected by the API server without an address", func() {
			conn := &temporalv1alpha1.Connection{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec:       temporalv1alpha1.ConnectionSpec{},
			}
			Expect(k8sClient.Create(ctx, conn)).NotTo(Succeed())
		})

		It("should be rejected by the API server with both credential sources", func() {
			conn := &temporalv1alpha1.Connection{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec: temporalv1alpha1.ConnectionSpec{
					Address:              address,
					Credentials:          &temporalv1alpha1.Credentials{APIKey: "my-api-key"},
					CredentialsSecretRef: &corev1.LocalObjectReference{Name: "temporal-credentials"},
				},
			}

			err := k8sClient.Create(ctx, conn)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("mutually exclusive"))
		})
	})
})
