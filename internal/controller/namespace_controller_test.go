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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	temporalv1alpha1 "github.com/mrsimonemms/temporal-resource-operator/api/v1alpha1"
	"github.com/mrsimonemms/temporal-resource-operator/internal/connection"
	"github.com/mrsimonemms/temporal-resource-operator/internal/temporal"
)

// createdNamespace records a CreateNamespace call.
type createdNamespace struct {
	name      string
	retention time.Duration
}

// fakeNamespaceClient stands in for a Temporal Service. Namespaces listed in
// existing are reported as present; everything else is reported missing in the
// same shape the real client uses.
type fakeNamespaceClient struct {
	existing    map[string]*temporal.Namespace
	describeErr error
	createErr   error

	described []string
	created   []createdNamespace
	closed    bool
}

func (f *fakeNamespaceClient) DescribeNamespace(_ context.Context, name string) (*temporal.Namespace, error) {
	f.described = append(f.described, name)

	if f.describeErr != nil {
		return nil, f.describeErr
	}

	if ns, ok := f.existing[name]; ok {
		return ns, nil
	}

	return nil, fmt.Errorf("%w: %w", temporal.ErrNamespaceNotFound, serviceerror.NewNamespaceNotFound(name))
}

func (f *fakeNamespaceClient) CreateNamespace(_ context.Context, name string, retention time.Duration) error {
	f.created = append(f.created, createdNamespace{name: name, retention: retention})

	return f.createErr
}

func (f *fakeNamespaceClient) Close() { f.closed = true }

var _ = Describe("Namespace Controller", func() {
	const (
		namespace = "default"
		address   = "localhost:7233"
	)

	var (
		reconciler     *NamespaceReconciler
		temporalClient *fakeNamespaceClient
		dialErr        error
		dialled        int
		name           string
		connectionName string
		key            types.NamespacedName
	)

	// reconcile runs a single reconcile for the resource under test.
	reconcile := func() (ctrl.Result, error) {
		return reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	}

	// readyCondition returns the Ready condition on the resource under test.
	readyCondition := func() *metav1.Condition {
		ns := &temporalv1alpha1.Namespace{}
		Expect(k8sClient.Get(ctx, key, ns)).To(Succeed())

		return meta.FindStatusCondition(ns.Status.Conditions, temporalv1alpha1.ConditionTypeReady)
	}

	// createConnection persists a Connection and, when status is non-empty,
	// gives it a Ready condition with that status.
	createConnection := func(status metav1.ConditionStatus) *temporalv1alpha1.Connection {
		conn := &temporalv1alpha1.Connection{
			ObjectMeta: metav1.ObjectMeta{Name: connectionName, Namespace: namespace},
			Spec:       temporalv1alpha1.ConnectionSpec{Address: address},
		}
		Expect(k8sClient.Create(ctx, conn)).To(Succeed())

		if status == "" {
			return conn
		}

		meta.SetStatusCondition(&conn.Status.Conditions, metav1.Condition{
			Type:               temporalv1alpha1.ConditionTypeReady,
			Status:             status,
			Reason:             ReasonConnected,
			Message:            "Temporal Service is reachable and healthy",
			ObservedGeneration: conn.Generation,
		})
		Expect(k8sClient.Status().Update(ctx, conn)).To(Succeed())

		return conn
	}

	// createNamespace persists a Namespace referencing the test Connection.
	createNamespace := func(retention *metav1.Duration) *temporalv1alpha1.Namespace {
		ns := &temporalv1alpha1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: temporalv1alpha1.NamespaceSpec{
				ConnectionRef: corev1.LocalObjectReference{Name: connectionName},
				Retention:     retention,
			},
		}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())

		return ns
	}

	BeforeEach(func() {
		temporalClient = &fakeNamespaceClient{existing: map[string]*temporal.Namespace{}}
		dialErr = nil
		dialled = 0

		// Unique names per spec keep the specs independent. Namespace names are
		// DNS-1123 labels, so they cannot start with a digit.
		suffix := GinkgoRandomSeed() + int64(CurrentSpecReport().LineNumber())
		name = fmt.Sprintf("ns-%d", suffix)
		connectionName = fmt.Sprintf("conn-%d", suffix)
		key = types.NamespacedName{Name: name, Namespace: namespace}

		reconciler = &NamespaceReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Resolver: connection.NewResolver(k8sClient),
			Connect: func(_ context.Context, _ *sdkclient.Options) (TemporalNamespaceClient, error) {
				dialled++
				if dialErr != nil {
					return nil, dialErr
				}

				return temporalClient, nil
			},
		}
	})

	AfterEach(func() {
		ns := &temporalv1alpha1.Namespace{}
		if err := k8sClient.Get(ctx, key, ns); err == nil {
			Expect(k8sClient.Delete(ctx, ns)).To(Succeed())
		}

		conn := &temporalv1alpha1.Connection{}
		connKey := types.NamespacedName{Name: connectionName, Namespace: namespace}
		if err := k8sClient.Get(ctx, connKey, conn); err == nil {
			Expect(k8sClient.Delete(ctx, conn)).To(Succeed())
		}
	})

	Context("when the Namespace does not exist", func() {
		It("should return cleanly without touching Temporal", func() {
			key = types.NamespacedName{Name: "never-created", Namespace: namespace}

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeTrue())
			Expect(dialled).To(BeZero())
		})

		It("should return cleanly after the Namespace has been deleted", func() {
			createConnection(metav1.ConditionTrue)
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			ns := &temporalv1alpha1.Namespace{}
			Expect(k8sClient.Get(ctx, key, ns)).To(Succeed())
			Expect(k8sClient.Delete(ctx, ns)).To(Succeed())

			// No finalizers, so the resource is gone immediately.
			Eventually(func() bool {
				return apierrors.IsNotFound(k8sClient.Get(ctx, key, ns))
			}).Should(BeTrue())

			dialled = 0
			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeTrue())
			Expect(dialled).To(BeZero())
		})
	})

	Context("when the Connection is not usable", func() {
		It("should report ConnectionNotFound when it does not exist", func() {
			createNamespace(nil)

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred(), "a missing dependency is not a controller failure")
			Expect(result.RequeueAfter).To(Equal(dependencyRetryInterval))
			Expect(dialled).To(BeZero())

			condition := readyCondition()
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonConnectionNotFound))
			Expect(condition.Message).To(ContainSubstring(connectionName))
		})

		It("should report ConnectionNotReady when it has no Ready condition yet", func() {
			createConnection("")
			createNamespace(nil)

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(dependencyRetryInterval))
			Expect(dialled).To(BeZero())

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonConnectionNotReady))
			Expect(condition.Message).To(ContainSubstring("not been validated yet"))
		})

		It("should report ConnectionNotReady when Ready=False", func() {
			createConnection(metav1.ConditionFalse)
			createNamespace(nil)

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(dependencyRetryInterval))
			Expect(dialled).To(BeZero())

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonConnectionNotReady))
		})
	})

	Context("when the Connection is Ready", func() {
		BeforeEach(func() {
			createConnection(metav1.ConditionTrue)
		})

		It("should register a missing Temporal namespace and report Created", func() {
			ns := createNamespace(&metav1.Duration{Duration: 24 * time.Hour})

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(namespaceResyncInterval))

			Expect(dialled).To(Equal(1))
			Expect(temporalClient.described).To(Equal([]string{name}))
			Expect(temporalClient.created).To(Equal([]createdNamespace{{name: name, retention: 24 * time.Hour}}))
			Expect(temporalClient.closed).To(BeTrue(), "the Temporal client should be closed")

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(ReasonCreated))
			Expect(condition.ObservedGeneration).To(Equal(ns.Generation))

			updated := &temporalv1alpha1.Namespace{}
			Expect(k8sClient.Get(ctx, key, updated)).To(Succeed())
			Expect(updated.Status.ObservedGeneration).To(Equal(ns.Generation))
		})

		It("should default retention to 72h", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(HaveLen(1))
			Expect(temporalClient.created[0].retention).To(Equal(temporalv1alpha1.DefaultRetention))
			Expect(temporalv1alpha1.DefaultRetention).To(Equal(72 * time.Hour))
		})

		It("should leave an existing Temporal namespace alone and report Reconciled", func() {
			createNamespace(&metav1.Duration{Duration: 24 * time.Hour})
			temporalClient.existing[name] = &temporal.Namespace{Name: name, Retention: 720 * time.Hour}

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(namespaceResyncInterval))

			Expect(temporalClient.described).To(Equal([]string{name}))
			Expect(temporalClient.created).To(BeEmpty(), "an existing namespace must not be recreated")

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(ReasonReconciled))
		})

		It("should be idempotent across repeated reconciles", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(temporalClient.created).To(HaveLen(1))

			// The namespace now exists as far as Temporal is concerned.
			temporalClient.existing[name] = &temporal.Namespace{Name: name}

			before := &temporalv1alpha1.Namespace{}
			Expect(k8sClient.Get(ctx, key, before)).To(Succeed())

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(temporalClient.created).To(HaveLen(1), "the namespace must not be created twice")
		})

		It("should not rewrite an unchanged status", func() {
			createNamespace(nil)
			temporalClient.existing[name] = &temporal.Namespace{Name: name}

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			before := &temporalv1alpha1.Namespace{}
			Expect(k8sClient.Get(ctx, key, before)).To(Succeed())

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			after := &temporalv1alpha1.Namespace{}
			Expect(k8sClient.Get(ctx, key, after)).To(Succeed())
			Expect(after.ResourceVersion).To(Equal(before.ResourceVersion),
				"a no-op reconcile should not write the status")
		})

		It("should report ConnectionFailed when Temporal cannot be dialled", func() {
			createNamespace(nil)
			dialErr = errors.New("connection refused")

			_, err := reconcile()
			Expect(err).To(MatchError(dialErr))
			Expect(temporalClient.described).To(BeEmpty())

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonConnectionFailed))
		})

		It("should report DescribeFailed and not create on a describe failure", func() {
			createNamespace(nil)
			temporalClient.describeErr = errors.New("permission denied")

			_, err := reconcile()
			Expect(err).To(MatchError(temporalClient.describeErr))
			Expect(temporalClient.created).To(BeEmpty(), "a failed describe must not trigger a create")
			Expect(temporalClient.closed).To(BeTrue())

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonDescribeFailed))
			Expect(condition.Message).To(ContainSubstring("permission denied"))
		})

		It("should report CreateFailed when registration fails", func() {
			createNamespace(nil)
			temporalClient.createErr = errors.New("retention too short")

			_, err := reconcile()
			Expect(err).To(MatchError(temporalClient.createErr))

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonCreateFailed))
			Expect(condition.Message).To(ContainSubstring("retention too short"))
		})

		It("should recover to Ready=True once Temporal accepts the namespace", func() {
			createNamespace(nil)
			temporalClient.createErr = errors.New("retention too short")

			_, err := reconcile()
			Expect(err).To(HaveOccurred())
			Expect(readyCondition().Status).To(Equal(metav1.ConditionFalse))

			temporalClient.createErr = nil
			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
		})

		It("should report InvalidConfiguration when the Connection cannot be resolved", func() {
			conn := &temporalv1alpha1.Connection{}
			connKey := types.NamespacedName{Name: connectionName, Namespace: namespace}
			Expect(k8sClient.Get(ctx, connKey, conn)).To(Succeed())

			conn.Spec.CredentialsSecretRef = &corev1.LocalObjectReference{Name: "missing-secret"}
			Expect(k8sClient.Update(ctx, conn)).To(Succeed())

			createNamespace(nil)

			_, err := reconcile()
			Expect(err).To(HaveOccurred())
			Expect(dialled).To(BeZero(), "Temporal should not be dialled with unresolved credentials")

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonInvalidConfiguration))
			Expect(condition.Message).To(ContainSubstring("missing-secret"))
		})
	})

	Context("when the Namespace has no optional fields", func() {
		It("should not panic", func() {
			ns := &temporalv1alpha1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec: temporalv1alpha1.NamespaceSpec{
					ConnectionRef: corev1.LocalObjectReference{Name: connectionName},
				},
			}
			Expect(ns.Spec.Retention).To(BeNil())
			Expect(ns.Spec.RetentionDuration()).To(Equal(temporalv1alpha1.DefaultRetention))

			Expect(k8sClient.Create(ctx, ns)).To(Succeed())

			Expect(func() {
				_, _ = reconcile()
			}).NotTo(Panic())
		})
	})

	Context("when the Namespace is invalid", func() {
		It("should be rejected by the API server without a connectionRef name", func() {
			ns := &temporalv1alpha1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec:       temporalv1alpha1.NamespaceSpec{},
			}

			err := k8sClient.Create(ctx, ns)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("connectionRef.name is required"))
		})

		It("should default retention to 72h at admission", func() {
			createConnection(metav1.ConditionTrue)
			createNamespace(nil)

			stored := &temporalv1alpha1.Namespace{}
			Expect(k8sClient.Get(ctx, key, stored)).To(Succeed())
			Expect(stored.Spec.Retention).NotTo(BeNil())
			Expect(stored.Spec.Retention.Duration).To(Equal(72 * time.Hour))
		})
	})
})
