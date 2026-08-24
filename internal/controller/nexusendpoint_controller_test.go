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
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	temporalv1beta1 "github.com/mrsimonemms/temporal-resource-operator/api/v1beta1"
	"github.com/mrsimonemms/temporal-resource-operator/internal/connection"
	"github.com/mrsimonemms/temporal-resource-operator/internal/temporal"
)

// endpointTarget is where an endpoint routes to.
type endpointTarget struct {
	namespace string
	taskQueue string
}

// endpointCall records a call against the fake Service.
type endpointCall struct {
	name   string
	target endpointTarget
}

// fakeNexusEndpointClient stands in for a Temporal Service. Endpoints are keyed
// by their Service-wide name, as Temporal keys them.
type fakeNexusEndpointClient struct {
	existing map[string]*temporal.NexusEndpoint

	describeErr error
	createErr   error
	updateErr   error
	deleteErr   error

	described []string
	created   []endpointCall
	updated   []endpointCall
	deleted   []string
	closed    bool

	nextID int
}

func newFakeNexusEndpointClient() *fakeNexusEndpointClient {
	return &fakeNexusEndpointClient{existing: map[string]*temporal.NexusEndpoint{}}
}

// put seeds the fake with an endpoint, as though it were already registered.
func (f *fakeNexusEndpointClient) put(name, namespace, taskQueue string) *temporal.NexusEndpoint {
	f.nextID++
	endpoint := &temporal.NexusEndpoint{
		ID:              fmt.Sprintf("endpoint-%d", f.nextID),
		Version:         1,
		Name:            name,
		TargetNamespace: namespace,
		TaskQueue:       taskQueue,
	}
	f.existing[name] = endpoint

	return endpoint
}

func (f *fakeNexusEndpointClient) DescribeNexusEndpoint(
	_ context.Context,
	name string,
) (*temporal.NexusEndpoint, error) {
	f.described = append(f.described, name)

	if f.describeErr != nil {
		return nil, f.describeErr
	}

	if endpoint, ok := f.existing[name]; ok {
		// Return a copy, as the real client does, so a later change cannot
		// mutate what the caller is holding.
		found := *endpoint

		return &found, nil
	}

	return nil, fmt.Errorf("%w: %s", temporal.ErrNexusEndpointNotFound, name)
}

func (f *fakeNexusEndpointClient) CreateNexusEndpoint(
	_ context.Context,
	name, namespace, taskQueue string,
) (*temporal.NexusEndpoint, error) {
	f.created = append(f.created, endpointCall{
		name: name, target: endpointTarget{namespace: namespace, taskQueue: taskQueue},
	})

	if f.createErr != nil {
		return nil, f.createErr
	}

	if _, taken := f.existing[name]; taken {
		return nil, fmt.Errorf("%w: %s", temporal.ErrNexusEndpointExists, name)
	}

	return f.put(name, namespace, taskQueue), nil
}

func (f *fakeNexusEndpointClient) UpdateNexusEndpointTarget(
	_ context.Context,
	endpoint *temporal.NexusEndpoint,
	namespace, taskQueue string,
) (*temporal.NexusEndpoint, error) {
	f.updated = append(f.updated, endpointCall{
		name: endpoint.Name, target: endpointTarget{namespace: namespace, taskQueue: taskQueue},
	})

	if f.updateErr != nil {
		return nil, f.updateErr
	}

	stored, ok := f.existing[endpoint.Name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", temporal.ErrNexusEndpointNotFound, endpoint.Name)
	}

	// Temporal refuses a write carrying a version other than the current one.
	if endpoint.Version != stored.Version {
		return nil, fmt.Errorf("%w: %s", temporal.ErrNexusEndpointChanged, endpoint.Name)
	}

	stored.TargetNamespace = namespace
	stored.TaskQueue = taskQueue
	stored.Version++

	updated := *stored

	return &updated, nil
}

func (f *fakeNexusEndpointClient) DeleteNexusEndpoint(
	_ context.Context,
	endpoint *temporal.NexusEndpoint,
) error {
	f.deleted = append(f.deleted, endpoint.Name)

	if f.deleteErr != nil {
		return f.deleteErr
	}

	if _, ok := f.existing[endpoint.Name]; !ok {
		return fmt.Errorf("%w: %s", temporal.ErrNexusEndpointNotFound, endpoint.Name)
	}

	delete(f.existing, endpoint.Name)

	return nil
}

func (f *fakeNexusEndpointClient) Close() { f.closed = true }

var _ = Describe("NexusEndpoint Controller", func() {
	const (
		k8sNamespace      = saNamespace
		address           = "localhost:7233"
		temporalNamespace = "payments"
		taskQueue         = "payments-nexus"
		otherTaskQueue    = "orders-nexus"
	)

	var (
		reconciler     *NexusEndpointReconciler
		temporalClient *fakeNexusEndpointClient
		dialErr        error
		dialled        int
		// The Kubernetes resource's name and the Temporal endpoint's are
		// deliberately different, and differently cased.
		resourceName   string
		endpointName   string
		connectionName string
		key            types.NamespacedName
	)

	reconcile := func() (ctrl.Result, error) {
		return reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	}

	stored := func() *temporalv1beta1.NexusEndpoint {
		endpoint := &temporalv1beta1.NexusEndpoint{}
		Expect(k8sClient.Get(ctx, key, endpoint)).To(Succeed())

		return endpoint
	}

	readyCondition := func() *metav1.Condition {
		return meta.FindStatusCondition(stored().Status.Conditions, temporalv1beta1.ConditionTypeReady)
	}

	ownership := func() temporalv1beta1.NexusEndpointOwnership {
		return stored().Status.Ownership
	}

	hasFinalizer := func() bool {
		return controllerutil.ContainsFinalizer(stored(), temporalv1beta1.NexusEndpointFinalizer)
	}

	isGone := func() bool {
		return apierrors.IsNotFound(k8sClient.Get(ctx, key, &temporalv1beta1.NexusEndpoint{}))
	}

	createConnection := func(status metav1.ConditionStatus) *temporalv1beta1.Connection {
		conn := &temporalv1beta1.Connection{
			ObjectMeta: metav1.ObjectMeta{Name: connectionName, Namespace: k8sNamespace},
			Spec:       temporalv1beta1.ConnectionSpec{Address: address},
		}
		Expect(k8sClient.Create(ctx, conn)).To(Succeed())

		if status == "" {
			return conn
		}

		meta.SetStatusCondition(&conn.Status.Conditions, metav1.Condition{
			Type: temporalv1beta1.ConditionTypeReady, Status: status,
			Reason: ReasonConnected, Message: connectionReadyMessage,
		})
		Expect(k8sClient.Status().Update(ctx, conn)).To(Succeed())

		return conn
	}

	createTemporalNamespace := func(status metav1.ConditionStatus) *temporalv1beta1.Namespace {
		ns := &temporalv1beta1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: temporalNamespace, Namespace: k8sNamespace},
			Spec: temporalv1beta1.NamespaceSpec{
				ConnectionRef: corev1.LocalObjectReference{Name: connectionName},
			},
		}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())

		if status == "" {
			return ns
		}

		meta.SetStatusCondition(&ns.Status.Conditions, metav1.Condition{
			Type: temporalv1beta1.ConditionTypeReady, Status: status,
			Reason: ReasonCreated, Message: "Registered Temporal namespace",
		})
		Expect(k8sClient.Status().Update(ctx, ns)).To(Succeed())

		return ns
	}

	createEndpoint := func(queue string) *temporalv1beta1.NexusEndpoint {
		endpoint := &temporalv1beta1.NexusEndpoint{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: k8sNamespace},
			Spec: temporalv1beta1.NexusEndpointSpec{
				Name:          endpointName,
				ConnectionRef: corev1.LocalObjectReference{Name: connectionName},
				NamespaceRef:  corev1.LocalObjectReference{Name: temporalNamespace},
				TaskQueue:     queue,
			},
		}
		Expect(k8sClient.Create(ctx, endpoint)).To(Succeed())

		return endpoint
	}

	setOwnership := func(value temporalv1beta1.NexusEndpointOwnership) {
		endpoint := stored()
		endpoint.Status.Ownership = value
		Expect(k8sClient.Status().Update(ctx, endpoint)).To(Succeed())
	}

	setDeletionPolicy := func(policy temporalv1beta1.NexusEndpointDeletionPolicy) {
		endpoint := stored()
		endpoint.Spec.DeletionPolicy = policy
		Expect(k8sClient.Update(ctx, endpoint)).To(Succeed())
	}

	setTaskQueue := func(queue string) {
		endpoint := stored()
		endpoint.Spec.TaskQueue = queue
		Expect(k8sClient.Update(ctx, endpoint)).To(Succeed())
	}

	beginDeletion := func() {
		Expect(hasFinalizer()).To(BeTrue(), "the resource needs a finalizer to survive deletion")
		Expect(k8sClient.Delete(ctx, stored())).To(Succeed())
		Expect(stored().GetDeletionTimestamp().IsZero()).To(BeFalse())
	}

	resetTemporalCalls := func() {
		dialled = 0
		temporalClient.described = nil
		temporalClient.created = nil
		temporalClient.updated = nil
		temporalClient.deleted = nil
	}

	expectNoTemporalContact := func() {
		Expect(dialled).To(BeZero(), "Temporal should not have been dialled")
		Expect(temporalClient.described).To(BeEmpty())
		Expect(temporalClient.created).To(BeEmpty())
		Expect(temporalClient.updated).To(BeEmpty())
		Expect(temporalClient.deleted).To(BeEmpty())
	}

	deleteConnection := func() {
		conn := &temporalv1beta1.Connection{}
		connKey := types.NamespacedName{Name: connectionName, Namespace: k8sNamespace}
		Expect(k8sClient.Get(ctx, connKey, conn)).To(Succeed())
		Expect(k8sClient.Delete(ctx, conn)).To(Succeed())
	}

	BeforeEach(func() {
		temporalClient = newFakeNexusEndpointClient()
		dialErr = nil
		dialled = 0

		suffix := GinkgoRandomSeed() + int64(CurrentSpecReport().LineNumber())
		resourceName = fmt.Sprintf("payments-nexus-%d", suffix)
		endpointName = fmt.Sprintf("PaymentsNexus%d", suffix)
		connectionName = fmt.Sprintf("conn-%d", suffix)
		key = types.NamespacedName{Name: resourceName, Namespace: k8sNamespace}

		reconciler = &NexusEndpointReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Resolver: connection.NewResolver(k8sClient),
			Connect: func(_ context.Context, _ *sdkclient.Options) (TemporalNexusEndpointClient, error) {
				dialled++
				if dialErr != nil {
					return nil, dialErr
				}

				return temporalClient, nil
			},
		}
	})

	AfterEach(func() {
		endpoint := &temporalv1beta1.NexusEndpoint{}
		if err := k8sClient.Get(ctx, key, endpoint); err == nil {
			if controllerutil.RemoveFinalizer(endpoint, temporalv1beta1.NexusEndpointFinalizer) {
				Expect(k8sClient.Update(ctx, endpoint)).To(Succeed())
			}
			Expect(ctrlclient.IgnoreNotFound(k8sClient.Delete(ctx, endpoint))).To(Succeed())
		}

		for _, obj := range []ctrlclient.Object{
			&temporalv1beta1.Connection{
				ObjectMeta: metav1.ObjectMeta{Name: connectionName, Namespace: k8sNamespace},
			},
			&temporalv1beta1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: temporalNamespace, Namespace: k8sNamespace},
			},
		} {
			Expect(ctrlclient.IgnoreNotFound(k8sClient.Delete(ctx, obj))).To(Succeed())
		}
	})

	Context("when the resource does not exist", func() {
		It("should return cleanly without touching Temporal", func() {
			key = types.NamespacedName{Name: "absent-endpoint", Namespace: k8sNamespace}

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeTrue())
			expectNoTemporalContact()
		})
	})

	Context("when a dependency is not usable", func() {
		DescribeTable(
			"should wait rather than contacting Temporal",
			func(setup func(), reason string) {
				setup()
				createEndpoint(taskQueue)

				result, err := reconcile()
				Expect(err).NotTo(HaveOccurred(), "a missing dependency is not a controller failure")
				Expect(result.RequeueAfter).To(Equal(dependencyRetryInterval))
				expectNoTemporalContact()

				condition := readyCondition()
				Expect(condition.Status).To(Equal(metav1.ConditionFalse))
				Expect(condition.Reason).To(Equal(reason))
				Expect(ownership()).To(BeEmpty())
			},
			Entry("no Connection", func() {}, ReasonConnectionNotFound),
			Entry("an unvalidated Connection", func() { createConnection("") }, ReasonConnectionNotReady),
			Entry("a failed Connection", func() { createConnection(metav1.ConditionFalse) }, ReasonConnectionNotReady),
			Entry("no Namespace", func() { createConnection(metav1.ConditionTrue) }, ReasonNamespaceNotFound),
			Entry("an unvalidated Namespace", func() {
				createConnection(metav1.ConditionTrue)
				createTemporalNamespace("")
			}, ReasonNamespaceNotReady),
			Entry("a failed Namespace", func() {
				createConnection(metav1.ConditionTrue)
				createTemporalNamespace(metav1.ConditionFalse)
			}, ReasonNamespaceNotReady),
		)

		It("should proceed once the dependencies come good", func() {
			createConnection(metav1.ConditionTrue)
			createEndpoint(taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(readyCondition().Reason).To(Equal(ReasonNamespaceNotFound))

			createTemporalNamespace(metav1.ConditionTrue)

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCondition().Reason).To(Equal(ReasonCreated))
		})
	})

	Context("when the dependencies are ready", func() {
		BeforeEach(func() {
			createConnection(metav1.ConditionTrue)
			createTemporalNamespace(metav1.ConditionTrue)
		})

		It("should create a missing endpoint under the Temporal name", func() {
			resource := createEndpoint(taskQueue)

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(namespaceResyncInterval))

			Expect(temporalClient.created).To(Equal([]endpointCall{{
				name:   endpointName,
				target: endpointTarget{namespace: temporalNamespace, taskQueue: taskQueue},
			}}))
			Expect(temporalClient.existing).NotTo(HaveKey(resourceName),
				"the Kubernetes resource name must never reach Temporal")
			Expect(temporalClient.closed).To(BeTrue())

			Expect(ownership()).To(Equal(temporalv1beta1.NexusEndpointOwnershipCreated))
			Expect(stored().Status.EndpointID).NotTo(BeEmpty(), "the server ID is recorded as an observation")

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(ReasonCreated))
			Expect(condition.ObservedGeneration).To(Equal(resource.Generation))
		})

		It("should persist Creating before asking Temporal to create anything", func() {
			createEndpoint(taskQueue)

			var ownershipAtCreate temporalv1beta1.NexusEndpointOwnership
			original := reconciler.Connect
			reconciler.Connect = func(c context.Context, o *sdkclient.Options) (TemporalNexusEndpointClient, error) {
				inner, err := original(c, o)

				return &recordingEndpointClient{
					TemporalNexusEndpointClient: inner,
					onCreate: func() {
						// Read from the API server: the marker has to be
						// durable by now, not merely set in memory.
						ownershipAtCreate = stored().Status.Ownership
					},
				}, err
			}

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(ownershipAtCreate).To(Equal(temporalv1beta1.NexusEndpointOwnershipCreating))
		})

		It("should adopt an existing endpoint whose target already matches", func() {
			createEndpoint(taskQueue)
			temporalClient.put(endpointName, temporalNamespace, taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(BeEmpty())
			Expect(temporalClient.updated).To(BeEmpty(), "a matching target needs no update")
			Expect(ownership()).To(Equal(temporalv1beta1.NexusEndpointOwnershipAdopted))
			Expect(readyCondition().Reason).To(Equal(ReasonAdopted))
		})

		It("should keep ownership stable across repeated reconciles", func() {
			createEndpoint(taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.NexusEndpointOwnershipCreated))

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(ownership()).To(Equal(temporalv1beta1.NexusEndpointOwnershipCreated))
			Expect(readyCondition().Reason).To(Equal(ReasonReconciled))
			Expect(temporalClient.created).To(HaveLen(1))
		})

		It("should recover Created from the Creating marker when the status write fails", func() {
			createEndpoint(taskQueue)
			setOwnership(temporalv1beta1.NexusEndpointOwnershipCreating)

			// The endpoint is there under exactly the requested name, which is
			// the state an interrupted create leaves behind.
			temporalClient.put(endpointName, temporalNamespace, taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(ownership()).To(Equal(temporalv1beta1.NexusEndpointOwnershipCreated),
				"an interrupted create must not be mistaken for an adoption")
			Expect(temporalClient.created).To(BeEmpty())
		})

		It("should not rewrite an unchanged status", func() {
			createEndpoint(taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			before := stored()
			Expect(before.Status.Conditions[0].Reason).To(Equal(ReasonReconciled))

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(stored().ResourceVersion).To(Equal(before.ResourceVersion))
		})

		It("should report CreateFailed when Temporal refuses", func() {
			createEndpoint(taskQueue)
			temporalClient.createErr = errors.New("could not verify namespace referenced by target exists")

			_, err := reconcile()
			Expect(err).To(MatchError(temporalClient.createErr))

			Expect(readyCondition().Reason).To(Equal(ReasonCreateFailed))
			Expect(hasFinalizer()).To(BeTrue())
		})

		It("should report EndpointTaken when the name was claimed in the gap", func() {
			createEndpoint(taskQueue)
			temporalClient.createErr = fmt.Errorf("%w: %s", temporal.ErrNexusEndpointExists, endpointName)

			_, err := reconcile()
			Expect(err).To(MatchError(temporal.ErrNexusEndpointExists))

			Expect(readyCondition().Reason).To(Equal(ReasonEndpointTaken))
			Expect(ownership()).To(Equal(temporalv1beta1.NexusEndpointOwnershipCreating),
				"a name taken by something else settles nothing")
		})

		It("should report DescribeFailed without creating anything", func() {
			createEndpoint(taskQueue)
			temporalClient.describeErr = errors.New("permission denied")

			_, err := reconcile()
			Expect(err).To(MatchError(temporalClient.describeErr))

			Expect(temporalClient.created).To(BeEmpty())
			Expect(ownership()).To(BeEmpty())
			Expect(readyCondition().Reason).To(Equal(ReasonDescribeFailed))
		})

		It("should recreate an externally removed endpoint and stay Created", func() {
			createEndpoint(taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.NexusEndpointOwnershipCreated))

			delete(temporalClient.existing, endpointName)
			resetTemporalCalls()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(HaveLen(1), "the endpoint should be put back")
			Expect(temporalClient.created[0].name).To(Equal(endpointName))
			Expect(ownership()).To(Equal(temporalv1beta1.NexusEndpointOwnershipCreated))
		})

		It("should recreate an externally removed adopted endpoint and stay Adopted", func() {
			createEndpoint(taskQueue)
			temporalClient.put(endpointName, temporalNamespace, taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.NexusEndpointOwnershipAdopted))

			delete(temporalClient.existing, endpointName)

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(HaveLen(1))
			Expect(ownership()).To(Equal(temporalv1beta1.NexusEndpointOwnershipAdopted),
				"restoring someone else's endpoint does not make it the operator's to delete")
		})
	})

	Context("when the target has drifted", func() {
		BeforeEach(func() {
			createConnection(metav1.ConditionTrue)
			createTemporalNamespace(metav1.ConditionTrue)
		})

		It("should update an endpoint it created in place", func() {
			createEndpoint(taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			created := temporalClient.existing[endpointName]
			Expect(created.TaskQueue).To(Equal(taskQueue))

			setTaskQueue(otherTaskQueue)
			resetTemporalCalls()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.updated).To(Equal([]endpointCall{{
				name:   endpointName,
				target: endpointTarget{namespace: temporalNamespace, taskQueue: otherTaskQueue},
			}}))
			Expect(temporalClient.created).To(BeEmpty(), "the endpoint must not be replaced")
			Expect(temporalClient.deleted).To(BeEmpty())
			Expect(temporalClient.existing[endpointName].ID).To(Equal(created.ID),
				"the endpoint keeps its identity through an update")

			Expect(ownership()).To(Equal(temporalv1beta1.NexusEndpointOwnershipCreated))
			Expect(readyCondition().Reason).To(Equal(ReasonUpdated))
			Expect(readyCondition().Message).To(ContainSubstring(otherTaskQueue))
		})

		It("should manage drift on an adopted endpoint without taking it over", func() {
			createEndpoint(taskQueue)
			temporalClient.put(endpointName, temporalNamespace, otherTaskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			// Adoption means the operator keeps it in step, not that it owns it.
			Expect(temporalClient.updated).To(HaveLen(1))
			Expect(temporalClient.existing[endpointName].TaskQueue).To(Equal(taskQueue))
			Expect(ownership()).To(Equal(temporalv1beta1.NexusEndpointOwnershipAdopted))
			Expect(readyCondition().Reason).To(Equal(ReasonUpdated))
		})

		It("should not update when the target already matches", func() {
			createEndpoint(taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			resetTemporalCalls()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.updated).To(BeEmpty())
			Expect(readyCondition().Reason).To(Equal(ReasonReconciled))
		})

		It("should report a conflict when the endpoint changed under it", func() {
			createEndpoint(taskQueue)
			temporalClient.put(endpointName, temporalNamespace, otherTaskQueue)
			temporalClient.updateErr = fmt.Errorf("%w: %s", temporal.ErrNexusEndpointChanged, endpointName)

			_, err := reconcile()
			Expect(err).To(MatchError(temporal.ErrNexusEndpointChanged))

			Expect(readyCondition().Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCondition().Reason).To(Equal(ReasonConflict))

			// The retry starts again from a fresh read rather than guessing a
			// newer version, so once the interference stops it settles.
			temporalClient.updateErr = nil
			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
			Expect(temporalClient.existing[endpointName].TaskQueue).To(Equal(taskQueue))
		})

		It("should report UpdateFailed for anything else", func() {
			createEndpoint(taskQueue)
			temporalClient.put(endpointName, temporalNamespace, otherTaskQueue)
			temporalClient.updateErr = errors.New("service unavailable")

			_, err := reconcile()
			Expect(err).To(MatchError(temporalClient.updateErr))

			Expect(readyCondition().Reason).To(Equal(ReasonUpdateFailed))
		})
	})

	Context("when the resource is deleted", func() {
		BeforeEach(func() {
			createConnection(metav1.ConditionTrue)
			createTemporalNamespace(metav1.ConditionTrue)
		})

		It("should remove an endpoint it created", func() {
			createEndpoint(taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.NexusEndpointOwnershipCreated))

			beginDeletion()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.deleted).To(Equal([]string{endpointName}))
			Expect(temporalClient.existing).NotTo(HaveKey(endpointName))
			Expect(isGone()).To(BeTrue())
		})

		It("should leave an adopted endpoint alone", func() {
			createEndpoint(taskQueue)
			temporalClient.put(endpointName, temporalNamespace, taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.NexusEndpointOwnershipAdopted))

			beginDeletion()
			resetTemporalCalls()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			expectNoTemporalContact()
			Expect(temporalClient.existing).To(HaveKey(endpointName))
			Expect(isGone()).To(BeTrue())
		})

		DescribeTable(
			"should release without contacting Temporal under Orphan",
			func(ownershipValue temporalv1beta1.NexusEndpointOwnership) {
				createEndpoint(taskQueue)

				_, err := reconcile()
				Expect(err).NotTo(HaveOccurred())

				setOwnership(ownershipValue)
				setDeletionPolicy(temporalv1beta1.NexusEndpointDeletionPolicyOrphan)
				beginDeletion()
				resetTemporalCalls()

				_, err = reconcile()
				Expect(err).NotTo(HaveOccurred())

				expectNoTemporalContact()
				Expect(temporalClient.existing).To(HaveKey(endpointName))
				Expect(isGone()).To(BeTrue())
			},
			Entry("Created", temporalv1beta1.NexusEndpointOwnershipCreated),
			Entry("Creating", temporalv1beta1.NexusEndpointOwnershipCreating),
			Entry("Adopted", temporalv1beta1.NexusEndpointOwnershipAdopted),
		)

		It("should release under Orphan even with no Connection at all", func() {
			createEndpoint(taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			deleteConnection()
			setDeletionPolicy(temporalv1beta1.NexusEndpointDeletionPolicyOrphan)
			beginDeletion()
			resetTemporalCalls()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			expectNoTemporalContact()
			Expect(isGone()).To(BeTrue())
		})

		It("should hold the finalizer under Delete when the Connection has gone", func() {
			createEndpoint(taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			deleteConnection()
			beginDeletion()
			resetTemporalCalls()

			_, err = reconcile()
			Expect(err).To(MatchError(ContainSubstring(connectionName)))

			Expect(hasFinalizer()).To(BeTrue(), "an unprovable external state must not be assumed away")
			Expect(temporalClient.existing).To(HaveKey(endpointName))
			Expect(readyCondition().Reason).To(Equal(ReasonConnectionNotFound))
		})

		It("should treat an endpoint that has already gone as deleted", func() {
			createEndpoint(taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			delete(temporalClient.existing, endpointName)
			beginDeletion()
			resetTemporalCalls()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.deleted).To(BeEmpty(), "nothing to remove")
			Expect(isGone()).To(BeTrue())
		})

		It("should still delete when the Namespace resource has gone", func() {
			createEndpoint(taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			// A Nexus endpoint belongs to the Service, not to a namespace. The
			// Namespace resource going away says nothing about whether the
			// endpoint is still registered, and finalisation never asks it.
			ns := &temporalv1beta1.Namespace{}
			nsKey := types.NamespacedName{Name: temporalNamespace, Namespace: k8sNamespace}
			Expect(k8sClient.Get(ctx, nsKey, ns)).To(Succeed())
			Expect(k8sClient.Delete(ctx, ns)).To(Succeed())

			beginDeletion()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.deleted).To(Equal([]string{endpointName}))
			Expect(isGone()).To(BeTrue())
		})

		It("should hold the finalizer when the removal fails", func() {
			createEndpoint(taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			temporalClient.deleteErr = errors.New("service unavailable")
			beginDeletion()

			_, err = reconcile()
			Expect(err).To(MatchError(temporalClient.deleteErr))

			Expect(hasFinalizer()).To(BeTrue())
			Expect(readyCondition().Reason).To(Equal(ReasonDeleteFailed))

			temporalClient.deleteErr = nil
			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(isGone()).To(BeTrue())
		})

		It("should hold the finalizer when the lookup fails", func() {
			createEndpoint(taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			temporalClient.describeErr = errors.New("service unavailable")
			beginDeletion()
			resetTemporalCalls()

			_, err = reconcile()
			Expect(err).To(MatchError(temporalClient.describeErr))

			Expect(temporalClient.deleted).To(BeEmpty(),
				"an endpoint that cannot be read must not be assumed gone")
			Expect(hasFinalizer()).To(BeTrue())
			Expect(readyCondition().Reason).To(Equal(ReasonDescribeFailed))
		})

		It("should add the finalizer without disturbing the spec", func() {
			resource := createEndpoint(taskQueue)
			Expect(resource.Generation).To(Equal(int64(1)))

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			after := stored()
			Expect(controllerutil.ContainsFinalizer(after, temporalv1beta1.NexusEndpointFinalizer)).To(BeTrue())
			Expect(after.Generation).To(Equal(int64(1)),
				"adding the finalizer must not change the spec")
		})
	})

	Context("validation and defaulting", func() {
		It("should default the deletion policy to Delete without admission", func() {
			spec := temporalv1beta1.NexusEndpointSpec{}
			Expect(spec.DeletionPolicy).To(BeEmpty())
			Expect(spec.DeletionPolicyValue()).
				To(Equal(temporalv1beta1.NexusEndpointDeletionPolicyDelete))
			Expect(temporalv1beta1.DefaultNexusEndpointDeletionPolicy).
				To(Equal(temporalv1beta1.NexusEndpointDeletionPolicyDelete))
		})

		It("should default the deletion policy at admission", func() {
			createEndpoint(taskQueue)

			Expect(stored().Spec.DeletionPolicy).
				To(Equal(temporalv1beta1.NexusEndpointDeletionPolicyDelete))
		})

		It("should map the Kubernetes resource onto the Temporal endpoint", func() {
			resource := createEndpoint(taskQueue)

			Expect(resourceName).NotTo(Equal(endpointName))
			Expect(resource.TemporalName()).To(Equal(endpointName))
			Expect(resource.TemporalNamespace()).To(Equal(temporalNamespace))
		})

		DescribeTable(
			"should be rejected by the API server",
			func(mutate func(*temporalv1beta1.NexusEndpointSpec), expected string) {
				spec := temporalv1beta1.NexusEndpointSpec{
					Name:          endpointName,
					ConnectionRef: corev1.LocalObjectReference{Name: connectionName},
					NamespaceRef:  corev1.LocalObjectReference{Name: temporalNamespace},
					TaskQueue:     taskQueue,
				}
				mutate(&spec)

				err := k8sClient.Create(ctx, &temporalv1beta1.NexusEndpoint{
					ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: k8sNamespace},
					Spec:       spec,
				})
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(expected))
			},
			Entry("with no Temporal name", func(s *temporalv1beta1.NexusEndpointSpec) {
				s.Name = ""
			}, "spec.name"),
			Entry("with no connectionRef name", func(s *temporalv1beta1.NexusEndpointSpec) {
				s.ConnectionRef.Name = ""
			}, "connectionRef.name is required"),
			Entry("with no namespaceRef name", func(s *temporalv1beta1.NexusEndpointSpec) {
				s.NamespaceRef.Name = ""
			}, "namespaceRef.name is required"),
			Entry("with no task queue", func(s *temporalv1beta1.NexusEndpointSpec) {
				s.TaskQueue = ""
			}, "spec.taskQueue"),
			Entry("with a deletion policy the API does not define", func(s *temporalv1beta1.NexusEndpointSpec) {
				s.DeletionPolicy = temporalv1beta1.NexusEndpointDeletionPolicy("Abandon")
			}, "Unsupported value"),
		)

		DescribeTable(
			"should refuse to change what the resource points at",
			func(mutate func(*temporalv1beta1.NexusEndpoint), expected string) {
				createEndpoint(taskQueue)

				resource := stored()
				mutate(resource)

				err := k8sClient.Update(ctx, resource)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(expected))
			},
			Entry("the Temporal name", func(e *temporalv1beta1.NexusEndpoint) {
				e.Spec.Name = "SomethingElse"
			}, "name is immutable"),
			Entry("the Connection", func(e *temporalv1beta1.NexusEndpoint) {
				e.Spec.ConnectionRef.Name = "another-connection"
			}, "connectionRef.name is immutable"),
		)

		It("should allow the target and policy to change", func() {
			createEndpoint(taskQueue)

			// The target is configuration, not identity: Temporal updates it in
			// place, so the API must let it move.
			resource := stored()
			resource.Spec.TaskQueue = otherTaskQueue
			resource.Spec.NamespaceRef.Name = "another-namespace"
			resource.Spec.DeletionPolicy = temporalv1beta1.NexusEndpointDeletionPolicyOrphan
			Expect(k8sClient.Update(ctx, resource)).To(Succeed())

			Expect(stored().Spec.TaskQueue).To(Equal(otherTaskQueue))
			Expect(stored().Spec.NamespaceRef.Name).To(Equal("another-namespace"))
		})

		It("should keep the deletion policy mutable while terminating", func() {
			createConnection(metav1.ConditionTrue)
			createTemporalNamespace(metav1.ConditionTrue)
			createEndpoint(taskQueue)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			beginDeletion()
			setDeletionPolicy(temporalv1beta1.NexusEndpointDeletionPolicyOrphan)

			Expect(stored().Spec.DeletionPolicy).
				To(Equal(temporalv1beta1.NexusEndpointDeletionPolicyOrphan))
			Expect(stored().GetDeletionTimestamp().IsZero()).To(BeFalse())

			resetTemporalCalls()
			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			expectNoTemporalContact()
			Expect(isGone()).To(BeTrue())
		})
	})
})

// recordingEndpointClient wraps the fake so a spec can observe the moment a
// call is made.
type recordingEndpointClient struct {
	TemporalNexusEndpointClient

	onCreate func()
}

func (c *recordingEndpointClient) CreateNexusEndpoint(
	ctx context.Context,
	name, namespace, taskQueue string,
) (*temporal.NexusEndpoint, error) {
	if c.onCreate != nil {
		c.onCreate()
	}

	return c.TemporalNexusEndpointClient.CreateNexusEndpoint(ctx, name, namespace, taskQueue)
}

var _ = Describe("NexusEndpoint dependencies", func() {
	const (
		connectionA = "conn-a"
		namespaceA  = "ns-a"
	)

	dependant := func(name, conn, ns string) *temporalv1beta1.NexusEndpoint {
		return &temporalv1beta1.NexusEndpoint{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: saNamespace},
			Spec: temporalv1beta1.NexusEndpointSpec{
				Name:          "Endpoint",
				ConnectionRef: corev1.LocalObjectReference{Name: conn},
				NamespaceRef:  corev1.LocalObjectReference{Name: ns},
				TaskQueue:     "queue",
			},
		}
	}

	request := func(k8sNamespace, name string) ctrl.Request {
		return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: k8sNamespace, Name: name}}
	}

	Describe("the field indexes", func() {
		DescribeTable(
			"should index a NexusEndpoint by what it references",
			func(index func(ctrlclient.Object) []string, obj ctrlclient.Object, expected []string) {
				Expect(index(obj)).To(Equal(expected))
			},
			Entry("its Connection", indexNexusEndpointByConnection,
				dependant("by-connection", connectionA, namespaceA), []string{connectionA}),
			Entry("its Namespace", indexNexusEndpointByNamespace,
				dependant("by-namespace", connectionA, namespaceA), []string{namespaceA}),
			Entry("an empty Connection reference", indexNexusEndpointByConnection,
				dependant("no-connection", "", namespaceA), []string(nil)),
			Entry("an empty Namespace reference", indexNexusEndpointByNamespace,
				dependant("no-namespace", connectionA, ""), []string(nil)),
			Entry("a nil NexusEndpoint", indexNexusEndpointByConnection,
				(*temporalv1beta1.NexusEndpoint)(nil), []string(nil)),
			Entry("an object of another kind", indexNexusEndpointByNamespace,
				&temporalv1beta1.Connection{ObjectMeta: metav1.ObjectMeta{Name: connectionA}}, []string(nil)),
		)
	})

	Describe("mapping a dependency event onto its dependants", func() {
		var mapper *NexusEndpointReconciler

		BeforeEach(func() {
			elsewhere := dependant("ep-elsewhere", connectionA, namespaceA)
			elsewhere.Namespace = otherNamespace

			mapper = &NexusEndpointReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(k8sClient.Scheme()).
					WithIndex(&temporalv1beta1.NexusEndpoint{},
						nexusEndpointConnectionRefIndex, indexNexusEndpointByConnection).
					WithIndex(&temporalv1beta1.NexusEndpoint{},
						nexusEndpointNamespaceRefIndex, indexNexusEndpointByNamespace).
					WithObjects(
						dependant("ep-one", connectionA, namespaceA),
						dependant("ep-two", connectionA, namespaceA),
						dependant("ep-other", "conn-b", "ns-b"),
						elsewhere,
					).
					Build(),
			}
		})

		It("should map a Connection to its dependants beside it", func() {
			conn := &temporalv1beta1.Connection{
				ObjectMeta: metav1.ObjectMeta{Name: connectionA, Namespace: saNamespace},
			}

			requests := mapper.nexusEndpointsForDependency(nexusEndpointConnectionRefIndex)(ctx, conn)

			Expect(requests).To(ConsistOf(request(saNamespace, "ep-one"), request(saNamespace, "ep-two")))
			Expect(requests).NotTo(ContainElement(request(saNamespace, "ep-other")))
			Expect(requests).NotTo(ContainElement(request(otherNamespace, "ep-elsewhere")))
		})

		It("should map a Namespace to its dependants beside it", func() {
			ns := &temporalv1beta1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: namespaceA, Namespace: saNamespace},
			}

			requests := mapper.nexusEndpointsForDependency(nexusEndpointNamespaceRefIndex)(ctx, ns)

			Expect(requests).To(ConsistOf(request(saNamespace, "ep-one"), request(saNamespace, "ep-two")))
			Expect(requests).NotTo(ContainElement(request(otherNamespace, "ep-elsewhere")))
		})

		It("should map the same however the event arose", func() {
			ready := &temporalv1beta1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: namespaceA, Namespace: saNamespace},
			}
			meta.SetStatusCondition(&ready.Status.Conditions, metav1.Condition{
				Type: temporalv1beta1.ConditionTypeReady, Status: metav1.ConditionTrue, Reason: ReasonCreated,
			})

			deleted := ready.DeepCopy()
			deleted.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}

			expected := ConsistOf(request(saNamespace, "ep-one"), request(saNamespace, "ep-two"))
			mapNamespace := mapper.nexusEndpointsForDependency(nexusEndpointNamespaceRefIndex)

			Expect(mapNamespace(ctx, ready)).To(expected, "on a status change")
			Expect(mapNamespace(ctx, deleted)).To(expected, "on delete")
		})

		It("should return nothing when nothing depends on the event", func() {
			unused := &temporalv1beta1.Connection{
				ObjectMeta: metav1.ObjectMeta{Name: "unreferenced-connection", Namespace: saNamespace},
			}

			Expect(mapper.nexusEndpointsForDependency(nexusEndpointConnectionRefIndex)(ctx, unused)).
				To(BeEmpty())
		})

		It("should give up quietly when the list fails", func() {
			failing := &NexusEndpointReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(k8sClient.Scheme()).
					WithIndex(&temporalv1beta1.NexusEndpoint{},
						nexusEndpointConnectionRefIndex, indexNexusEndpointByConnection).
					WithInterceptorFuncs(interceptor.Funcs{
						List: func(
							context.Context, ctrlclient.WithWatch,
							ctrlclient.ObjectList, ...ctrlclient.ListOption,
						) error {
							return errors.New("cache is not ready")
						},
					}).
					Build(),
			}

			conn := &temporalv1beta1.Connection{
				ObjectMeta: metav1.ObjectMeta{Name: connectionA, Namespace: saNamespace},
			}

			var requests []ctrl.Request
			Expect(func() {
				requests = failing.nexusEndpointsForDependency(nexusEndpointConnectionRefIndex)(ctx, conn)
			}).NotTo(Panic())

			Expect(requests).To(BeEmpty())
		})
	})

	Describe("controller setup", func() {
		It("should register both indexes and both watches", func() {
			mgr, err := ctrl.NewManager(cfg, ctrl.Options{
				Scheme:                 k8sClient.Scheme(),
				Metrics:                metricsserver.Options{BindAddress: "0"},
				HealthProbeBindAddress: "0",
			})
			Expect(err).NotTo(HaveOccurred())

			reconciler := &NexusEndpointReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}

			Expect(reconciler.SetupWithManager(mgr)).To(Succeed())
		})
	})
})
