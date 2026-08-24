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
	"maps"
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
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	temporalv1beta1 "github.com/mrsimonemms/temporal-resource-operator/api/v1beta1"
	"github.com/mrsimonemms/temporal-resource-operator/internal/connection"
	"github.com/mrsimonemms/temporal-resource-operator/internal/temporal"
)

// missingSecretName names a Secret that deliberately does not exist, used to
// make resolving a Connection fail.
const missingSecretName = "missing-secret"

// connectionReadyMessage mirrors what the Connection controller reports on a
// healthy Connection.
const connectionReadyMessage = "Temporal Service is reachable and healthy"

// namespaceCall records a Temporal namespace call and the retention it carried.
type namespaceCall struct {
	name      string
	retention time.Duration
}

// createCall records a registration and the metadata it carried.
type createCall struct {
	name      string
	retention time.Duration
	data      map[string]string
}

// fakeNamespaceClient stands in for a Temporal Service. Namespaces in existing
// are reported as present; everything else is reported missing in the same
// shape the real client uses.
type fakeNamespaceClient struct {
	existing    map[string]*temporal.Namespace
	describeErr error
	createErr   error
	updateErr   error
	deleteErr   error

	// onCreate runs just before a namespace is registered, so specs can assert
	// what the operator had persisted by that point.
	onCreate func(name string)

	// onDelete runs after a delete has been answered, letting a spec make the
	// next attempt behave differently.
	onDelete func()

	described []string
	created   []createCall
	updated   []namespaceCall
	deleted   []string
	closed    bool
}

func (f *fakeNamespaceClient) DescribeNamespace(_ context.Context, name string) (*temporal.Namespace, error) {
	f.described = append(f.described, name)

	if f.describeErr != nil {
		return nil, f.describeErr
	}

	if ns, ok := f.existing[name]; ok {
		// Return a copy, as the real client does. Handing back the stored
		// pointer would let a later update mutate what the caller is holding.
		described := *ns
		described.Data = maps.Clone(ns.Data)

		return &described, nil
	}

	return nil, namespaceNotFound(name)
}

func (f *fakeNamespaceClient) CreateNamespace(
	_ context.Context,
	name string,
	retention time.Duration,
	data map[string]string,
) error {
	if f.onCreate != nil {
		f.onCreate(name)
	}

	f.created = append(f.created, createCall{name: name, retention: retention, data: maps.Clone(data)})

	if f.createErr != nil {
		return f.createErr
	}

	// A real Service would now report the namespace, metadata and all, which is
	// what makes the interrupted-create specs meaningful.
	f.existing[name] = &temporal.Namespace{Name: name, Retention: retention, Data: maps.Clone(data)}

	return nil
}

func (f *fakeNamespaceClient) UpdateNamespaceRetention(_ context.Context, name string, retention time.Duration) error {
	f.updated = append(f.updated, namespaceCall{name: name, retention: retention})

	if f.updateErr != nil {
		return f.updateErr
	}

	if ns, ok := f.existing[name]; ok {
		ns.Retention = retention
	}

	return nil
}

func (f *fakeNamespaceClient) DeleteNamespace(_ context.Context, name string) error {
	f.deleted = append(f.deleted, name)

	if f.onDelete != nil {
		defer f.onDelete()
	}

	if f.deleteErr != nil {
		return f.deleteErr
	}

	if _, ok := f.existing[name]; !ok {
		return namespaceNotFound(name)
	}

	delete(f.existing, name)

	return nil
}

// deleteNotFoundOnce makes the next delete report the namespace missing while
// leaving it in place, reproducing the Service answering describe from storage
// but delete from a namespace registry that has not caught up yet.
func (f *fakeNamespaceClient) deleteNotFoundOnce() {
	f.deleteErr = namespaceNotFound("stale registry")

	f.onDelete = func() { f.deleteErr = nil }
}

func (f *fakeNamespaceClient) Close() { f.closed = true }

// namespaceNotFound builds the error the real client produces for a missing
// namespace, so the specs exercise the same errors.Is contract the controller
// relies on.
func namespaceNotFound(name string) error {
	return fmt.Errorf("%w: %w", temporal.ErrNamespaceNotFound, serviceerror.NewNamespaceNotFound(name))
}

// flakyClient lets a spec fail a status write without disturbing the API
// server, which is how the create-then-fail-to-persist window is reproduced.
type flakyClient struct {
	ctrlclient.Client

	// onStatusUpdate is consulted before every status write. A non-nil result
	// fails the write.
	onStatusUpdate func(ns *temporalv1beta1.Namespace) error
}

func (c *flakyClient) Status() ctrlclient.SubResourceWriter {
	return &flakyStatusWriter{SubResourceWriter: c.Client.Status(), client: c}
}

type flakyStatusWriter struct {
	ctrlclient.SubResourceWriter
	client *flakyClient
}

func (w *flakyStatusWriter) Update(
	ctx context.Context,
	obj ctrlclient.Object,
	opts ...ctrlclient.SubResourceUpdateOption,
) error {
	if hook := w.client.onStatusUpdate; hook != nil {
		if ns, ok := obj.(*temporalv1beta1.Namespace); ok {
			if err := hook(ns); err != nil {
				return err
			}
		}
	}

	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

var _ = Describe("Namespace Connection dependency", func() {
	// The Connection two of the fixture Namespaces depend on, and one they do
	// not.
	const (
		dependedOn = "foo"
		unrelated  = "bar"
	)

	// indexedClient builds a client with the real index registered, so the
	// mapper is exercised against the same lookup it uses in production.
	indexedClient := func(objs ...ctrlclient.Object) ctrlclient.Client {
		return fake.NewClientBuilder().
			WithScheme(k8sClient.Scheme()).
			WithIndex(&temporalv1beta1.Namespace{}, namespaceConnectionRefIndex, indexNamespaceByConnection).
			WithObjects(objs...).
			Build()
	}

	// dependant builds a Namespace in the given Kubernetes namespace pointing at
	// the named Connection.
	dependant := func(k8sNamespace, name, connection string) *temporalv1beta1.Namespace {
		return &temporalv1beta1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: k8sNamespace},
			Spec: temporalv1beta1.NamespaceSpec{
				ConnectionRef: corev1.LocalObjectReference{Name: connection},
			},
		}
	}

	// request is the reconcile request the watch should produce for a dependant.
	request := func(k8sNamespace, name string) ctrl.Request {
		return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: k8sNamespace, Name: name}}
	}

	Describe("the field index", func() {
		DescribeTable(
			"should index a Namespace by the Connection it references",
			func(obj ctrlclient.Object, expected []string) {
				Expect(indexNamespaceByConnection(obj)).To(Equal(expected))
			},
			Entry("a referenced Connection", dependant("default", "ns", dependedOn), []string{dependedOn}),
			Entry("an empty reference",
				dependant("default", "ns", ""), []string(nil)),
			Entry("a nil Namespace",
				(*temporalv1beta1.Namespace)(nil), []string(nil)),
			Entry("an object of another kind",
				&temporalv1beta1.Connection{ObjectMeta: metav1.ObjectMeta{Name: dependedOn}}, []string(nil)),
		)

		It("should not be affected by the rest of the spec", func() {
			ns := dependant("default", "ns", dependedOn)
			ns.Spec.Retention = &temporalv1beta1.Duration{Duration: time.Hour}
			ns.Spec.DeletionPolicy = temporalv1beta1.NamespaceDeletionPolicyOrphan
			ns.Status.Ownership = temporalv1beta1.NamespaceOwnershipCreated

			Expect(indexNamespaceByConnection(ns)).To(Equal([]string{dependedOn}))
		})
	})

	Describe("mapping a Connection event onto its dependants", func() {
		var (
			mapper *NamespaceReconciler
			foo    *temporalv1beta1.Connection
		)

		BeforeEach(func() {
			// default/ns-a and default/ns-b depend on foo; default/ns-c uses a
			// different Connection, and other/ns-d is a like-named Connection in
			// a different Kubernetes namespace entirely.
			mapper = &NamespaceReconciler{
				Client: indexedClient(
					dependant("default", "ns-a", dependedOn),
					dependant("default", "ns-b", dependedOn),
					dependant("default", "ns-c", unrelated),
					dependant("other", "ns-d", dependedOn),
				),
			}

			foo = &temporalv1beta1.Connection{
				ObjectMeta: metav1.ObjectMeta{Name: dependedOn, Namespace: "default"},
			}
		})

		It("should enqueue only the dependants beside the Connection", func() {
			requests := mapper.namespacesForConnection(ctx, foo)

			Expect(requests).To(ConsistOf(
				request("default", "ns-a"),
				request("default", "ns-b"),
			))
			Expect(requests).NotTo(ContainElement(request("default", "ns-c")),
				"a Namespace referencing another Connection is not a dependant")
			Expect(requests).NotTo(ContainElement(request("other", "ns-d")),
				"a like-named Connection in another Kubernetes namespace is a different Connection")
		})

		It("should produce no duplicates", func() {
			requests := mapper.namespacesForConnection(ctx, foo)

			seen := map[ctrl.Request]int{}
			for _, req := range requests {
				seen[req]++
			}

			for req, count := range seen {
				Expect(count).To(Equal(1), "%s was enqueued more than once", req)
			}
		})

		It("should map the same however the event arose", func() {
			// The handler runs this for creates, updates and deletes alike, and
			// a status-only update carries the same object identity, so all of
			// them resolve to the same dependants.
			ready := foo.DeepCopy()
			meta.SetStatusCondition(&ready.Status.Conditions, metav1.Condition{
				Type:   temporalv1beta1.ConditionTypeReady,
				Status: metav1.ConditionTrue,
				Reason: ReasonConnected,
			})

			deleted := foo.DeepCopy()
			deleted.DeletionTimestamp = &metav1.Time{Time: time.Now()}

			expected := ConsistOf(request("default", "ns-a"), request("default", "ns-b"))

			Expect(mapper.namespacesForConnection(ctx, foo)).To(expected, "on create")
			Expect(mapper.namespacesForConnection(ctx, ready)).To(expected, "on a status change")
			Expect(mapper.namespacesForConnection(ctx, deleted)).To(expected, "on delete")
		})

		It("should return nothing when no Namespace depends on the Connection", func() {
			unused := &temporalv1beta1.Connection{
				ObjectMeta: metav1.ObjectMeta{Name: "unused", Namespace: "default"},
			}

			Expect(mapper.namespacesForConnection(ctx, unused)).To(BeEmpty())
		})

		It("should give up quietly when the list fails", func() {
			listErr := errors.New("cache is not ready")
			failing := &NamespaceReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(k8sClient.Scheme()).
					WithIndex(&temporalv1beta1.Namespace{},
						namespaceConnectionRefIndex, indexNamespaceByConnection).
					WithInterceptorFuncs(interceptor.Funcs{
						List: func(
							context.Context, ctrlclient.WithWatch,
							ctrlclient.ObjectList, ...ctrlclient.ListOption,
						) error {
							return listErr
						},
					}).
					Build(),
			}

			var requests []ctrl.Request
			Expect(func() {
				requests = failing.namespacesForConnection(ctx, foo)
			}).NotTo(Panic())

			Expect(requests).To(BeEmpty())
		})
	})

	Describe("controller setup", func() {
		It("should register the index and the Connection watch", func() {
			mgr, err := ctrl.NewManager(cfg, ctrl.Options{
				Scheme:                 k8sClient.Scheme(),
				Metrics:                metricsserver.Options{BindAddress: "0"},
				HealthProbeBindAddress: "0",
			})
			Expect(err).NotTo(HaveOccurred())

			reconciler := &NamespaceReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}

			// This fails if the index cannot be registered - a duplicate name,
			// the wrong object type - or if the watch cannot be built.
			Expect(reconciler.SetupWithManager(mgr)).To(Succeed())
		})
	})
})

var _ = Describe("Namespace Controller", func() {
	const (
		namespace = "default"
		address   = "localhost:7233"
	)

	var (
		reconciler     *NamespaceReconciler
		k8s            *flakyClient
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

	// stored returns the resource under test as the API server holds it.
	stored := func() *temporalv1beta1.Namespace {
		ns := &temporalv1beta1.Namespace{}
		Expect(k8sClient.Get(ctx, key, ns)).To(Succeed())

		return ns
	}

	// readyCondition returns the Ready condition on the resource under test.
	readyCondition := func() *metav1.Condition {
		return meta.FindStatusCondition(stored().Status.Conditions, temporalv1beta1.ConditionTypeReady)
	}

	// ownership returns the persisted ownership of the resource under test.
	ownership := func() temporalv1beta1.NamespaceOwnership {
		return stored().Status.Ownership
	}

	// hasFinalizer reports whether the stored resource still holds the
	// finalizer.
	hasFinalizer := func() bool {
		return controllerutil.ContainsFinalizer(stored(), temporalv1beta1.NamespaceFinalizer)
	}

	// isGone reports whether the resource has actually left the API server.
	isGone := func() bool {
		return apierrors.IsNotFound(k8sClient.Get(ctx, key, &temporalv1beta1.Namespace{}))
	}

	// createConnection persists a Connection and, when status is non-empty,
	// gives it a Ready condition with that status.
	createConnection := func(status metav1.ConditionStatus) *temporalv1beta1.Connection {
		conn := &temporalv1beta1.Connection{
			ObjectMeta: metav1.ObjectMeta{Name: connectionName, Namespace: namespace},
			Spec:       temporalv1beta1.ConnectionSpec{Address: address},
		}
		Expect(k8sClient.Create(ctx, conn)).To(Succeed())

		if status == "" {
			return conn
		}

		meta.SetStatusCondition(&conn.Status.Conditions, metav1.Condition{
			Type:               temporalv1beta1.ConditionTypeReady,
			Status:             status,
			Reason:             ReasonConnected,
			Message:            connectionReadyMessage,
			ObservedGeneration: conn.Generation,
		})
		Expect(k8sClient.Status().Update(ctx, conn)).To(Succeed())

		return conn
	}

	// createNamespace persists a Namespace referencing the test Connection.
	createNamespace := func(retention *temporalv1beta1.Duration) *temporalv1beta1.Namespace {
		ns := &temporalv1beta1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: temporalv1beta1.NamespaceSpec{
				ConnectionRef: corev1.LocalObjectReference{Name: connectionName},
				Retention:     retention,
			},
		}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())

		return ns
	}

	// ownerMarker builds the Temporal metadata the operator stamps on a
	// namespace it registers.
	ownerMarker := func(uid string) map[string]string {
		return map[string]string{namespaceOwnerKey: uid}
	}

	// currentUID returns the UID Kubernetes assigned to the resource under
	// test.
	currentUID := func() string {
		return string(stored().UID)
	}

	// putTemporalNamespace seeds the fake Service with a namespace, with or
	// without an ownership marker.
	putTemporalNamespace := func(retention time.Duration, data map[string]string) {
		temporalClient.existing[name] = &temporal.Namespace{
			Name:      name,
			Retention: retention,
			Data:      data,
		}
	}

	// setDeletionPolicy patches the deletion policy on the stored resource. It
	// works on a terminating resource too, which is the recovery path out of a
	// deletion the operator cannot complete.
	setDeletionPolicy := func(policy temporalv1beta1.NamespaceDeletionPolicy) {
		ns := stored()
		ns.Spec.DeletionPolicy = policy
		Expect(k8sClient.Update(ctx, ns)).To(Succeed())
	}

	// deleteConnection removes the referenced Connection, so that anything
	// needing it during finalisation has nothing to work with.
	deleteConnection := func() {
		conn := &temporalv1beta1.Connection{}
		connKey := types.NamespacedName{Name: connectionName, Namespace: namespace}
		Expect(k8sClient.Get(ctx, connKey, conn)).To(Succeed())
		Expect(k8sClient.Delete(ctx, conn)).To(Succeed())
	}

	// breakConnection points the Connection at a Secret that does not exist, so
	// that resolving it fails.
	breakConnection := func() {
		conn := &temporalv1beta1.Connection{}
		connKey := types.NamespacedName{Name: connectionName, Namespace: namespace}
		Expect(k8sClient.Get(ctx, connKey, conn)).To(Succeed())
		conn.Spec.CredentialsSecretRef = &corev1.LocalObjectReference{Name: missingSecretName}
		Expect(k8sClient.Update(ctx, conn)).To(Succeed())
	}

	// resetTemporalCalls forgets everything recorded so far, so that an
	// assertion can speak about a single reconcile rather than the whole spec.
	resetTemporalCalls := func() {
		dialled = 0
		temporalClient.described = nil
		temporalClient.created = nil
		temporalClient.updated = nil
		temporalClient.deleted = nil
	}

	// expectNoTemporalContact asserts the operator went nowhere near Temporal.
	expectNoTemporalContact := func() {
		Expect(dialled).To(BeZero(), "Temporal should not have been dialled")
		Expect(temporalClient.described).To(BeEmpty(), "Temporal should not have been described")
		Expect(temporalClient.created).To(BeEmpty())
		Expect(temporalClient.updated).To(BeEmpty())
		Expect(temporalClient.deleted).To(BeEmpty())
	}

	// setOwnership forces a starting ownership, standing in for whatever an
	// earlier reconcile would have left behind.
	setOwnership := func(value temporalv1beta1.NamespaceOwnership) {
		ns := stored()
		ns.Status.Ownership = value
		Expect(k8sClient.Status().Update(ctx, ns)).To(Succeed())
	}

	// clearOwnership returns the resource to never-established ownership. The
	// enum rejects an empty value on the way in, so it goes via a merge patch.
	clearOwnership := func() {
		Expect(k8sClient.Status().Patch(
			ctx, stored(),
			ctrlclient.RawPatch(types.MergePatchType, []byte(`{"status":{"ownership":null}}`)),
		)).To(Succeed())
		Expect(ownership()).To(BeEmpty())
	}

	// beginDeletion marks the resource for deletion. The finalizer keeps it
	// present, so the deletion reconcile has something to work on.
	beginDeletion := func() {
		Expect(hasFinalizer()).To(BeTrue(), "the resource needs a finalizer to survive deletion")
		Expect(k8sClient.Delete(ctx, stored())).To(Succeed())
		Expect(stored().GetDeletionTimestamp().IsZero()).To(BeFalse())
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

		k8s = &flakyClient{Client: k8sClient}

		reconciler = &NamespaceReconciler{
			Client:   k8s,
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
		// Release the finalizer before cleaning up, so a spec that deliberately
		// wedges deletion cannot leave the resource behind.
		ns := &temporalv1beta1.Namespace{}
		if err := k8sClient.Get(ctx, key, ns); err == nil {
			if controllerutil.RemoveFinalizer(ns, temporalv1beta1.NamespaceFinalizer) {
				Expect(k8sClient.Update(ctx, ns)).To(Succeed())
			}
			Expect(ctrlclient.IgnoreNotFound(k8sClient.Delete(ctx, ns))).To(Succeed())
		}

		conn := &temporalv1beta1.Connection{}
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

		It("should return cleanly once the resource has really gone", func() {
			createConnection(metav1.ConditionTrue)
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			beginDeletion()

			// The finalizer flow releases the resource...
			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(isGone()).To(BeTrue())

			// ...and a reconcile arriving afterwards is a no-op.
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

			Expect(readyCondition().Reason).To(Equal(ReasonConnectionNotReady))
			Expect(ownership()).To(BeEmpty(), "ownership must not be guessed at without talking to Temporal")
		})
	})

	Context("when establishing ownership", func() {
		BeforeEach(func() {
			createConnection(metav1.ConditionTrue)
		})

		It("should record Created for a namespace it registers", func() {
			ns := createNamespace(&temporalv1beta1.Duration{Duration: 24 * time.Hour})

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(namespaceResyncInterval))

			Expect(temporalClient.created).To(HaveLen(1))
			Expect(temporalClient.created[0].name).To(Equal(name))
			Expect(temporalClient.created[0].retention).To(Equal(24*time.Hour),
				"the requested retention should reach Temporal unchanged")
			Expect(temporalClient.created[0].data).To(Equal(ownerMarker(currentUID())))
			Expect(temporalClient.closed).To(BeTrue(), "the Temporal client should be closed")

			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreated))

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(ReasonCreated))
			Expect(condition.ObservedGeneration).To(Equal(ns.Generation))
		})

		It("should send a day-based retention to Temporal as hours", func() {
			// The retention that caused the outage, reconciled end to end. "7d"
			// is 168 hours, and Temporal is told the duration, not the string.
			retention, err := temporalv1beta1.ParseDuration("7d")
			Expect(err).NotTo(HaveOccurred())

			createNamespace(&temporalv1beta1.Duration{Duration: retention})

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(HaveLen(1))
			Expect(temporalClient.created[0].retention).To(Equal(168*time.Hour),
				"7d should reach Temporal as 168h")

			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
		})

		It("should send the minimum retention to Temporal as 24h", func() {
			retention, err := temporalv1beta1.ParseDuration("1d")
			Expect(err).NotTo(HaveOccurred())

			createNamespace(&temporalv1beta1.Duration{Duration: retention})

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(HaveLen(1))
			Expect(temporalClient.created[0].retention).To(Equal(24 * time.Hour))
			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
		})

		It("should send the maximum retention to Temporal as 2160h", func() {
			retention, err := temporalv1beta1.ParseDuration("90d")
			Expect(err).NotTo(HaveOccurred())

			createNamespace(&temporalv1beta1.Duration{Duration: retention})

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(HaveLen(1))
			Expect(temporalClient.created[0].retention).To(Equal(2160 * time.Hour))
			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
		})

		DescribeTable(
			"should reconcile equivalent notations to the same Temporal duration",
			func(notation string, expected time.Duration) {
				retention, err := temporalv1beta1.ParseDuration(notation)
				Expect(err).NotTo(HaveOccurred())

				createNamespace(&temporalv1beta1.Duration{Duration: retention})

				_, err = reconcile()
				Expect(err).NotTo(HaveOccurred())

				Expect(temporalClient.created).To(HaveLen(1))
				Expect(temporalClient.created[0].retention).To(Equal(expected),
					"%q should reach Temporal as %s", notation, expected)
			},
			Entry("1d", "1d", 24*time.Hour),
			Entry("24h", "24h", 24*time.Hour),
			Entry("1440m", "1440m", 24*time.Hour),
			Entry("86400s", "86400s", 24*time.Hour),
			Entry("7d", "7d", 168*time.Hour),
			Entry("168h", "168h", 168*time.Hour),
			Entry("90d", "90d", 2160*time.Hour),
			Entry("2160h", "2160h", 2160*time.Hour),
		)

		It("should default an omitted retention to 72h", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(HaveLen(1))
			Expect(temporalClient.created[0].retention).To(Equal(temporalv1beta1.DefaultRetention))
			Expect(temporalClient.created[0].retention).To(Equal(72 * time.Hour))
		})

		// An out-of-range retention cannot be created any more, because the CRD
		// rejects it - which is why these specs are backed by a fake client
		// holding the object directly. That stands in for an object stored
		// before the range existed: it still arrives through the cache, and the
		// answer is to say so rather than to contact Temporal with a duration
		// nobody asked for.
		DescribeTable(
			"should refuse to reconcile an out-of-range retention",
			func(retention time.Duration) {
				stale := &temporalv1beta1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name:       name,
						Namespace:  namespace,
						Finalizers: []string{temporalv1beta1.NamespaceFinalizer},
					},
					Spec: temporalv1beta1.NamespaceSpec{
						ConnectionRef: corev1.LocalObjectReference{Name: connectionName},
						Retention:     &temporalv1beta1.Duration{Duration: retention},
					},
				}

				stored := fake.NewClientBuilder().
					WithScheme(k8sClient.Scheme()).
					WithIndex(&temporalv1beta1.Namespace{}, namespaceConnectionRefIndex,
						indexNamespaceByConnection).
					WithObjects(stale).
					WithStatusSubresource(stale).
					Build()

				invalid := &NamespaceReconciler{
					Client:   stored,
					Scheme:   stored.Scheme(),
					Resolver: reconciler.Resolver,
					Connect:  reconciler.Connect,
				}

				result, err := invalid.Reconcile(ctx, ctrl.Request{NamespacedName: key})
				Expect(err).NotTo(HaveOccurred(), "an invalid spec is not a retryable failure")
				Expect(result.RequeueAfter).To(BeZero(), "only a spec change can fix this")

				Expect(temporalClient.created).To(BeEmpty(), "Temporal must not be contacted")
				Expect(temporalClient.existing).To(BeEmpty())
				Expect(dialled).To(BeZero(), "the Connection must not even be dialled")

				// The resource says why, rather than looking merely unready.
				current := &temporalv1beta1.Namespace{}
				Expect(stored.Get(ctx, key, current)).To(Succeed())

				condition := meta.FindStatusCondition(
					current.Status.Conditions, temporalv1beta1.ConditionTypeReady,
				)
				Expect(condition).NotTo(BeNil())
				Expect(condition.Status).To(Equal(metav1.ConditionFalse))
				Expect(condition.Reason).To(Equal(ReasonInvalidRetention))
				Expect(condition.Message).To(ContainSubstring("out of range"))
			},
			Entry("zero", time.Duration(0)),
			Entry("negative", -time.Hour),
			Entry("below the minimum", 23*time.Hour),
			Entry("above the maximum", 91*24*time.Hour),
			Entry("a nanosecond over the maximum", 2160*time.Hour+1),
		)

		It("should follow a change from an hour-based to a day-based retention", func() {
			// Drift correction has to work across the two notations, since a
			// user moving from "36h" to "2d" is expressing the same intent in a
			// friendlier way - and one that used to break the controller.
			createNamespace(&temporalv1beta1.Duration{Duration: 36 * time.Hour})

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(temporalClient.existing[name].Retention).To(Equal(36 * time.Hour))

			By("asking for 2d instead")
			twoDays, err := temporalv1beta1.ParseDuration("2d")
			Expect(err).NotTo(HaveOccurred())

			ns := stored()
			ns.Spec.Retention = &temporalv1beta1.Duration{Duration: twoDays}
			Expect(k8sClient.Update(ctx, ns)).To(Succeed())

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.existing[name].Retention).To(Equal(48*time.Hour),
				"2d should be applied as 48h")
		})

		It("should record Adopted for an unmarked namespace that already exists", func() {
			createNamespace(&temporalv1beta1.Duration{Duration: 24 * time.Hour})
			putTemporalNamespace(24*time.Hour, nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(BeEmpty(), "an existing namespace must not be recreated")
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipAdopted))

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(ReasonAdopted))
		})

		It("should not stamp its marker on a namespace it merely adopts", func() {
			createNamespace(nil)
			putTemporalNamespace(temporalv1beta1.DefaultRetention, nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipAdopted))

			Expect(temporalClient.existing[name].Data).To(BeEmpty(),
				"adopting a namespace must not claim it")
		})

		It("should record Created for a namespace already carrying its own marker", func() {
			createNamespace(nil)
			putTemporalNamespace(temporalv1beta1.DefaultRetention, ownerMarker(currentUID()))

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(BeEmpty())
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreated),
				"Temporal's own metadata proves this resource registered it")
		})

		It("should keep Created across repeated reconciles", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreated))

			// The namespace now exists, which is exactly the situation that
			// would tempt a naive implementation into calling it Adopted.
			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreated))
			Expect(readyCondition().Reason).To(Equal(ReasonReconciled))
			Expect(temporalClient.created).To(HaveLen(1), "the namespace must not be created twice")
		})

		It("should keep Adopted across repeated reconciles", func() {
			createNamespace(nil)
			putTemporalNamespace(temporalv1beta1.DefaultRetention, nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipAdopted))

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipAdopted))
			Expect(readyCondition().Reason).To(Equal(ReasonReconciled))
		})

		It("should keep Adopted even if the namespace has to be put back", func() {
			createNamespace(nil)
			putTemporalNamespace(temporalv1beta1.DefaultRetention, nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipAdopted))

			// Someone removes it behind the operator's back.
			delete(temporalClient.existing, name)

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(HaveLen(1), "the namespace should be restored")
			Expect(temporalClient.created[0].data).To(BeEmpty(),
				"a restored adopted namespace must not be claimed either")
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipAdopted),
				"restoring a user's namespace does not make it the operator's to delete")
		})

		It("should not establish ownership when Temporal cannot be reached", func() {
			createNamespace(nil)
			dialErr = errors.New("connection refused")

			_, err := reconcile()
			Expect(err).To(MatchError(dialErr))

			Expect(ownership()).To(BeEmpty())
			Expect(readyCondition().Reason).To(Equal(ReasonConnectionFailed))
		})

		It("should not establish ownership when the describe fails", func() {
			createNamespace(nil)
			temporalClient.describeErr = errors.New("permission denied")

			_, err := reconcile()
			Expect(err).To(MatchError(temporalClient.describeErr))

			Expect(temporalClient.created).To(BeEmpty(), "a failed describe must not trigger a create")
			Expect(ownership()).To(BeEmpty())
			Expect(readyCondition().Reason).To(Equal(ReasonDescribeFailed))
		})
	})

	Context("when a create is interrupted before its result is recorded", func() {
		BeforeEach(func() {
			createConnection(metav1.ConditionTrue)
		})

		It("should persist Creating before asking Temporal to register anything", func() {
			createNamespace(nil)

			var ownershipAtCreate temporalv1beta1.NamespaceOwnership
			temporalClient.onCreate = func(string) {
				// Read straight from the API server: the marker has to be
				// durable by now, not merely set in memory.
				ownershipAtCreate = stored().Status.Ownership
			}

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(ownershipAtCreate).To(Equal(temporalv1beta1.NamespaceOwnershipCreating),
				"the intent to create must be durable before the namespace is registered")
		})

		It("should recover Created from the marker when the status write fails", func() {
			createNamespace(nil)

			// Fail exactly the write that would record the finished create.
			// The earlier Creating write is allowed through.
			statusErr := errors.New("status update failed")
			k8s.onStatusUpdate = func(ns *temporalv1beta1.Namespace) error {
				if ns.Status.Ownership == temporalv1beta1.NamespaceOwnershipCreated {
					return statusErr
				}

				return nil
			}

			_, err := reconcile()
			Expect(err).To(MatchError(statusErr))

			// Temporal has the namespace; Kubernetes only knows we were trying.
			Expect(temporalClient.created).To(HaveLen(1))
			Expect(temporalClient.existing).To(HaveKey(name))
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreating))

			// The registered namespace carries this resource's marker, which is
			// what the retry recognises.
			Expect(temporalClient.existing[name].Data).To(Equal(ownerMarker(currentUID())))

			// The controller restarts and reconciles again. The namespace now
			// exists, so a naive implementation would adopt it.
			k8s.onStatusUpdate = nil
			temporalClient.described = nil

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreated),
				"the marker proves the namespace is this resource's own work")
			Expect(temporalClient.created).To(HaveLen(1), "the namespace must not be created twice")
			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
		})

		It("should not claim an unmarked namespace that appeared while Creating", func() {
			createNamespace(nil)
			setOwnership(temporalv1beta1.NamespaceOwnershipCreating)

			// Another actor registered this name in the window between the
			// operator seeing it missing and getting round to creating it. It
			// carries no marker, so it is not the operator's work.
			putTemporalNamespace(temporalv1beta1.DefaultRetention, nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(ownership()).NotTo(Equal(temporalv1beta1.NamespaceOwnershipCreated),
				"existence alone must never be read as ownership")
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipAdopted),
				"an unowned namespace is managed, never destroyed")
			Expect(temporalClient.created).To(BeEmpty())
			Expect(temporalClient.existing[name].Data).To(BeEmpty())
		})

		It("should report a conflict when the namespace that appeared belongs to another resource", func() {
			createNamespace(nil)
			setOwnership(temporalv1beta1.NamespaceOwnershipCreating)
			putTemporalNamespace(temporalv1beta1.DefaultRetention, ownerMarker("some-other-uid"))

			_, err := reconcile()
			Expect(err).To(MatchError(ContainSubstring("some-other-uid")))

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonOwnershipConflict))

			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreating),
				"a conflict settles nothing")
			Expect(temporalClient.created).To(BeEmpty())
			Expect(temporalClient.updated).To(BeEmpty())
			Expect(temporalClient.deleted).To(BeEmpty())
			Expect(temporalClient.existing[name].Data).To(Equal(ownerMarker("some-other-uid")),
				"another resource's marker must be left exactly as it was")
		})

		It("should retry the create when the interruption happened before registering", func() {
			createNamespace(nil)
			setOwnership(temporalv1beta1.NamespaceOwnershipCreating)

			// Creating was persisted but the namespace never appeared, so the
			// earlier attempt died before - or during - the register call.
			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(HaveLen(1))
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreated))
		})

		It("should delete a Creating namespace that carries its own marker", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(temporalClient.existing[name].Data).To(Equal(ownerMarker(currentUID())))

			// Rewind to the state an interrupted create would have left.
			setOwnership(temporalv1beta1.NamespaceOwnershipCreating)
			beginDeletion()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.deleted).To(Equal([]string{name}),
				"a namespace we were mid-way through creating is ours to remove")
			Expect(isGone()).To(BeTrue())
		})
	})

	Context("when the retention has drifted", func() {
		BeforeEach(func() {
			createConnection(metav1.ConditionTrue)
		})

		It("should leave an adopted namespace alone when the retention matches", func() {
			createNamespace(&temporalv1beta1.Duration{Duration: 24 * time.Hour})
			putTemporalNamespace(24*time.Hour, nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.updated).To(BeEmpty(), "a matching retention needs no update")
			Expect(readyCondition().Reason).To(Equal(ReasonAdopted))
		})

		It("should update an adopted namespace whose retention differs", func() {
			createNamespace(&temporalv1beta1.Duration{Duration: 24 * time.Hour})
			putTemporalNamespace(720*time.Hour, nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.updated).To(Equal([]namespaceCall{{name: name, retention: 24 * time.Hour}}))
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipAdopted),
				"correcting drift does not change who owns the namespace")

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(ReasonUpdated))
			Expect(condition.Message).To(ContainSubstring("720h0m0s"))
			Expect(condition.Message).To(ContainSubstring("24h0m0s"))
		})

		It("should update a created namespace whose retention differs", func() {
			createNamespace(&temporalv1beta1.Duration{Duration: 24 * time.Hour})

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreated))

			ns := stored()
			ns.Spec.Retention = &temporalv1beta1.Duration{Duration: 96 * time.Hour}
			Expect(k8sClient.Update(ctx, ns)).To(Succeed())

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.updated).To(Equal([]namespaceCall{{name: name, retention: 96 * time.Hour}}))
			Expect(temporalClient.existing[name].Retention).To(Equal(96*time.Hour),
				"the requested retention should reach Temporal unchanged")
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreated))
			Expect(readyCondition().Reason).To(Equal(ReasonUpdated))
		})

		It("should report UpdateFailed when Temporal rejects the update", func() {
			createNamespace(&temporalv1beta1.Duration{Duration: 24 * time.Hour})
			putTemporalNamespace(720*time.Hour, nil)
			temporalClient.updateErr = errors.New("retention too short")

			_, err := reconcile()
			Expect(err).To(MatchError(temporalClient.updateErr))

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonUpdateFailed))
			Expect(condition.Message).To(ContainSubstring("retention too short"))

			// Ownership is still settled - the namespace is ours to manage even
			// though this attempt to configure it failed.
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipAdopted))
		})

		It("should recover to Ready=True once the update succeeds", func() {
			createNamespace(&temporalv1beta1.Duration{Duration: 24 * time.Hour})
			putTemporalNamespace(720*time.Hour, nil)
			temporalClient.updateErr = errors.New("retention too short")

			_, err := reconcile()
			Expect(err).To(HaveOccurred())
			Expect(readyCondition().Status).To(Equal(metav1.ConditionFalse))

			temporalClient.updateErr = nil
			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCondition().Reason).To(Equal(ReasonUpdated))
		})

		It("should default retention to 72h and treat that as the desired value", func() {
			createNamespace(nil)
			putTemporalNamespace(time.Hour, nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.updated).To(HaveLen(1))
			Expect(temporalClient.updated[0].retention).To(Equal(temporalv1beta1.DefaultRetention))
			Expect(temporalv1beta1.DefaultRetention).To(Equal(72 * time.Hour))
		})
	})

	Context("when the namespace belongs to another resource", func() {
		BeforeEach(func() {
			createConnection(metav1.ConditionTrue)
		})

		It("should refuse to adopt, reconfigure or recreate it", func() {
			createNamespace(&temporalv1beta1.Duration{Duration: 24 * time.Hour})
			putTemporalNamespace(720*time.Hour, ownerMarker("some-other-uid"))

			_, err := reconcile()
			Expect(err).To(HaveOccurred())

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonOwnershipConflict))
			Expect(condition.Message).To(ContainSubstring("some-other-uid"))
			Expect(condition.Message).To(ContainSubstring(currentUID()))

			Expect(ownership()).To(BeEmpty(), "a conflicting namespace is neither created nor adopted")
			Expect(temporalClient.created).To(BeEmpty())
			Expect(temporalClient.deleted).To(BeEmpty())

			By("leaving the retention drift alone")
			Expect(temporalClient.updated).To(BeEmpty(),
				"a conflict outranks drift reconciliation")
			Expect(temporalClient.existing[name].Retention).To(Equal(720 * time.Hour))
			Expect(temporalClient.existing[name].Data).To(Equal(ownerMarker("some-other-uid")))
		})

		It("should refuse even when this resource believes it owns the namespace", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreated))

			// The namespace was replaced behind the operator's back by one
			// another resource owns.
			putTemporalNamespace(720*time.Hour, ownerMarker("some-other-uid"))

			_, err = reconcile()
			Expect(err).To(HaveOccurred())

			Expect(readyCondition().Reason).To(Equal(ReasonOwnershipConflict))
			Expect(temporalClient.updated).To(BeEmpty(),
				"status.ownership does not license mutating someone else's namespace")
		})

		It("should not let a recreated resource inherit the old one's namespace", func() {
			// CR A registers the namespace and stamps its UID on it.
			createNamespace(nil)
			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			uidA := currentUID()
			Expect(temporalClient.existing[name].Data).To(Equal(ownerMarker(uidA)))

			// CR A is force-removed - finalizer stripped by hand - leaving the
			// Temporal namespace behind.
			nsA := stored()
			Expect(controllerutil.RemoveFinalizer(nsA, temporalv1beta1.NamespaceFinalizer)).To(BeTrue())
			Expect(k8sClient.Update(ctx, nsA)).To(Succeed())
			Expect(k8sClient.Delete(ctx, nsA)).To(Succeed())
			Eventually(isGone).Should(BeTrue())

			// CR B arrives under the same namespace/name, and so gets a new UID.
			createNamespace(nil)
			uidB := currentUID()
			Expect(uidB).NotTo(Equal(uidA), "a recreated resource must have a fresh identity")

			_, err = reconcile()
			Expect(err).To(HaveOccurred())

			Expect(readyCondition().Reason).To(Equal(ReasonOwnershipConflict))
			Expect(ownership()).To(BeEmpty())
			Expect(temporalClient.deleted).To(BeEmpty(),
				"the previous resource's namespace is not the replacement's to remove")
			Expect(temporalClient.existing[name].Data).To(Equal(ownerMarker(uidA)))
		})
	})

	Context("when the Namespace is deleted", func() {
		BeforeEach(func() {
			createConnection(metav1.ConditionTrue)
		})

		It("should add the finalizer before registering anything in Temporal", func() {
			createNamespace(nil)

			var finalizerAtDescribe bool
			temporalClient.onCreate = func(string) {
				finalizerAtDescribe = controllerutil.ContainsFinalizer(
					stored(), temporalv1beta1.NamespaceFinalizer,
				)
			}

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(finalizerAtDescribe).To(BeTrue(),
				"registering before the finalizer exists would risk orphaning the namespace")
			Expect(hasFinalizer()).To(BeTrue())
		})

		It("should add the finalizer without disturbing the spec", func() {
			ns := createNamespace(&temporalv1beta1.Duration{Duration: 36 * time.Hour})
			Expect(ns.Generation).To(Equal(int64(1)))

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			after := stored()
			Expect(controllerutil.ContainsFinalizer(after, temporalv1beta1.NamespaceFinalizer)).To(BeTrue())

			// A full update would round-trip the spec and rewrite the duration
			// into canonical form, bumping the generation and provoking another
			// reconcile for no reason.
			Expect(after.Generation).To(Equal(int64(1)), "adding the finalizer must not change the spec")
			Expect(after.Spec.Retention.Duration).To(Equal(36 * time.Hour))
			Expect(after.Status.Conditions[0].ObservedGeneration).To(Equal(int64(1)))
		})

		It("should delete an owned Temporal namespace and release the finalizer", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreated))

			beginDeletion()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.deleted).To(Equal([]string{name}))
			Expect(temporalClient.existing).NotTo(HaveKey(name))
			Expect(isGone()).To(BeTrue())
		})

		It("should leave an adopted Temporal namespace alone", func() {
			createNamespace(nil)
			putTemporalNamespace(temporalv1beta1.DefaultRetention, nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipAdopted))

			beginDeletion()
			dialled = 0
			temporalClient.described = nil

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.deleted).To(BeEmpty(), "an adopted namespace must survive its resource")
			Expect(temporalClient.described).To(BeEmpty(), "there is nothing to look up")
			Expect(temporalClient.existing).To(HaveKey(name))
			Expect(dialled).To(BeZero(), "an adopted deletion needs no Temporal connection at all")
			Expect(isGone()).To(BeTrue())
		})

		It("should refuse to delete a namespace whose marker has gone", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreated))

			// The marker is stripped, or the namespace was replaced by an
			// unmarked one of the same name. Either way ownership can no longer
			// be proven.
			putTemporalNamespace(temporalv1beta1.DefaultRetention, nil)
			beginDeletion()

			_, err = reconcile()
			Expect(err).To(MatchError(ContainSubstring(namespaceOwnerKey)))

			Expect(temporalClient.deleted).To(BeEmpty(), "unverifiable ownership must not destroy anything")
			Expect(hasFinalizer()).To(BeTrue())
			Expect(isGone()).To(BeFalse())

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonOwnershipUnverified))
			Expect(condition.Message).To(ContainSubstring(temporalv1beta1.NamespaceFinalizer),
				"the message should say how to resolve it by hand")
		})

		It("should refuse to delete a namespace another resource owns", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			putTemporalNamespace(temporalv1beta1.DefaultRetention, ownerMarker("some-other-uid"))
			beginDeletion()

			_, err = reconcile()
			Expect(err).To(MatchError(ContainSubstring("some-other-uid")))

			Expect(temporalClient.deleted).To(BeEmpty())
			Expect(hasFinalizer()).To(BeTrue())
			Expect(readyCondition().Reason).To(Equal(ReasonOwnershipConflict))
			Expect(temporalClient.existing[name].Data).To(Equal(ownerMarker("some-other-uid")))
		})

		It("should not mistake a stale not-found from delete for success", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			// The Service answers describe from storage but delete from a
			// registry that lags, so a namespace registered moments ago can be
			// described and then reported missing by the delete.
			temporalClient.deleteNotFoundOnce()
			beginDeletion()

			_, err = reconcile()
			Expect(err).To(MatchError(temporal.ErrNamespaceNotFound))

			Expect(hasFinalizer()).To(BeTrue(),
				"the describe proved the namespace is there, so it must not be abandoned")
			Expect(temporalClient.existing).To(HaveKey(name))
			Expect(readyCondition().Reason).To(Equal(ReasonDeleteFailed))

			// The registry catches up and the retry finishes the job.
			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.existing).NotTo(HaveKey(name))
			Expect(isGone()).To(BeTrue())
		})

		It("should hold the finalizer when the namespace cannot be described", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			temporalClient.describeErr = errors.New("service unavailable")
			beginDeletion()

			_, err = reconcile()
			Expect(err).To(MatchError(temporalClient.describeErr))

			Expect(temporalClient.deleted).To(BeEmpty(),
				"ownership must be confirmed before anything is removed")
			Expect(hasFinalizer()).To(BeTrue())
			Expect(readyCondition().Reason).To(Equal(ReasonDescribeFailed))
		})

		It("should treat a namespace that has already gone as deleted", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			// Something removed it between reconciles - or an earlier deletion
			// attempt succeeded but failed to release the finalizer.
			delete(temporalClient.existing, name)
			beginDeletion()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.deleted).To(BeEmpty(),
				"a namespace that is already gone needs no delete call")
			Expect(isGone()).To(BeTrue())
		})

		It("should hold the finalizer when the Temporal deletion fails", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			temporalClient.deleteErr = errors.New("service unavailable")
			beginDeletion()

			_, err = reconcile()
			Expect(err).To(MatchError(temporalClient.deleteErr))

			Expect(hasFinalizer()).To(BeTrue(), "releasing now would orphan the namespace")
			Expect(isGone()).To(BeFalse())

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonDeleteFailed))
			Expect(condition.Message).To(ContainSubstring("service unavailable"))

			// Once Temporal recovers, the resource finishes leaving.
			temporalClient.deleteErr = nil
			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(isGone()).To(BeTrue())
		})

		It("should hold the finalizer when the Connection has gone", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			conn := &temporalv1beta1.Connection{}
			connKey := types.NamespacedName{Name: connectionName, Namespace: namespace}
			Expect(k8sClient.Get(ctx, connKey, conn)).To(Succeed())
			Expect(k8sClient.Delete(ctx, conn)).To(Succeed())

			beginDeletion()

			_, err = reconcile()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(connectionName))

			Expect(hasFinalizer()).To(BeTrue(), "an owned namespace must not be silently orphaned")
			Expect(temporalClient.deleted).To(BeEmpty())
		})

		It("should hold the finalizer when the Connection cannot be resolved", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			conn := &temporalv1beta1.Connection{}
			connKey := types.NamespacedName{Name: connectionName, Namespace: namespace}
			Expect(k8sClient.Get(ctx, connKey, conn)).To(Succeed())
			conn.Spec.CredentialsSecretRef = &corev1.LocalObjectReference{Name: missingSecretName}
			Expect(k8sClient.Update(ctx, conn)).To(Succeed())

			beginDeletion()
			dialled = 0

			_, err = reconcile()
			Expect(err).To(MatchError(ContainSubstring(missingSecretName)))

			Expect(hasFinalizer()).To(BeTrue())
			Expect(dialled).To(BeZero())
		})

		It("should delete an owned namespace even if the Connection is not Ready", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			// Readiness is a cached judgement. Refusing to act on it would
			// strand a namespace the operator is responsible for.
			conn := &temporalv1beta1.Connection{}
			connKey := types.NamespacedName{Name: connectionName, Namespace: namespace}
			Expect(k8sClient.Get(ctx, connKey, conn)).To(Succeed())
			meta.SetStatusCondition(&conn.Status.Conditions, metav1.Condition{
				Type:    temporalv1beta1.ConditionTypeReady,
				Status:  metav1.ConditionFalse,
				Reason:  ReasonConnectionFailed,
				Message: "stale",
			})
			Expect(k8sClient.Status().Update(ctx, conn)).To(Succeed())

			beginDeletion()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.deleted).To(Equal([]string{name}))
			Expect(isGone()).To(BeTrue())
		})

		It("should release the finalizer without deleting when ownership is unknown", func() {
			createNamespace(nil)
			putTemporalNamespace(temporalv1beta1.DefaultRetention, nil)

			// The finalizer got added but ownership was never settled - the
			// Connection was unusable, Temporal was down, or the process died.
			Expect(reconciler.ensureFinalizer(ctx, stored())).To(Succeed())
			Expect(ownership()).To(BeEmpty())

			beginDeletion()
			dialled = 0

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.deleted).To(BeEmpty(),
				"nothing may be deleted on the strength of an unknown ownership")
			Expect(temporalClient.existing).To(HaveKey(name))
			Expect(dialled).To(BeZero())
			Expect(isGone()).To(BeTrue())
		})

		It("should return cleanly when the finalizer is already gone", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			beginDeletion()

			ns := stored()
			Expect(controllerutil.RemoveFinalizer(ns, temporalv1beta1.NamespaceFinalizer)).To(BeTrue())
			Expect(k8sClient.Update(ctx, ns)).To(Succeed())
			Expect(isGone()).To(BeTrue())

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.IsZero()).To(BeTrue())
			Expect(temporalClient.deleted).To(BeEmpty())
		})
	})

	Context("when its Connection changes", func() {
		// requestsForConnection runs the real mapper, with the real index
		// registered, over the resource as it currently stands - standing in
		// for what the watch would enqueue on a Connection event.
		requestsForConnection := func(conn *temporalv1beta1.Connection) []ctrl.Request {
			mapper := &NamespaceReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(k8sClient.Scheme()).
					WithIndex(&temporalv1beta1.Namespace{},
						namespaceConnectionRefIndex, indexNamespaceByConnection).
					WithObjects(stored()).
					Build(),
			}

			return mapper.namespacesForConnection(ctx, conn)
		}

		// thisNamespace is the reconcile request the watch should produce for
		// the resource under test.
		thisNamespace := func() []ctrl.Request {
			return []ctrl.Request{{NamespacedName: key}}
		}

		It("should be woken when the Connection it was waiting for appears", func() {
			createNamespace(nil)

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(readyCondition().Reason).To(Equal(ReasonConnectionNotFound))
			Expect(result.RequeueAfter).To(Equal(dependencyRetryInterval),
				"the timed retry stays as a backstop")

			conn := createConnection(metav1.ConditionTrue)

			By("confirming the Connection event names this Namespace")
			Expect(requestsForConnection(conn)).To(Equal(thisNamespace()))

			By("confirming the reconcile it triggers gets on with the work")
			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCondition().Reason).To(Equal(ReasonCreated))
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreated))
		})

		It("should be woken when its Connection becomes Ready", func() {
			conn := createConnection("")
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(readyCondition().Reason).To(Equal(ReasonConnectionNotReady))
			Expect(dialled).To(BeZero())

			By("marking the Connection Ready")
			connKey := types.NamespacedName{Name: connectionName, Namespace: namespace}
			Expect(k8sClient.Get(ctx, connKey, conn)).To(Succeed())
			meta.SetStatusCondition(&conn.Status.Conditions, metav1.Condition{
				Type:    temporalv1beta1.ConditionTypeReady,
				Status:  metav1.ConditionTrue,
				Reason:  ReasonConnected,
				Message: connectionReadyMessage,
			})
			Expect(k8sClient.Status().Update(ctx, conn)).To(Succeed())

			// Readiness lives in the status, so this is exactly the kind of
			// event a generation-based predicate would have thrown away.
			By("confirming the status-only change still names this Namespace")
			Expect(requestsForConnection(conn)).To(Equal(thisNamespace()))

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
			Expect(dialled).To(Equal(1))
		})

		It("should be woken when its Connection is deleted", func() {
			conn := createConnection(metav1.ConditionTrue)
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))

			Expect(k8sClient.Delete(ctx, conn)).To(Succeed())

			// The handler maps the last known object, so the dependants of a
			// Connection that has just gone are still found.
			By("confirming the delete event still names this Namespace")
			Expect(requestsForConnection(conn)).To(Equal(thisNamespace()))

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(readyCondition().Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCondition().Reason).To(Equal(ReasonConnectionNotFound))
			Expect(result.RequeueAfter).To(Equal(dependencyRetryInterval))
		})
	})

	Context("when the deletion policy is Orphan", func() {
		BeforeEach(func() {
			createConnection(metav1.ConditionTrue)
		})

		// The Temporal namespace must survive whoever the operator thinks owns
		// it, and it must do so without asking Temporal anything at all.
		DescribeTable(
			"should release the finalizer without touching Temporal",
			func(ownership temporalv1beta1.NamespaceOwnership) {
				createNamespace(nil)

				_, err := reconcile()
				Expect(err).NotTo(HaveOccurred())

				if ownership == "" {
					clearOwnership()
				} else {
					setOwnership(ownership)
				}

				setDeletionPolicy(temporalv1beta1.NamespaceDeletionPolicyOrphan)
				beginDeletion()

				resetTemporalCalls()

				_, err = reconcile()
				Expect(err).NotTo(HaveOccurred())

				Expect(isGone()).To(BeTrue())
				expectNoTemporalContact()
				Expect(temporalClient.existing).To(HaveKey(name), "the Temporal namespace stays")
			},
			Entry("when it created the namespace", temporalv1beta1.NamespaceOwnershipCreated),
			Entry("when it was part-way through creating it", temporalv1beta1.NamespaceOwnershipCreating),
			Entry("when it adopted the namespace", temporalv1beta1.NamespaceOwnershipAdopted),
			Entry("when ownership was never settled", temporalv1beta1.NamespaceOwnership("")),
		)

		It("should leave the ownership marker exactly as it was", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			marker := ownerMarker(currentUID())
			Expect(temporalClient.existing[name].Data).To(Equal(marker))

			setDeletionPolicy(temporalv1beta1.NamespaceDeletionPolicyOrphan)
			beginDeletion()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(isGone()).To(BeTrue())
			Expect(temporalClient.existing[name].Data).To(Equal(marker),
				"orphaning must not rewrite or strip the marker")
		})

		It("should not care that the Connection has gone", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreated))

			deleteConnection()
			setDeletionPolicy(temporalv1beta1.NamespaceDeletionPolicyOrphan)
			beginDeletion()

			resetTemporalCalls()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(isGone()).To(BeTrue())
			expectNoTemporalContact()
			Expect(temporalClient.existing).To(HaveKey(name))
		})

		It("should not care that the Connection cannot be resolved", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			breakConnection()
			setDeletionPolicy(temporalv1beta1.NamespaceDeletionPolicyOrphan)
			beginDeletion()

			resetTemporalCalls()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(isGone()).To(BeTrue())
			expectNoTemporalContact()
		})

		It("should release a namespace another resource owns", func() {
			createNamespace(nil)
			putTemporalNamespace(temporalv1beta1.DefaultRetention, ownerMarker("some-other-uid"))

			_, err := reconcile()
			Expect(err).To(HaveOccurred())
			Expect(readyCondition().Reason).To(Equal(ReasonOwnershipConflict))

			setDeletionPolicy(temporalv1beta1.NamespaceDeletionPolicyOrphan)
			beginDeletion()

			resetTemporalCalls()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(isGone()).To(BeTrue())
			expectNoTemporalContact()
			Expect(temporalClient.existing[name].Data).To(Equal(ownerMarker("some-other-uid")))
		})
	})

	Context("when a deletion cannot be completed", func() {
		BeforeEach(func() {
			createConnection(metav1.ConditionTrue)
		})

		It("should hold the finalizer under Delete when the Connection has gone", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreated))

			deleteConnection()
			beginDeletion()

			_, err = reconcile()
			Expect(err).To(MatchError(ContainSubstring(connectionName)))

			Expect(hasFinalizer()).To(BeTrue(), "Delete must never quietly become Orphan")
			Expect(isGone()).To(BeFalse())
			Expect(temporalClient.existing).To(HaveKey(name))

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonConnectionNotFound))
		})

		It("should let the policy be switched to Orphan to release it", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			marker := ownerMarker(currentUID())
			deleteConnection()
			beginDeletion()

			_, err = reconcile()
			Expect(err).To(HaveOccurred())
			Expect(hasFinalizer()).To(BeTrue())

			// The API server allows the spec of a terminating resource to be
			// changed, which is what makes this the recovery path rather than
			// an annotation.
			By("switching the policy while the resource is terminating")
			setDeletionPolicy(temporalv1beta1.NamespaceDeletionPolicyOrphan)
			Expect(stored().GetDeletionTimestamp().IsZero()).To(BeFalse())

			resetTemporalCalls()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(isGone()).To(BeTrue())
			expectNoTemporalContact()
			Expect(temporalClient.existing[name].Data).To(Equal(marker),
				"the namespace and its marker are deliberately left behind")
		})
	})

	Context("when the deletion policy changes outside deletion", func() {
		BeforeEach(func() {
			createConnection(metav1.ConditionTrue)
		})

		It("should not touch Temporal when switching between policies", func() {
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(temporalClient.created).To(HaveLen(1))

			for _, policy := range []temporalv1beta1.NamespaceDeletionPolicy{
				temporalv1beta1.NamespaceDeletionPolicyOrphan,
				temporalv1beta1.NamespaceDeletionPolicyDelete,
			} {
				setDeletionPolicy(policy)

				_, err = reconcile()
				Expect(err).NotTo(HaveOccurred())

				// A reconcile still reads Temporal, as any resync would, but
				// changing the policy must not create, reconfigure or remove
				// anything.
				Expect(temporalClient.created).To(HaveLen(1), "no re-creation")
				Expect(temporalClient.updated).To(BeEmpty(), "no reconfiguration")
				Expect(temporalClient.deleted).To(BeEmpty(), "no deletion")
				Expect(ownership()).To(Equal(temporalv1beta1.NamespaceOwnershipCreated))
				Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
			}
		})
	})

	Context("in general", func() {
		It("should not rewrite an unchanged status", func() {
			createConnection(metav1.ConditionTrue)
			createNamespace(nil)

			// The first reconcile registers the namespace and the second
			// settles on Reconciled; both are genuine status changes. Churn is
			// about what happens once the resource has come to rest.
			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			before := stored()
			Expect(before.Status.Conditions[0].Reason).To(Equal(ReasonReconciled))

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(stored().ResourceVersion).To(Equal(before.ResourceVersion),
				"a no-op reconcile should not write the status")
		})

		It("should not panic when the optional fields are absent", func() {
			ns := &temporalv1beta1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec: temporalv1beta1.NamespaceSpec{
					ConnectionRef: corev1.LocalObjectReference{Name: connectionName},
				},
			}
			Expect(ns.Spec.Retention).To(BeNil())
			Expect(ns.Spec.RetentionDuration()).To(Equal(temporalv1beta1.DefaultRetention))
			Expect(ns.Status.Ownership).To(BeEmpty())
			Expect(ns.Status.Ownership.OwnsTemporalNamespace()).To(BeFalse())
			Expect(ns.Status.Ownership.IsEstablished()).To(BeFalse())

			Expect(k8sClient.Create(ctx, ns)).To(Succeed())

			Expect(func() {
				_, _ = reconcile()
			}).NotTo(Panic())
		})

		It("should treat an unset deletion policy as Delete without admission", func() {
			// Built in memory, so nothing has defaulted it.
			ns := &temporalv1beta1.Namespace{
				Spec: temporalv1beta1.NamespaceSpec{
					ConnectionRef: corev1.LocalObjectReference{Name: connectionName},
				},
			}
			Expect(ns.Spec.DeletionPolicy).To(BeEmpty())
			Expect(ns.Spec.DeletionPolicyValue()).To(Equal(temporalv1beta1.NamespaceDeletionPolicyDelete))
			Expect(temporalv1beta1.DefaultDeletionPolicy).
				To(Equal(temporalv1beta1.NamespaceDeletionPolicyDelete))
		})

		It("should default the deletion policy to Delete at admission", func() {
			createConnection(metav1.ConditionTrue)
			createNamespace(nil)

			Expect(stored().Spec.DeletionPolicy).
				To(Equal(temporalv1beta1.NamespaceDeletionPolicyDelete))
		})

		It("should delete an owned namespace when Delete is set explicitly", func() {
			createConnection(metav1.ConditionTrue)
			createNamespace(nil)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			setDeletionPolicy(temporalv1beta1.NamespaceDeletionPolicyDelete)
			beginDeletion()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.deleted).To(Equal([]string{name}))
			Expect(temporalClient.existing).NotTo(HaveKey(name))
			Expect(isGone()).To(BeTrue())
		})

		It("should reject a deletion policy the API does not define", func() {
			ns := &temporalv1beta1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec: temporalv1beta1.NamespaceSpec{
					ConnectionRef:  corev1.LocalObjectReference{Name: connectionName},
					DeletionPolicy: temporalv1beta1.NamespaceDeletionPolicy("Abandon"),
				},
			}

			err := k8sClient.Create(ctx, ns)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Unsupported value"))
		})

		It("should be rejected by the API server without a connectionRef name", func() {
			ns := &temporalv1beta1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec:       temporalv1beta1.NamespaceSpec{},
			}

			err := k8sClient.Create(ctx, ns)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("connectionRef.name is required"))
		})

		It("should reject an ownership the API does not define", func() {
			createConnection(metav1.ConditionTrue)
			createNamespace(nil)

			ns := stored()
			ns.Status.Ownership = temporalv1beta1.NamespaceOwnership("Borrowed")

			err := k8sClient.Status().Update(ctx, ns)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Unsupported value"))
		})
	})
})
