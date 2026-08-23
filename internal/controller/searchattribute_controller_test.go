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

	temporalv1alpha1 "github.com/mrsimonemms/temporal-resource-operator/api/v1alpha1"
	"github.com/mrsimonemms/temporal-resource-operator/internal/connection"
	"github.com/mrsimonemms/temporal-resource-operator/internal/temporal"
)

// Kubernetes namespaces the search attribute specs work in.
const (
	saNamespace    = "default"
	otherNamespace = "other"
)

// searchAttributeCall records a search attribute call and the type it carried.
type searchAttributeCall struct {
	namespace string
	name      string
	attrType  temporal.SearchAttributeType
}

// fakeSearchAttributeClient stands in for a Temporal Service. Attributes in
// existing are reported as registered, keyed by namespace then name; anything
// else is reported missing in the same shape the real client uses.
type fakeSearchAttributeClient struct {
	existing map[string]map[string]temporal.SearchAttributeType

	// namespaces that Temporal will admit to having. A namespace absent from
	// here is reported gone, which is what happens once it is deleted.
	missingNamespaces map[string]bool

	describeErr error
	createErr   error
	deleteErr   error

	described []searchAttributeCall
	created   []searchAttributeCall
	deleted   []searchAttributeCall
	closed    bool
}

func newFakeSearchAttributeClient() *fakeSearchAttributeClient {
	return &fakeSearchAttributeClient{
		existing:          map[string]map[string]temporal.SearchAttributeType{},
		missingNamespaces: map[string]bool{},
	}
}

func (f *fakeSearchAttributeClient) DescribeSearchAttribute(
	_ context.Context,
	namespace, name string,
) (*temporal.SearchAttribute, error) {
	f.described = append(f.described, searchAttributeCall{namespace: namespace, name: name})

	if f.describeErr != nil {
		return nil, f.describeErr
	}

	if f.missingNamespaces[namespace] {
		return nil, fmt.Errorf("%w: %s", temporal.ErrNamespaceNotFound, namespace)
	}

	if attrType, ok := f.existing[namespace][name]; ok {
		return &temporal.SearchAttribute{Name: name, Type: attrType}, nil
	}

	return nil, fmt.Errorf("%w: %s on namespace %s", temporal.ErrSearchAttributeNotFound, name, namespace)
}

func (f *fakeSearchAttributeClient) CreateSearchAttribute(
	_ context.Context,
	namespace, name string,
	attrType temporal.SearchAttributeType,
) error {
	f.created = append(f.created, searchAttributeCall{namespace: namespace, name: name, attrType: attrType})

	if f.createErr != nil {
		return f.createErr
	}

	if f.missingNamespaces[namespace] {
		return fmt.Errorf("%w: %s", temporal.ErrNamespaceNotFound, namespace)
	}

	if f.existing[namespace] == nil {
		f.existing[namespace] = map[string]temporal.SearchAttributeType{}
	}

	// Temporal keeps the original type when asked to register a name that is
	// already taken, rather than reporting a problem.
	if _, taken := f.existing[namespace][name]; !taken {
		f.existing[namespace][name] = attrType
	}

	return nil
}

func (f *fakeSearchAttributeClient) DeleteSearchAttribute(_ context.Context, namespace, name string) error {
	f.deleted = append(f.deleted, searchAttributeCall{namespace: namespace, name: name})

	if f.deleteErr != nil {
		return f.deleteErr
	}

	if f.missingNamespaces[namespace] {
		return fmt.Errorf("%w: %s", temporal.ErrNamespaceNotFound, namespace)
	}

	// Removing an attribute that is not registered is success as far as
	// Temporal is concerned.
	delete(f.existing[namespace], name)

	return nil
}

func (f *fakeSearchAttributeClient) Close() { f.closed = true }

var _ = Describe("SearchAttribute Controller", func() {
	const (
		k8sNamespace      = saNamespace
		address           = "localhost:7233"
		temporalNamespace = "payments"
	)

	var (
		reconciler     *SearchAttributeReconciler
		temporalClient *fakeSearchAttributeClient
		dialErr        error
		dialled        int
		// resourceName is the Kubernetes resource's name and attributeName is
		// the Temporal search attribute's. They are deliberately different, and
		// differently cased, so that anything reaching for the wrong one shows
		// up immediately.
		resourceName   string
		attributeName  string
		connectionName string
		key            types.NamespacedName
	)

	reconcile := func() (ctrl.Result, error) {
		return reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	}

	stored := func() *temporalv1alpha1.SearchAttribute {
		attribute := &temporalv1alpha1.SearchAttribute{}
		Expect(k8sClient.Get(ctx, key, attribute)).To(Succeed())

		return attribute
	}

	readyCondition := func() *metav1.Condition {
		return meta.FindStatusCondition(stored().Status.Conditions, temporalv1alpha1.ConditionTypeReady)
	}

	ownership := func() temporalv1alpha1.SearchAttributeOwnership {
		return stored().Status.Ownership
	}

	hasFinalizer := func() bool {
		return controllerutil.ContainsFinalizer(stored(), temporalv1alpha1.SearchAttributeFinalizer)
	}

	isGone := func() bool {
		return apierrors.IsNotFound(k8sClient.Get(ctx, key, &temporalv1alpha1.SearchAttribute{}))
	}

	// createConnection persists a Connection and, when status is non-empty,
	// gives it a Ready condition with that status.
	createConnection := func(status metav1.ConditionStatus) *temporalv1alpha1.Connection {
		conn := &temporalv1alpha1.Connection{
			ObjectMeta: metav1.ObjectMeta{Name: connectionName, Namespace: k8sNamespace},
			Spec:       temporalv1alpha1.ConnectionSpec{Address: address},
		}
		Expect(k8sClient.Create(ctx, conn)).To(Succeed())

		if status == "" {
			return conn
		}

		meta.SetStatusCondition(&conn.Status.Conditions, metav1.Condition{
			Type:    temporalv1alpha1.ConditionTypeReady,
			Status:  status,
			Reason:  ReasonConnected,
			Message: connectionReadyMessage,
		})
		Expect(k8sClient.Status().Update(ctx, conn)).To(Succeed())

		return conn
	}

	// createTemporalNamespace persists a Namespace resource whose metadata.name
	// is the Temporal namespace the attribute lives in.
	createTemporalNamespace := func(status metav1.ConditionStatus) *temporalv1alpha1.Namespace {
		ns := &temporalv1alpha1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: temporalNamespace, Namespace: k8sNamespace},
			Spec: temporalv1alpha1.NamespaceSpec{
				ConnectionRef: corev1.LocalObjectReference{Name: connectionName},
			},
		}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())

		if status == "" {
			return ns
		}

		meta.SetStatusCondition(&ns.Status.Conditions, metav1.Condition{
			Type:    temporalv1alpha1.ConditionTypeReady,
			Status:  status,
			Reason:  ReasonCreated,
			Message: "Registered Temporal namespace",
		})
		Expect(k8sClient.Status().Update(ctx, ns)).To(Succeed())

		return ns
	}

	// createSearchAttribute persists a SearchAttribute of the given type.
	createSearchAttribute := func(attrType temporalv1alpha1.SearchAttributeType) *temporalv1alpha1.SearchAttribute {
		attribute := &temporalv1alpha1.SearchAttribute{
			ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: k8sNamespace},
			Spec: temporalv1alpha1.SearchAttributeSpec{
				Name:          attributeName,
				ConnectionRef: corev1.LocalObjectReference{Name: connectionName},
				NamespaceRef:  corev1.LocalObjectReference{Name: temporalNamespace},
				Type:          attrType,
			},
		}
		Expect(k8sClient.Create(ctx, attribute)).To(Succeed())

		return attribute
	}

	// putSearchAttribute seeds the fake Service with a registered attribute.
	putSearchAttribute := func(attrType temporal.SearchAttributeType) {
		if temporalClient.existing[temporalNamespace] == nil {
			temporalClient.existing[temporalNamespace] = map[string]temporal.SearchAttributeType{}
		}
		temporalClient.existing[temporalNamespace][attributeName] = attrType
	}

	setOwnership := func(value temporalv1alpha1.SearchAttributeOwnership) {
		attribute := stored()
		attribute.Status.Ownership = value
		Expect(k8sClient.Status().Update(ctx, attribute)).To(Succeed())
	}

	setDeletionPolicy := func(policy temporalv1alpha1.SearchAttributeDeletionPolicy) {
		attribute := stored()
		attribute.Spec.DeletionPolicy = policy
		Expect(k8sClient.Update(ctx, attribute)).To(Succeed())
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
		temporalClient.deleted = nil
	}

	expectNoTemporalContact := func() {
		Expect(dialled).To(BeZero(), "Temporal should not have been dialled")
		Expect(temporalClient.described).To(BeEmpty())
		Expect(temporalClient.created).To(BeEmpty())
		Expect(temporalClient.deleted).To(BeEmpty())
	}

	BeforeEach(func() {
		temporalClient = newFakeSearchAttributeClient()
		dialErr = nil
		dialled = 0

		// Unique names per spec keep the specs independent. The Kubernetes name
		// is lowercase because it has to be; the Temporal name is PascalCase
		// because real ones are.
		suffix := GinkgoRandomSeed() + int64(CurrentSpecReport().LineNumber())
		resourceName = fmt.Sprintf("customer-id-%d", suffix)
		attributeName = fmt.Sprintf("CustomerId%d", suffix)
		connectionName = fmt.Sprintf("conn-%d", suffix)
		key = types.NamespacedName{Name: resourceName, Namespace: k8sNamespace}

		reconciler = &SearchAttributeReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Resolver: connection.NewResolver(k8sClient),
			Connect: func(_ context.Context, _ *sdkclient.Options) (TemporalSearchAttributeClient, error) {
				dialled++
				if dialErr != nil {
					return nil, dialErr
				}

				return temporalClient, nil
			},
		}
	})

	AfterEach(func() {
		attribute := &temporalv1alpha1.SearchAttribute{}
		if err := k8sClient.Get(ctx, key, attribute); err == nil {
			if controllerutil.RemoveFinalizer(attribute, temporalv1alpha1.SearchAttributeFinalizer) {
				Expect(k8sClient.Update(ctx, attribute)).To(Succeed())
			}
			Expect(ctrlclient.IgnoreNotFound(k8sClient.Delete(ctx, attribute))).To(Succeed())
		}

		for _, obj := range []ctrlclient.Object{
			&temporalv1alpha1.Connection{
				ObjectMeta: metav1.ObjectMeta{Name: connectionName, Namespace: k8sNamespace},
			},
			&temporalv1alpha1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: temporalNamespace, Namespace: k8sNamespace},
			},
		} {
			Expect(ctrlclient.IgnoreNotFound(k8sClient.Delete(ctx, obj))).To(Succeed())
		}
	})

	Context("when the resource does not exist", func() {
		It("should return cleanly without touching Temporal", func() {
			key = types.NamespacedName{Name: "absent-attribute", Namespace: k8sNamespace}

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
				createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

				result, err := reconcile()
				Expect(err).NotTo(HaveOccurred(), "a missing dependency is not a controller failure")
				Expect(result.RequeueAfter).To(Equal(dependencyRetryInterval))
				expectNoTemporalContact()

				condition := readyCondition()
				Expect(condition.Status).To(Equal(metav1.ConditionFalse))
				Expect(condition.Reason).To(Equal(reason))
				Expect(ownership()).To(BeEmpty(), "nothing may be settled without talking to Temporal")
			},
			Entry("no Connection", func() {}, ReasonConnectionNotFound),
			Entry("an unvalidated Connection", func() {
				createConnection("")
			}, ReasonConnectionNotReady),
			Entry("a failed Connection", func() {
				createConnection(metav1.ConditionFalse)
			}, ReasonConnectionNotReady),
			Entry("no Namespace", func() {
				createConnection(metav1.ConditionTrue)
			}, ReasonNamespaceNotFound),
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
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

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

		It("should register a missing search attribute", func() {
			attribute := createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeywordList)

			result, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(namespaceResyncInterval))

			Expect(temporalClient.created).To(Equal([]searchAttributeCall{{
				namespace: temporalNamespace,
				name:      attributeName,
				attrType:  temporal.SearchAttributeType("KeywordList"),
			}}), "the requested type should reach Temporal unchanged")
			Expect(temporalClient.closed).To(BeTrue())

			Expect(ownership()).To(Equal(temporalv1alpha1.SearchAttributeOwnershipCreated))

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(ReasonCreated))
			Expect(condition.ObservedGeneration).To(Equal(attribute.Generation))
		})

		It("should persist Creating before asking Temporal to register anything", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			var ownershipAtCreate temporalv1alpha1.SearchAttributeOwnership
			temporalClient.createErr = nil
			original := reconciler.Connect
			reconciler.Connect = func(c context.Context, o *sdkclient.Options) (TemporalSearchAttributeClient, error) {
				client, err := original(c, o)
				// Read straight from the API server inside the create call: the
				// marker has to be durable by then, not merely set in memory.
				return &recordingClient{
					TemporalSearchAttributeClient: client,
					onCreate: func() {
						ownershipAtCreate = stored().Status.Ownership
					},
				}, err
			}

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(ownershipAtCreate).To(Equal(temporalv1alpha1.SearchAttributeOwnershipCreating),
				"the intent to create must be durable before the attribute is registered")
		})

		It("should adopt an existing attribute of the same type", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)
			putSearchAttribute("Keyword")

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(BeEmpty(), "an existing attribute must not be re-registered")
			Expect(ownership()).To(Equal(temporalv1alpha1.SearchAttributeOwnershipAdopted))

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(ReasonAdopted))
		})

		It("should keep ownership stable across repeated reconciles", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1alpha1.SearchAttributeOwnershipCreated))

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(ownership()).To(Equal(temporalv1alpha1.SearchAttributeOwnershipCreated))
			Expect(readyCondition().Reason).To(Equal(ReasonReconciled))
			Expect(temporalClient.created).To(HaveLen(1))
		})

		It("should recover Created from the Creating marker when the status write fails", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)
			setOwnership(temporalv1alpha1.SearchAttributeOwnershipCreating)

			// The attribute is there with exactly the requested type, which is
			// the state an interrupted create leaves behind.
			putSearchAttribute("Keyword")

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(ownership()).To(Equal(temporalv1alpha1.SearchAttributeOwnershipCreated),
				"an interrupted create must not be mistaken for an adoption")
			Expect(temporalClient.created).To(BeEmpty())
		})

		It("should not rewrite an unchanged status", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

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
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)
			temporalClient.createErr = errors.New("reserved by system")

			_, err := reconcile()
			Expect(err).To(MatchError(temporalClient.createErr))

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonCreateFailed))
			Expect(hasFinalizer()).To(BeTrue(), "the finalizer stays so a partial create is still cleaned up")
		})

		It("should report DescribeFailed without creating anything", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)
			temporalClient.describeErr = errors.New("permission denied")

			_, err := reconcile()
			Expect(err).To(MatchError(temporalClient.describeErr))

			Expect(temporalClient.created).To(BeEmpty())
			Expect(ownership()).To(BeEmpty())
			Expect(readyCondition().Reason).To(Equal(ReasonDescribeFailed))
		})

		It("should recreate an externally removed attribute and stay Created", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1alpha1.SearchAttributeOwnershipCreated))

			delete(temporalClient.existing[temporalNamespace], attributeName)

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(HaveLen(2), "the attribute should be put back")
			Expect(ownership()).To(Equal(temporalv1alpha1.SearchAttributeOwnershipCreated))
		})

		It("should recreate an externally removed adopted attribute and stay Adopted", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)
			putSearchAttribute("Keyword")

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1alpha1.SearchAttributeOwnershipAdopted))

			delete(temporalClient.existing[temporalNamespace], attributeName)

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(HaveLen(1), "the attribute should be put back")
			Expect(ownership()).To(Equal(temporalv1alpha1.SearchAttributeOwnershipAdopted),
				"restoring someone else's attribute does not make it the operator's to delete")
		})
	})

	Context("when the type does not match", func() {
		BeforeEach(func() {
			createConnection(metav1.ConditionTrue)
			createTemporalNamespace(metav1.ConditionTrue)
		})

		It("should report a conflict and leave the attribute alone", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeText)
			putSearchAttribute("Keyword")

			_, err := reconcile()
			Expect(err).To(HaveOccurred())

			condition := readyCondition()
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonTypeConflict))
			Expect(condition.Message).To(ContainSubstring("Keyword"))
			Expect(condition.Message).To(ContainSubstring("Text"))

			Expect(ownership()).To(BeEmpty(), "a conflicting attribute is neither created nor adopted")
			Expect(temporalClient.created).To(BeEmpty(), "no attempt to retype it")
			Expect(temporalClient.deleted).To(BeEmpty(), "no drop and re-add")
			Expect(temporalClient.existing[temporalNamespace][attributeName]).To(Equal(temporal.SearchAttributeType("Keyword")))
		})

		It("should refuse even when it believes it created the attribute", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeText)
			setOwnership(temporalv1alpha1.SearchAttributeOwnershipCreating)

			// A Creating marker is not evidence when the type is wrong: the
			// operator would have registered it with the type it asked for.
			putSearchAttribute("Keyword")

			_, err := reconcile()
			Expect(err).To(HaveOccurred())

			Expect(readyCondition().Reason).To(Equal(ReasonTypeConflict))
			Expect(ownership()).To(Equal(temporalv1alpha1.SearchAttributeOwnershipCreating),
				"a conflict settles nothing")
		})
	})

	Context("when the resource is deleted", func() {
		BeforeEach(func() {
			createConnection(metav1.ConditionTrue)
			createTemporalNamespace(metav1.ConditionTrue)
		})

		It("should remove an attribute it registered", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1alpha1.SearchAttributeOwnershipCreated))

			beginDeletion()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.deleted).To(HaveLen(1))
			Expect(temporalClient.deleted[0].namespace).To(Equal(temporalNamespace))
			Expect(temporalClient.deleted[0].name).To(Equal(attributeName))
			Expect(temporalClient.existing[temporalNamespace]).NotTo(HaveKey(attributeName))
			Expect(isGone()).To(BeTrue())
		})

		It("should leave an adopted attribute alone", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)
			putSearchAttribute("Keyword")

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1alpha1.SearchAttributeOwnershipAdopted))

			beginDeletion()
			resetTemporalCalls()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			expectNoTemporalContact()
			Expect(temporalClient.existing[temporalNamespace]).To(HaveKey(attributeName))
			Expect(isGone()).To(BeTrue())
		})

		DescribeTable(
			"should release without contacting Temporal under Orphan",
			func(ownershipValue temporalv1alpha1.SearchAttributeOwnership) {
				createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

				_, err := reconcile()
				Expect(err).NotTo(HaveOccurred())

				setOwnership(ownershipValue)
				setDeletionPolicy(temporalv1alpha1.SearchAttributeDeletionPolicyOrphan)
				beginDeletion()
				resetTemporalCalls()

				_, err = reconcile()
				Expect(err).NotTo(HaveOccurred())

				expectNoTemporalContact()
				Expect(temporalClient.existing[temporalNamespace]).To(HaveKey(attributeName))
				Expect(isGone()).To(BeTrue())
			},
			Entry("Created", temporalv1alpha1.SearchAttributeOwnershipCreated),
			Entry("Creating", temporalv1alpha1.SearchAttributeOwnershipCreating),
			Entry("Adopted", temporalv1alpha1.SearchAttributeOwnershipAdopted),
		)

		It("should release under Orphan even with no Connection at all", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			conn := &temporalv1alpha1.Connection{}
			connKey := types.NamespacedName{Name: connectionName, Namespace: k8sNamespace}
			Expect(k8sClient.Get(ctx, connKey, conn)).To(Succeed())
			Expect(k8sClient.Delete(ctx, conn)).To(Succeed())

			setDeletionPolicy(temporalv1alpha1.SearchAttributeDeletionPolicyOrphan)
			beginDeletion()
			resetTemporalCalls()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			expectNoTemporalContact()
			Expect(isGone()).To(BeTrue())
		})

		It("should hold the finalizer under Delete when the Connection has gone", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			conn := &temporalv1alpha1.Connection{}
			connKey := types.NamespacedName{Name: connectionName, Namespace: k8sNamespace}
			Expect(k8sClient.Get(ctx, connKey, conn)).To(Succeed())
			Expect(k8sClient.Delete(ctx, conn)).To(Succeed())

			beginDeletion()
			resetTemporalCalls()

			_, err = reconcile()
			Expect(err).To(MatchError(ContainSubstring(connectionName)))

			Expect(hasFinalizer()).To(BeTrue(), "an unprovable external state must not be assumed away")
			Expect(isGone()).To(BeFalse())
			Expect(readyCondition().Reason).To(Equal(ReasonConnectionNotFound))
		})

		It("should treat an attribute that has already gone as deleted", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			delete(temporalClient.existing[temporalNamespace], attributeName)
			beginDeletion()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(isGone()).To(BeTrue())
		})

		It("should treat a vanished Temporal namespace as deletion already done", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(ownership()).To(Equal(temporalv1alpha1.SearchAttributeOwnershipCreated))

			// The Namespace resource finalised first and took its Temporal
			// namespace - and every search attribute on it - with it.
			temporalClient.missingNamespaces[temporalNamespace] = true
			beginDeletion()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred(),
				"a search attribute cannot outlive the namespace holding it")

			Expect(isGone()).To(BeTrue())
		})

		It("should still delete when only the Namespace resource has gone", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			// The Namespace resource is gone but its Temporal namespace was
			// orphaned, so the attribute is still there and still ours.
			ns := &temporalv1alpha1.Namespace{}
			nsKey := types.NamespacedName{Name: temporalNamespace, Namespace: k8sNamespace}
			Expect(k8sClient.Get(ctx, nsKey, ns)).To(Succeed())
			Expect(k8sClient.Delete(ctx, ns)).To(Succeed())

			beginDeletion()

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.deleted).To(HaveLen(1),
				"a missing Namespace resource does not prove the Temporal namespace is missing")
			Expect(temporalClient.existing[temporalNamespace]).NotTo(HaveKey(attributeName))
			Expect(isGone()).To(BeTrue())
		})

		It("should hold the finalizer when the removal fails", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

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

		It("should hold the finalizer when Temporal cannot be dialled", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			dialErr = errors.New("connection refused")
			beginDeletion()
			resetTemporalCalls()

			_, err = reconcile()
			Expect(err).To(MatchError(dialErr))

			Expect(temporalClient.deleted).To(BeEmpty())
			Expect(hasFinalizer()).To(BeTrue())
			Expect(readyCondition().Reason).To(Equal(ReasonConnectionFailed))
		})

		It("should add the finalizer without disturbing the spec", func() {
			attribute := createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)
			Expect(attribute.Generation).To(Equal(int64(1)))

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			after := stored()
			Expect(controllerutil.ContainsFinalizer(after, temporalv1alpha1.SearchAttributeFinalizer)).To(BeTrue())
			Expect(after.Generation).To(Equal(int64(1)),
				"adding the finalizer must not change the spec")
		})
	})

	Context("validation and defaulting", func() {
		It("should default the deletion policy to Delete without admission", func() {
			spec := temporalv1alpha1.SearchAttributeSpec{}
			Expect(spec.DeletionPolicy).To(BeEmpty())
			Expect(spec.DeletionPolicyValue()).
				To(Equal(temporalv1alpha1.SearchAttributeDeletionPolicyDelete))
			Expect(temporalv1alpha1.DefaultSearchAttributeDeletionPolicy).
				To(Equal(temporalv1alpha1.SearchAttributeDeletionPolicyDelete))
		})

		It("should default the deletion policy at admission", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			Expect(stored().Spec.DeletionPolicy).
				To(Equal(temporalv1alpha1.SearchAttributeDeletionPolicyDelete))
		})

		It("should send the Temporal name, never the Kubernetes one", func() {
			createConnection(metav1.ConditionTrue)
			createTemporalNamespace(metav1.ConditionTrue)
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			Expect(resourceName).NotTo(Equal(attributeName), "the two names must differ for this to prove anything")

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			By("looking it up under the Temporal name")
			Expect(temporalClient.described).To(HaveLen(1))
			Expect(temporalClient.described[0].name).To(Equal(attributeName))

			By("registering it under the Temporal name")
			Expect(temporalClient.created).To(HaveLen(1))
			Expect(temporalClient.created[0].name).To(Equal(attributeName))
			Expect(temporalClient.existing[temporalNamespace]).To(HaveKey(attributeName))
			Expect(temporalClient.existing[temporalNamespace]).NotTo(HaveKey(resourceName),
				"the Kubernetes name must never reach Temporal")

			By("removing it under the Temporal name")
			beginDeletion()
			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.deleted).To(HaveLen(1))
			Expect(temporalClient.deleted[0].name).To(Equal(attributeName))
		})

		It("should adopt a mixed-case attribute that already exists", func() {
			createConnection(metav1.ConditionTrue)
			createTemporalNamespace(metav1.ConditionTrue)

			// The real-world shape: a PascalCase attribute registered long ago,
			// managed by a resource Kubernetes insists on calling something
			// lowercase.
			attributeName = "CustomerId"
			resourceName = "customer-id"
			key = types.NamespacedName{Name: resourceName, Namespace: k8sNamespace}

			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)
			putSearchAttribute("Keyword")

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.described[0].name).To(Equal("CustomerId"))
			Expect(temporalClient.created).To(BeEmpty(), "an existing attribute must not be re-registered")
			Expect(ownership()).To(Equal(temporalv1alpha1.SearchAttributeOwnershipAdopted))
			Expect(readyCondition().Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCondition().Reason).To(Equal(ReasonAdopted))
		})

		It("should conflict on a mixed-case attribute of the wrong type", func() {
			createConnection(metav1.ConditionTrue)
			createTemporalNamespace(metav1.ConditionTrue)

			attributeName = "CustomerId"
			resourceName = "customer-id"
			key = types.NamespacedName{Name: resourceName, Namespace: k8sNamespace}

			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeText)
			putSearchAttribute("Keyword")

			_, err := reconcile()
			Expect(err).To(HaveOccurred())

			Expect(readyCondition().Reason).To(Equal(ReasonTypeConflict))
			Expect(readyCondition().Message).To(ContainSubstring("CustomerId"),
				"the message should name the Temporal attribute, not the resource")
			Expect(temporalClient.existing[temporalNamespace]["CustomerId"]).
				To(Equal(temporal.SearchAttributeType("Keyword")))
		})

		It("should recreate under the Temporal name after an external removal", func() {
			createConnection(metav1.ConditionTrue)
			createTemporalNamespace(metav1.ConditionTrue)
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			delete(temporalClient.existing[temporalNamespace], attributeName)

			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			Expect(temporalClient.created).To(HaveLen(2))
			Expect(temporalClient.created[1].name).To(Equal(attributeName))
			Expect(ownership()).To(Equal(temporalv1alpha1.SearchAttributeOwnershipCreated))
		})

		It("should resolve the Temporal namespace from the reference alone", func() {
			attribute := createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			Expect(attribute.TemporalName()).To(Equal(attributeName))
			Expect(attribute.TemporalNamespace()).To(Equal(temporalNamespace),
				"finalisation must not need the Namespace resource to know where to look")
		})

		DescribeTable(
			"should be rejected by the API server",
			func(mutate func(*temporalv1alpha1.SearchAttributeSpec), expected string) {
				spec := temporalv1alpha1.SearchAttributeSpec{
					Name:          attributeName,
					ConnectionRef: corev1.LocalObjectReference{Name: connectionName},
					NamespaceRef:  corev1.LocalObjectReference{Name: temporalNamespace},
					Type:          temporalv1alpha1.SearchAttributeTypeKeyword,
				}
				mutate(&spec)

				err := k8sClient.Create(ctx, &temporalv1alpha1.SearchAttribute{
					ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: k8sNamespace},
					Spec:       spec,
				})
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(expected))
			},
			Entry("with no connectionRef name", func(s *temporalv1alpha1.SearchAttributeSpec) {
				s.ConnectionRef.Name = ""
			}, "connectionRef.name is required"),
			Entry("with no namespaceRef name", func(s *temporalv1alpha1.SearchAttributeSpec) {
				s.NamespaceRef.Name = ""
			}, "namespaceRef.name is required"),
			Entry("with a type the API does not define", func(s *temporalv1alpha1.SearchAttributeSpec) {
				s.Type = temporalv1alpha1.SearchAttributeType("Blob")
			}, "Unsupported value"),
			Entry("with a deletion policy the API does not define", func(s *temporalv1alpha1.SearchAttributeSpec) {
				s.DeletionPolicy = temporalv1alpha1.SearchAttributeDeletionPolicy("Abandon")
			}, "Unsupported value"),
		)

		DescribeTable(
			"should refuse to change what the resource points at",
			func(mutate func(*temporalv1alpha1.SearchAttribute), expected string) {
				createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

				attribute := stored()
				mutate(attribute)

				err := k8sClient.Update(ctx, attribute)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(expected))
			},
			Entry("the Temporal name", func(a *temporalv1alpha1.SearchAttribute) {
				a.Spec.Name = "SomethingElse"
			}, "name is immutable"),
			Entry("the Connection", func(a *temporalv1alpha1.SearchAttribute) {
				a.Spec.ConnectionRef.Name = "another-connection"
			}, "connectionRef.name is immutable"),
			Entry("the Namespace", func(a *temporalv1alpha1.SearchAttribute) {
				a.Spec.NamespaceRef.Name = "another-namespace"
			}, "namespaceRef.name is immutable"),
			Entry("the type", func(a *temporalv1alpha1.SearchAttribute) {
				a.Spec.Type = temporalv1alpha1.SearchAttributeTypeText
			}, "type is immutable"),
		)

		It("should require a Temporal name", func() {
			err := k8sClient.Create(ctx, &temporalv1alpha1.SearchAttribute{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: k8sNamespace},
				Spec: temporalv1alpha1.SearchAttributeSpec{
					ConnectionRef: corev1.LocalObjectReference{Name: connectionName},
					NamespaceRef:  corev1.LocalObjectReference{Name: temporalNamespace},
					Type:          temporalv1alpha1.SearchAttributeTypeKeyword,
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.name"))
		})

		It("should keep the deletion policy mutable, terminating or not", func() {
			createConnection(metav1.ConditionTrue)
			createTemporalNamespace(metav1.ConditionTrue)
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			_, err := reconcile()
			Expect(err).NotTo(HaveOccurred())

			By("switching it while the resource is live")
			setDeletionPolicy(temporalv1alpha1.SearchAttributeDeletionPolicyOrphan)
			Expect(stored().Spec.DeletionPolicy).
				To(Equal(temporalv1alpha1.SearchAttributeDeletionPolicyOrphan))

			setDeletionPolicy(temporalv1alpha1.SearchAttributeDeletionPolicyDelete)

			By("switching it while the resource is terminating")
			beginDeletion()
			setDeletionPolicy(temporalv1alpha1.SearchAttributeDeletionPolicyOrphan)

			// The immutability rules must not have caught this: it is the way
			// out of a deletion blocked on a broken dependency.
			Expect(stored().Spec.DeletionPolicy).
				To(Equal(temporalv1alpha1.SearchAttributeDeletionPolicyOrphan))
			Expect(stored().GetDeletionTimestamp().IsZero()).To(BeFalse())

			resetTemporalCalls()
			_, err = reconcile()
			Expect(err).NotTo(HaveOccurred())

			expectNoTemporalContact()
			Expect(isGone()).To(BeTrue())
		})

		It("should refuse to change the type", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			attribute := stored()
			attribute.Spec.Type = temporalv1alpha1.SearchAttributeTypeText

			err := k8sClient.Update(ctx, attribute)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("type is immutable"))
		})

		It("should allow everything else to change", func() {
			createSearchAttribute(temporalv1alpha1.SearchAttributeTypeKeyword)

			attribute := stored()
			attribute.Spec.DeletionPolicy = temporalv1alpha1.SearchAttributeDeletionPolicyOrphan
			Expect(k8sClient.Update(ctx, attribute)).To(Succeed())
		})
	})
})

// recordingClient wraps the fake so a spec can observe the moment a call is
// made.
type recordingClient struct {
	TemporalSearchAttributeClient

	onCreate func()
}

func (c *recordingClient) CreateSearchAttribute(
	ctx context.Context,
	namespace, name string,
	attrType temporal.SearchAttributeType,
) error {
	if c.onCreate != nil {
		c.onCreate()
	}

	return c.TemporalSearchAttributeClient.CreateSearchAttribute(ctx, namespace, name, attrType)
}

var _ = Describe("SearchAttribute dependencies", func() {
	const (
		connectionA = "conn-a"
		namespaceA  = "ns-a"
	)

	// dependant builds a SearchAttribute in the test namespace referencing the
	// given dependencies.
	dependant := func(name, conn, ns string) *temporalv1alpha1.SearchAttribute {
		return &temporalv1alpha1.SearchAttribute{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: saNamespace},
			Spec: temporalv1alpha1.SearchAttributeSpec{
				ConnectionRef: corev1.LocalObjectReference{Name: conn},
				NamespaceRef:  corev1.LocalObjectReference{Name: ns},
				Type:          temporalv1alpha1.SearchAttributeTypeKeyword,
			},
		}
	}

	request := func(k8sNamespace, name string) ctrl.Request {
		return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: k8sNamespace, Name: name}}
	}

	Describe("the field indexes", func() {
		DescribeTable(
			"should index a SearchAttribute by what it references",
			func(index func(ctrlclient.Object) []string, obj ctrlclient.Object, expected []string) {
				Expect(index(obj)).To(Equal(expected))
			},
			Entry("its Connection", indexSearchAttributeByConnection,
				dependant("by-connection", connectionA, namespaceA), []string{connectionA}),
			Entry("its Namespace", indexSearchAttributeByNamespace,
				dependant("by-namespace", connectionA, namespaceA), []string{namespaceA}),
			Entry("an empty Connection reference", indexSearchAttributeByConnection,
				dependant("no-connection", "", namespaceA), []string(nil)),
			Entry("an empty Namespace reference", indexSearchAttributeByNamespace,
				dependant("no-namespace", connectionA, ""), []string(nil)),
			Entry("a nil SearchAttribute", indexSearchAttributeByConnection,
				(*temporalv1alpha1.SearchAttribute)(nil), []string(nil)),
			Entry("an object of another kind", indexSearchAttributeByNamespace,
				&temporalv1alpha1.Connection{ObjectMeta: metav1.ObjectMeta{Name: connectionA}}, []string(nil)),
		)
	})

	Describe("mapping a dependency event onto its dependants", func() {
		var mapper *SearchAttributeReconciler

		BeforeEach(func() {
			// A like-named dependency in another Kubernetes namespace, which
			// must never be woken by events in the test namespace.
			elsewhere := dependant("attr-elsewhere", connectionA, namespaceA)
			elsewhere.Namespace = otherNamespace

			mapper = &SearchAttributeReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(k8sClient.Scheme()).
					WithIndex(&temporalv1alpha1.SearchAttribute{},
						searchAttributeConnectionRefIndex, indexSearchAttributeByConnection).
					WithIndex(&temporalv1alpha1.SearchAttribute{},
						searchAttributeNamespaceRefIndex, indexSearchAttributeByNamespace).
					WithObjects(
						dependant("attr-one", connectionA, namespaceA),
						dependant("attr-two", connectionA, namespaceA),
						dependant("attr-other", "conn-b", "ns-b"),
						elsewhere,
					).
					Build(),
			}
		})

		It("should map a Connection to its dependants beside it", func() {
			conn := &temporalv1alpha1.Connection{
				ObjectMeta: metav1.ObjectMeta{Name: connectionA, Namespace: saNamespace},
			}

			requests := mapper.searchAttributesForDependency(searchAttributeConnectionRefIndex)(ctx, conn)

			Expect(requests).To(ConsistOf(request(saNamespace, "attr-one"), request(saNamespace, "attr-two")))
			Expect(requests).NotTo(ContainElement(request(saNamespace, "attr-other")))
			Expect(requests).NotTo(ContainElement(request(otherNamespace, "attr-elsewhere")),
				"a like-named Connection in another Kubernetes namespace is a different Connection")
		})

		It("should map a Namespace to its dependants beside it", func() {
			ns := &temporalv1alpha1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: namespaceA, Namespace: saNamespace},
			}

			requests := mapper.searchAttributesForDependency(searchAttributeNamespaceRefIndex)(ctx, ns)

			Expect(requests).To(ConsistOf(request(saNamespace, "attr-one"), request(saNamespace, "attr-two")))
			Expect(requests).NotTo(ContainElement(request(otherNamespace, "attr-elsewhere")))
		})

		It("should map the same however the event arose", func() {
			ready := &temporalv1alpha1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: namespaceA, Namespace: saNamespace},
			}
			meta.SetStatusCondition(&ready.Status.Conditions, metav1.Condition{
				Type:   temporalv1alpha1.ConditionTypeReady,
				Status: metav1.ConditionTrue,
				Reason: ReasonCreated,
			})
			ready.Status.Ownership = temporalv1alpha1.NamespaceOwnershipCreated

			deleted := ready.DeepCopy()
			deleted.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}

			expected := ConsistOf(request(saNamespace, "attr-one"), request(saNamespace, "attr-two"))
			mapNamespace := mapper.searchAttributesForDependency(searchAttributeNamespaceRefIndex)

			Expect(mapNamespace(ctx, ready)).To(expected, "on a status change")
			Expect(mapNamespace(ctx, deleted)).To(expected, "on delete")
		})

		It("should return nothing when nothing depends on the event", func() {
			unused := &temporalv1alpha1.Connection{
				ObjectMeta: metav1.ObjectMeta{Name: "unused", Namespace: saNamespace},
			}

			Expect(mapper.searchAttributesForDependency(searchAttributeConnectionRefIndex)(ctx, unused)).
				To(BeEmpty())
		})

		It("should give up quietly when the list fails", func() {
			failing := &SearchAttributeReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(k8sClient.Scheme()).
					WithIndex(&temporalv1alpha1.SearchAttribute{},
						searchAttributeConnectionRefIndex, indexSearchAttributeByConnection).
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

			conn := &temporalv1alpha1.Connection{
				ObjectMeta: metav1.ObjectMeta{Name: connectionA, Namespace: saNamespace},
			}

			var requests []ctrl.Request
			Expect(func() {
				requests = failing.searchAttributesForDependency(searchAttributeConnectionRefIndex)(ctx, conn)
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

			reconciler := &SearchAttributeReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}

			Expect(reconciler.SetupWithManager(mgr)).To(Succeed())
		})
	})
})
