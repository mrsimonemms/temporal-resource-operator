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

package temporal

import (
	"context"
	"errors"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/operatorservicemock/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"
	sdkmocks "go.temporal.io/sdk/mocks"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

var _ = Describe("Namespace operations", func() {
	const namespaceName = "payments"

	var (
		ctx             context.Context
		workflowService *workflowservicemock.MockWorkflowServiceClient
		operatorService *operatorservicemock.MockOperatorServiceClient
		temporalClient  *Client
	)

	BeforeEach(func() {
		ctx = context.Background()

		ctrl := gomock.NewController(GinkgoT())
		DeferCleanup(ctrl.Finish)

		workflowService = workflowservicemock.NewMockWorkflowServiceClient(ctrl)
		operatorService = operatorservicemock.NewMockOperatorServiceClient(ctrl)

		sdkClient := &sdkmocks.Client{}
		sdkClient.On("WorkflowService").Return(workflowService)
		sdkClient.On("OperatorService").Return(operatorService)

		temporalClient = &Client{client: sdkClient}
	})

	Describe("DescribeNamespace", func() {
		It("should return the namespace and its retention", func() {
			workflowService.EXPECT().
				DescribeNamespace(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *workflowservice.DescribeNamespaceRequest,
					_ ...grpc.CallOption,
				) (*workflowservice.DescribeNamespaceResponse, error) {
					Expect(req.GetNamespace()).To(Equal(namespaceName))

					return &workflowservice.DescribeNamespaceResponse{
						NamespaceInfo: &namespacepb.NamespaceInfo{Name: namespaceName},
						Config: &namespacepb.NamespaceConfig{
							WorkflowExecutionRetentionTtl: durationpb.New(48 * time.Hour),
						},
					}, nil
				})

			ns, err := temporalClient.DescribeNamespace(ctx, namespaceName)
			Expect(err).NotTo(HaveOccurred())

			Expect(ns.Name).To(Equal(namespaceName))
			Expect(ns.Retention).To(Equal(48 * time.Hour))
			Expect(ns.Data).To(BeEmpty())
		})

		It("should return the namespace's custom metadata as a copy", func() {
			// The SDK owns this map; the caller must not be handed it.
			sdkOwned := map[string]string{"owner": "abc", "team": "payments"}

			workflowService.EXPECT().
				DescribeNamespace(gomock.Any(), gomock.Any()).
				Return(&workflowservice.DescribeNamespaceResponse{
					NamespaceInfo: &namespacepb.NamespaceInfo{Name: namespaceName, Data: sdkOwned},
					Config:        &namespacepb.NamespaceConfig{},
				}, nil)

			ns, err := temporalClient.DescribeNamespace(ctx, namespaceName)
			Expect(err).NotTo(HaveOccurred())

			Expect(ns.Data).To(Equal(map[string]string{"owner": "abc", "team": "payments"}))

			ns.Data["owner"] = "tampered"
			Expect(sdkOwned).To(HaveKeyWithValue("owner", "abc"),
				"mutating the returned map must not reach back into the SDK's")
		})

		It("should report a missing namespace as ErrNamespaceNotFound", func() {
			temporalErr := serviceerror.NewNamespaceNotFound(namespaceName)
			workflowService.EXPECT().
				DescribeNamespace(gomock.Any(), gomock.Any()).
				Return(nil, temporalErr)

			_, err := temporalClient.DescribeNamespace(ctx, namespaceName)
			Expect(err).To(MatchError(ErrNamespaceNotFound))

			// The underlying Temporal error stays in the chain, so callers can
			// still report what the Service actually said.
			Expect(err).To(MatchError(temporalErr))
		})

		It("should not mistake another failure for a missing namespace", func() {
			workflowService.EXPECT().
				DescribeNamespace(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewUnavailable("service down"))

			_, err := temporalClient.DescribeNamespace(ctx, namespaceName)
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, ErrNamespaceNotFound)).To(BeFalse())
			Expect(err).To(MatchError(ContainSubstring("service down")))
		})

		It("should reject an empty name without calling Temporal", func() {
			_, err := temporalClient.DescribeNamespace(ctx, "")
			Expect(err).To(MatchError(ErrNoNamespaceName))
		})
	})

	Describe("CreateNamespace", func() {
		It("should register the namespace with the given retention", func() {
			workflowService.EXPECT().
				RegisterNamespace(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *workflowservice.RegisterNamespaceRequest,
					_ ...grpc.CallOption,
				) (*workflowservice.RegisterNamespaceResponse, error) {
					Expect(req.GetNamespace()).To(Equal(namespaceName))
					Expect(req.GetWorkflowExecutionRetentionPeriod().AsDuration()).To(Equal(72 * time.Hour))
					Expect(req.GetData()).To(BeEmpty())

					return &workflowservice.RegisterNamespaceResponse{}, nil
				})

			Expect(temporalClient.CreateNamespace(ctx, namespaceName, 72*time.Hour, nil)).To(Succeed())
		})

		It("should register custom metadata as part of the same request", func() {
			caller := map[string]string{"temporal.simonemms.com/owner-uid": "uid-1"}

			workflowService.EXPECT().
				RegisterNamespace(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *workflowservice.RegisterNamespaceRequest,
					_ ...grpc.CallOption,
				) (*workflowservice.RegisterNamespaceResponse, error) {
					// Registering with the data attached means the namespace can
					// never exist without it, closing the window an update after
					// creation would leave open.
					Expect(req.GetData()).To(Equal(map[string]string{
						"temporal.simonemms.com/owner-uid": "uid-1",
					}))

					// The request must not share the caller's map.
					req.Data["temporal.simonemms.com/owner-uid"] = "tampered"

					return &workflowservice.RegisterNamespaceResponse{}, nil
				})

			Expect(temporalClient.CreateNamespace(ctx, namespaceName, 72*time.Hour, caller)).To(Succeed())
			Expect(caller).To(HaveKeyWithValue("temporal.simonemms.com/owner-uid", "uid-1"))
		})

		It("should wrap a registration failure", func() {
			workflowService.EXPECT().
				RegisterNamespace(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewNamespaceAlreadyExists("already there"))

			err := temporalClient.CreateNamespace(ctx, namespaceName, 72*time.Hour, nil)
			Expect(err).To(MatchError(ContainSubstring("registering namespace payments")))
			Expect(err).To(MatchError(ContainSubstring("already there")))
		})

		It("should reject an empty name without calling Temporal", func() {
			Expect(temporalClient.CreateNamespace(ctx, "", 72*time.Hour, nil)).
				To(MatchError(ErrNoNamespaceName))
		})
	})

	Describe("UpdateNamespaceRetention", func() {
		It("should set the retention and touch nothing else", func() {
			workflowService.EXPECT().
				UpdateNamespace(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *workflowservice.UpdateNamespaceRequest,
					_ ...grpc.CallOption,
				) (*workflowservice.UpdateNamespaceResponse, error) {
					Expect(req.GetNamespace()).To(Equal(namespaceName))
					Expect(req.GetConfig().GetWorkflowExecutionRetentionTtl().AsDuration()).To(Equal(48 * time.Hour))

					// Only the retention is set, so the Service leaves the
					// namespace's other settings alone. UpdateInfo in particular
					// stays nil: that is where Data lives, and sending it would
					// risk disturbing the ownership marker.
					Expect(req.GetUpdateInfo()).To(BeNil())
					Expect(req.GetUpdateInfo().GetData()).To(BeEmpty())
					Expect(req.GetReplicationConfig()).To(BeNil())
					Expect(req.GetConfig().GetHistoryArchivalUri()).To(BeEmpty())

					return &workflowservice.UpdateNamespaceResponse{}, nil
				})

			Expect(temporalClient.UpdateNamespaceRetention(ctx, namespaceName, 48*time.Hour)).To(Succeed())
		})

		It("should report a missing namespace as ErrNamespaceNotFound", func() {
			workflowService.EXPECT().
				UpdateNamespace(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewNamespaceNotFound(namespaceName))

			Expect(temporalClient.UpdateNamespaceRetention(ctx, namespaceName, 48*time.Hour)).
				To(MatchError(ErrNamespaceNotFound))
		})

		It("should wrap any other failure", func() {
			workflowService.EXPECT().
				UpdateNamespace(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewInvalidArgument("retention too short"))

			err := temporalClient.UpdateNamespaceRetention(ctx, namespaceName, time.Minute)
			Expect(errors.Is(err, ErrNamespaceNotFound)).To(BeFalse())
			Expect(err).To(MatchError(ContainSubstring("updating namespace payments")))
			Expect(err).To(MatchError(ContainSubstring("retention too short")))
		})

		It("should reject an empty name without calling Temporal", func() {
			Expect(temporalClient.UpdateNamespaceRetention(ctx, "", 48*time.Hour)).
				To(MatchError(ErrNoNamespaceName))
		})
	})

	Describe("DeleteNamespace", func() {
		It("should delete the namespace through the operator service", func() {
			operatorService.EXPECT().
				DeleteNamespace(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *operatorservice.DeleteNamespaceRequest,
					_ ...grpc.CallOption,
				) (*operatorservice.DeleteNamespaceResponse, error) {
					Expect(req.GetNamespace()).To(Equal(namespaceName))

					return &operatorservice.DeleteNamespaceResponse{}, nil
				})

			Expect(temporalClient.DeleteNamespace(ctx, namespaceName)).To(Succeed())
		})

		It("should report a missing namespace as ErrNamespaceNotFound", func() {
			temporalErr := serviceerror.NewNamespaceNotFound(namespaceName)
			operatorService.EXPECT().
				DeleteNamespace(gomock.Any(), gomock.Any()).
				Return(nil, temporalErr)

			// Reusing the sentinel is what lets the caller make deletion
			// idempotent without inspecting Temporal error types itself.
			err := temporalClient.DeleteNamespace(ctx, namespaceName)
			Expect(err).To(MatchError(ErrNamespaceNotFound))
			Expect(err).To(MatchError(temporalErr))
		})

		It("should not mistake another failure for a missing namespace", func() {
			operatorService.EXPECT().
				DeleteNamespace(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewPermissionDenied("nope", "no reason"))

			err := temporalClient.DeleteNamespace(ctx, namespaceName)
			Expect(errors.Is(err, ErrNamespaceNotFound)).To(BeFalse())
			Expect(err).To(MatchError(ContainSubstring("deleting namespace payments")))
		})

		It("should reject an empty name without calling Temporal", func() {
			Expect(temporalClient.DeleteNamespace(ctx, "")).To(MatchError(ErrNoNamespaceName))
		})
	})
})
