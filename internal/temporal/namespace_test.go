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
		temporalClient  *Client
	)

	BeforeEach(func() {
		ctx = context.Background()

		ctrl := gomock.NewController(GinkgoT())
		DeferCleanup(ctrl.Finish)

		workflowService = workflowservicemock.NewMockWorkflowServiceClient(ctrl)

		sdkClient := &sdkmocks.Client{}
		sdkClient.On("WorkflowService").Return(workflowService)

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

					return &workflowservice.RegisterNamespaceResponse{}, nil
				})

			Expect(temporalClient.CreateNamespace(ctx, namespaceName, 72*time.Hour)).To(Succeed())
		})

		It("should wrap a registration failure", func() {
			workflowService.EXPECT().
				RegisterNamespace(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewNamespaceAlreadyExists("already there"))

			err := temporalClient.CreateNamespace(ctx, namespaceName, 72*time.Hour)
			Expect(err).To(MatchError(ContainSubstring("registering namespace payments")))
			Expect(err).To(MatchError(ContainSubstring("already there")))
		})

		It("should reject an empty name without calling Temporal", func() {
			Expect(temporalClient.CreateNamespace(ctx, "", 72*time.Hour)).
				To(MatchError(ErrNoNamespaceName))
		})
	})
})
