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

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	commonpb "go.temporal.io/api/common/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/operatorservicemock/v1"
	"go.temporal.io/api/serviceerror"
	sdkmocks "go.temporal.io/sdk/mocks"
	"google.golang.org/grpc"
)

var _ = Describe("Nexus endpoint operations", func() {
	const (
		endpointName = "PaymentsNexus"
		endpointID   = "6f1d3a2e-0000-4000-8000-000000000001"
		namespaceOne = "payments"
		taskQueueOne = "payments-nexus"
	)

	var (
		ctx             context.Context
		operatorService *operatorservicemock.MockOperatorServiceClient
		temporalClient  *Client
	)

	BeforeEach(func() {
		ctx = context.Background()

		ctrl := gomock.NewController(GinkgoT())
		DeferCleanup(ctrl.Finish)

		operatorService = operatorservicemock.NewMockOperatorServiceClient(ctrl)

		sdkClient := &sdkmocks.Client{}
		sdkClient.On("OperatorService").Return(operatorService)

		temporalClient = &Client{client: sdkClient}
	})

	// serverEndpoint builds an endpoint as the Service would report it.
	serverEndpoint := func(version int64, namespace, taskQueue string, description *commonpb.Payload) *nexuspb.Endpoint {
		return &nexuspb.Endpoint{
			Id:      endpointID,
			Version: version,
			Spec: &nexuspb.EndpointSpec{
				Name:        endpointName,
				Description: description,
				Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{Namespace: namespace, TaskQueue: taskQueue},
				}},
			},
		}
	}

	Describe("DescribeNexusEndpoint", func() {
		It("should find an endpoint by its Service-wide name", func() {
			operatorService.EXPECT().
				ListNexusEndpoints(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *operatorservice.ListNexusEndpointsRequest,
					_ ...grpc.CallOption,
				) (*operatorservice.ListNexusEndpointsResponse, error) {
					// The name filter is what makes a name a usable handle:
					// Temporal returns at most one endpoint for it.
					Expect(req.GetName()).To(Equal(endpointName))
					Expect(req.GetPageSize()).To(BeNumerically(">", 0))

					return &operatorservice.ListNexusEndpointsResponse{
						Endpoints: []*nexuspb.Endpoint{serverEndpoint(3, namespaceOne, taskQueueOne, nil)},
					}, nil
				})

			endpoint, err := temporalClient.DescribeNexusEndpoint(ctx, endpointName)
			Expect(err).NotTo(HaveOccurred())

			Expect(endpoint.ID).To(Equal(endpointID))
			Expect(endpoint.Version).To(Equal(int64(3)))
			Expect(endpoint.Name).To(Equal(endpointName))
			Expect(endpoint.TargetNamespace).To(Equal(namespaceOne))
			Expect(endpoint.TaskQueue).To(Equal(taskQueueOne))
		})

		It("should report an empty result as not found", func() {
			// Temporal answers an unknown name with an empty list rather than
			// an error, so this is what "absent" looks like.
			operatorService.EXPECT().
				ListNexusEndpoints(gomock.Any(), gomock.Any()).
				Return(&operatorservice.ListNexusEndpointsResponse{}, nil)

			_, err := temporalClient.DescribeNexusEndpoint(ctx, endpointName)
			Expect(err).To(MatchError(ErrNexusEndpointNotFound))
		})

		It("should wrap a lookup failure", func() {
			operatorService.EXPECT().
				ListNexusEndpoints(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewUnavailable("service down"))

			_, err := temporalClient.DescribeNexusEndpoint(ctx, endpointName)
			Expect(errors.Is(err, ErrNexusEndpointNotFound)).To(BeFalse())
			Expect(err).To(MatchError(ContainSubstring("looking up nexus endpoint PaymentsNexus")))
		})

		It("should reject an empty name without calling Temporal", func() {
			_, err := temporalClient.DescribeNexusEndpoint(ctx, "")
			Expect(err).To(MatchError(ErrNoNexusEndpointName))
		})
	})

	Describe("CreateNexusEndpoint", func() {
		It("should create a worker target and return what the Service recorded", func() {
			operatorService.EXPECT().
				CreateNexusEndpoint(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *operatorservice.CreateNexusEndpointRequest,
					_ ...grpc.CallOption,
				) (*operatorservice.CreateNexusEndpointResponse, error) {
					spec := req.GetSpec()
					Expect(spec.GetName()).To(Equal(endpointName))

					worker := spec.GetTarget().GetWorker()
					Expect(worker).NotTo(BeNil(), "the operator only manages worker targets")
					Expect(worker.GetNamespace()).To(Equal(namespaceOne))
					Expect(worker.GetTaskQueue()).To(Equal(taskQueueOne))

					return &operatorservice.CreateNexusEndpointResponse{
						Endpoint: serverEndpoint(1, namespaceOne, taskQueueOne, nil),
					}, nil
				})

			created, err := temporalClient.CreateNexusEndpoint(ctx, endpointName, namespaceOne, taskQueueOne)
			Expect(err).NotTo(HaveOccurred())

			Expect(created.ID).To(Equal(endpointID))
			Expect(created.Version).To(Equal(int64(1)))
		})

		It("should report a name that is already taken", func() {
			// Endpoint names are unique Service-wide, so this is how a race for
			// the same name surfaces.
			operatorService.EXPECT().
				CreateNexusEndpoint(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewAlreadyExists("Endpoint with name PaymentsNexus already registered"))

			_, err := temporalClient.CreateNexusEndpoint(ctx, endpointName, namespaceOne, taskQueueOne)
			Expect(err).To(MatchError(ErrNexusEndpointExists))
		})

		It("should wrap any other failure", func() {
			// Temporal refuses a target namespace that does not exist.
			operatorService.EXPECT().
				CreateNexusEndpoint(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewFailedPrecondition(
					"could not verify namespace referenced by target exists",
				))

			_, err := temporalClient.CreateNexusEndpoint(ctx, endpointName, "ghost", taskQueueOne)
			Expect(errors.Is(err, ErrNexusEndpointExists)).To(BeFalse())
			Expect(err).To(MatchError(ContainSubstring("creating nexus endpoint PaymentsNexus")))
		})

		It("should reject an empty name without calling Temporal", func() {
			_, err := temporalClient.CreateNexusEndpoint(ctx, "", namespaceOne, taskQueueOne)
			Expect(err).To(MatchError(ErrNoNexusEndpointName))
		})
	})

	Describe("UpdateNexusEndpointTarget", func() {
		It("should send the ID and version it was given", func() {
			current := newNexusEndpoint(serverEndpoint(4, namespaceOne, taskQueueOne, nil))

			operatorService.EXPECT().
				UpdateNexusEndpoint(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *operatorservice.UpdateNexusEndpointRequest,
					_ ...grpc.CallOption,
				) (*operatorservice.UpdateNexusEndpointResponse, error) {
					Expect(req.GetId()).To(Equal(endpointID))
					Expect(req.GetVersion()).To(Equal(int64(4)), "the version read is what guards the write")

					worker := req.GetSpec().GetTarget().GetWorker()
					Expect(worker.GetNamespace()).To(Equal("orders"))
					Expect(worker.GetTaskQueue()).To(Equal("orders-nexus"))

					return &operatorservice.UpdateNexusEndpointResponse{
						Endpoint: serverEndpoint(5, "orders", "orders-nexus", nil),
					}, nil
				})

			updated, err := temporalClient.UpdateNexusEndpointTarget(ctx, current, "orders", "orders-nexus")
			Expect(err).NotTo(HaveOccurred())

			Expect(updated.Version).To(Equal(int64(5)))
			Expect(updated.TargetNamespace).To(Equal("orders"))
		})

		It("should keep the parts of the spec it does not manage", func() {
			// Temporal replaces the whole spec on update, so a description set
			// by hand would be lost if the update did not carry it back.
			description := &commonpb.Payload{Data: []byte(`"set by a human"`)}
			current := newNexusEndpoint(serverEndpoint(1, namespaceOne, taskQueueOne, description))

			operatorService.EXPECT().
				UpdateNexusEndpoint(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *operatorservice.UpdateNexusEndpointRequest,
					_ ...grpc.CallOption,
				) (*operatorservice.UpdateNexusEndpointResponse, error) {
					Expect(req.GetSpec().GetDescription().GetData()).To(Equal([]byte(`"set by a human"`)))

					return &operatorservice.UpdateNexusEndpointResponse{
						Endpoint: serverEndpoint(2, "orders", "orders-nexus", description),
					}, nil
				})

			_, err := temporalClient.UpdateNexusEndpointTarget(ctx, current, "orders", "orders-nexus")
			Expect(err).NotTo(HaveOccurred())

			// The endpoint handed in is left alone, so a failed update does not
			// leave the caller holding something half-changed.
			Expect(current.TargetNamespace).To(Equal(namespaceOne))
		})

		It("should report a version mismatch as a change under it", func() {
			current := newNexusEndpoint(serverEndpoint(1, namespaceOne, taskQueueOne, nil))

			operatorService.EXPECT().
				UpdateNexusEndpoint(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewFailedPrecondition(
					"nexus endpoint version mismatch. received: 1 expected 7",
				))

			_, err := temporalClient.UpdateNexusEndpointTarget(ctx, current, "orders", "orders-nexus")
			Expect(err).To(MatchError(ErrNexusEndpointChanged))
			Expect(err).To(MatchError(ContainSubstring("version mismatch")))
		})

		It("should reject a nil endpoint without calling Temporal", func() {
			_, err := temporalClient.UpdateNexusEndpointTarget(ctx, nil, namespaceOne, taskQueueOne)
			Expect(err).To(MatchError(ErrNoNexusEndpointName))
		})
	})

	Describe("DeleteNexusEndpoint", func() {
		It("should delete by ID", func() {
			current := newNexusEndpoint(serverEndpoint(2, namespaceOne, taskQueueOne, nil))

			operatorService.EXPECT().
				DeleteNexusEndpoint(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *operatorservice.DeleteNexusEndpointRequest,
					_ ...grpc.CallOption,
				) (*operatorservice.DeleteNexusEndpointResponse, error) {
					Expect(req.GetId()).To(Equal(endpointID))
					Expect(req.GetVersion()).To(Equal(int64(2)))

					return &operatorservice.DeleteNexusEndpointResponse{}, nil
				})

			Expect(temporalClient.DeleteNexusEndpoint(ctx, current)).To(Succeed())
		})

		It("should report an endpoint that has already gone", func() {
			current := newNexusEndpoint(serverEndpoint(2, namespaceOne, taskQueueOne, nil))

			operatorService.EXPECT().
				DeleteNexusEndpoint(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewNotFound("error deleting nexus endpoint"))

			Expect(temporalClient.DeleteNexusEndpoint(ctx, current)).
				To(MatchError(ErrNexusEndpointNotFound))
		})

		It("should wrap any other failure", func() {
			current := newNexusEndpoint(serverEndpoint(2, namespaceOne, taskQueueOne, nil))

			operatorService.EXPECT().
				DeleteNexusEndpoint(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewPermissionDenied("nope", "no reason"))

			err := temporalClient.DeleteNexusEndpoint(ctx, current)
			Expect(errors.Is(err, ErrNexusEndpointNotFound)).To(BeFalse())
			Expect(err).To(MatchError(ContainSubstring("deleting nexus endpoint PaymentsNexus")))
		})

		It("should reject a nil endpoint without calling Temporal", func() {
			Expect(temporalClient.DeleteNexusEndpoint(ctx, nil)).To(MatchError(ErrNoNexusEndpointName))
		})
	})

	Describe("TargetMatches", func() {
		DescribeTable(
			"should compare both halves of the target",
			func(namespace, taskQueue string, expected bool) {
				endpoint := newNexusEndpoint(serverEndpoint(1, namespaceOne, taskQueueOne, nil))
				Expect(endpoint.TargetMatches(namespace, taskQueue)).To(Equal(expected))
			},
			Entry("both the same", namespaceOne, taskQueueOne, true),
			Entry("a different namespace", "orders", taskQueueOne, false),
			Entry("a different task queue", namespaceOne, "orders-nexus", false),
			Entry("both different", "orders", "orders-nexus", false),
		)
	})
})
