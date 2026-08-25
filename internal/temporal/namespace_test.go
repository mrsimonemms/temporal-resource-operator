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

	enumspb "go.temporal.io/api/enums/v1"
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

		It("should return the namespace's archival configuration", func() {
			workflowService.EXPECT().
				DescribeNamespace(gomock.Any(), gomock.Any()).
				Return(&workflowservice.DescribeNamespaceResponse{
					NamespaceInfo: &namespacepb.NamespaceInfo{Name: namespaceName},
					Config: &namespacepb.NamespaceConfig{
						HistoryArchivalState:    enumspb.ARCHIVAL_STATE_ENABLED,
						HistoryArchivalUri:      "s3://bucket/history",
						VisibilityArchivalState: enumspb.ARCHIVAL_STATE_DISABLED,
						VisibilityArchivalUri:   "s3://bucket/visibility",
					},
				}, nil)

			ns, err := temporalClient.DescribeNamespace(ctx, namespaceName)
			Expect(err).NotTo(HaveOccurred())

			Expect(ns.HistoryArchival).To(Equal(ArchivalConfig{
				State: ArchivalStateEnabled,
				URI:   "s3://bucket/history",
			}))
			Expect(ns.VisibilityArchival).To(Equal(ArchivalConfig{
				State: ArchivalStateDisabled,
				URI:   "s3://bucket/visibility",
			}))
		})

		It("should report an unspecified archival state as no opinion at all", func() {
			// A namespace registered on a Service with no archival configuration
			// comes back with the state unset, and the operator's zero value has
			// to mean the same thing.
			workflowService.EXPECT().
				DescribeNamespace(gomock.Any(), gomock.Any()).
				Return(&workflowservice.DescribeNamespaceResponse{
					NamespaceInfo: &namespacepb.NamespaceInfo{Name: namespaceName},
					Config:        &namespacepb.NamespaceConfig{},
				}, nil)

			ns, err := temporalClient.DescribeNamespace(ctx, namespaceName)
			Expect(err).NotTo(HaveOccurred())

			Expect(ns.HistoryArchival).To(Equal(ArchivalConfig{}))
			Expect(ns.HistoryArchival.State).To(Equal(ArchivalStateUnspecified))
			Expect(ns.VisibilityArchival).To(Equal(ArchivalConfig{}))
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

					// Nothing was said about archival, so the Service is left to
					// apply whatever its own defaults are for a new namespace.
					Expect(req.GetHistoryArchivalState()).To(Equal(enumspb.ARCHIVAL_STATE_UNSPECIFIED))
					Expect(req.GetHistoryArchivalUri()).To(BeEmpty())
					Expect(req.GetVisibilityArchivalState()).To(Equal(enumspb.ARCHIVAL_STATE_UNSPECIFIED))
					Expect(req.GetVisibilityArchivalUri()).To(BeEmpty())

					return &workflowservice.RegisterNamespaceResponse{}, nil
				})

			Expect(temporalClient.CreateNamespace(ctx, &Namespace{
				Name:      namespaceName,
				Retention: 72 * time.Hour,
			})).To(Succeed())
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

			Expect(temporalClient.CreateNamespace(ctx, &Namespace{
				Name:      namespaceName,
				Retention: 72 * time.Hour,
				Data:      caller,
			})).To(Succeed())
			Expect(caller).To(HaveKeyWithValue("temporal.simonemms.com/owner-uid", "uid-1"))
		})

		It("should register archival as part of the same request", func() {
			workflowService.EXPECT().
				RegisterNamespace(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *workflowservice.RegisterNamespaceRequest,
					_ ...grpc.CallOption,
				) (*workflowservice.RegisterNamespaceResponse, error) {
					// Archival is part of registration, so a namespace the
					// operator creates is never briefly unarchived.
					Expect(req.GetHistoryArchivalState()).To(Equal(enumspb.ARCHIVAL_STATE_ENABLED))
					Expect(req.GetHistoryArchivalUri()).To(Equal("s3://bucket/history"))
					Expect(req.GetVisibilityArchivalState()).To(Equal(enumspb.ARCHIVAL_STATE_DISABLED))
					Expect(req.GetVisibilityArchivalUri()).To(BeEmpty())

					return &workflowservice.RegisterNamespaceResponse{}, nil
				})

			Expect(temporalClient.CreateNamespace(ctx, &Namespace{
				Name:               namespaceName,
				Retention:          72 * time.Hour,
				HistoryArchival:    ArchivalConfig{State: ArchivalStateEnabled, URI: "s3://bucket/history"},
				VisibilityArchival: ArchivalConfig{State: ArchivalStateDisabled},
			})).To(Succeed())
		})

		It("should enable archival without a URI, leaving the Service to supply one", func() {
			workflowService.EXPECT().
				RegisterNamespace(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *workflowservice.RegisterNamespaceRequest,
					_ ...grpc.CallOption,
				) (*workflowservice.RegisterNamespaceResponse, error) {
					Expect(req.GetHistoryArchivalState()).To(Equal(enumspb.ARCHIVAL_STATE_ENABLED))
					Expect(req.GetHistoryArchivalUri()).To(BeEmpty())

					return &workflowservice.RegisterNamespaceResponse{}, nil
				})

			Expect(temporalClient.CreateNamespace(ctx, &Namespace{
				Name:            namespaceName,
				Retention:       72 * time.Hour,
				HistoryArchival: ArchivalConfig{State: ArchivalStateEnabled},
			})).To(Succeed())
		})

		It("should refuse an archival state Temporal does not know", func() {
			// No mock expectation: this must be caught before the network.
			err := temporalClient.CreateNamespace(ctx, &Namespace{
				Name:            namespaceName,
				Retention:       72 * time.Hour,
				HistoryArchival: ArchivalConfig{State: "Sometimes"},
			})
			Expect(err).To(MatchError(ErrUnknownArchivalState))
			Expect(err).To(MatchError(ContainSubstring("history archival")))

			err = temporalClient.CreateNamespace(ctx, &Namespace{
				Name:               namespaceName,
				Retention:          72 * time.Hour,
				VisibilityArchival: ArchivalConfig{State: "Sometimes"},
			})
			Expect(err).To(MatchError(ErrUnknownArchivalState))
			Expect(err).To(MatchError(ContainSubstring("visibility archival")))
		})

		It("should wrap a registration failure", func() {
			workflowService.EXPECT().
				RegisterNamespace(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewNamespaceAlreadyExists("already there"))

			err := temporalClient.CreateNamespace(ctx, &Namespace{
				Name:      namespaceName,
				Retention: 72 * time.Hour,
			})
			Expect(err).To(MatchError(ContainSubstring("registering namespace payments")))
			Expect(err).To(MatchError(ContainSubstring("already there")))
		})

		It("should reject a missing name or namespace without calling Temporal", func() {
			Expect(temporalClient.CreateNamespace(ctx, &Namespace{Retention: 72 * time.Hour})).
				To(MatchError(ErrNoNamespaceName))
			Expect(temporalClient.CreateNamespace(ctx, nil)).
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

					// Both kinds of archival stay unspecified with no URI, which
					// is what Temporal's state machine reads as "no change
					// requested". Correcting a retention must never disturb an
					// archival setting the operator was not asked about.
					Expect(req.GetConfig().GetHistoryArchivalState()).
						To(Equal(enumspb.ARCHIVAL_STATE_UNSPECIFIED))
					Expect(req.GetConfig().GetHistoryArchivalUri()).To(BeEmpty())
					Expect(req.GetConfig().GetVisibilityArchivalState()).
						To(Equal(enumspb.ARCHIVAL_STATE_UNSPECIFIED))
					Expect(req.GetConfig().GetVisibilityArchivalUri()).To(BeEmpty())

					// Nor anything else the Service merges rather than replaces.
					Expect(req.GetConfig().GetBadBinaries()).To(BeNil())
					Expect(req.GetConfig().GetCustomSearchAttributeAliases()).To(BeEmpty())

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

	Describe("UpdateNamespaceArchival", func() {
		It("should set both kinds of archival and touch nothing else", func() {
			workflowService.EXPECT().
				UpdateNamespace(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *workflowservice.UpdateNamespaceRequest,
					_ ...grpc.CallOption,
				) (*workflowservice.UpdateNamespaceResponse, error) {
					Expect(req.GetNamespace()).To(Equal(namespaceName))
					Expect(req.GetConfig().GetHistoryArchivalState()).To(Equal(enumspb.ARCHIVAL_STATE_ENABLED))
					Expect(req.GetConfig().GetHistoryArchivalUri()).To(Equal("s3://bucket/history"))
					Expect(req.GetConfig().GetVisibilityArchivalState()).
						To(Equal(enumspb.ARCHIVAL_STATE_DISABLED))

					// The retention is deliberately absent, so the Service does
					// not touch it. UpdateInfo stays nil for the same reason it
					// does on the retention update: that is where Data lives.
					//
					// Everything else on Config is left unset too. The Service
					// merges rather than replaces each of these, so an unset
					// field is genuinely "leave it alone" - which is what makes
					// correcting archival safe for retention, the ownership
					// marker and the search attribute aliases alike.
					Expect(req.GetConfig().GetWorkflowExecutionRetentionTtl()).To(BeNil())
					Expect(req.GetUpdateInfo()).To(BeNil())
					Expect(req.GetUpdateInfo().GetData()).To(BeEmpty())
					Expect(req.GetReplicationConfig()).To(BeNil())
					Expect(req.GetConfig().GetBadBinaries()).To(BeNil())
					Expect(req.GetConfig().GetCustomSearchAttributeAliases()).To(BeEmpty())
					Expect(req.GetPromoteNamespace()).To(BeFalse())

					return &workflowservice.UpdateNamespaceResponse{}, nil
				})

			Expect(temporalClient.UpdateNamespaceArchival(
				ctx, namespaceName,
				&ArchivalConfig{State: ArchivalStateEnabled, URI: "s3://bucket/history"},
				&ArchivalConfig{State: ArchivalStateDisabled},
			)).To(Succeed())
		})

		It("should leave the other kind unspecified when only one is given", func() {
			// An unspecified state with no URI is how Temporal's own state
			// machine is told "no change requested", so the kind that was not
			// asked about is genuinely left alone.
			workflowService.EXPECT().
				UpdateNamespace(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *workflowservice.UpdateNamespaceRequest,
					_ ...grpc.CallOption,
				) (*workflowservice.UpdateNamespaceResponse, error) {
					Expect(req.GetConfig().GetVisibilityArchivalState()).
						To(Equal(enumspb.ARCHIVAL_STATE_ENABLED))
					Expect(req.GetConfig().GetHistoryArchivalState()).
						To(Equal(enumspb.ARCHIVAL_STATE_UNSPECIFIED))
					Expect(req.GetConfig().GetHistoryArchivalUri()).To(BeEmpty())

					return &workflowservice.UpdateNamespaceResponse{}, nil
				})

			Expect(temporalClient.UpdateNamespaceArchival(
				ctx, namespaceName, nil, &ArchivalConfig{State: ArchivalStateEnabled},
			)).To(Succeed())
		})

		It("should do nothing at all when neither kind is given", func() {
			// No mock expectation: an update that could not change anything is
			// not worth a round trip.
			Expect(temporalClient.UpdateNamespaceArchival(ctx, namespaceName, nil, nil)).To(Succeed())
		})

		It("should refuse an archival state Temporal does not know", func() {
			err := temporalClient.UpdateNamespaceArchival(
				ctx, namespaceName, &ArchivalConfig{State: "Sometimes"}, nil,
			)
			Expect(err).To(MatchError(ErrUnknownArchivalState))
			Expect(err).To(MatchError(ContainSubstring("history archival")))

			err = temporalClient.UpdateNamespaceArchival(
				ctx, namespaceName, nil, &ArchivalConfig{State: "Sometimes"},
			)
			Expect(err).To(MatchError(ErrUnknownArchivalState))
			Expect(err).To(MatchError(ContainSubstring("visibility archival")))
		})

		It("should report a missing namespace as ErrNamespaceNotFound", func() {
			workflowService.EXPECT().
				UpdateNamespace(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewNamespaceNotFound(namespaceName))

			Expect(temporalClient.UpdateNamespaceArchival(
				ctx, namespaceName, &ArchivalConfig{State: ArchivalStateEnabled}, nil,
			)).To(MatchError(ErrNamespaceNotFound))
		})

		It("should wrap the Service refusing to change an existing URI", func() {
			// Temporal's own words, and the reason the operator checks the URI
			// before sending one.
			workflowService.EXPECT().
				UpdateNamespace(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewInvalidArgument("Cannot update existing archival URI"))

			err := temporalClient.UpdateNamespaceArchival(
				ctx, namespaceName,
				&ArchivalConfig{State: ArchivalStateEnabled, URI: "s3://elsewhere"}, nil,
			)
			Expect(errors.Is(err, ErrNamespaceNotFound)).To(BeFalse())
			Expect(err).To(MatchError(ContainSubstring("updating namespace payments archival")))
			Expect(err).To(MatchError(ContainSubstring("Cannot update existing archival URI")))
		})

		It("should reject an empty name without calling Temporal", func() {
			Expect(temporalClient.UpdateNamespaceArchival(
				ctx, "", &ArchivalConfig{State: ArchivalStateEnabled}, nil,
			)).To(MatchError(ErrNoNamespaceName))
		})
	})

	Describe("archival states", func() {
		It("should map every state the pinned Temporal API knows", func() {
			// Derived from Temporal's own table, so a new state in a future
			// version cannot go unnoticed.
			for name, value := range enumspb.ArchivalState_shorthandValue {
				state := enumspb.ArchivalState(value)

				expected := ArchivalState(name)
				if state == enumspb.ARCHIVAL_STATE_UNSPECIFIED {
					// The operator uses the empty string for this one, so that a
					// zero-valued ArchivalConfig means "no opinion".
					expected = ArchivalStateUnspecified
				}

				Expect(archivalStateOf(state)).To(Equal(expected), name)

				got, err := expected.archivalState()
				Expect(err).NotTo(HaveOccurred(), name)
				Expect(got).To(Equal(state), name)
			}
		})

		It("should accept Temporal's own Unspecified shorthand", func() {
			got, err := ArchivalState("Unspecified").archivalState()
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(enumspb.ARCHIVAL_STATE_UNSPECIFIED))
		})

		It("should name an unrecognised state rather than guessing", func() {
			_, err := ArchivalState("Sometimes").archivalState()
			Expect(err).To(MatchError(ErrUnknownArchivalState))
			Expect(err).To(MatchError(ContainSubstring(`"Sometimes"`)))
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
