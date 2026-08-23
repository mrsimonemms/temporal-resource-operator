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

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/operatorservicemock/v1"
	"go.temporal.io/api/serviceerror"
	sdkmocks "go.temporal.io/sdk/mocks"
	"google.golang.org/grpc"
)

var _ = Describe("Search attribute operations", func() {
	const (
		namespaceName = "payments"
		attributeName = "customerid"
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

	// listReturns primes a ListSearchAttributes call.
	listReturns := func(custom, system map[string]enumspb.IndexedValueType) {
		operatorService.EXPECT().
			ListSearchAttributes(gomock.Any(), gomock.Any()).
			DoAndReturn(func(
				_ context.Context,
				req *operatorservice.ListSearchAttributesRequest,
				_ ...grpc.CallOption,
			) (*operatorservice.ListSearchAttributesResponse, error) {
				Expect(req.GetNamespace()).To(Equal(namespaceName))

				return &operatorservice.ListSearchAttributesResponse{
					CustomAttributes: custom,
					SystemAttributes: system,
				}, nil
			})
	}

	Describe("the type mapping", func() {
		DescribeTable(
			"should round-trip every type the API exposes",
			func(shorthand SearchAttributeType, expected enumspb.IndexedValueType) {
				value, err := shorthand.indexedValueType()
				Expect(err).NotTo(HaveOccurred())
				Expect(value).To(Equal(expected))

				// And the reverse, which is what a describe reports.
				Expect(searchAttributeTypeNames[expected]).To(Equal(shorthand))
			},
			Entry("Bool", SearchAttributeType("Bool"), enumspb.INDEXED_VALUE_TYPE_BOOL),
			Entry("Datetime", SearchAttributeType("Datetime"), enumspb.INDEXED_VALUE_TYPE_DATETIME),
			Entry("Double", SearchAttributeType("Double"), enumspb.INDEXED_VALUE_TYPE_DOUBLE),
			Entry("Int", SearchAttributeType("Int"), enumspb.INDEXED_VALUE_TYPE_INT),
			Entry("Keyword", SearchAttributeType("Keyword"), enumspb.INDEXED_VALUE_TYPE_KEYWORD),
			Entry("KeywordList", SearchAttributeType("KeywordList"), enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST),
			Entry("Text", SearchAttributeType("Text"), enumspb.INDEXED_VALUE_TYPE_TEXT),
		)

		DescribeTable(
			"should refuse anything else",
			func(shorthand SearchAttributeType) {
				_, err := shorthand.indexedValueType()
				Expect(err).To(MatchError(ErrUnknownSearchAttributeType))
			},
			Entry("an invented type", SearchAttributeType("Blob")),
			Entry("the unspecified type", SearchAttributeType("Unspecified")),
			Entry("an empty type", SearchAttributeType("")),
		)
	})

	Describe("DescribeSearchAttribute", func() {
		It("should return a registered custom attribute", func() {
			listReturns(map[string]enumspb.IndexedValueType{
				attributeName: enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
				"other":       enumspb.INDEXED_VALUE_TYPE_INT,
			}, nil)

			attribute, err := temporalClient.DescribeSearchAttribute(ctx, namespaceName, attributeName)
			Expect(err).NotTo(HaveOccurred())

			Expect(attribute.Name).To(Equal(attributeName))
			Expect(attribute.Type).To(Equal(SearchAttributeType("KeywordList")))
		})

		It("should report an unregistered attribute as not found", func() {
			listReturns(map[string]enumspb.IndexedValueType{
				"other": enumspb.INDEXED_VALUE_TYPE_INT,
			}, nil)

			_, err := temporalClient.DescribeSearchAttribute(ctx, namespaceName, attributeName)
			Expect(err).To(MatchError(ErrSearchAttributeNotFound))
		})

		It("should ignore system attributes", func() {
			// A system attribute is Temporal's, not the operator's: it cannot
			// be registered or removed by name, so it must not look adoptable.
			listReturns(nil, map[string]enumspb.IndexedValueType{
				attributeName: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			})

			_, err := temporalClient.DescribeSearchAttribute(ctx, namespaceName, attributeName)
			Expect(err).To(MatchError(ErrSearchAttributeNotFound))
		})

		It("should report a missing namespace with the shared sentinel", func() {
			temporalErr := serviceerror.NewNamespaceNotFound(namespaceName)
			operatorService.EXPECT().
				ListSearchAttributes(gomock.Any(), gomock.Any()).
				Return(nil, temporalErr)

			_, err := temporalClient.DescribeSearchAttribute(ctx, namespaceName, attributeName)
			Expect(err).To(MatchError(ErrNamespaceNotFound))
			Expect(err).To(MatchError(temporalErr))
			Expect(errors.Is(err, ErrSearchAttributeNotFound)).To(BeFalse(),
				"a missing namespace is not the same as a missing attribute")
		})

		It("should wrap any other failure", func() {
			operatorService.EXPECT().
				ListSearchAttributes(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewPermissionDenied("nope", "no reason"))

			_, err := temporalClient.DescribeSearchAttribute(ctx, namespaceName, attributeName)
			Expect(errors.Is(err, ErrNamespaceNotFound)).To(BeFalse())
			Expect(errors.Is(err, ErrSearchAttributeNotFound)).To(BeFalse())
			Expect(err).To(MatchError(ContainSubstring("listing search attributes on namespace payments")))
		})

		It("should reject an empty name without calling Temporal", func() {
			_, err := temporalClient.DescribeSearchAttribute(ctx, namespaceName, "")
			Expect(err).To(MatchError(ErrNoSearchAttributeName))
		})
	})

	Describe("CreateSearchAttribute", func() {
		It("should register the attribute with the requested type", func() {
			operatorService.EXPECT().
				AddSearchAttributes(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *operatorservice.AddSearchAttributesRequest,
					_ ...grpc.CallOption,
				) (*operatorservice.AddSearchAttributesResponse, error) {
					Expect(req.GetNamespace()).To(Equal(namespaceName))
					Expect(req.GetSearchAttributes()).To(Equal(map[string]enumspb.IndexedValueType{
						attributeName: enumspb.INDEXED_VALUE_TYPE_DATETIME,
					}))

					return &operatorservice.AddSearchAttributesResponse{}, nil
				})

			Expect(temporalClient.CreateSearchAttribute(ctx, namespaceName, attributeName, "Datetime")).
				To(Succeed())
		})

		It("should refuse a type Temporal does not know without calling it", func() {
			Expect(temporalClient.CreateSearchAttribute(ctx, namespaceName, attributeName, "Blob")).
				To(MatchError(ErrUnknownSearchAttributeType))
		})

		It("should report a missing namespace with the shared sentinel", func() {
			operatorService.EXPECT().
				AddSearchAttributes(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewNamespaceNotFound(namespaceName))

			Expect(temporalClient.CreateSearchAttribute(ctx, namespaceName, attributeName, "Keyword")).
				To(MatchError(ErrNamespaceNotFound))
		})

		It("should reject an empty name without calling Temporal", func() {
			Expect(temporalClient.CreateSearchAttribute(ctx, namespaceName, "", "Keyword")).
				To(MatchError(ErrNoSearchAttributeName))
		})
	})

	Describe("DeleteSearchAttribute", func() {
		It("should remove just the named attribute", func() {
			operatorService.EXPECT().
				RemoveSearchAttributes(gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					_ context.Context,
					req *operatorservice.RemoveSearchAttributesRequest,
					_ ...grpc.CallOption,
				) (*operatorservice.RemoveSearchAttributesResponse, error) {
					Expect(req.GetNamespace()).To(Equal(namespaceName))
					Expect(req.GetSearchAttributes()).To(Equal([]string{attributeName}))

					return &operatorservice.RemoveSearchAttributesResponse{}, nil
				})

			Expect(temporalClient.DeleteSearchAttribute(ctx, namespaceName, attributeName)).To(Succeed())
		})

		It("should report a missing namespace with the shared sentinel", func() {
			// This is what lets the caller tell "the namespace holding it has
			// gone" apart from "the removal failed".
			operatorService.EXPECT().
				RemoveSearchAttributes(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewNamespaceNotFound(namespaceName))

			Expect(temporalClient.DeleteSearchAttribute(ctx, namespaceName, attributeName)).
				To(MatchError(ErrNamespaceNotFound))
		})

		It("should wrap any other failure", func() {
			operatorService.EXPECT().
				RemoveSearchAttributes(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewUnavailable("service down"))

			err := temporalClient.DeleteSearchAttribute(ctx, namespaceName, attributeName)
			Expect(errors.Is(err, ErrNamespaceNotFound)).To(BeFalse())
			Expect(err).To(MatchError(ContainSubstring("removing search attribute")))
		})

		It("should reject an empty name without calling Temporal", func() {
			Expect(temporalClient.DeleteSearchAttribute(ctx, namespaceName, "")).
				To(MatchError(ErrNoSearchAttributeName))
		})
	})

	Describe("ListSearchAttributes", func() {
		It("should return the custom attributes sorted by name", func() {
			listReturns(map[string]enumspb.IndexedValueType{
				"zebra":       enumspb.INDEXED_VALUE_TYPE_TEXT,
				attributeName: enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				"alpha":       enumspb.INDEXED_VALUE_TYPE_BOOL,
			}, map[string]enumspb.IndexedValueType{
				"WorkflowId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			})

			attributes, err := temporalClient.ListSearchAttributes(ctx, namespaceName)
			Expect(err).NotTo(HaveOccurred())

			Expect(attributes).To(Equal([]SearchAttribute{
				{Name: "alpha", Type: "Bool"},
				{Name: attributeName, Type: "Keyword"},
				{Name: "zebra", Type: "Text"},
			}), "system attributes are not the operator's to manage")
		})

		It("should report a missing namespace with the shared sentinel", func() {
			operatorService.EXPECT().
				ListSearchAttributes(gomock.Any(), gomock.Any()).
				Return(nil, serviceerror.NewNamespaceNotFound(namespaceName))

			_, err := temporalClient.ListSearchAttributes(ctx, namespaceName)
			Expect(err).To(MatchError(ErrNamespaceNotFound))
		})
	})
})
