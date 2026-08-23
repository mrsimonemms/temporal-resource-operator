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
	"fmt"
	"maps"
	"slices"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
)

var (
	// ErrSearchAttributeNotFound reports that a search attribute is not
	// registered on the namespace. It is the expected outcome of describing one
	// that has yet to be created.
	ErrSearchAttributeNotFound = errors.New("temporal search attribute not found")

	// ErrNoSearchAttributeName is returned when an operation is given no search
	// attribute name.
	ErrNoSearchAttributeName = errors.New("no search attribute name given")

	// ErrUnknownSearchAttributeType is returned for a type Temporal does not
	// recognise.
	ErrUnknownSearchAttributeType = errors.New("unknown search attribute type")
)

// SearchAttributeType is a search attribute's value type, in the shorthand form
// Temporal itself uses - "Keyword", "KeywordList", "Datetime" and so on.
//
// Keeping the shorthand rather than the protobuf enum means callers never have
// to deal with Temporal's generated types, and the strings match what the
// Temporal CLI shows.
type SearchAttributeType string

// searchAttributeTypeNames maps Temporal's enum back to the shorthand, derived
// from the API's own table so that it cannot drift from the pinned version.
var searchAttributeTypeNames = func() map[enumspb.IndexedValueType]SearchAttributeType {
	names := make(map[enumspb.IndexedValueType]SearchAttributeType, len(enumspb.IndexedValueType_shorthandValue))
	for name, value := range enumspb.IndexedValueType_shorthandValue {
		names[enumspb.IndexedValueType(value)] = SearchAttributeType(name)
	}

	return names
}()

// indexedValueType converts the shorthand into the enum Temporal expects.
func (t SearchAttributeType) indexedValueType() (enumspb.IndexedValueType, error) {
	value, err := enumspb.IndexedValueTypeFromString(string(t))
	if err != nil || value == enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED {
		return enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, fmt.Errorf("%w: %q", ErrUnknownSearchAttributeType, t)
	}

	return value, nil
}

// SearchAttribute is a Temporal search attribute, reduced to what this operator
// manages.
//
// There is deliberately nothing else on it: a search attribute really is just a
// name and a type, with nowhere to record who created it.
type SearchAttribute struct {
	// Name is the search attribute's name.
	Name string

	// Type is the type of the values it holds.
	Type SearchAttributeType
}

// DescribeSearchAttribute looks up a single custom search attribute on a
// namespace.
//
// Temporal has no per-attribute lookup, so this reads the namespace's custom
// attributes and picks the one asked for. System attributes are deliberately
// not considered: they are Temporal's, not the operator's, and Temporal refuses
// to register or remove them by name anyway.
//
// A missing attribute produces an error satisfying
// errors.Is(err, ErrSearchAttributeNotFound); a missing namespace produces one
// satisfying errors.Is(err, ErrNamespaceNotFound).
func (c *Client) DescribeSearchAttribute(ctx context.Context, namespace, name string) (*SearchAttribute, error) {
	if name == "" {
		return nil, ErrNoSearchAttributeName
	}

	attributes, err := c.listSearchAttributes(ctx, namespace)
	if err != nil {
		return nil, err
	}

	value, ok := attributes[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s on namespace %s", ErrSearchAttributeNotFound, name, namespace)
	}

	attributeType, ok := searchAttributeTypeNames[value]
	if !ok {
		return nil, fmt.Errorf("%w: %s on namespace %s is %s", ErrUnknownSearchAttributeType, name, namespace, value)
	}

	return &SearchAttribute{Name: name, Type: attributeType}, nil
}

// CreateSearchAttribute registers a custom search attribute on a namespace.
//
// Temporal accepts a request naming an attribute that already exists, and where
// the type differs it keeps the original rather than reporting a problem, so
// callers must not read a successful return as proof that the attribute now has
// the requested type.
func (c *Client) CreateSearchAttribute(
	ctx context.Context,
	namespace, name string,
	attributeType SearchAttributeType,
) error {
	if name == "" {
		return ErrNoSearchAttributeName
	}

	value, err := attributeType.indexedValueType()
	if err != nil {
		return err
	}

	_, err = c.client.OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace:        namespace,
		SearchAttributes: map[string]enumspb.IndexedValueType{name: value},
	})
	if err != nil {
		return mapNamespaceError(fmt.Sprintf("adding search attribute %s to namespace %s", name, namespace), err)
	}

	return nil
}

// DeleteSearchAttribute removes a custom search attribute from a namespace.
//
// Temporal treats removing an attribute that is not there as success, so this
// is idempotent. A missing namespace produces an error satisfying
// errors.Is(err, ErrNamespaceNotFound), leaving the caller to decide what that
// means for it.
func (c *Client) DeleteSearchAttribute(ctx context.Context, namespace, name string) error {
	if name == "" {
		return ErrNoSearchAttributeName
	}

	_, err := c.client.OperatorService().RemoveSearchAttributes(ctx, &operatorservice.RemoveSearchAttributesRequest{
		Namespace:        namespace,
		SearchAttributes: []string{name},
	})
	if err != nil {
		return mapNamespaceError(fmt.Sprintf("removing search attribute %s from namespace %s", name, namespace), err)
	}

	return nil
}

// ListSearchAttributes returns every custom search attribute registered on a
// namespace, sorted by name.
func (c *Client) ListSearchAttributes(ctx context.Context, namespace string) ([]SearchAttribute, error) {
	attributes, err := c.listSearchAttributes(ctx, namespace)
	if err != nil {
		return nil, err
	}

	found := make([]SearchAttribute, 0, len(attributes))
	for _, name := range slices.Sorted(maps.Keys(attributes)) {
		attributeType, ok := searchAttributeTypeNames[attributes[name]]
		if !ok {
			return nil, fmt.Errorf("%w: %s on namespace %s is %s",
				ErrUnknownSearchAttributeType, name, namespace, attributes[name])
		}

		found = append(found, SearchAttribute{Name: name, Type: attributeType})
	}

	return found, nil
}

// listSearchAttributes fetches the namespace's custom search attributes.
func (c *Client) listSearchAttributes(
	ctx context.Context,
	namespace string,
) (map[string]enumspb.IndexedValueType, error) {
	resp, err := c.client.OperatorService().ListSearchAttributes(
		ctx,
		&operatorservice.ListSearchAttributesRequest{Namespace: namespace},
	)
	if err != nil {
		return nil, mapNamespaceError(fmt.Sprintf("listing search attributes on namespace %s", namespace), err)
	}

	return resp.GetCustomAttributes(), nil
}
