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
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	// ErrNamespaceNotFound reports that a Temporal namespace does not exist.
	// It is the expected outcome of describing a namespace that has yet to be
	// registered, so callers can tell it apart from a genuine failure with
	// errors.Is.
	ErrNamespaceNotFound = errors.New("temporal namespace not found")

	// ErrNoNamespaceName is returned when an operation is given no namespace
	// name.
	ErrNoNamespaceName = errors.New("no namespace name given")
)

// Namespace is a Temporal namespace, reduced to the settings this operator
// manages. Keeping the protobuf types inside this package means callers never
// have to deal with them.
type Namespace struct {
	// Name is the Temporal namespace name.
	Name string

	// Retention is how long closed workflow executions are kept.
	Retention time.Duration
}

// DescribeNamespace looks up a single Temporal namespace by name.
//
// When the namespace does not exist the returned error satisfies
// errors.Is(err, ErrNamespaceNotFound); the underlying Temporal error is kept
// in the chain.
func (c *Client) DescribeNamespace(ctx context.Context, name string) (*Namespace, error) {
	if name == "" {
		return nil, ErrNoNamespaceName
	}

	resp, err := c.client.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: name,
	})
	if err != nil {
		var notFound *serviceerror.NamespaceNotFound
		if errors.As(err, &notFound) {
			return nil, fmt.Errorf("%w: %w", ErrNamespaceNotFound, err)
		}

		return nil, fmt.Errorf("describing namespace %s: %w", name, err)
	}

	return &Namespace{
		Name:      resp.GetNamespaceInfo().GetName(),
		Retention: resp.GetConfig().GetWorkflowExecutionRetentionTtl().AsDuration(),
	}, nil
}

// CreateNamespace registers a new Temporal namespace with the given retention.
func (c *Client) CreateNamespace(ctx context.Context, name string, retention time.Duration) error {
	if name == "" {
		return ErrNoNamespaceName
	}

	_, err := c.client.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        name,
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
	})
	if err != nil {
		return fmt.Errorf("registering namespace %s: %w", name, err)
	}

	return nil
}
