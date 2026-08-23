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
	"time"

	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/operatorservice/v1"
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

	// Data is the namespace's custom key/value metadata. It is always a copy,
	// so callers cannot reach back into anything the SDK owns.
	Data map[string]string
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
		return nil, mapNamespaceError(fmt.Sprintf("describing namespace %s", name), err)
	}

	return &Namespace{
		Name:      resp.GetNamespaceInfo().GetName(),
		Retention: resp.GetConfig().GetWorkflowExecutionRetentionTtl().AsDuration(),
		Data:      copyData(resp.GetNamespaceInfo().GetData()),
	}, nil
}

// CreateNamespace registers a new Temporal namespace with the given retention
// and custom metadata.
//
// The metadata is part of the registration request, so a namespace can never
// exist without whatever the caller wanted stamped on it - there is no window
// between the namespace appearing and its data being written.
func (c *Client) CreateNamespace(
	ctx context.Context,
	name string,
	retention time.Duration,
	data map[string]string,
) error {
	if name == "" {
		return ErrNoNamespaceName
	}

	_, err := c.client.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        name,
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		Data:                             copyData(data),
	})
	if err != nil {
		return fmt.Errorf("registering namespace %s: %w", name, err)
	}

	return nil
}

// UpdateNamespaceRetention sets the workflow execution retention on an existing
// Temporal namespace, leaving every other setting untouched.
func (c *Client) UpdateNamespaceRetention(ctx context.Context, name string, retention time.Duration) error {
	if name == "" {
		return ErrNoNamespaceName
	}

	// Only the fields set on Config are updated, so this does not disturb
	// archival, search attribute aliases or anything else on the namespace.
	_, err := c.client.WorkflowService().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: name,
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: durationpb.New(retention),
		},
	})
	if err != nil {
		return mapNamespaceError(fmt.Sprintf("updating namespace %s", name), err)
	}

	return nil
}

// DeleteNamespace removes a Temporal namespace. The Service deletes the
// namespace record synchronously and reclaims its data in the background.
//
// A namespace that is already gone produces an error satisfying
// errors.Is(err, ErrNamespaceNotFound), leaving it to the caller to decide
// whether that counts as success.
func (c *Client) DeleteNamespace(ctx context.Context, name string) error {
	if name == "" {
		return ErrNoNamespaceName
	}

	_, err := c.client.OperatorService().DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: name,
	})
	if err != nil {
		return mapNamespaceError(fmt.Sprintf("deleting namespace %s", name), err)
	}

	return nil
}

// copyData returns a copy of a namespace's custom metadata, or nil when there
// is none. Copying keeps SDK-owned and caller-owned maps from being shared.
func copyData(data map[string]string) map[string]string {
	if len(data) == 0 {
		return nil
	}

	return maps.Clone(data)
}

// mapNamespaceError turns Temporal's "namespace not found" into the package's
// own sentinel, keeping the original error in the chain so callers can still
// report what the Service said. Everything else is wrapped with context.
func mapNamespaceError(action string, err error) error {
	var notFound *serviceerror.NamespaceNotFound
	if errors.As(err, &notFound) {
		return fmt.Errorf("%w: %w", ErrNamespaceNotFound, err)
	}

	return fmt.Errorf("%s: %w", action, err)
}
