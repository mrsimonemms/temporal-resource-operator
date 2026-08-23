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

	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrNexusEndpointNotFound reports that no Nexus endpoint of that name is
	// registered on the Service. It is the expected outcome of describing one
	// that has yet to be created.
	ErrNexusEndpointNotFound = errors.New("temporal nexus endpoint not found")

	// ErrNexusEndpointExists reports that the Service already has an endpoint
	// of that name. Endpoint names are unique Service-wide.
	ErrNexusEndpointExists = errors.New("temporal nexus endpoint already exists")

	// ErrNexusEndpointChanged reports that the endpoint was modified between
	// being read and being written, so the change was refused. Re-read it and
	// try again.
	ErrNexusEndpointChanged = errors.New("temporal nexus endpoint changed since it was read")

	// ErrNoNexusEndpointName is returned when an operation is given no endpoint
	// name.
	ErrNoNexusEndpointName = errors.New("no nexus endpoint name given")
)

// NexusEndpoint is a Temporal Nexus endpoint, reduced to what this operator
// manages.
//
// The whole server-side spec is carried along unread so that an update can put
// back everything the operator does not manage - a description set by hand, for
// instance. Temporal's update replaces the spec wholesale, so anything not sent
// back is lost.
type NexusEndpoint struct {
	// ID is the server-assigned endpoint ID. Updates and deletes go by ID, not
	// by name.
	ID string

	// Version is the endpoint's data version, which an update must match.
	Version int64

	// Name is the endpoint's Service-wide unique name.
	Name string

	// TargetNamespace is the Temporal namespace requests are routed to.
	TargetNamespace string

	// TaskQueue is the Nexus task queue requests are routed to.
	TaskQueue string

	// spec is the server's own spec, kept so that an update can preserve the
	// parts of it the operator has no opinion about.
	spec *nexuspb.EndpointSpec
}

// TargetMatches reports whether the endpoint already routes where the caller
// wants it to.
func (e *NexusEndpoint) TargetMatches(namespace, taskQueue string) bool {
	return e.TargetNamespace == namespace && e.TaskQueue == taskQueue
}

// DescribeNexusEndpoint looks up a Nexus endpoint by name.
//
// Temporal identifies endpoints by a server-assigned ID, but names are unique
// Service-wide and the list API filters by one, returning at most a single
// endpoint. That is what makes a name a usable handle from the outside.
//
// A missing endpoint produces an error satisfying
// errors.Is(err, ErrNexusEndpointNotFound).
func (c *Client) DescribeNexusEndpoint(ctx context.Context, name string) (*NexusEndpoint, error) {
	if name == "" {
		return nil, ErrNoNexusEndpointName
	}

	resp, err := c.client.OperatorService().ListNexusEndpoints(ctx, &operatorservice.ListNexusEndpointsRequest{
		Name:     name,
		PageSize: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("looking up nexus endpoint %s: %w", name, err)
	}

	endpoints := resp.GetEndpoints()
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNexusEndpointNotFound, name)
	}

	return newNexusEndpoint(endpoints[0]), nil
}

// CreateNexusEndpoint registers a Nexus endpoint routing to a namespace and
// task queue, returning it as the Service recorded it.
//
// Temporal refuses to create an endpoint whose target namespace does not exist,
// and refuses a name that is already taken - the latter surfaces as an error
// satisfying errors.Is(err, ErrNexusEndpointExists).
func (c *Client) CreateNexusEndpoint(
	ctx context.Context,
	name, namespace, taskQueue string,
) (*NexusEndpoint, error) {
	if name == "" {
		return nil, ErrNoNexusEndpointName
	}

	resp, err := c.client.OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: workerEndpointSpec(name, namespace, taskQueue),
	})
	if err != nil {
		var exists *serviceerror.AlreadyExists
		if errors.As(err, &exists) {
			return nil, fmt.Errorf("%w: %s: %w", ErrNexusEndpointExists, name, err)
		}

		return nil, fmt.Errorf("creating nexus endpoint %s: %w", name, err)
	}

	return newNexusEndpoint(resp.GetEndpoint()), nil
}

// UpdateNexusEndpointTarget points an existing endpoint at a different
// namespace and task queue, leaving the rest of its spec as it was.
//
// The endpoint must be one just read from the Service: the update carries its
// version, and Temporal refuses it if anything has changed in the meantime.
// That refusal surfaces as an error satisfying
// errors.Is(err, ErrNexusEndpointChanged), which callers should answer by
// reading the endpoint again rather than by guessing a newer version.
func (c *Client) UpdateNexusEndpointTarget(
	ctx context.Context,
	endpoint *NexusEndpoint,
	namespace, taskQueue string,
) (*NexusEndpoint, error) {
	if endpoint == nil {
		return nil, ErrNoNexusEndpointName
	}

	// Start from what the Service holds so that anything the operator does not
	// manage - a description, most likely - survives the update.
	spec, _ := proto.Clone(endpoint.spec).(*nexuspb.EndpointSpec)
	if spec == nil {
		spec = &nexuspb.EndpointSpec{Name: endpoint.Name}
	}

	spec.Target = workerTarget(namespace, taskQueue)

	resp, err := c.client.OperatorService().UpdateNexusEndpoint(ctx, &operatorservice.UpdateNexusEndpointRequest{
		Id:      endpoint.ID,
		Version: endpoint.Version,
		Spec:    spec,
	})
	if err != nil {
		var stale *serviceerror.FailedPrecondition
		if errors.As(err, &stale) {
			return nil, fmt.Errorf("%w: %s: %w", ErrNexusEndpointChanged, endpoint.Name, err)
		}

		return nil, fmt.Errorf("updating nexus endpoint %s: %w", endpoint.Name, err)
	}

	return newNexusEndpoint(resp.GetEndpoint()), nil
}

// DeleteNexusEndpoint removes a Nexus endpoint.
//
// An endpoint that has already gone produces an error satisfying
// errors.Is(err, ErrNexusEndpointNotFound), leaving the caller to decide
// whether that counts as success.
func (c *Client) DeleteNexusEndpoint(ctx context.Context, endpoint *NexusEndpoint) error {
	if endpoint == nil {
		return ErrNoNexusEndpointName
	}

	_, err := c.client.OperatorService().DeleteNexusEndpoint(ctx, &operatorservice.DeleteNexusEndpointRequest{
		Id:      endpoint.ID,
		Version: endpoint.Version,
	})
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return fmt.Errorf("%w: %s: %w", ErrNexusEndpointNotFound, endpoint.Name, err)
		}

		return fmt.Errorf("deleting nexus endpoint %s: %w", endpoint.Name, err)
	}

	return nil
}

// newNexusEndpoint reduces the server's endpoint to the operator's view of it.
func newNexusEndpoint(endpoint *nexuspb.Endpoint) *NexusEndpoint {
	spec := endpoint.GetSpec()
	worker := spec.GetTarget().GetWorker()

	return &NexusEndpoint{
		ID:              endpoint.GetId(),
		Version:         endpoint.GetVersion(),
		Name:            spec.GetName(),
		TargetNamespace: worker.GetNamespace(),
		TaskQueue:       worker.GetTaskQueue(),
		spec:            spec,
	}
}

// workerEndpointSpec builds the spec for an endpoint routing to a worker in a
// namespace.
func workerEndpointSpec(name, namespace, taskQueue string) *nexuspb.EndpointSpec {
	return &nexuspb.EndpointSpec{
		Name:   name,
		Target: workerTarget(namespace, taskQueue),
	}
}

// workerTarget builds a worker target. The operator only manages worker
// targets: an external target is a URL to somewhere outside Temporal, which is
// not something this resource models.
func workerTarget(namespace, taskQueue string) *nexuspb.EndpointTarget {
	return &nexuspb.EndpointTarget{
		Variant: &nexuspb.EndpointTarget_Worker_{
			Worker: &nexuspb.EndpointTarget_Worker{
				Namespace: namespace,
				TaskQueue: taskQueue,
			},
		},
	}
}
