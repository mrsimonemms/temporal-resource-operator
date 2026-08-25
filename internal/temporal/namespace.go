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

	enumspb "go.temporal.io/api/enums/v1"
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

	// ErrUnknownArchivalState is returned for an archival state Temporal does
	// not recognise.
	ErrUnknownArchivalState = errors.New("unknown archival state")
)

// ArchivalState is whether Archival is on for a namespace, in the shorthand
// form Temporal itself uses - "Enabled", "Disabled".
//
// Keeping the shorthand rather than the protobuf enum means callers never have
// to deal with Temporal's generated types, and the strings match what the
// Temporal CLI shows.
type ArchivalState string

const (
	// ArchivalStateUnspecified is Temporal's "no opinion". Registering a
	// namespace with it takes the Service's default; updating one with it
	// leaves the setting exactly as it is.
	//
	// It is the empty string rather than Temporal's "Unspecified" shorthand, so
	// that a zero-valued ArchivalConfig means "say nothing" without the caller
	// having to spell it out.
	ArchivalStateUnspecified ArchivalState = ""

	// ArchivalStateDisabled turns Archival off for the namespace.
	ArchivalStateDisabled ArchivalState = "Disabled"

	// ArchivalStateEnabled turns Archival on for the namespace.
	ArchivalStateEnabled ArchivalState = "Enabled"
)

// archivalStateNames maps Temporal's enum back to the shorthand, derived from
// the API's own table so that it cannot drift from the pinned version.
//
// Unspecified is the exception, for the reason given on
// ArchivalStateUnspecified.
var archivalStateNames = func() map[enumspb.ArchivalState]ArchivalState {
	names := make(map[enumspb.ArchivalState]ArchivalState, len(enumspb.ArchivalState_shorthandValue))
	for name, value := range enumspb.ArchivalState_shorthandValue {
		names[enumspb.ArchivalState(value)] = ArchivalState(name)
	}

	names[enumspb.ARCHIVAL_STATE_UNSPECIFIED] = ArchivalStateUnspecified

	return names
}()

// archivalState converts the shorthand into the enum Temporal expects.
func (s ArchivalState) archivalState() (enumspb.ArchivalState, error) {
	if s == ArchivalStateUnspecified {
		return enumspb.ARCHIVAL_STATE_UNSPECIFIED, nil
	}

	value, err := enumspb.ArchivalStateFromString(string(s))
	if err != nil {
		return enumspb.ARCHIVAL_STATE_UNSPECIFIED, fmt.Errorf("%w: %q", ErrUnknownArchivalState, s)
	}

	return value, nil
}

// archivalStateOf turns Temporal's enum into the shorthand, naming the raw
// value for anything the pinned API does not know about.
func archivalStateOf(state enumspb.ArchivalState) ArchivalState {
	if name, ok := archivalStateNames[state]; ok {
		return name
	}

	return ArchivalState(state.String())
}

// ArchivalConfig is one kind of Archival on a namespace.
//
// The zero value says nothing at all, which is what makes it safe to send: an
// unspecified state leaves the Service's own setting alone.
type ArchivalConfig struct {
	// State is whether Archival is on, off, or unspecified.
	State ArchivalState

	// URI is the Archival destination. It is empty when the namespace has none
	// and, on a request, when the caller has no opinion about it.
	//
	// Temporal will not change a URI it already holds, and supplies its
	// configured default when Archival is enabled without one.
	URI string
}

// archivalConfigOf reads one kind of Archival off a namespace's config.
func archivalConfigOf(state enumspb.ArchivalState, uri string) ArchivalConfig {
	return ArchivalConfig{State: archivalStateOf(state), URI: uri}
}

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

	// HistoryArchival is Archival of closed workflows' event histories.
	HistoryArchival ArchivalConfig

	// VisibilityArchival is Archival of closed workflows' visibility records.
	VisibilityArchival ArchivalConfig
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

	config := resp.GetConfig()

	return &Namespace{
		Name:      resp.GetNamespaceInfo().GetName(),
		Retention: config.GetWorkflowExecutionRetentionTtl().AsDuration(),
		Data:      copyData(resp.GetNamespaceInfo().GetData()),
		HistoryArchival: archivalConfigOf(
			config.GetHistoryArchivalState(), config.GetHistoryArchivalUri(),
		),
		VisibilityArchival: archivalConfigOf(
			config.GetVisibilityArchivalState(), config.GetVisibilityArchivalUri(),
		),
	}, nil
}

// CreateNamespace registers a new Temporal namespace with the retention,
// custom metadata and Archival configuration the given Namespace describes.
//
// Everything is part of the registration request, so a namespace can never
// exist without whatever the caller wanted stamped on it - there is no window
// between the namespace appearing and its data or Archival being written.
//
// An unspecified Archival state takes the Service's default for new namespaces,
// which is what a caller with no opinion should send.
func (c *Client) CreateNamespace(ctx context.Context, ns *Namespace) error {
	if ns == nil || ns.Name == "" {
		return ErrNoNamespaceName
	}

	history, err := ns.HistoryArchival.State.archivalState()
	if err != nil {
		return fmt.Errorf("registering namespace %s: history archival: %w", ns.Name, err)
	}

	visibility, err := ns.VisibilityArchival.State.archivalState()
	if err != nil {
		return fmt.Errorf("registering namespace %s: visibility archival: %w", ns.Name, err)
	}

	_, err = c.client.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        ns.Name,
		WorkflowExecutionRetentionPeriod: durationpb.New(ns.Retention),
		Data:                             copyData(ns.Data),
		HistoryArchivalState:             history,
		HistoryArchivalUri:               ns.HistoryArchival.URI,
		VisibilityArchivalState:          visibility,
		VisibilityArchivalUri:            ns.VisibilityArchival.URI,
	})
	if err != nil {
		return fmt.Errorf("registering namespace %s: %w", ns.Name, err)
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

// UpdateNamespaceArchival sets the Archival configuration on an existing
// Temporal namespace, leaving every other setting untouched.
//
// Each kind is sent only when it is given: a nil ArchivalConfig arrives at the
// Service as an unspecified state with no URI, which its own state machine
// treats as "no change requested". Passing neither does nothing at all rather
// than sending an update that could not change anything.
//
// Two of Temporal's rules are worth knowing, because they surface as errors
// here rather than being worked around:
//
//   - A namespace's Archival URI is immutable once the Service holds one.
//     Sending a different one is refused with "Cannot update existing archival
//     URI"; sending none leaves the existing URI in place, disabled Archival
//     included.
//   - A Service not configured for Archival ignores the request instead of
//     refusing it, so a successful call is not proof the setting took. Callers
//     that care must read the namespace back.
func (c *Client) UpdateNamespaceArchival(
	ctx context.Context,
	name string,
	history *ArchivalConfig,
	visibility *ArchivalConfig,
) error {
	if name == "" {
		return ErrNoNamespaceName
	}

	if history == nil && visibility == nil {
		return nil
	}

	config := &namespacepb.NamespaceConfig{}

	if history != nil {
		state, err := history.State.archivalState()
		if err != nil {
			return fmt.Errorf("updating namespace %s: history archival: %w", name, err)
		}

		config.HistoryArchivalState = state
		config.HistoryArchivalUri = history.URI
	}

	if visibility != nil {
		state, err := visibility.State.archivalState()
		if err != nil {
			return fmt.Errorf("updating namespace %s: visibility archival: %w", name, err)
		}

		config.VisibilityArchivalState = state
		config.VisibilityArchivalUri = visibility.URI
	}

	// The retention is deliberately left off Config, so this does not disturb
	// it - or the search attribute aliases, or anything else. UpdateInfo stays
	// nil for the same reason it does on the retention update: that is where
	// Data lives, and sending it would risk the ownership marker.
	_, err := c.client.WorkflowService().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: name,
		Config:    config,
	})
	if err != nil {
		return mapNamespaceError(fmt.Sprintf("updating namespace %s archival", name), err)
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
