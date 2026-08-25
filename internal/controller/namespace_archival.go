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

package controller

import (
	"context"
	"fmt"
	"strings"

	temporalv1beta1 "github.com/mrsimonemms/temporal-resource-operator/api/v1beta1"
	"github.com/mrsimonemms/temporal-resource-operator/internal/temporal"
)

// Deciding what to do about Archival is the fiddliest part of reconciling a
// Namespace, because Temporal's own rules are asymmetric: a state can be
// changed as often as you like, a URI can be set once and never again, and a
// Service that is not configured for Archival ignores the request rather than
// refusing it. It lives here so the reconcile loop reads as the sequence of
// steps it is.

// archivalKind names one of the two kinds of Archival. It exists to keep the
// two halves of every message and error apart without repeating the literals.
type archivalKind string

const (
	archivalHistory    archivalKind = "history"
	archivalVisibility archivalKind = "visibility"
)

// namespaceArchival is what a reconcile decided to do about Archival: the
// change to send for each kind, and nil for a kind that needs nothing.
type namespaceArchival struct {
	history    *temporal.ArchivalConfig
	visibility *temporal.ArchivalConfig
}

// wanted reports whether there is anything to send.
func (a namespaceArchival) wanted() bool {
	return a.history != nil || a.visibility != nil
}

// summary describes the change for the Ready condition, naming each kind that
// is being set and what it is being set to.
func (a namespaceArchival) summary() string {
	parts := make([]string, 0, 2)

	for _, change := range []struct {
		kind   archivalKind
		config *temporal.ArchivalConfig
	}{
		{archivalHistory, a.history},
		{archivalVisibility, a.visibility},
	} {
		if change.config == nil {
			continue
		}

		part := fmt.Sprintf("%s archival to %s", change.kind, strings.ToLower(string(change.config.State)))
		if change.config.URI != "" {
			part = fmt.Sprintf("%s at %s", part, change.config.URI)
		}

		parts = append(parts, part)
	}

	return strings.Join(parts, " and ")
}

// desiredArchival turns one Archival block from the spec into the form the
// Temporal client wrapper takes. An absent block is not managed, so it produces
// nil rather than a state to send.
func desiredArchival(spec *temporalv1beta1.ArchivalConfig) *temporal.ArchivalConfig {
	if spec == nil {
		return nil
	}

	state := temporal.ArchivalStateDisabled
	if spec.Enabled {
		state = temporal.ArchivalStateEnabled
	}

	return &temporal.ArchivalConfig{State: state, URI: spec.URI}
}

// archivalChange works out what to send for one kind of Archival, given what
// the namespace already holds.
//
// It returns nil when there is nothing to do - either the spec does not manage
// this kind, or the Service already agrees with it - and an error when the spec
// asks for something Temporal will not do.
//
// The comparison is deliberately one-sided about the URI. An absent URI in the
// spec means "no opinion", not "no URI": the Service supplies its configured
// default when Archival is enabled without one, and will not clear a URI it
// already holds, so asking for nothing can never be drift. A URI that is
// present and different from the one Temporal holds is refused here rather than
// sent, because the Service refuses it too and a retry would only ask again.
func archivalChange(
	kind archivalKind,
	name string,
	desired *temporal.ArchivalConfig,
	actual temporal.ArchivalConfig,
) (*temporal.ArchivalConfig, error) {
	if desired == nil {
		return nil, nil
	}

	if desired.URI != "" && actual.URI != "" && desired.URI != actual.URI {
		return nil, fmt.Errorf(
			"temporal namespace %q already archives %s to %s: an archival URI cannot be changed once Temporal "+
				"holds one, so %s cannot be applied; remove the uri to keep the existing destination, or "+
				"replace the namespace",
			name, kind, actual.URI, desired.URI,
		)
	}

	if desired.State == actual.State && (desired.URI == "" || desired.URI == actual.URI) {
		return nil, nil
	}

	return desired, nil
}

// archivalUpdate works out the whole Archival change a namespace needs, for
// both kinds at once.
//
// The two are independent: one may need setting while the other is already
// right or not managed at all.
func archivalUpdate(
	ns *temporalv1beta1.Namespace,
	actual *temporal.Namespace,
) (namespaceArchival, error) {
	name := ns.TemporalName()

	history, err := archivalChange(
		archivalHistory, name, desiredArchival(ns.Spec.HistoryArchival()), actual.HistoryArchival,
	)
	if err != nil {
		return namespaceArchival{}, err
	}

	visibility, err := archivalChange(
		archivalVisibility, name, desiredArchival(ns.Spec.VisibilityArchival()), actual.VisibilityArchival,
	)
	if err != nil {
		return namespaceArchival{}, err
	}

	return namespaceArchival{history: history, visibility: visibility}, nil
}

// confirmArchival reads the namespace back and checks that the Archival the
// spec asked for is what the Service now holds.
//
// This is not belt-and-braces. A Temporal Service that is not configured for
// Archival - which includes Temporal Cloud, where Export takes Archival's place
// - drops the archival part of a register or update request and answers
// successfully, so a successful call proves nothing. Without this check the
// operator would report "Updated" on every resync, for ever, while the
// namespace never changed.
//
// It runs only after a write that claimed to change something, so the ordinary
// in-step reconcile costs no extra call.
func confirmArchival(
	ctx context.Context,
	ns *temporalv1beta1.Namespace,
	temporalClient TemporalNamespaceClient,
) (string, error) {
	name := ns.TemporalName()

	actual, err := temporalClient.DescribeNamespace(ctx, name)
	if err != nil {
		return ReasonDescribeFailed, wrapf(err, "confirming archival on temporal namespace %s", name)
	}

	outstanding, err := archivalUpdate(ns, actual)
	if err != nil {
		// The namespace has a URI that cannot become the one asked for. Only
		// reachable if the Service assigned a default other than the requested
		// URI, which is worth reporting as itself rather than as a failed
		// confirmation.
		return ReasonArchivalURIImmutable, err
	}

	if !outstanding.wanted() {
		return "", nil
	}

	return ReasonArchivalUnavailable, fmt.Errorf(
		"temporal namespace %q did not take the requested archival configuration (%s): the Temporal Service "+
			"accepted the request and ignored it, which is what a Service not configured for archival does; "+
			"enable archival on the Service, or remove spec.archival",
		name, outstanding.summary(),
	)
}
