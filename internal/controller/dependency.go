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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	temporalv1alpha1 "github.com/mrsimonemms/temporal-resource-operator/api/v1alpha1"
)

// Three resources now depend on a Connection and find each other the same way.
// The two pieces they share exactly - looking a Connection up, and turning a
// dependency event into reconcile requests - live here. The rest of each
// controller stays its own, because what each does with the answer differs.

// wrapf annotates an error with context.
func wrapf(err error, format string, args ...any) error {
	return fmt.Errorf("%s: %w", fmt.Sprintf(format, args...), err)
}

// getConnection fetches a referenced Connection without judging its readiness.
//
// A Connection that cannot be read is reported as "not ready" rather than
// "not found": it may well be fine, and the two are retried the same way, but
// only one of them means the reference is wrong.
func getConnection(
	ctx context.Context,
	reader client.Reader,
	ref string,
	k8sNamespace string,
) (*temporalv1alpha1.Connection, string, error) {
	conn := &temporalv1alpha1.Connection{}
	key := types.NamespacedName{Namespace: k8sNamespace, Name: ref}

	if err := reader.Get(ctx, key, conn); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, ReasonConnectionNotFound, wrapf(err, "connection %s not found", key)
		}

		return nil, ReasonConnectionNotReady, wrapf(err, "getting connection %s", key)
	}

	return conn, "", nil
}

// dependants finds the resources that reference a dependency through the given
// index and turns them into reconcile requests.
//
// Every kind involved is namespaced, and a dependant only ever references
// things beside it, so the list is confined to the dependency's own Kubernetes
// namespace as well as being filtered by the index. list is an empty list of
// the dependant kind, which decides what is looked for.
//
// A map function has nowhere to return an error to, and taking the controller
// down over a failed list would be worse than missing the wake-up, so a failure
// is logged and nothing is enqueued. The dependency requeue picks those up.
func dependants(
	ctx context.Context,
	reader client.Reader,
	list client.ObjectList,
	index string,
	dependency client.Object,
) []ctrl.Request {
	log := logf.FromContext(ctx)

	if err := reader.List(
		ctx, list,
		client.InNamespace(dependency.GetNamespace()),
		client.MatchingFields{index: dependency.GetName()},
	); err != nil {
		log.Error(err, "Failed to find resources depending on a change",
			"index", index, "dependency", client.ObjectKeyFromObject(dependency))

		return nil
	}

	items, err := apimeta.ExtractList(list)
	if err != nil {
		log.Error(err, "Failed to read the list of dependants", "index", index)

		return nil
	}

	requests := make([]ctrl.Request, 0, len(items))
	for _, item := range items {
		obj, ok := item.(client.Object)
		if !ok {
			continue
		}

		requests = append(requests, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(obj)})
	}

	return requests
}
