#!/usr/bin/env bash
#
# Copyright 2026 Simon Emms <simon@simonemms.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Installs the Helm chart into a Kind cluster using a locally built controller
# image, proves the operator comes up, then removes it again. Deliberately does
# not touch Temporal: this tests the chart, not reconciliation. The e2e suite
# covers the operator's behaviour.

set -euo pipefail

HELM="${HELM:-helm}"
KUBECTL="${KUBECTL:-kubectl}"
KIND="${KIND:-kind}"
KIND_CLUSTER="${KIND_CLUSTER:-temporal-resource-operator-test-e2e}"
CONTAINER_TOOL="${CONTAINER_TOOL:-docker}"
CHART_DIR="${CHART_DIR:-charts/temporal-resource-operator}"
CHART_RELEASE="${CHART_RELEASE:-temporal-resource-operator}"
CHART_NAMESPACE="${CHART_NAMESPACE:-temporal-resource-operator-system}"

# A local-only tag. The chart's default image tag is the chart appVersion,
# which for a development chart points at an image that was never published, so
# the smoke test always overrides it.
SMOKE_IMAGE_REPO="${SMOKE_IMAGE_REPO:-example.com/temporal-resource-operator}"
SMOKE_IMAGE_TAG="${SMOKE_IMAGE_TAG:-chart-smoke}"
SMOKE_IMAGE="${SMOKE_IMAGE_REPO}:${SMOKE_IMAGE_TAG}"

CRDS=(
  connections.temporal.simonemms.com
  namespaces.temporal.simonemms.com
  nexusendpoints.temporal.simonemms.com
  schedules.temporal.simonemms.com
  searchattributes.temporal.simonemms.com
)

log() { printf '\n=== %s\n' "$1"; }

cleanup() {
  local status=$?
  if [[ "${status}" -ne 0 ]]; then
    log "Smoke test failed; dumping state"
    "${KUBECTL}" -n "${CHART_NAMESPACE}" get all || true
    "${KUBECTL}" -n "${CHART_NAMESPACE}" describe deployment \
      "${CHART_RELEASE}" || true
    "${KUBECTL}" -n "${CHART_NAMESPACE}" logs \
      "deployment/${CHART_RELEASE}" --tail=200 || true
  fi
  return "${status}"
}
trap cleanup EXIT

log "Building the controller image as ${SMOKE_IMAGE}"
"${CONTAINER_TOOL}" build -t "${SMOKE_IMAGE}" .

log "Loading ${SMOKE_IMAGE} into Kind cluster ${KIND_CLUSTER}"
"${KIND}" load docker-image "${SMOKE_IMAGE}" --name "${KIND_CLUSTER}"

log "Installing the chart as release ${CHART_RELEASE}"
"${HELM}" install "${CHART_RELEASE}" "${CHART_DIR}" \
  --namespace "${CHART_NAMESPACE}" \
  --create-namespace \
  --set "image.repository=${SMOKE_IMAGE_REPO}" \
  --set "image.tag=${SMOKE_IMAGE_TAG}" \
  --set image.pullPolicy=IfNotPresent \
  --wait \
  --timeout 5m

log "Checking every CRD is installed"
for crd in "${CRDS[@]}"; do
  "${KUBECTL}" get crd "${crd}" -o name
done

log "Checking the controller Deployment is Ready"
"${KUBECTL}" -n "${CHART_NAMESPACE}" rollout status \
  "deployment/${CHART_RELEASE}" --timeout=2m
ready="$("${KUBECTL}" -n "${CHART_NAMESPACE}" get deployment \
  "${CHART_RELEASE}" -o jsonpath='{.status.readyReplicas}')"
if [[ "${ready}" != "1" ]]; then
  echo "Expected 1 ready replica, got '${ready}'" >&2
  exit 1
fi

log "Checking the metrics Service exists"
"${KUBECTL}" -n "${CHART_NAMESPACE}" get service \
  "${CHART_RELEASE}-metrics" -o name

log "Checking the manager started without RBAC failures"
# Give the manager a moment to acquire leadership and start its controllers,
# which is when missing permissions would show up.
"${KUBECTL}" -n "${CHART_NAMESPACE}" wait --for=condition=Available \
  "deployment/${CHART_RELEASE}" --timeout=2m
logs="$("${KUBECTL}" -n "${CHART_NAMESPACE}" logs \
  "deployment/${CHART_RELEASE}" --tail=-1)"
if grep -Eqi 'is forbidden|cannot list resource|cannot get resource|cannot create resource' <<<"${logs}"; then
  echo "Found RBAC failures in the manager log:" >&2
  grep -Ei 'is forbidden|cannot (list|get|create) resource' <<<"${logs}" >&2
  exit 1
fi
if ! grep -q 'Starting workers' <<<"${logs}"; then
  echo "Manager never reported starting its controllers:" >&2
  echo "${logs}" >&2
  exit 1
fi

log "Uninstalling the release"
"${HELM}" uninstall "${CHART_RELEASE}" --namespace "${CHART_NAMESPACE}" --wait

log "Removing the CRDs Helm left behind"
# Helm does not delete anything from crds/ on uninstall. Doing it here keeps
# the cluster reusable and documents the behaviour.
for crd in "${CRDS[@]}"; do
  "${KUBECTL}" delete crd "${crd}" --ignore-not-found
done
"${KUBECTL}" delete namespace "${CHART_NAMESPACE}" --ignore-not-found

log "Helm chart smoke test passed"
