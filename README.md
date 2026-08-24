# Temporal Resource Operator

A Kubernetes operator for declaratively managing resources in an existing
[Temporal Service](https://temporal.io), whether Temporal Cloud or self-hosted.

[![Build](https://github.com/mrsimonemms/temporal-resource-operator/actions/workflows/build.yml/badge.svg)](https://github.com/mrsimonemms/temporal-resource-operator/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

<!-- toc -->

* [What it does](#what-it-does)
* [What it does not do](#what-it-does-not-do)
  * [How is this different from temporal-operator?](#how-is-this-different-from-temporal-operator)
* [Supported resources](#supported-resources)
* [Installation](#installation)
  * [Prerequisites](#prerequisites)
  * [Install with Helm](#install-with-helm)
  * [Install from source](#install-from-source)
  * [Generate a single install manifest](#generate-a-single-install-manifest)
  * [Uninstall](#uninstall)
* [Quick start](#quick-start)
  * [1. Describe the Temporal Service](#1-describe-the-temporal-service)
  * [2. Create a Temporal Namespace](#2-create-a-temporal-namespace)
  * [3. Add a Search Attribute and a Nexus Endpoint](#3-add-a-search-attribute-and-a-nexus-endpoint)
  * [4. Confirm from Temporal](#4-confirm-from-temporal)
* [Resource reference](#resource-reference)
  * [Connection](#connection)
    * [Connection fields](#connection-fields)
    * [Credentials in a Secret](#credentials-in-a-secret)
    * [Authentication examples](#authentication-examples)
    * [Connection readiness](#connection-readiness)
  * [Namespace](#namespace)
    * [Namespace fields](#namespace-fields)
    * [Create](#create)
    * [Adopt](#adopt)
    * [Ownership](#ownership)
    * [Delete vs Orphan](#delete-vs-orphan)
  * [SearchAttribute](#searchattribute)
    * [Two names, and why](#two-names-and-why)
    * [SearchAttribute fields](#searchattribute-fields)
    * [Supported types](#supported-types)
    * [Adoption and type conflicts](#adoption-and-type-conflicts)
    * [Deleting the parent Namespace](#deleting-the-parent-namespace)
    * [Second example: adopting an existing attribute](#second-example-adopting-an-existing-attribute)
  * [NexusEndpoint](#nexusendpoint)
    * [NexusEndpoint fields](#nexusendpoint-fields)
    * [Endpoint names are Service-wide](#endpoint-names-are-service-wide)
    * [Retargeting](#retargeting)
    * [Adoption](#adoption)
    * [Namespace deletion is blocked by endpoints](#namespace-deletion-is-blocked-by-endpoints)
    * [NexusEndpoint limitations](#nexusendpoint-limitations)
* [Ownership and adoption](#ownership-and-adoption)
* [Deletion policy](#deletion-policy)
* [Dependency model](#dependency-model)
* [Status and troubleshooting](#status-and-troubleshooting)
  * [Reading status](#reading-status)
  * [Ready reasons](#ready-reasons)
  * [A resource is stuck in Terminating](#a-resource-is-stuck-in-terminating)
* [Temporal Cloud and self-hosted](#temporal-cloud-and-self-hosted)
* [Limitations](#limitations)
* [Contributing](#contributing)
  * [Open in a container](#open-in-a-container)
  * [Local development](#local-development)
  * [Commit style](#commit-style)

<!-- Regenerate with "pre-commit run -a markdown-toc" -->

<!-- tocstop -->

## What it does

Temporal Namespaces, Search Attributes and Nexus Endpoints are normally created
by hand with `temporal` CLI commands, or by a bootstrap script that nobody
wants to own. This operator lets you describe them as Kubernetes resources and
keeps the Temporal Service in step with what you declared.

```text
Kubernetes CRs
      |
      v
temporal-resource-operator
      |
      v
Existing Temporal Service
  Cloud or self-hosted
```

The operator manages resources **inside** an existing Temporal Service. You
tell it where the Service is with a `Connection` resource, and it talks to that
Service's gRPC endpoint like any other Temporal client.

## What it does not do

* It does **not** install, run, upgrade or operate the Temporal cluster itself.
  Temporal must already be running, whether that is Temporal Cloud, a Helm
  install, or a dev server in your cluster.
* It does not deploy your workers, workflows or Nexus service handlers.
* It does not manage Temporal Cloud account-level objects such as users, API
  keys or regions.

If you need something to deploy Temporal itself, this is the wrong tool.

### How is this different from temporal-operator?

[alexandrevilain/temporal-operator](https://github.com/alexandrevilain/temporal-operator)
is primarily a Kubernetes operator for deploying and operating Temporal clusters,
including their persistence, services, upgrades and supporting infrastructure.

`temporal-resource-operator` starts one level higher: it assumes the Temporal
Service already exists — whether that is Temporal Cloud or a self-hosted
cluster — and manages resources inside it, such as Namespaces, Search
Attributes and Nexus Endpoints.

There is some overlap around Namespace management, but the two projects solve
different problems and can be complementary: one can operate the Temporal
cluster while this operator manages resources within it.

## Supported resources

All resources are namespaced and live in the `temporal.simonemms.com/v1beta1`
API group. Every reference between them is to a resource in the *same*
Kubernetes namespace.

| Resource | Short name | Purpose |
| --- | --- | --- |
| `Connection` | none | Describes how to reach a Temporal Service |
| `Namespace` | `tns` | Manages one Temporal Namespace |
| `SearchAttribute` | `tsa` | Manages one custom Search Attribute |
| `NexusEndpoint` | `tnx` | Manages one Nexus Endpoint |

`Connection` deliberately has no short name; use `kubectl get connections`.

## Installation

### Prerequisites

* A Kubernetes cluster and `kubectl` configured against it
* A reachable Temporal Service (Temporal Cloud or self-hosted)
* Helm 4

Container images are published to the GitHub Container Registry as
`ghcr.io/mrsimonemms/temporal-resource-operator`. Tagged releases are built for
`linux/amd64` and `linux/arm64`; commits on `main` are published as
`linux/amd64` under their commit SHA.

The Helm chart is published beside them, as an OCI artifact at
`ghcr.io/mrsimonemms/charts/temporal-resource-operator`. Chart versions drop the
leading `v`, so the chart matching image `v0.1.0` is chart `0.1.0`, and that
image is what the chart installs by default.

### Install with Helm

Helm is the recommended way to install the operator:

```sh
helm install temporal-resource-operator \
  oci://ghcr.io/mrsimonemms/charts/temporal-resource-operator \
  --version <version> \
  --namespace temporal-resource-operator-system \
  --create-namespace
```

Charts are published from release tags only, so `<version>` is a released
[chart version](https://github.com/mrsimonemms/temporal-resource-operator/pkgs/container/charts%2Ftemporal-resource-operator).

The namespace is a convention rather than a requirement — the chart installs
into whichever namespace you give it. It installs the four CRDs, the controller
Deployment, its ServiceAccount and RBAC, and an HTTPS metrics Service. It does
not install Temporal, and it creates no Temporal resources of its own.

```sh
kubectl -n temporal-resource-operator-system \
  rollout status deployment/temporal-resource-operator
kubectl -n temporal-resource-operator-system \
  logs deployment/temporal-resource-operator -f
```

Values, upgrade instructions and the CRD caveat that comes with Helm are in the
[chart README](charts/temporal-resource-operator/README.md).

### Install from source

The Makefile drives a Kustomize install from a clone. This is the development
path — use it when working on the operator, or when you want the install to
track your working tree rather than a release:

```sh
git clone https://github.com/mrsimonemms/temporal-resource-operator.git
cd temporal-resource-operator

export IMG=ghcr.io/mrsimonemms/temporal-resource-operator:<tag>

make install            # install the four CRDs
make deploy IMG=$IMG    # install RBAC and the controller manager
```

`make deploy` creates the `temporal-resource-operator-system` namespace and a
deployment named `temporal-resource-operator-controller-manager` inside it.
Check that it came up:

```sh
kubectl -n temporal-resource-operator-system \
  rollout status deployment/temporal-resource-operator-controller-manager
kubectl -n temporal-resource-operator-system \
  logs deployment/temporal-resource-operator-controller-manager -f
```

### Generate a single install manifest

If you would rather review or vendor one file, or install without cloning on
the target machine:

```sh
make build-installer IMG=$IMG
kubectl apply -f dist/install.yaml
```

`dist/install.yaml` contains the CRDs, RBAC and the manager deployment with
your image already substituted. It is generated, not committed.

### Uninstall

Order matters here. The custom resources use finalizers, and only the operator
releases them, so remove the resources **before** the operator:

```sh
kubectl delete tnx,tsa,tns,connections --all --all-namespaces
```

Then remove the operator, whichever way you installed it:

```sh
# Helm
helm uninstall temporal-resource-operator \
  --namespace temporal-resource-operator-system

# From source
make undeploy    # remove the controller manager and RBAC
make uninstall   # remove the CRDs
```

`helm uninstall` deliberately leaves the CRDs behind, because deleting a CRD
deletes every resource of that kind. Remove them explicitly when you mean it —
see the [chart README](charts/temporal-resource-operator/README.md).

Anything with `deletionPolicy: Delete` has its Temporal object removed as part
of that first step, so patch the policy to `Orphan` first if you want the
Temporal side to survive — see [Deletion policy](#deletion-policy).

If you remove the operator while custom resources still exist, those resources
will sit in `Terminating` with nothing left to release their finalizers, and
`make uninstall` will block. Redeploy the operator and let it finish, or strip
the finalizers by hand.

## Quick start

This walks through all four resources and verifies the resulting objects in
Temporal. The names used here (`production`, `payments`) carry through the rest
of this document.

### 1. Describe the Temporal Service

For a self-hosted Service reachable from inside the cluster with no
authentication:

```yaml
apiVersion: temporal.simonemms.com/v1beta1
kind: Connection
metadata:
  name: production
spec:
  address: cluster.temporal.svc.cluster.local:7233
```

For Temporal Cloud with an API key, put the key in a Secret first:

```sh
kubectl create secret generic temporal-credentials \
  --from-literal=apiKey="$TEMPORAL_API_KEY"
```

```yaml
apiVersion: temporal.simonemms.com/v1beta1
kind: Connection
metadata:
  name: production
spec:
  address: my-namespace.a1b2c.tmprl.cloud:7233
  tls: true
  credentialsSecretRef:
    name: temporal-credentials
```

Apply it and wait for it to report `Ready`:

```sh
kubectl get connections
```

```text
NAME         ADDRESS                                    READY   REASON      AGE
production   cluster.temporal.svc.cluster.local:7233    True    Connected   8s
```

### 2. Create a Temporal Namespace

The resource's `metadata.name` is the Temporal Namespace name.

```yaml
apiVersion: temporal.simonemms.com/v1beta1
kind: Namespace
metadata:
  name: payments
spec:
  connectionRef:
    name: production
  retention: 7d
```

```sh
kubectl get tns
```

```text
NAME       CONNECTION   RETENTION   OWNERSHIP   READY   REASON     AGE
payments   production   7d          Created     True    Created    5s
```

### 3. Add a Search Attribute and a Nexus Endpoint

```yaml
apiVersion: temporal.simonemms.com/v1beta1
kind: SearchAttribute
metadata:
  name: customer-id
spec:
  name: CustomerId
  connectionRef:
    name: production
  namespaceRef:
    name: payments
  type: Keyword
---
apiVersion: temporal.simonemms.com/v1beta1
kind: NexusEndpoint
metadata:
  name: payments-nexus
spec:
  name: PaymentsNexus
  connectionRef:
    name: production
  namespaceRef:
    name: payments
  taskQueue: payments-nexus
```

```sh
kubectl get tsa
kubectl get tnx
```

```text
NAME          ATTRIBUTE    TYPE      TEMPORAL NS   OWNERSHIP   READY   REASON    AGE
customer-id   CustomerId   Keyword   payments      Created     True    Created   4s

NAME             ENDPOINT        TEMPORAL NS   TASK QUEUE       OWNERSHIP   READY   REASON    AGE
payments-nexus   PaymentsNexus   payments      payments-nexus   Created     True    Created   4s
```

### 4. Confirm from Temporal

```sh
temporal operator namespace describe -n payments
temporal operator search-attribute list -n payments
temporal operator nexus endpoint get --name PaymentsNexus
```

## Resource reference

Every resource exposes the same status shape: a `Ready` condition, an
`observedGeneration`, and — for the three that manage an external object — an
`ownership` field. Those are described under
[Status and troubleshooting](#status-and-troubleshooting).

### Connection

A `Connection` describes how to reach one Temporal Service. Every other
resource points at one. It is the only resource that does not create anything
in Temporal — reconciling it dials the Service and reports whether it is
healthy.

```yaml
apiVersion: temporal.simonemms.com/v1beta1
kind: Connection
metadata:
  name: production
spec:
  address: my-namespace.a1b2c.tmprl.cloud:7233
  tls: true
  tlsServerName: my-namespace.a1b2c.tmprl.cloud
  credentialsSecretRef:
    name: temporal-credentials
```

#### Connection fields

| Field | Type | Required | Default | Mutable | Description |
| --- | --- | --- | --- | --- | --- |
| `spec.address` | string | yes | — | yes | `host:port` of the Temporal Service's gRPC endpoint. Must be non-empty. |
| `spec.tls` | boolean | no | `false` | yes | Enables TLS. Also implied by `tlsServerName`, by an mTLS client certificate, and by an API key. |
| `spec.tlsServerName` | string | no | — | yes | Overrides the server name used to verify the Service's certificate. Setting it enables TLS. |
| `spec.credentials` | object | no | — | yes | Credentials supplied inline. Mutually exclusive with `credentialsSecretRef`. |
| `spec.credentials.apiKey` | string | no | — | yes | Temporal API key, sent as a bearer token. |
| `spec.credentials.clientCert` | string | no | — | yes | PEM-encoded mTLS client certificate. Must accompany `clientKey`. |
| `spec.credentials.clientKey` | string | no | — | yes | PEM-encoded private key for `clientCert`. |
| `spec.credentialsSecretRef` | object | no | — | yes | Names a Secret in the same Kubernetes namespace holding the credentials. Mutually exclusive with `credentials`. |
| `spec.credentialsSecretRef.name` | string | no | `""` | yes | Secret name. |

Setting both `credentials` and `credentialsSecretRef` is rejected by the API
server:

```text
credentials and credentialsSecretRef are mutually exclusive
```

#### Credentials in a Secret

`credentialsSecretRef` points at a Secret in the same Kubernetes namespace as
the `Connection`. The Secret may contain any of these keys, and they mirror the
inline field names exactly:

| Secret key | Purpose |
| --- | --- |
| `apiKey` | Temporal API key, sent as a bearer token |
| `clientCert` | PEM-encoded mTLS client certificate |
| `clientKey` | PEM-encoded private key for `clientCert` |

Keys you do not need can simply be absent. Inline `credentials` exist for
development and testing; use a Secret anywhere you care about the values.

#### Authentication examples

Self-hosted, in-cluster, no authentication:

```yaml
spec:
  address: cluster.temporal.svc.cluster.local:7233
```

Self-hosted with TLS but no client authentication:

```yaml
spec:
  address: temporal.example.com:7233
  tls: true
```

Temporal Cloud with an API key:

```yaml
spec:
  address: my-namespace.a1b2c.tmprl.cloud:7233
  tls: true
  credentialsSecretRef:
    name: temporal-credentials    # key: apiKey
```

Temporal Cloud or self-hosted with mTLS:

```yaml
spec:
  address: my-namespace.a1b2c.tmprl.cloud:7233
  credentialsSecretRef:
    name: temporal-credentials    # keys: clientCert, clientKey
```

An mTLS client certificate enables TLS on its own, so `tls: true` is redundant
there — harmless, but unnecessary. Add `tlsServerName` when the name you dial
does not match the name on the Service's certificate, which is typical behind
an ingress or a port-forward.

#### Connection readiness

`Ready=True` with reason `Connected` means the operator dialled the Service and
it reported itself healthy. A healthy `Connection` is re-checked every five
minutes, because nothing outside the cluster will tell the operator when the
Service becomes unreachable.

The other resources refuse to act until their `Connection` is `Ready=True`, so
this is the first thing to look at when nothing is happening.

Note that the operator reads referenced Secrets when it resolves a
`Connection`; it does not watch them. Changing a Secret's contents is picked up
on the next revalidation rather than immediately.

### Namespace

A `Namespace` manages one Temporal Namespace. The resource's `metadata.name`
**is** the Temporal Namespace name — there is no `spec.name` — so a Temporal
Namespace is named once and cannot drift from the resource identifying it.

```yaml
apiVersion: temporal.simonemms.com/v1beta1
kind: Namespace
metadata:
  name: payments
spec:
  connectionRef:
    name: production
  retention: 7d
  deletionPolicy: Delete
```

#### Namespace fields

| Field | Type | Required | Default | Mutable | Description |
| --- | --- | --- | --- | --- | --- |
| `metadata.name` | string | yes | — | no | The Temporal Namespace name. Kubernetes forbids renaming a resource. |
| `spec.connectionRef.name` | string | yes | — | yes | `Connection` in the same Kubernetes namespace. Must be non-empty. |
| `spec.retention` | duration | no | `72h` | yes | How long closed workflow histories are kept. Between 1 and 90 days inclusive. |
| `spec.deletionPolicy` | enum | no | `Delete` | yes | `Delete` or `Orphan`. See [Deletion policy](#deletion-policy). |

`retention` is a duration string. Go's units all work — `72h`, `1h30m`, `90m` —
and so does a leading whole number of days, where **one day is exactly 24
hours**: `7d` is `168h`, `1d12h` is `36h`, `7d30m` is `168h30m`. Days are not a
calendar concept, so `1d` stays 24 hours across a clock change.

**Retention must be between 1 and 90 days inclusive.** The limit is on the
duration, not on how you write it, so all of these are the minimum and all are
accepted:

```text
1d   24h   1440m   86400s
```

and both of these are the maximum:

```text
90d   2160h
```

Anything shorter than `1d` or longer than `90d` is refused, whichever notation
it is written in — `1h`, `23h59m59s` and `1439m` are all too short, and `91d`,
`2161h` and even `90d1ns` are all too long. So are malformed values such as
`7days`, `7dd`, `1d2d`, `1.5d` and arbitrary text. All of it is rejected when you
apply it, rather than being stored and failing later.

A negative or zero retention is rejected too, rather than being treated as
"unset". Leave the field out entirely if you want the default.

Changing `retention` reconciles the Temporal Namespace in place.

`connectionRef` is mutable, unlike on the other resources, but repointing it at
a different Temporal Service moves management rather than migrating anything:
the operator will create the Namespace on the new Service and leave the
original where it is. Change it to correct a reference, not to move a Namespace
between Services.

#### Create

If the Temporal Namespace named by `metadata.name` does not exist, the operator
registers it and records `status.ownership: Created`.

#### Adopt

If a Temporal Namespace of that name already exists and carries no other
resource's ownership marker, the operator adopts it: it records
`status.ownership: Adopted` and manages its retention from then on, but will
never delete it.

#### Ownership

`Namespace` has stronger ownership guarantees than the other resources, because
Temporal Namespaces carry arbitrary metadata and the operator uses it. When the
operator creates a Namespace it writes the Kubernetes resource's UID into the
Temporal Namespace's metadata under the key
`temporal.simonemms.com/owner-uid`.

That makes ownership externally verifiable rather than a note the operator
keeps to itself:

* Before deleting a Temporal Namespace, the operator re-reads that marker and
  refuses if it does not name this exact resource. A resource recreated under
  the same name gets a new UID, so it cannot inherit the previous one's
  Namespace.
* If the marker names some *other* resource, the operator reports
  `OwnershipConflict` and will not adopt, reconfigure or remove it.
* If a resource believes it owns a Namespace but the marker cannot confirm it,
  the operator reports `OwnershipUnverified` and refuses the destructive
  operation.

#### Delete vs Orphan

With `deletionPolicy: Delete` (the default), deleting the resource deletes the
Temporal Namespace — but only where ownership is `Created` and the marker
confirms it. That requires a working `Connection`, so deletion waits, holding
the `temporal.simonemms.com/namespace` finalizer, rather than abandoning a
Namespace it is responsible for. You will see `Ready=False` with reason
`DeleteFailed` while it waits.

With `deletionPolicy: Orphan`, the Temporal Namespace is left alone and the
operator contacts Temporal not at all.

That last point is the recovery route. If a deletion is stuck because the
`Connection` has been deleted, switch the policy — this is supported while the
resource is already terminating:

```sh
kubectl patch tns payments --type=merge \
  -p '{"spec":{"deletionPolicy":"Orphan"}}'
```

The next reconcile releases the finalizer and the resource goes away, leaving
the Temporal Namespace in place.

### SearchAttribute

A `SearchAttribute` manages one custom Search Attribute on one Temporal
Namespace.

```yaml
apiVersion: temporal.simonemms.com/v1beta1
kind: SearchAttribute
metadata:
  name: customer-id
spec:
  name: CustomerId
  connectionRef:
    name: production
  namespaceRef:
    name: payments
  type: Keyword
  deletionPolicy: Delete
```

#### Two names, and why

```text
metadata.name = customer-id   the Kubernetes resource name
spec.name     = CustomerId    the exact Temporal Search Attribute name
```

They are separate fields because Kubernetes only accepts lowercase resource
names and a CRD cannot relax that, whereas Search Attributes are conventionally
PascalCase. `spec.name` is sent to Temporal verbatim; `metadata.name` never
leaves Kubernetes. `spec.name` is not defaulted from `metadata.name` — you must
set it.

#### SearchAttribute fields

| Field | Type | Required | Default | Mutable | Description |
| --- | --- | --- | --- | --- | --- |
| `spec.name` | string | yes | — | **no** | The Temporal Search Attribute name. Must be non-empty. |
| `spec.connectionRef.name` | string | yes | — | **no** | `Connection` in the same Kubernetes namespace. Must be non-empty. |
| `spec.namespaceRef.name` | string | yes | — | **no** | `Namespace` resource in the same Kubernetes namespace. Must be non-empty. |
| `spec.type` | enum | yes | — | **no** | One of the types below. |
| `spec.deletionPolicy` | enum | no | `Delete` | yes | `Delete` or `Orphan`. See [Deletion policy](#deletion-policy). |

Everything that decides *which* Search Attribute this is, and what shape it
has, is immutable. The API server rejects a change:

```text
name is immutable
```

Only `deletionPolicy` can be changed after creation. To move or retype an
attribute, delete the resource and create a new one.

#### Supported types

| Value | Temporal type |
| --- | --- |
| `Bool` | Boolean |
| `Datetime` | Timestamp |
| `Double` | Floating-point number |
| `Int` | 64-bit integer |
| `Keyword` | Exact-match string |
| `KeywordList` | List of exact-match strings |
| `Text` | Full-text searchable string |

#### Adoption and type conflicts

* No attribute of that name on the Namespace: the operator registers it, and
  ownership becomes `Created`.
* An attribute of that name exists with the **same** type: it is adopted, and
  ownership becomes `Adopted`.
* An attribute of that name exists with a **different** type: the operator
  reports `Ready=False` with reason `TypeConflict` and changes nothing.
  Temporal cannot retype a Search Attribute, so there is no correct automatic
  action.

Temporal exposes no ownership metadata on Search Attributes — an attribute is
just a name and a type — so `Created` versus `Adopted` is bookkeeping held in
the resource's status, not something the operator can re-verify against
Temporal. It is still honoured: an `Adopted` attribute is never deleted.

Because of that, two Kubernetes resources must not target the same Search
Attribute on the same Namespace. Nothing stops you creating them, and they will
fight over it.

#### Deleting the parent Namespace

Deleting a `Namespace` takes its Search Attributes with it, and Temporal does
not object. If the operator goes to remove an attribute and Temporal reports
the Namespace absent, it treats the attribute as already gone and releases the
finalizer, rather than holding the resource open for a Namespace that will
never come back. Deleting a `Namespace` and its `SearchAttribute` resources
together therefore works in either order.

Nexus Endpoints behave differently — see
[Namespace deletion is blocked by endpoints](#namespace-deletion-is-blocked-by-endpoints).

#### Second example: adopting an existing attribute

An attribute registered before the operator arrived. It is kept in step, but
never removed, whatever the policy says:

```yaml
apiVersion: temporal.simonemms.com/v1beta1
kind: SearchAttribute
metadata:
  name: order-status
spec:
  name: OrderStatus
  connectionRef:
    name: production
  namespaceRef:
    name: payments
  type: Keyword
```

### NexusEndpoint

A `NexusEndpoint` manages one Nexus Endpoint, routing Nexus requests to a
worker polling a given Namespace and Task Queue.

```yaml
apiVersion: temporal.simonemms.com/v1beta1
kind: NexusEndpoint
metadata:
  name: payments-nexus
spec:
  name: PaymentsNexus
  connectionRef:
    name: production
  namespaceRef:
    name: payments
  taskQueue: payments-nexus
  deletionPolicy: Delete
```

As with `SearchAttribute`, `metadata.name` is Kubernetes identity and
`spec.name` is what Temporal is asked for.

#### NexusEndpoint fields

| Field | Type | Required | Default | Mutable | Description |
| --- | --- | --- | --- | --- | --- |
| `spec.name` | string | yes | — | **no** | The Nexus Endpoint name. Must be non-empty. Unique across the whole Temporal Service. |
| `spec.connectionRef.name` | string | yes | — | **no** | `Connection` in the same Kubernetes namespace. Must be non-empty. |
| `spec.namespaceRef.name` | string | yes | — | yes | `Namespace` resource whose Temporal Namespace the endpoint targets. Must be non-empty. |
| `spec.taskQueue` | string | yes | — | yes | Task Queue a handler worker polls. Must be non-empty. |
| `spec.deletionPolicy` | enum | no | `Delete` | yes | `Delete` or `Orphan`. See [Deletion policy](#deletion-policy). |

Identity is immutable; the target is not. `spec.name` fixes which endpoint this
is, and `connectionRef` fixes which Service it lives in — changing either would
silently point at a different object, so both are rejected. `namespaceRef` and
`taskQueue` describe where the endpoint routes to, and can be changed freely.

#### Endpoint names are Service-wide

Nexus Endpoint names are unique across an entire Temporal Service, not per
Namespace. Two Namespaces on the same Service cannot both have a
`PaymentsNexus`. If the operator is asked to create an endpoint whose name is
already taken by something it did not create, it reports `Ready=False` with
reason `EndpointTaken`.

#### Retargeting

Changing `namespaceRef` or `taskQueue` reconciles the endpoint **in place**
using Temporal's endpoint update, which is versioned. The endpoint keeps its
server-assigned ID, so callers that have already resolved it are undisturbed.

If something else modified the endpoint between the operator reading it and
writing it, Temporal refuses the write and the operator reports `Ready=False`
with reason `Conflict`. The next reconcile starts again from a fresh read; no
action is needed.

`status.endpointId` records the server-assigned ID for reference. It is an
observation only, not proof of ownership.

#### Adoption

An endpoint that already exists under `spec.name` is adopted, and ownership
becomes `Adopted`. The operator still manages target drift on an adopted
endpoint — the resource is the declared configuration either way. Adoption
changes *deletion rights* only: an adopted endpoint is never deleted.

As with Search Attributes, Nexus Endpoints carry no ownership metadata, so
`Created` versus `Adopted` is controller bookkeeping rather than something
verifiable against Temporal.

#### Namespace deletion is blocked by endpoints

This is the one piece of operational behaviour worth knowing before you need
it:

> Temporal refuses to delete a Namespace while a Nexus Endpoint targets it.

The Service returns an error along the lines of `cannot delete a namespace that
is a target of a Nexus endpoint`. In practice that means:

* Deleting a `Namespace` resource whose Temporal Namespace is still targeted
  will not complete. The `Namespace` sits in `Terminating`, holding its
  finalizer, reporting `Ready=False` with reason `DeleteFailed`, and retrying.
* Delete the `NexusEndpoint` resource first. Once the endpoint is gone, the
  `Namespace` finishes on its next reconcile with no further intervention.

The operator does not try to work around this or reorder it for you; it retries
until you resolve it. Deleting both at once is safe — it simply takes until the
endpoint has actually gone.

#### NexusEndpoint limitations

* **Worker targets only.** External URL targets are not modelled. Every
  endpoint the operator manages routes to a Namespace and Task Queue.
* **Description is preserved but not managed.** If you set a description on an
  endpoint by other means, the operator keeps it when it updates the target,
  but you cannot declare one here.
* **One resource per endpoint.** Two resources naming the same Service-wide
  endpoint are unsupported and will fight over it.

## Ownership and adoption

`Namespace`, `SearchAttribute` and `NexusEndpoint` each record how they came to
manage their Temporal object in `status.ownership`:

| Value | Meaning |
| --- | --- |
| `Creating` | The object was missing and the operator is registering it. Written before the create call, so an interrupted reconcile can still tell its own work from something to adopt. |
| `Created` | The operator caused the object to exist. |
| `Adopted` | The object already existed and is now managed by the operator. |

The rule that follows from it:

> **Adopted resources are never deleted by the operator.** Deleting the
> Kubernetes resource leaves the Temporal object exactly where it was.

Ownership is set once and then left alone. An established value never flips
between `Created` and `Adopted`, because whether the Temporal object happens to
exist right now says nothing about who put it there. If someone deletes a
`Created` object behind the operator's back, it is recreated and stays
`Created`.

The three differ in how strongly ownership can be trusted:

| Resource | Strength |
| --- | --- |
| `Namespace` | **Externally verifiable.** The owning resource's UID is stored in Temporal Namespace metadata and re-checked before anything destructive. |
| `SearchAttribute` | **Bookkeeping.** Temporal exposes no ownership metadata for Search Attributes. |
| `NexusEndpoint` | **Bookkeeping.** Temporal exposes no ownership metadata for Nexus Endpoints. |

For the two bookkeeping cases, ownership survives normal operation but cannot
be reconstructed if the resource's status is lost. The practical consequence is
the one already stated: do not point two resources at the same Temporal object.

## Deletion policy

`Namespace`, `SearchAttribute` and `NexusEndpoint` all take
`spec.deletionPolicy`. It is mutable on all three, and defaults to `Delete`.

| Value | Behaviour |
| --- | --- |
| `Delete` | On deletion, the operator removes the Temporal object it created. Requires a working `Connection`. |
| `Orphan` | On deletion, the Kubernetes resource goes away and Temporal is not contacted at all. |

Two things are worth being precise about.

**Adopted objects are retained regardless.** `Delete` only ever applies to
objects the operator created. It cannot grant the right to delete something the
operator merely adopted.

**`Orphan` deliberately skips dependency resolution.** It short-circuits before
the operator even looks up the `Connection`, which is what makes it the
recovery mechanism when a `Connection` has been deleted out from under a
terminating resource. Switching to `Orphan` is supported while the resource is
already in `Terminating`:

```sh
kubectl patch tns payments      --type=merge -p '{"spec":{"deletionPolicy":"Orphan"}}'
kubectl patch tsa customer-id   --type=merge -p '{"spec":{"deletionPolicy":"Orphan"}}'
kubectl patch tnx payments-nexus --type=merge -p '{"spec":{"deletionPolicy":"Orphan"}}'
```

## Dependency model

Resources reference each other by name, always within the same Kubernetes
namespace:

```text
        Connection
             ^
             |
         Namespace
             ^
             |
      SearchAttribute
```

```text
    Connection <--- NexusEndpoint ---> Namespace
```

Read as: a `Namespace` needs a `Connection`. A `SearchAttribute` needs a
`Namespace` and a `Connection`. A `NexusEndpoint` needs both as well, and
`namespaceRef` is the one it can be repointed at.

A dependent resource does nothing until its dependencies report `Ready=True`.
`SearchAttribute` and `NexusEndpoint` both gate on the `Namespace` as well as
the `Connection`, because Temporal will not accept either object for a
Namespace that does not exist yet.

The operator watches its dependencies, so a resource waiting on one reacts
within milliseconds of that dependency becoming `Ready` — you do not wait out a
polling interval. Periodic reconciliation still runs underneath as a backstop:
every five minutes for a settled resource, and every thirty seconds for one
waiting on a dependency.

Because of the watches, applying everything at once works fine. Resources sort
themselves out in dependency order rather than failing.

## Status and troubleshooting

### Reading status

Every resource carries:

* `status.conditions`, containing a `Ready` condition whose `status`, `reason`
  and `message` say what is happening
* `status.observedGeneration`, the `metadata.generation` last reconciled — if
  it lags behind `metadata.generation`, your change has not been processed yet
* `status.ownership`, on all but `Connection`

The condition `reason` is surfaced as a print column, so most triage is one
command:

```sh
kubectl get connections
kubectl get tns
kubectl get tsa
kubectl get tnx
```

For the full picture:

```sh
kubectl describe tns payments
kubectl get tnx payments-nexus -o yaml
kubectl -n temporal-resource-operator-system \
  logs deployment/temporal-resource-operator-controller-manager -f
```

### Ready reasons

Success reasons:

| Reason | Meaning |
| --- | --- |
| `Connected` | (`Connection`) The Service was reached and reports itself healthy. |
| `Created` | The Temporal object was registered by this reconcile. |
| `Adopted` | A pre-existing Temporal object was taken under management. |
| `Updated` | Configuration had drifted and was corrected. |
| `Reconciled` | The Temporal object already matched the spec. |

Failure reasons, and what to do:

| Reason | Meaning and action |
| --- | --- |
| `InvalidConfiguration` | (`Connection`) The spec could not be turned into usable client options — a missing Secret, an unparseable certificate. Check the Secret exists and its keys are correct. |
| `ConnectionFailed` | (`Connection`) The Service could not be dialled. Check `spec.address`, network policy and TLS settings. |
| `HealthCheckFailed` | (`Connection`) The Service was dialled but did not report itself healthy. Look at the Temporal Service. |
| `ConnectionNotFound` | The referenced `Connection` does not exist in this Kubernetes namespace. Check the name. |
| `ConnectionNotReady` | The `Connection` exists but is not `Ready=True`. Fix the `Connection` first. |
| `NamespaceNotFound` | The referenced `Namespace` resource does not exist. Check the name, or create it. |
| `NamespaceNotReady` | The `Namespace` exists but is not `Ready=True` yet. Usually transient. |
| `DescribeFailed` | The Temporal object could not be looked up. Usually a Service-side or permissions problem. |
| `CreateFailed` | The Temporal object could not be created. The message carries Temporal's error. |
| `UpdateFailed` | Drifted configuration could not be corrected. |
| `DeleteFailed` | An owned Temporal object could not be removed, so the finalizer is being held. See the message — for a `Namespace`, a `NexusEndpoint` still targeting it is a common cause. |
| `OwnershipConflict` | (`Namespace`) The Temporal Namespace carries another resource's ownership marker. The operator will not touch it. |
| `OwnershipUnverified` | (`Namespace`) Ownership could not be confirmed from Temporal's metadata, so a destructive operation was refused. |
| `TypeConflict` | (`SearchAttribute`) An attribute of that name exists with a different type. Temporal cannot retype it; pick a different name or remove the existing attribute. |
| `EndpointTaken` | (`NexusEndpoint`) An endpoint of that name already exists Service-wide and was not created by this resource. |
| `Conflict` | (`NexusEndpoint`) The endpoint changed between being read and written. Self-correcting; the next reconcile retries from a fresh read. |

### A resource is stuck in Terminating

It is holding its finalizer because it cannot complete a `Delete`. Check the
`Ready` reason:

* `DeleteFailed` on a `Namespace` with a Nexus message: delete the
  `NexusEndpoint` targeting it first.
* `ConnectionNotFound` or `DeleteFailed` after the `Connection` was removed:
  patch `spec.deletionPolicy` to `Orphan`, as shown under
  [Deletion policy](#deletion-policy).

Finalizer names, should you need to identify them:

| Resource | Finalizer |
| --- | --- |
| `Namespace` | `temporal.simonemms.com/namespace` |
| `SearchAttribute` | `temporal.simonemms.com/search-attribute` |
| `NexusEndpoint` | `temporal.simonemms.com/nexus-endpoint` |

## Temporal Cloud and self-hosted

Both are supported, and the only thing that differs is the `Connection`.
Temporal Cloud needs TLS and credentials — an API key or an mTLS certificate
pair — while a self-hosted dev server typically needs neither. Everything
downstream of the `Connection` is written the same way against either.

The operator does not run or manage Temporal in either case.

## Limitations

* The operator manages resources inside an existing Temporal Service. It does
  not install or operate the Service itself.
* CRDs shipped through Helm's `crds/` mechanism are installed if absent, but
  Helm does not upgrade or remove them. A release that changes a CRD therefore
  needs the new definitions applied separately.
* `SearchAttribute` and `NexusEndpoint` ownership is controller bookkeeping,
  not externally verifiable, because Temporal exposes no ownership metadata for
  either. Only `Namespace` ownership can be re-checked against Temporal.
* Two Kubernetes resources targeting the same Temporal Search Attribute or the
  same Nexus Endpoint are unsupported and will fight over it.
* Nexus Endpoints support worker targets only; external URL targets are not
  modelled.
* A Nexus Endpoint's description is preserved across updates but cannot be
  declared.
* Losing the `Connection` blocks `Delete` finalisation for resources that
  depend on it, until the `Connection` is restored or `deletionPolicy` is set
  to `Orphan`.
* Secrets referenced by a `Connection` are read but not watched, so credential
  changes are picked up on the next revalidation rather than immediately.
* The API is `v1beta1`. It is intended for real use, but breaking changes
  may still occur before `v1`.

## Contributing

### Open in a container

* [Open in a container](https://code.visualstudio.com/docs/devcontainers/containers)

### Local development

The repository targets a [Kind](https://kind.sigs.k8s.io) cluster with a
Temporal dev server deployed alongside the operator:

```sh
make setup-test-e2e        # create the Kind cluster and deploy Temporal
make apply                 # generate, build, load the image, deploy, apply samples
make cleanup-test-e2e      # tear the cluster down
```

The main checks:

```sh
make manifests generate    # regenerate CRDs, RBAC and deepcopy code
make test                  # unit and controller tests (envtest)
make test-e2e              # end-to-end tests against Kind and a real Temporal
make lint
make build
pre-commit run -a
```

`config/crd/bases`, `config/rbac/role.yaml`, `PROJECT` and `zz_generated.*.go`
are generated — edit the `+kubebuilder` markers in `api/v1beta1/*_types.go` and
re-run `make manifests generate` instead.

See [AGENTS.md](AGENTS.md) for the full repository conventions.

### Commit style

All commits must be done in the [Conventional Commit](https://www.conventionalcommits.org)
format.

```git
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```
