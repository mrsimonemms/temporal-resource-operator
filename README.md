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
  * [3. Add a Search Attribute, a Nexus Endpoint and a Schedule](#3-add-a-search-attribute-a-nexus-endpoint-and-a-schedule)
  * [4. Confirm from Temporal](#4-confirm-from-temporal)
* [Resource reference](#resource-reference)
  * [Connection](#connection)
    * [Connection fields](#connection-fields)
    * [Credentials in a Secret](#credentials-in-a-secret)
    * [Authentication examples](#authentication-examples)
    * [Connection readiness](#connection-readiness)
  * [Namespace](#namespace)
    * [Namespace fields](#namespace-fields)
    * [Archival](#archival)
      * [Omitted means unmanaged](#omitted-means-unmanaged)
    * [The Archival URI](#the-archival-uri)
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
    * [Descriptions](#descriptions)
    * [Endpoint names are Service-wide](#endpoint-names-are-service-wide)
    * [Retargeting](#retargeting)
    * [Adoption](#adoption)
    * [Namespace deletion is blocked by endpoints](#namespace-deletion-is-blocked-by-endpoints)
    * [NexusEndpoint limitations](#nexusendpoint-limitations)
  * [Schedule](#schedule)
    * [Two names again](#two-names-again)
    * [Schedule fields](#schedule-fields)
    * [Timing](#timing)
      * [Time zone, jitter, start and end](#time-zone-jitter-start-and-end)
    * [The workflow action](#the-workflow-action)
      * [Input, memo and Search Attributes](#input-memo-and-search-attributes)
    * [Policies](#policies)
    * [State: pausing](#state-pausing)
    * [Drift, and why Temporal makes it awkward](#drift-and-why-temporal-makes-it-awkward)
    * [Unmanaged fields are preserved](#unmanaged-fields-are-preserved)
    * [Adopting an existing Schedule](#adopting-an-existing-schedule)
    * [Validation](#validation)
    * [Limits mirrored from Temporal](#limits-mirrored-from-temporal)
    * [Schedule limitations](#schedule-limitations)
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

Temporal Namespaces, Search Attributes, Nexus Endpoints and Schedules are
normally created by hand with `temporal` CLI commands, or by a bootstrap script
that nobody wants to own. This operator lets you describe them as Kubernetes
resources and keeps the Temporal Service in step with what you declared.

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
Attributes, Nexus Endpoints and Schedules.

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
| `Schedule` | `tsc` | Manages one Temporal Schedule |

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
into whichever namespace you give it. It installs the five CRDs, the controller
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

make install            # install the five CRDs
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

This walks through all five resources and verifies the resulting objects in
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

### 3. Add a Search Attribute, a Nexus Endpoint and a Schedule

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
---
apiVersion: temporal.simonemms.com/v1beta1
kind: Schedule
metadata:
  name: nightly-payments
spec:
  scheduleId: PaymentsNightly
  connectionRef:
    name: production
  namespaceRef:
    name: payments
  schedule:
    cron:
      - "30 2 * * *"
  action:
    workflow:
      type: ReconcilePayments
      taskQueue: payments
```

```sh
kubectl get tsa
kubectl get tnx
kubectl get tsc
```

```text
NAME          ATTRIBUTE    TYPE      TEMPORAL NS   OWNERSHIP   READY   REASON    AGE
customer-id   CustomerId   Keyword   payments      Created     True    Created   4s

NAME             ENDPOINT        TEMPORAL NS   TASK QUEUE       OWNERSHIP   READY   REASON    AGE
payments-nexus   PaymentsNexus   payments      payments-nexus   Created     True    Created   4s

NAME               SCHEDULE ID       TEMPORAL NS   WORKFLOW            OWNERSHIP   READY   REASON    AGE
nightly-payments   PaymentsNightly   payments      ReconcilePayments   Created     True    Created   4s
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
  archival:
    history:
      enabled: true
    visibility:
      enabled: true
```

#### Namespace fields

| Field | Type | Required | Default | Mutable | Description |
| --- | --- | --- | --- | --- | --- |
| `metadata.name` | string | yes | — | no | The Temporal Namespace name. Kubernetes forbids renaming a resource. |
| `spec.connectionRef.name` | string | yes | — | yes | `Connection` in the same Kubernetes namespace. Must be non-empty. |
| `spec.retention` | duration | no | `72h` | yes | How long closed workflow histories are kept. Between 1 and 90 days inclusive. |
| `spec.deletionPolicy` | enum | no | `Delete` | yes | `Delete` or `Orphan`. See [Deletion policy](#deletion-policy). |
| `spec.archival` | object | no | — | yes | Archival configuration. Omitted, the Namespace's Archival is not managed. See [Archival](#archival). |
| `spec.archival.history.enabled` | boolean | yes, within `history` | — | yes | Archive closed workflows' event histories. |
| `spec.archival.history.uri` | string | no | Service default | no, once set | Where histories are archived to. |
| `spec.archival.visibility.enabled` | boolean | yes, within `visibility` | — | yes | Archive closed workflows' visibility records. |
| `spec.archival.visibility.uri` | string | no | Service default | no, once set | Where visibility records are archived to. |

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

#### Archival

Temporal's own UI tells you to configure Archival by hand:

```sh
temporal operator namespace update --history-archival-state enabled payments
temporal operator namespace update --visibility-archival-state enabled payments
```

`spec.archival` is that, declared — here in full, against a self-hosted Service:

```yaml
apiVersion: temporal.simonemms.com/v1beta1
kind: Namespace
metadata:
  name: payments
spec:
  connectionRef:
    name: production
  retention: 7d
  archival:
    history:
      enabled: true
    visibility:
      enabled: true
```

**The operator configures Archival; it does not provision the storage.** The
bucket, the credentials and the archiver plugin are the Temporal Service's
business, set up in its own configuration before any Namespace can use them.
This field is the per-Namespace switch, and nothing more.

##### Omitted means unmanaged

The two kinds are independent, and **each is managed only while it is present**:

| Written | Result |
| --- | --- |
| `archival` omitted | Neither kind is managed. |
| `archival.history` omitted | History Archival is left exactly as it is. |
| `archival.visibility` omitted | Visibility Archival is left exactly as it is. |
| `archival.history.enabled: true` | History Archival is managed, and on. |
| `archival.history.enabled: false` | History Archival is managed, and off. |

`enabled` is **required** within a block, precisely so that
`history: {enabled: false}` and no `history` block at all cannot be confused.
The first is an instruction to turn Archival off; the second is the operator
having no opinion. There is no default — a `history` block without `enabled` is
rejected by the API server.

So a `Namespace` written before this field existed keeps behaving as it always
did, and this manages history alone while ignoring whatever the Namespace does
about visibility:

```yaml
spec:
  archival:
    history:
      enabled: true
```

Removing a block again **stops managing that kind and preserves its current
value** — the same way removing a `NexusEndpoint` description does. It is not a
reset.

Archival is reconciled like retention, not just applied at creation time. A
Namespace switched off behind the operator's back is switched back on at the
next resync, and a change to `enabled` is applied in place. What the operator
never does is write a value the spec has not asked for: Temporal holding a URI
or a state the CR is silent about is not drift.

**The Temporal Service has to be configured for Archival first.** Archival is
set up at the Service level — a provider, a bucket, a default URI — and switched
on per Namespace; a Namespace cannot enable something the Service does not
offer. What makes this worth spelling out is *how* a Service refuses: it does
not. A Service with no Archival configuration accepts the request, drops the
Archival part of it and answers successfully. The operator therefore reads the
setting back after every change it makes, and reports `Ready=False` with reason
`ArchivalUnavailable` rather than claiming a success that did not happen. That
is also what you will see against **Temporal Cloud**, where Archival does not
exist at all and [Workflow History
Export](https://docs.temporal.io/cloud/export) takes its place.

#### The Archival URI

`uri` is optional, and usually best left out — the Service supplies its
configured default when a Namespace enables Archival without one. Set it to send
a Namespace somewhere other than that default:

```yaml
spec:
  archival:
    history:
      enabled: true
      uri: s3://temporal-archive/history
    visibility:
      enabled: true
      uri: s3://temporal-archive/visibility
```

Which URI schemes work is entirely up to the archivers your Service is
configured with — `s3://` above is an illustration, not a promise. Temporal
ships filestore, S3 and GCS archivers, and a Service may have any subset of them
or a custom one. The operator puts no scheme restriction in the schema for that
reason, and hands whatever you write to Temporal to validate.

**A Namespace's Archival URI cannot be changed once Temporal holds one.** That
is Temporal's rule, and it applies whether the URI came from this field or from
the Service's default. Three consequences:

* Setting a `uri` on a Namespace that has none works, enabled or not.
* Asking for a *different* `uri` is refused. The operator refuses it locally,
  reporting `Ready=False` with reason `ArchivalURIImmutable`, rather than
  sending a request the Service would reject on every retry. The message names
  both URIs.
* Removing `uri` does not clear it. It only stops the operator having an opinion
  — which is how you resolve an `ArchivalURIImmutable` without replacing the
  Namespace.

Disabling Archival leaves the URI in place too, so re-enabling it later returns
to the same destination.

#### Create

If the Temporal Namespace named by `metadata.name` does not exist, the operator
registers it and records `status.ownership: Created`. Archival goes in the
registration request rather than an update after it, so a Namespace the operator
creates is never briefly unarchived.

#### Adopt

If a Temporal Namespace of that name already exists and carries no other
resource's ownership marker, the operator adopts it: it records
`status.ownership: Adopted` and manages its retention from then on, but will
never delete it.

Adoption only ever takes over the settings the spec actually asks for. An
adopted Namespace's Archival is untouched unless `spec.archival` says otherwise,
and a Namespace belonging to *another* resource is not reconfigured at all — the
ownership check happens before anything is written.

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
  description: |
    ## Payments Nexus

    Handles operations for the **Payments** service.

    See the internal runbook for ownership and escalation.
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
| `spec.description` | string | no | — | yes | Markdown shown on the endpoint's page in Temporal's UI. See [Descriptions](#descriptions). |
| `spec.deletionPolicy` | enum | no | `Delete` | yes | `Delete` or `Orphan`. See [Deletion policy](#deletion-policy). |

Identity is immutable; the target is not. `spec.name` fixes which endpoint this
is, and `connectionRef` fixes which Service it lives in — changing either would
silently point at a different object, so both are rejected. `namespaceRef` and
`taskQueue` describe where the endpoint routes to, and can be changed freely.

#### Descriptions

`spec.description` is the Markdown Temporal's UI renders on the endpoint's page.
It is where to say what the endpoint is for and who looks after it, so whoever
finds it does not have to ask.

```yaml
spec:
  description: |
    ## Payments Nexus

    Handles operations for the **Payments** service.

    See the internal runbook for ownership and escalation.
```

The field is optional, and whether it is present is what decides who owns the
description:

* **Omitted** — the operator leaves the description alone. One set by hand, by
  the Temporal CLI or by another tool survives every update the operator makes.
  This is what the operator has always done, and is still the default.
* **Set** — the description becomes managed state like anything else in the
  spec. The operator writes it on create, and puts it back when it is edited
  behind the operator's back.
* **Set to `""`** — the description is removed.

Removing the field again hands the description back: the operator stops writing
it and leaves whatever is there.

A description change is corrected in the same update as a retarget, because
Temporal replaces the endpoint's spec wholesale and each write bumps a version
the next one has to match.

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
* **Descriptions are managed only when declared.** Leaving `spec.description`
  out preserves whatever is there, as it always has; setting it hands the
  description to the operator. See [Descriptions](#descriptions).
* **One resource per endpoint.** Two resources naming the same Service-wide
  endpoint are unsupported and will fight over it.

### Schedule

A `Schedule` manages one Temporal Schedule in one Temporal Namespace. A Schedule
starts a Workflow Execution automatically, on a timetable you declare.

```yaml
apiVersion: temporal.simonemms.com/v1beta1
kind: Schedule
metadata:
  name: nightly-payments
spec:
  connectionRef:
    name: production
  namespaceRef:
    name: payments

  scheduleId: PaymentsNightly

  schedule:
    cron:
      - "30 2 * * *"
    timeZone: Europe/London
    jitter: 5m

  action:
    workflow:
      type: ReconcilePayments
      taskQueue: payments
      workflowId: payments-nightly
      input:
        - '{"mode":"nightly"}'
      timeouts:
        run: 1h

  policies:
    overlap: Skip
    catchupWindow: 1h
    pauseOnFailure: true

  deletionPolicy: Delete
```

#### Two names again

```text
metadata.name = nightly-payments   the Kubernetes resource name
spec.scheduleId = PaymentsNightly  the Temporal Schedule ID
```

Kubernetes only accepts lowercase names for a resource, and Temporal Schedule
IDs are conventionally PascalCase. Nothing is derived from `metadata.name`:
what you write in `spec.scheduleId` is what Temporal is asked for, casing and
all. This is the same split `SearchAttribute` and `NexusEndpoint` use.

#### Schedule fields

| Field | Type | Required | Default | Mutable | Description |
| --- | --- | --- | --- | --- | --- |
| `spec.scheduleId` | string | yes | — | no | The Temporal Schedule ID. At most 977 characters. |
| `spec.connectionRef.name` | string | yes | — | no | `Connection` in the same Kubernetes namespace. |
| `spec.namespaceRef.name` | string | yes | — | no | `Namespace` in the same Kubernetes namespace. |
| `spec.schedule` | object | yes | — | yes | When the schedule acts. See [Timing](#timing). |
| `spec.action.workflow` | object | yes | — | yes | What it does. See [The workflow action](#the-workflow-action). |
| `spec.policies` | object | no | — | yes | See [Policies](#policies). `catchupWindow` is unmanaged when omitted. |
| `spec.state` | object | no | — | yes | Pausing. See [State](#state-pausing). |
| `spec.notes` | string | no | unmanaged | yes | A human-readable note on the schedule. |
| `spec.memo` | map | no | unmanaged | yes | Non-indexed metadata on the *schedule*. Values are JSON. |
| `spec.searchAttributes` | list | no | unmanaged | yes | Indexed metadata on the *schedule*. |
| `spec.deletionPolicy` | enum | no | `Delete` | yes | `Delete` or `Orphan`. |

`scheduleId`, `connectionRef` and `namespaceRef` are immutable. Changing one
would not move anything — it would point the resource at a different schedule
and strand whatever it was looking after. Delete the resource and make a new one
instead.

#### Timing

Temporal builds a schedule's times from the **union** of three kinds of rule,
minus any exclusions. You can use any combination, and at least one rule is
required or the schedule would never act.

**Cron**, for migrating an existing cron workflow without rewriting its timing:

```yaml
spec:
  schedule:
    cron:
      - "30 2 * * *"
```

Temporal accepts 5, 6 or 7 space-separated fields:

```text
5: minute hour dayOfMonth month dayOfWeek
6: minute hour dayOfMonth month dayOfWeek year
7: second minute hour dayOfMonth month dayOfWeek year
```

The shorthands `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`,
`@midnight` and `@hourly` work, and so does `@every <interval>[/<phase>]`, which
Temporal compiles into an interval rather than a calendar. A `CRON_TZ=<zone>` or
`TZ=<zone>` prefix sets the time zone — leave `timeZone` empty if you use it — and
a `#` comment may follow the expression. Month and day names work (`JAN`,
`MON-FRI`), and day-of-week accepts `7` as another way of writing Sunday.

Note that Temporal does **not** implement the special case some cron
implementations have of treating `dayOfMonth` and `dayOfWeek` as "or" rather
than "and" when both are set.

**Intervals**, counted from the Unix epoch rather than from when you created the
schedule:

```yaml
spec:
  schedule:
    intervals:
      - every: 6h
        offset: 5h
```

`every: 1h` matches every hour on the hour; adding `offset: 19m` matches every
`xx:19:00`. Temporal requires `every` to be at least one second, and `offset` to
be **shorter than** `every` — an offset of a whole period is the same as no
offset, and the Service refuses the ambiguity.

**Calendars**, which are cron written out in full and are far easier to read:

```yaml
spec:
  schedule:
    calendars:
      - hour:
          - start: 9
        minute:
          - start: 15
        dayOfWeek:
          - start: 1
            end: 5
        comment: Weekday mornings
```

Each field is a list of ranges, and a time matches when at least one range of
every field matches it. A range is a `start`, an optional inclusive `end`
(defaulting to `start`) and an optional `step` (defaulting to 1).

**The defaults are what make a calendar readable**, and they are Temporal's:

| Field | Range | Omitted means |
| --- | --- | --- |
| `second` | 0–59 | matches 0 |
| `minute` | 0–59 | matches 0 |
| `hour` | 0–23 | matches 0 |
| `dayOfMonth` | 1–31 | matches every day |
| `month` | 1–12 | matches every month |
| `dayOfWeek` | 0–6, **0 is Sunday** | matches every day |
| `year` | 2000–2100 | matches every year |

So the calendar above fires at 09:15:00 on weekdays, not sixty times a minute.
`dayOfWeek` counting Sunday as 0 is Temporal's numbering, taken from Go's
`time.Weekday`; it is the easiest thing to get backwards, so the API server's
rejection message says which way round it is.

**Mixing them** is supported, because Temporal supports it. This runs every six
hours *and* at 09:15 on weekdays, but never on the first of the month:

```yaml
spec:
  schedule:
    intervals:
      - every: 6h
    calendars:
      - hour:
          - start: 9
        minute:
          - start: 15
        dayOfWeek:
          - start: 1
            end: 5
    excludeCalendars:
      - dayOfMonth:
          - start: 1
```

Every field of an exclusion, seconds included, has to match a time for that time
to be skipped.

##### Time zone, jitter, start and end

```yaml
spec:
  schedule:
    timeZone: Europe/London
    jitter: 5m
    startAt: "2026-01-01T00:00:00Z"
    endAt: "2026-12-31T23:59:59Z"
```

`timeZone` is an IANA name and defaults to UTC. **Calendar matching is literal,
with no special handling of daylight saving**: a calendar firing at 02:30 in a
zone that observes DST will not fire on the day that has no 02:30, and one
firing at 01:30 will fire twice on the day that has two. Use UTC for a schedule
that must be entirely self-contained. The zone is loaded by the Temporal Service
from *its* environment, so a zone this operator can resolve is not automatically
one the Service can.

`jitter` spreads each action out by a random amount between zero and the value
given, so that many schedules sharing a time do not all fire at once. Temporal
caps the delay at the time until the next action, so jitter longer than the
period between actions cannot push an action past its successor.

`startAt` and `endAt` bound the whole schedule. `endAt` retires a schedule
without deleting it, and has to be after `startAt`.

#### The workflow action

```yaml
spec:
  action:
    workflow:
      type: ReconcilePayments      # required
      taskQueue: payments          # required
      workflowId: payments-nightly # optional
      input:
        - '{"mode":"nightly"}'
      memo:
        owner: '"payments-team"'
      searchAttributes:
        - name: CustomerId
          type: Keyword
          value: '"acme"'
      timeouts:
        task: 10s
        run: 1h
        execution: 2h
      priority:
        key: 2
        fairnessKey: tenant-acme
        fairnessWeight: "9"
      staticSummary: Nightly reconciliation
      staticDetails: |
        Runs after the overnight batch.
```

`workflowId` is optional — Temporal appends a timestamp to it so each run is
distinct, and generates a UUID per run when it is left out. Naming it makes runs
far easier to find in the UI.

**The action is fully managed**, which is different from the schedule-level
fields above. `spec.action.workflow` describes the workflow to start, so removing
`input` or `memo` from it means starting the workflow *without* them — not
leaving whatever was there. The preservation guarantee in
[Unmanaged fields are preserved](#unmanaged-fields-are-preserved) is about
fields this API does not model at all, such as a retry policy or a header; those
survive untouched.

`timeouts` are optional and take the namespace's defaults when omitted. A
`priority.key` is 1 to 5, where smaller means sooner; `fairnessKey` is limited to
64 **bytes**, not characters, and `fairnessWeight` is a decimal string between
`"0.001"` and `"1000"`. Temporal clamps a weight outside that range rather than
refusing it, so this API refuses it instead — a spec that does not describe what
happens is worse than a rejected one.

##### Input, memo and Search Attributes

`input` is a list, **one entry per workflow argument**, each written as JSON:

```yaml
input:
  - '{"mode":"nightly"}'   # first argument
  - '42'                   # second argument
```

This is the declarative equivalent of `temporal workflow start --input`: each
entry is encoded with Temporal's default data converter into a `json/plain`
payload, so a worker written against any SDK deserialises them the way it always
would. A workflow taking a single struct argument takes one entry.

The CLI's file and base64 input forms are deliberately absent. A CRD is not a
filesystem, and a payload that cannot be read in the spec cannot be reviewed in
a pull request.

`memo` values are JSON too, so a string value needs its quotes: `'"payments"'`,
not `payments`.

Search Attributes carry their type, because a value cannot be interpreted
without one — `"7"` is an `Int` or a `Text` depending entirely on how the
attribute was registered. The types are the same names a
[`SearchAttribute`](#supported-types) uses, and the attribute has to be
registered on the namespace first:

| Type | Value written as |
| --- | --- |
| `Bool` | `'true'` |
| `Int` | `'7'` |
| `Double` | `'1.5'` |
| `Keyword`, `Text` | `'"gold"'` |
| `KeywordList` | `'["a","b"]'` |
| `Datetime` | `'"2026-01-01T00:00:00Z"'` |

**The schedule and the workflow have separate memos and Search Attributes.**
`spec.memo` and `spec.searchAttributes` are attached to the schedule itself and
show up when listing schedules; `spec.action.workflow.memo` and
`spec.action.workflow.searchAttributes` are attached to each run the schedule
starts. Both are unmanaged when omitted, and setting either to an empty value
clears it.

#### Policies

```yaml
spec:
  policies:
    overlap: Skip
    catchupWindow: 1h
    pauseOnFailure: true
```

`overlap` decides what happens when an action would start while an earlier one
is still running. It defaults to `Skip`, which is Temporal's own default:

| Value | Meaning |
| --- | --- |
| `Skip` | Drop the new action. |
| `BufferOne` | Keep one action waiting; drop the rest. |
| `BufferAll` | Queue every action to run in turn. |
| `CancelOther` | Cancel the running action, then start the new one. |
| `TerminateOther` | Terminate the running action and start the new one. |
| `AllowAll` | Run actions concurrently. |

`catchupWindow` is how late an action may be taken when the Temporal Service was
unavailable at the time it should have run. **Omitted, it is not managed**: the
Service applies its own default of one year, stores it, and reports it back, so
the operator leaves whatever is there alone rather than fighting a value it did
not set. Its minimum is **ten seconds**; a shorter window is silently raised to
ten seconds by the Service, so this API refuses it instead.

`pauseOnFailure` pauses the whole schedule when an action fails or times out,
after its retry policy is exhausted. With `overlap: AllowAll` the pause may not
stop the next action, because that one may already have started.

#### State: pausing

Temporal treats a schedule's state as **operational** rather than declarative: a
person can pause a schedule from the UI, and `pauseOnFailure` pauses one without
anybody asking. So each field here is managed only while it is present:

```text
state omitted, or state.paused omitted
-> pausing is not managed; a human can pause and resume freely

state.paused: true
-> the operator keeps the schedule paused

state.paused: false
-> the operator keeps the schedule running, putting back a pause
   applied behind its back
```

That is what the pointer buys you. Somebody stopping a schedule to investigate
an incident will not find the operator resuming it five minutes later — unless
you asked for that explicitly.

```yaml
spec:
  state:
    paused: false
```

**Pausing is the only state you can declare.** Temporal's remaining-actions count
is runtime state consumed as Schedule actions execute — the Service ticks it down
from 10 to 9 to 8 as the schedule acts — so the operator deliberately does not
manage it as desired state. Reconciling it would write the declared number back
every time the operator looked, and a schedule asked to run ten times would run
for ever. The count is preserved untouched on every update; it is simply not
something this API lets you set. Use `temporal schedule update` if you need to
change it.

Pausing is different, and that is why it is here: it is a state rather than a
budget, and it stays where it is put until somebody moves it.

`spec.notes` is the same shape of decision as pausing. Temporal **overwrites**
the note itself — pausing a schedule from the UI or through `pauseOnFailure`
replaces it with the Service's own explanation — so declaring a note means the
operator will put yours back, erasing why the schedule paused.

#### Drift, and why Temporal makes it awkward

Temporal does not give back what it was given. A cron expression is compiled
into a structured calendar on the way in and the original discarded, so
describing the schedule afterwards never shows the cron you wrote. Comparing the
spec against what the Service reports would therefore see permanent drift and
rewrite the schedule on every resync.

The operator records two fingerprints in `status` instead:

* `status.desiredSpecHash` — the timing this resource last asked for. Comparing
  it against the current spec answers *has the resource changed since*.
* `status.appliedSpecHash` — the timing Temporal reported immediately after the
  operator last wrote it. Comparing it against what Temporal reports now answers
  *has anybody changed it on the Service since*.

Between them, the operator writes when something has actually changed and stays
quiet otherwise. Everything else — the action, the policies, the declared state
— round-trips faithfully and is compared directly.

#### Unmanaged fields are preserved

Temporal's update API **replaces the schedule wholesale**: its own protobuf says
the four main fields — spec, action, policies and state — "are replaced
completely by the values in this message". Anything not sent back is destroyed.

So the operator never builds an update from scratch. It reads the schedule,
overwrites only the fields this CRD declares, and sends the rest back untouched,
using Temporal's conflict token so a schedule modified in between is refused
rather than clobbered. A retry policy, a workflow header, a
`keepOriginalWorkflowId` — none of them is expressible in this CRD, and all of
them survive an update that changes the timing.

That extends to fields added to Temporal after this operator was written, which
is why the operator works at the protobuf level rather than through the Go SDK's
schedule client: the SDK's own model has no `keepOriginalWorkflowId` and no
workflow header, so updating through it would silently drop both.

#### Adopting an existing Schedule

If a Temporal Schedule with that ID already exists, the operator adopts it:
`status.ownership: Adopted`, and deleting the resource leaves the schedule
running.

**Adoption decides deletion rights, not whether declared fields are managed.**
An adopted schedule still gets the configuration the spec asks for, and the
reconcile that adopts it writes once — the operator has no recorded fingerprint
for a schedule it has never written, and cannot compare a cron expression
against a compiled calendar, so writing is the only answer that leaves the
schedule matching the spec. It is safe, because the update preserves every field
the operator does not manage. Afterwards the fingerprints are recorded and the
schedule is left alone until something actually changes.

Unlike a Temporal Namespace, a Schedule carries **no ownership marker the
operator can verify**. The only durable free-form space on a schedule is its
memo, and this CRD hands that to you — writing an operator key into a map you
also declare would mean either fighting over the map or letting a spec change
delete the marker. So ownership is controller bookkeeping in `status`, exactly
as it is for `SearchAttribute` and `NexusEndpoint`. See
[Ownership and adoption](#ownership-and-adoption).

#### Validation

A `Schedule` is checked in three places, and the split is deliberate:

1. **Kubernetes admission**, for everything OpenAPI and CEL can express:
   required fields, calendar bounds, the overlap enum, string and collection
   limits, the fairness key's 64-byte limit, immutability, and that at least one
   timing rule is present. Most mistakes are caught by `kubectl apply`.
2. **The operator, before it dials anything**, for what a schema cannot do:
   parsing cron expressions, loading IANA time zones, comparing durations
   (`offset` against `every`), and decoding Search Attribute values against
   their declared types. These report `Ready=False` with reason
   `InvalidSchedule` and do not retry — only a spec change fixes them.
3. **The Temporal Service**, which remains the final authority on anything
   specific to how it is configured.

Cron is validated against Temporal's own grammar rather than a general-purpose
cron library, because the general-purpose ones differ from Temporal in ways that
matter — field counts, shorthands, `@every` — and wrongly rejecting a schedule
Temporal would accept is worse than not checking at all.

#### Limits mirrored from Temporal

| Limit | Value | Source |
| --- | --- | --- |
| Schedule ID length | 977 | `limit.maxIDLength` (1000) minus the Service's `temporal-sys-scheduler:` prefix |
| Workflow ID length | 979 | the same limit minus the timestamp the Service appends |
| Calendar comment | 200 | `maxCommentLen` |
| Calendar year | 2000–2100 | `minCalendarYear`, `maxCalendarYear` |
| Interval minimum | 1s | `validateInterval` |
| Catch-up window minimum | 10s | `MinCatchupWindow` |
| Fairness key | 64 bytes | the `Priority` protobuf |
| Fairness weight | 0.001–1000 | the `Priority` protobuf |

**Nothing on a `Schedule` is capped by count.** Temporal imposes no limit on how
many calendars, intervals or cron expressions a schedule may have, so neither
does this: if Temporal accepts the collection, so does the operator.

That is also why each calendar field's bounds are plain `minimum` and `maximum`
on the field rather than a CEL rule. Kubernetes refuses a CRD whose CEL rules
could run over an unbounded list, so a rule there would have forced an arbitrary
cap on how many calendars a schedule may have. The one rule that genuinely needs
CEL — that a range's `end` is not before its `start` — is checked by the operator
instead, and reported as `InvalidSchedule`.

#### Schedule limitations

* **No backfill or trigger.** Both are imperative one-off operations rather than
  declarative state, so they stay with `temporal schedule backfill` and
  `temporal schedule trigger`.
* **No retry policy, header or versioning override on the action.** They are not
  modelled, but they are *preserved* — see
  [Unmanaged fields are preserved](#unmanaged-fields-are-preserved).
* **Ownership is not externally verifiable**, as above.
* **One resource per schedule.** Two resources naming the same schedule ID in
  the same Temporal Namespace are unsupported and will fight over it.
* **`spec.action` is a workflow and nothing else.** Temporal's action is a union
  with one member today; the nesting leaves room for whatever it adds.

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
| `ArchivalUnavailable` | (`Namespace`) The Service accepted an Archival change and did not apply it, which is what a Service not configured for Archival does — Temporal Cloud included. Configure Archival on the Service, or remove `spec.archival`. |
| `ArchivalURIImmutable` | (`Namespace`) `spec.archival` asks for an Archival URI other than the one Temporal already holds. Temporal will not change it. Remove the `uri` to keep the existing destination, or replace the Namespace. |
| `TypeConflict` | (`SearchAttribute`) An attribute of that name exists with a different type. Temporal cannot retype it; pick a different name or remove the existing attribute. |
| `EndpointTaken` | (`NexusEndpoint`) An endpoint of that name already exists Service-wide and was not created by this resource. |
| `Conflict` | (`NexusEndpoint`, `Schedule`) The object changed between being read and written. Self-correcting; the reconcile retries from a fresh read, and gives up to the next requeue if it never settles. |
| `InvalidSchedule` | (`Schedule`) The spec describes a schedule Temporal would not accept — an unparseable cron expression, an unknown time zone, an offset as long as its interval. Only a spec change fixes it, so it is not retried. |
| `SchedulesNotAllowed` | (`Schedule`) The Temporal Service has schedules switched off for this namespace. Enable them on the Service; rechecked on the ordinary resync. |

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
| `Schedule` | `temporal.simonemms.com/schedule` |

## Temporal Cloud and self-hosted

Both are supported, and the only thing that differs is the `Connection`.
Temporal Cloud needs TLS and credentials — an API key or an mTLS certificate
pair — while a self-hosted dev server typically needs neither. Everything
downstream of the `Connection` is written the same way against either.

The one exception is `spec.archival` on a `Namespace`:

> Temporal Cloud does not support Temporal Server Archival. Use Temporal Cloud
> [Export](https://docs.temporal.io/cloud/export) instead. Declaring archival
> against a Service where Archival is unavailable causes the `Namespace` to
> report `ArchivalUnavailable` rather than falsely reporting convergence.

For self-hosted Temporal, the Service itself must be configured with an archival
provider and its namespace defaults before any `Namespace` can enable Archival.
See [Archival](#archival) and Temporal's [self-hosted Archival
setup](https://docs.temporal.io/self-hosted-guide/archival). Cloud Export is
configured through Temporal Cloud rather than the Temporal Namespace API, so
this operator does not manage it.

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
* `spec.archival` configures Archival on a Namespace. It does not provision the
  archival storage, credentials or archiver plugin — those are Temporal Service
  configuration, and must exist before a Namespace can enable Archival.
* A Namespace's Archival URI cannot be changed once Temporal holds one. The
  operator reports `ArchivalURIImmutable` rather than retrying; moving a
  Namespace's archive destination means replacing the Temporal Namespace.
* Temporal Cloud has no Server Archival, so `spec.archival` cannot be satisfied
  there. Cloud Export is the Cloud capability, and it is configured outside the
  Temporal Namespace API, so the operator does not manage it.
* Two Kubernetes resources targeting the same Temporal Search Attribute or the
  same Nexus Endpoint are unsupported and will fight over it.
* Nexus Endpoints support worker targets only; external URL targets are not
  modelled.
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
