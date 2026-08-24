# temporal-resource-operator

A Kubernetes operator for declaratively managing resources in an existing
Temporal Service.

This chart installs the operator only. It does not install Temporal, and it
does not create any Temporal resources — you create those yourself as custom
resources once the operator is running.

## Install

The chart is published as an OCI artifact to the GitHub Container Registry:

```sh
oci://ghcr.io/mrsimonemms/charts/temporal-resource-operator
```

```sh
helm install temporal-resource-operator \
  oci://ghcr.io/mrsimonemms/charts/temporal-resource-operator \
  --version <version> \
  --namespace temporal-resource-operator-system \
  --create-namespace
```

Any namespace works — the chart installs into `.Release.Namespace`. The
namespace above is a convention, not a requirement.

Installing under the chart's own release name keeps the rendered names short
(`temporal-resource-operator`, `temporal-resource-operator-manager` and so on).
A different release name is prefixed in the usual Helm way.

## Upgrade

```sh
helm upgrade temporal-resource-operator \
  oci://ghcr.io/mrsimonemms/charts/temporal-resource-operator \
  --version <version> \
  --namespace temporal-resource-operator-system
```

## Uninstall

Custom resources use finalizers that only the operator releases, so delete them
**before** removing the operator:

```sh
kubectl delete tnx,tsa,tns,connections --all --all-namespaces
helm uninstall temporal-resource-operator \
  --namespace temporal-resource-operator-system
```

Anything with `deletionPolicy: Delete` has its Temporal object removed as part
of that first step. Patch the policy to `Orphan` first if the Temporal side
should survive.

## Custom resource definitions

The four CRDs (`Connection`, `Namespace`, `SearchAttribute`, `NexusEndpoint`)
ship in the chart's `crds/` directory. That is the standard Helm arrangement,
and it comes with Helm's standard limitation:

* CRDs are installed on first install, if they are not already present
* CRDs are **not** upgraded by `helm upgrade`
* CRDs are **not** removed by `helm uninstall`

So a release that changes a CRD needs the new definitions applied by hand
before or during the upgrade:

```sh
kubectl apply --server-side -f \
  https://raw.githubusercontent.com/mrsimonemms/temporal-resource-operator/<tag>/charts/temporal-resource-operator/crds/
```

Leaving CRDs behind on uninstall is deliberate on Helm's part — deleting a CRD
deletes every resource of that kind. Remove them explicitly when you really
mean it:

```sh
kubectl delete crd connections.temporal.simonemms.com \
  namespaces.temporal.simonemms.com \
  searchattributes.temporal.simonemms.com \
  nexusendpoints.temporal.simonemms.com
```

The chart's CRDs are copies of the ones generated from the Kubebuilder markers
in `api/v1alpha1`. CI fails if the two ever drift.

## Values

| Value | Default | Purpose |
| --- | --- | --- |
| `replicaCount` | `1` | Manager replicas. Leader election means only one is active. |
| `image.repository` | `ghcr.io/mrsimonemms/temporal-resource-operator` | Manager image. |
| `image.tag` | `""` | Defaults to the chart `appVersion`. |
| `image.pullPolicy` | `IfNotPresent` | |
| `imagePullSecrets` | `[]` | |
| `nameOverride` | `""` | Overrides the chart name in labels and names. |
| `fullnameOverride` | `""` | Replaces the generated resource name outright. |
| `serviceAccount.create` | `true` | Create a ServiceAccount for the manager. |
| `serviceAccount.name` | `""` | Existing ServiceAccount to use instead. |
| `serviceAccount.annotations` | `{}` | For workload identity and similar. |
| `podAnnotations` | `{}` | |
| `podLabels` | `{}` | |
| `podSecurityContext` | `runAsNonRoot`, `RuntimeDefault` seccomp | |
| `securityContext` | no privilege escalation, read-only root, all capabilities dropped | |
| `resources` | 500m/128Mi limits, 10m/64Mi requests | |
| `metrics.enabled` | `true` | Serve controller-runtime metrics over HTTPS. |
| `metrics.port` | `8443` | |
| `metrics.service.type` | `ClusterIP` | |
| `metrics.service.annotations` | `{}` | |
| `nodeSelector` | `{}` | |
| `tolerations` | `[]` | |
| `affinity` | `{}` | |
| `priorityClassName` | `""` | |
| `terminationGracePeriodSeconds` | `10` | |

### Metrics

With `metrics.enabled` the manager serves HTTPS metrics on `metrics.port`,
protected by Kubernetes authentication and authorisation. The chart creates the
`TokenReview`/`SubjectAccessReview` permissions the manager needs, a
`ClusterIP` Service, and a `<release>-metrics-reader` ClusterRole you can bind
to whatever scrapes it. Setting `metrics.enabled=false` switches the endpoint
off and creates none of that.

The chart does not create a `ServiceMonitor` and does not depend on
cert-manager.

### Granting users access to the custom resources

The chart creates the permissions the operator needs, and nothing else. The
per-kind `admin`/`editor`/`viewer` ClusterRoles that the Kustomize install
lays down are convenience roles for handing access to people, not something the
operator uses, so they are not part of the chart. Apply them from
[`config/rbac`](https://github.com/mrsimonemms/temporal-resource-operator/tree/main/config/rbac)
if you want them.

## Multiple releases

Cluster-scoped resources — the ClusterRoles and ClusterRoleBindings — are named
after the release, so two differently named releases coexist. Two releases with
the *same* name in different namespaces would collide on those names; use
distinct release names, or `fullnameOverride`, if you need that.

CRDs are cluster-scoped and named by the API, so they are shared by every
release on the cluster. Running two operator releases against the same CRDs is
not a supported configuration.

## Documentation

The custom resource schemas, ownership and adoption rules, deletion policies
and troubleshooting guide live in the
[project README](https://github.com/mrsimonemms/temporal-resource-operator#readme).
