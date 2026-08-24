# temporal-resource-operator - AI Agent Guide

## Project Structure

**Single-group layout (default):**

```text
cmd/main.go                    Manager entry (registers controllers/webhooks)
api/v1beta1/*_types.go         CRD schemas (+kubebuilder markers)
api/v1beta1/zz_generated.*     Auto-generated (DO NOT EDIT)
internal/controller/*          Reconciliation logic
internal/webhook/*             Validation/defaulting (if present)
config/crd/bases/*             Generated CRDs (DO NOT EDIT)
config/rbac/role.yaml          Generated RBAC (DO NOT EDIT)
config/samples/*               Example CRs (edit these)
Makefile                       Build/test/deploy commands
PROJECT                        Kubebuilder metadata Auto-generated (DO NOT EDIT)
charts/temporal-resource-operator/  Helm chart - the public install method
charts/.../crds/*              Derived from config/crd/bases, gitignored
```

**Multi-group layout** (for projects with multiple API groups):

```text
api/<group>/<version>/*_types.go       CRD schemas by group
internal/controller/<group>/*          Controllers by group
internal/webhook/<group>/<version>/*   Webhooks by group and version (if present)
```

Multi-group layout organizes APIs by group name (e.g., `batch`, `apps`).
Check the `PROJECT` file for `multigroup: true`.

**To convert to multi-group layout:**

1. Run: `kubebuilder edit --multigroup=true`
2. Move the APIs, controllers and webhooks into their group directories:

   ```bash
   mkdir -p api/<group> && mv api/<version> api/<group>/

   mkdir -p internal/controller/<group> && \
     mv internal/controller/*.go internal/controller/<group>/

   # Webhooks, if present
   mkdir -p internal/webhook/<group> && \
     mv internal/webhook/<version> internal/webhook/<group>/
   ```

3. Update import paths in all files
4. Fix `path` in `PROJECT` file for each resource
5. Update test suite CRD paths (add one more `..` to relative paths)

## Critical Rules

### The API Is `v1beta1`, And Only `v1beta1`

The Go package is `api/v1beta1`; the served API is
`temporal.simonemms.com/v1beta1`. All four CRDs expose exactly one version.

`v1alpha1` existed only while the repository was private and was renamed to
`v1beta1` before the first public release. It was never publicly served, so
there is nothing to convert and nothing to support: do not add an `api/v1alpha1`
package, a second CRD version, a conversion webhook or any migration tooling. A
future breaking change means a new version, deliberately introduced.

### Never Edit These (Auto-Generated)

- `config/crd/bases/*.yaml` - from `make manifests`
- `config/rbac/role.yaml` - from `make manifests`
- `config/webhook/manifests.yaml` - from `make manifests`
- `**/zz_generated.*.go` - from `make generate`
- `PROJECT` - from `kubebuilder [OPTIONS]`

### Never Remove Scaffold Markers

Do NOT delete `// +kubebuilder:scaffold:*` comments. CLI injects code at these
markers.

### Keep Project Structure

Do not move files around. The CLI expects files in specific locations.

### Always Use CLI Commands

Always use `kubebuilder create api` and `kubebuilder create webhook` to
scaffold. Do NOT create files manually.

### E2E Tests Require an Isolated Kind Cluster

The e2e tests are designed to validate the solution in an isolated environment
(similar to GitHub Actions CI). Ensure you run them against a dedicated
[Kind](https://kind.sigs.k8s.io/) cluster (not your “real” dev/prod cluster).

## After Making Changes

**After editing `*_types.go` or markers:**

```bash
make manifests  # Regenerate CRDs/RBAC from markers
make generate   # Regenerate DeepCopy methods
```

**After editing `*.go` files:**

```bash
make lint-fix   # Auto-fix code style
make test       # Run unit tests
```

## CLI Commands Cheat Sheet

### Create API (your own types)

```bash
kubebuilder create api --group <group> --version <version> --kind <Kind>
```

### Deploy Image Plugin (scaffold to deploy/manage ANY container image)

Generate a controller that deploys and manages a container image (nginx, redis,
memcached, your app, etc.):

```bash
# Example: deploying memcached
kubebuilder create api --group example.com --version v1alpha1 --kind Memcached \
  --image=memcached:alpine \
  --plugins=deploy-image.go.kubebuilder.io/v1-alpha
```

Scaffolds good-practice code: reconciliation logic, status conditions,
finalizers, RBAC. Use as a reference implementation.

### Create Webhooks

```bash
# Validation + defaulting
kubebuilder create webhook --group <group> --version <version> --kind <Kind> \
  --defaulting --programmatic-validation

# Conversion webhook (for multi-version APIs)
kubebuilder create webhook --group <group> --version v1 --kind <Kind> \
  --conversion --spoke v2
```

### Controller for Core Kubernetes Types

```bash
# Watch Pods
kubebuilder create api --group core --version v1 --kind Pod \
  --controller=true --resource=false

# Watch Deployments
kubebuilder create api --group apps --version v1 --kind Deployment \
  --controller=true --resource=false
```

### Controller for External Types (e.g., from other operators)

Watch resources from external APIs (cert-manager, Argo CD, Istio, etc.):

```bash
# Example: watching cert-manager Certificate resources
kubebuilder create api \
  --group cert-manager --version v1 --kind Certificate \
  --controller=true --resource=false \
  --external-api-path=github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1 \
  --external-api-domain=io \
  --external-api-module=github.com/cert-manager/cert-manager
```

**Note:** Use `--external-api-module=<module>@<version>` only if you need a
specific version. Otherwise, omit `@<version>` to use what's in `go.mod`.

### Webhook for External Types

```bash
# Example: validating external resources
kubebuilder create webhook \
  --group cert-manager --version v1 --kind Issuer \
  --defaulting \
  --external-api-path=github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1 \
  --external-api-domain=io \
  --external-api-module=github.com/cert-manager/cert-manager
```

## Testing & Development

```bash
make test              # Run unit tests (uses envtest: real K8s API + etcd)
make run               # Run locally (uses current kubeconfig context)
```

Tests use **Ginkgo + Gomega** (BDD style). Check `suite_test.go` for setup.

## Deployment Workflow

```bash
# 1. Regenerate manifests
make manifests generate

# 2. Build & deploy
export IMG=<registry>/<project>:tag
make docker-build docker-push IMG=$IMG  # Or: kind load docker-image $IMG --name <cluster>
make deploy IMG=$IMG

# 3. Test
kubectl apply -k config/samples/

# 4. Debug
kubectl logs -n <project>-system deployment/<project>-controller-manager -c manager -f
```

### API Design

**Key markers for** `api/<version>/*_types.go`:

```go
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"

// On fields:
// +kubebuilder:validation:Required
// +kubebuilder:validation:Minimum=1
// +kubebuilder:validation:MaxLength=100
// +kubebuilder:validation:Pattern="^[a-z]+$"
// +kubebuilder:default="value"
```

- **Use** `metav1.Condition` for status (not custom string fields)
- **Use predefined types**: `metav1.Time` instead of `string` for dates
- **Follow K8s API conventions**: Standard field names (`spec`, `status`,
  `metadata`)

### Controller Design

**RBAC markers in** `internal/controller/*_controller.go`:

```go
// +kubebuilder:rbac:groups=mygroup.example.com,resources=mykinds,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mygroup.example.com,resources=mykinds/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mygroup.example.com,resources=mykinds/finalizers,verbs=update
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
```

**Implementation rules:**

- **Idempotent reconciliation**: Safe to run multiple times
- **Re-fetch before updates**: `r.Get(ctx, req.NamespacedName, obj)` before
  `r.Update` to avoid conflicts
- **Structured logging**: `log := log.FromContext(ctx); log.Info("msg", "key",
  val)`
- **Owner references**: Enable automatic garbage collection
  (`SetControllerReference`)
- **Watch secondary resources**: Use `.Owns()` or `.Watches()`, not just
  `RequeueAfter`
- **Finalizers**: Clean up external resources (buckets, VMs, DNS entries)

### Logging

**Follow Kubernetes logging message style guidelines:**

- Start from a capital letter
- Do not end the message with a period
- Active voice: subject present (`"Deployment could not create Pod"`) or
  omitted (`"Could not create Pod"`)
- Past tense: `"Could not delete Pod"` not `"Cannot delete Pod"`
- Specify object type: `"Deleted Pod"` not `"Deleted"`
- Balanced key-value pairs

```go
log.Info("Starting reconciliation")
log.Info("Created Deployment", "name", deploy.Name)
log.Error(err, "Failed to create Pod", "name", name)
```

**Reference:**
[Kubernetes logging message style guidelines](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-instrumentation/logging.md#message-style-guidelines)

### Webhooks

- **Create all types together**:
  `--defaulting --programmatic-validation --conversion`
- **When `--force` is used**: Backup custom logic first, then restore after
  scaffolding
- **For multi-version APIs**: Use hub-and-spoke pattern (`--conversion --spoke
  v2`)
  - Hub version: Usually oldest stable version (v1)
  - Spoke versions: Newer versions that convert to/from hub (v2, v3)
  - Example: `--group crew --version v1 --kind Captain --conversion --spoke v2`
    (v1 is hub, v2 is spoke)

### Learning from Examples

The **deploy-image plugin** scaffolds a complete controller following good
practices. Use it as a reference implementation:

```bash
kubebuilder create api --group example --version v1alpha1 --kind MyApp \
  --image=<your-image> --plugins=deploy-image.go.kubebuilder.io/v1-alpha
```

Generated code includes: status conditions (`metav1.Condition`), finalizers,
owner references, events, idempotent reconciliation.

## Distribution Options

### Option 1: YAML Bundle (Kustomize)

```bash
# Generate dist/install.yaml from Kustomize manifests
make build-installer IMG=<registry>/<project>:tag
```

**Key points:**

- The `dist/install.yaml` is generated from Kustomize manifests (CRDs, RBAC,
  Deployment)
- Commit this file to your repository for easy distribution
- Users only need `kubectl` to install (no additional tools required)

**Example:** Users install with a single command:

```bash
kubectl apply -f https://raw.githubusercontent.com/<org>/<repo>/<tag>/dist/install.yaml
```

### Option 2: Helm Chart (the public install method)

The chart lives in `charts/temporal-resource-operator/` and is hand-maintained.
It is **not** generated by the `helm/v2-alpha` kubebuilder plugin, so do not run
`kubebuilder edit --plugins=helm/...` - it would write a second, conflicting
chart.

Releases publish it as an OCI artifact to
`ghcr.io/mrsimonemms/charts/temporal-resource-operator`. Chart versions drop the
`v`: Git tag `v0.1.0` gives chart `0.1.0` with `appVersion: v0.1.0`, and the
chart's default image is the image that tag published. CI packages release
versions with `helm package --version/--app-version`; `Chart.yaml` keeps a
`0.0.0-dev` version in source and is never rewritten by CI.

**CRDs are copied, not authored.** The source of truth is still the
`+kubebuilder:rbac`/CRD markers in `api/v1beta1`, via `config/crd/bases`. After
any change to `*_types.go`:

```bash
make manifests    # regenerate config/crd/bases
make helm-sync    # copy them into the chart
```

`make helm-check` fails if the two have drifted, and CI runs it. The chart's
RBAC templates are hand-written but must stay faithful to
`config/rbac/role.yaml` - cross-check after changing any RBAC marker.

**Validation:**

```bash
make helm-test    # helm-check, lint, unittest, template, package
make helm-smoke   # install into the current Kind cluster and check it works
```

`make helm-smoke` needs a Kind cluster to exist and builds/loads the controller
image itself. Unit tests are helm-unittest suites in `charts/*/tests/`, which
need the plugin:

```bash
helm plugin install --verify=false https://github.com/helm-unittest/helm-unittest
```

Chart templates must install into `.Release.Namespace`. Never hard-code
`temporal-resource-operator-system` in a template - that namespace belongs to
the Kustomize install only.

### Publish Container Image

```bash
export IMG=<registry>/<project>:<version>
make docker-build docker-push IMG=$IMG
```

## References

### Essential Reading

- [Kubebuilder Book](https://book.kubebuilder.io) (comprehensive guide)
- [controller-runtime FAQ](https://github.com/kubernetes-sigs/controller-runtime/blob/main/FAQ.md)
  (common patterns and questions)
- [Good Practices](https://book.kubebuilder.io/reference/good-practices.html)
  (why reconciliation is idempotent, status conditions, etc.)
- [Logging Conventions](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-instrumentation/logging.md#message-style-guidelines)
  (message style, verbosity levels)

### API Design & Implementation

- [API Conventions](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md)
- [Operator Pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/)
- [Markers Reference](https://book.kubebuilder.io/reference/markers.html)

### Tools & Libraries

- [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime)
- [controller-tools](https://github.com/kubernetes-sigs/controller-tools)
- [Kubebuilder Repo](https://github.com/kubernetes-sigs/kubebuilder)
