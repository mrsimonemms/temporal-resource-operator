//go:build e2e
// +build e2e

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

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mrsimonemms/temporal-resource-operator/test/utils"
)

// namespace where the project is deployed in
const namespace = "temporal-resource-operator-system"

// serviceAccountName created for the project
const serviceAccountName = "temporal-resource-operator-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "temporal-resource-operator-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "temporal-resource-operator-metrics-binding"

// controllerDeploymentName is the Deployment running the manager, in the
// namespace above.
const controllerDeploymentName = "temporal-resource-operator-controller-manager"

// watchResponseWindow is how long a Namespace is given to notice a change to
// its Connection.
//
// It is deliberately well under the controller's 30s fallback retry, so a pass
// cannot be explained away by the timer, and comfortably over the fraction of a
// second the watch actually takes.
const watchResponseWindow = 10 * time.Second

// ownerMarkerKey mirrors the Temporal namespace metadata key the controller
// stamps on namespaces it registers. It is spelled out here rather than
// imported, because it is the externally visible contract these specs exist to
// pin down.
const ownerMarkerKey = "temporal.simonemms.com/owner-uid"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		_, _ = utils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				By("getting the name of the controller-manager pod")
				cmd := exec.Command(
					"kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				By("validating the pod's status")
				cmd = exec.Command(
					"kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd := exec.Command(
				"kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=temporal-resource-operator-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("ensuring the controller pod is ready")
			verifyControllerPodReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", controllerPodName, "-n", namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"), "Controller pod not ready")
			}
			Eventually(verifyControllerPodReady, 3*time.Minute, time.Second).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("Serving metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted, 3*time.Minute, time.Second).Should(Succeed())

			// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": [
								"for i in $(seq 1 30); do curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics && exit 0 || sleep 2; done; exit 1"
							],
							"securityContext": {
								"readOnlyRootFilesystem": true,
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccountName": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			verifyMetricsAvailable := func(g Gomega) {
				metricsOutput, err := getMetricsOutput()
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
				g.Expect(metricsOutput).NotTo(BeEmpty())
				g.Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
			}
			Eventually(verifyMetricsAvailable, 2*time.Minute).Should(Succeed())
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks
	})

	Context("Connection", func() {
		// The Temporal dev server installed by `make install-dependencies`.
		const temporalAddress = "cluster.temporal.svc.cluster.local:7233"

		// Start every spec from a clean slate, so a leftover Connection cannot
		// make a spec pass against a status it did not produce.
		BeforeEach(func() {
			cmd := exec.Command("kubectl", "delete", "connection", "--all", "-n", "default", "--wait=true")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to remove existing Connections")
		})

		AfterEach(func() {
			cmd := exec.Command("kubectl", "delete", "connection", "--all", "-n", "default")
			_, _ = utils.Run(cmd)
		})

		It("should become Ready against the Temporal Service", func() {
			By("waiting for the Temporal Service to be available")
			cmd := exec.Command("kubectl", "wait", "--for=condition=Available",
				"deployment/temporal", "-n", "temporal", "--timeout=5m")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Temporal did not become available")

			By("applying the Connection sample")
			cmd = exec.Command("kubectl", "apply", "-n", "default",
				"-f", "config/samples/temporal_v1beta1_connection.yaml")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to apply the Connection sample")

			By("waiting for the Connection to report Ready=True")
			Eventually(func(g Gomega) {
				g.Expect(connectionField("connection-sample", "{.status.observedGeneration}")).
					To(Equal(connectionField("connection-sample", "{.metadata.generation}")))
				g.Expect(connectionReady("connection-sample")).To(Equal("True"))
				g.Expect(connectionReadyReason("connection-sample")).To(Equal("Connected"))
			}, 3*time.Minute).Should(Succeed())
		})

		It("should report Ready=False for an unreachable Temporal Service", func() {
			By("creating a Connection pointing at nothing")
			cmd := exec.Command("kubectl", "apply", "-n", "default", "-f", "-")
			cmd.Stdin = strings.NewReader(`apiVersion: temporal.simonemms.com/v1beta1
kind: Connection
metadata:
  name: connection-unreachable
spec:
  address: does-not-exist.temporal.svc.cluster.local:7233
`)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create the Connection")

			By("waiting for the Connection to report Ready=False")
			Eventually(func(g Gomega) {
				g.Expect(connectionReady("connection-unreachable")).To(Equal("False"))
				g.Expect(connectionReadyReason("connection-unreachable")).To(Equal("ConnectionFailed"))
			}, 3*time.Minute).Should(Succeed())
		})

		It("should reject a Connection with both credential sources", func() {
			cmd := exec.Command("kubectl", "apply", "-n", "default", "-f", "-")
			cmd.Stdin = strings.NewReader(`apiVersion: temporal.simonemms.com/v1beta1
kind: Connection
metadata:
  name: connection-invalid
spec:
  address: ` + temporalAddress + `
  credentials:
    apiKey: some-key
  credentialsSecretRef:
    name: some-secret
`)
			_, err := utils.Run(cmd)
			Expect(err).To(HaveOccurred(), "The CRD should reject conflicting credentials")
			Expect(err.Error()).To(ContainSubstring("mutually exclusive"))
		})
	})

	Context("Namespace", func() {
		// A name unique to this run and this spec. Temporal deletes namespaces
		// asynchronously, so reusing one across specs would let a leftover
		// namespace satisfy a later spec; a fresh name also means the "namespace
		// did not exist and was registered" path is genuinely exercised.
		var temporalNamespace string

		BeforeEach(func() {
			temporalNamespace = fmt.Sprintf("e2e-ns-%d-%d",
				GinkgoRandomSeed(), CurrentSpecReport().LineNumber())

			By("removing any Connections and Namespaces left by an earlier spec")
			clearOperatorResources()

			By("waiting for the Temporal Service to be available")
			cmd := exec.Command("kubectl", "wait", "--for=condition=Available",
				"deployment/temporal", "-n", "temporal", "--timeout=5m")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Temporal did not become available")
		})

		AfterEach(func() {
			By("removing the Kubernetes resources")
			clearOperatorResources()

			// Adopted and orphaned namespaces are deliberately left behind by
			// the operator, so remove this one here to keep a reused cluster
			// predictable. Namespaces the operator owned are already gone, and
			// asking again is a harmless no-op.
			By("removing the Temporal namespace")
			removeTemporalNamespace(temporalNamespace)
		})

		// Archival is configured at the Temporal Service level and switched on
		// per namespace. The e2e Service is `temporal server start-dev`, which
		// ships no archival provider, so there is no honest happy path to test
		// here - and inventing one would test the fake rather than the operator.
		//
		// What this Service *does* guarantee is the failure mode that matters
		// most, and it was confirmed against the same image the e2e deployment
		// uses (temporalio/temporal, Server 1.31.2) before this spec was
		// written:
		//
		//	$ temporal operator namespace update --history-archival-state enabled probe
		//	Namespace probe update succeeded.
		//	$ temporal operator namespace describe probe
		//	Config.HistoryArchivalState  Disabled
		//
		// The Service accepts the request, answers successfully and applies
		// nothing, because ClusterConfiguredForArchival() is false. Registration
		// behaves identically. That is exactly what the operator must not read
		// as convergence, so this spec proves end to end that it does not.
		//
		// Should the dev server ever gain a configured archival provider, this
		// spec fails loudly rather than silently passing - which is the right
		// way round. Rewrite it as a happy path at that point.
		It("should refuse to claim archival converged on a Service without it", func() {
			By("applying a ready Connection")
			applyConnectionSample()

			By("applying a Namespace asking for history archival")
			applyNamespaceWithArchival(temporalNamespace, "36h", true)

			By("waiting for the Namespace to report Ready=False with reason ArchivalUnavailable")
			Eventually(func(g Gomega) {
				g.Expect(namespaceField(temporalNamespace, "{.status.observedGeneration}")).
					To(Equal(namespaceField(temporalNamespace, "{.metadata.generation}")))
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("ArchivalUnavailable"))
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("False"))
			}, 3*time.Minute).Should(Succeed())

			By("confirming the Temporal Service really did ignore the request")
			described := describeTemporalNamespace(temporalNamespace)
			Expect(described.Config.HistoryArchivalState).To(Equal(archivalStateDisabled),
				"this spec is only meaningful while the dev server has no archival provider")

			By("confirming everything else about the namespace still converged")
			Expect(described.Config.WorkflowExecutionRetentionTTL).To(Equal(retentionSeconds(36)))
			Expect(namespaceOwnership(temporalNamespace)).To(Equal("Created"),
				"an unapplied archival setting must not disturb ownership")

			uid := namespaceField(temporalNamespace, "{.metadata.uid}")
			Expect(described.NamespaceInfo.Data).To(HaveKeyWithValue(ownerMarkerKey, uid))

			By("removing the archival block from the Namespace")
			applyNamespace(temporalNamespace, "36h")

			By("confirming it stops managing archival and goes Ready=True")
			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
			}, 3*time.Minute).Should(Succeed())

			Expect(describeTemporalNamespace(temporalNamespace).Config.HistoryArchivalState).
				To(Equal(archivalStateDisabled), "unmanaging a field must not write to it")

			By("deleting the Namespace")
			deleteNamespace(temporalNamespace)
		})

		It("should own the lifecycle of a namespace it creates", func() {
			By("applying a ready Connection")
			applyConnectionSample()

			By("applying a Namespace for a Temporal namespace that does not exist")
			Expect(temporalNamespaceExists(temporalNamespace)).To(BeFalse())
			applyNamespace(temporalNamespace, "36h")

			By("waiting for the Namespace to report Ready=True with reason Created")
			Eventually(func(g Gomega) {
				g.Expect(namespaceField(temporalNamespace, "{.status.observedGeneration}")).
					To(Equal(namespaceField(temporalNamespace, "{.metadata.generation}")))
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			By("confirming ownership was recorded as Created")
			Expect(namespaceOwnership(temporalNamespace)).To(Equal("Created"))

			By("confirming the namespace exists on the Temporal Service")
			described := describeTemporalNamespace(temporalNamespace)
			Expect(described.NamespaceInfo.Name).To(Equal(temporalNamespace))
			Expect(described.Config.WorkflowExecutionRetentionTTL).To(Equal(retentionSeconds(36)))

			By("confirming Temporal carries the Kubernetes UID as the ownership marker")
			uid := namespaceField(temporalNamespace, "{.metadata.uid}")
			Expect(uid).NotTo(BeEmpty())
			Expect(described.NamespaceInfo.Data).To(HaveKeyWithValue(ownerMarkerKey, uid))

			By("confirming the finalizer is holding the resource")
			Expect(namespaceField(temporalNamespace, "{.metadata.finalizers}")).
				To(ContainSubstring("temporal.simonemms.com/namespace"))

			By("deleting the Namespace")
			deleteNamespace(temporalNamespace)

			By("confirming the Temporal namespace was deleted with it")
			Eventually(func(g Gomega) {
				g.Expect(temporalNamespaceExists(temporalNamespace)).To(BeFalse())
			}, 2*time.Minute).Should(Succeed())
		})

		It("should manage but not own a namespace it adopts", func() {
			By("creating the Temporal namespace outside the operator")
			createTemporalNamespace(temporalNamespace, "36h")
			Expect(temporalNamespaceExists(temporalNamespace)).To(BeTrue())

			By("applying a ready Connection")
			applyConnectionSample()

			By("applying a matching Namespace")
			applyNamespace(temporalNamespace, "36h")

			By("waiting for the Namespace to report Ready=True with reason Adopted")
			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("Adopted"))
			}, 3*time.Minute).Should(Succeed())

			Expect(namespaceOwnership(temporalNamespace)).To(Equal("Adopted"))

			By("confirming the operator did not claim it")
			Expect(describeTemporalNamespace(temporalNamespace).NamespaceInfo.Data).
				NotTo(HaveKey(ownerMarkerKey), "adopting a namespace must not stamp ownership on it")

			By("changing the retention on the Namespace")
			applyNamespace(temporalNamespace, "48h")

			By("confirming the Temporal retention follows, without changing ownership")
			Eventually(func(g Gomega) {
				g.Expect(describeTemporalNamespace(temporalNamespace).Config.WorkflowExecutionRetentionTTL).
					To(Equal(retentionSeconds(48)))
			}, 3*time.Minute).Should(Succeed())

			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
			}, 3*time.Minute).Should(Succeed())
			Expect(namespaceOwnership(temporalNamespace)).To(Equal("Adopted"),
				"managing an adopted namespace must not make the operator its owner")

			By("deleting the Namespace")
			deleteNamespace(temporalNamespace)

			By("confirming the Temporal namespace survived, still unmarked")
			Expect(temporalNamespaceExists(temporalNamespace)).To(BeTrue())
			Expect(describeTemporalNamespace(temporalNamespace).NamespaceInfo.Data).NotTo(HaveKey(ownerMarkerKey))
		})

		It("should refuse a namespace carrying another owner's marker", func() {
			const foreignUID = "deadbeef-0000-1111-2222-333344445555"

			By("creating a Temporal namespace owned by something else")
			createTemporalNamespaceOwnedBy(temporalNamespace, "36h", foreignUID)

			applyConnectionSample()
			applyNamespace(temporalNamespace, "96h")

			By("waiting for the Namespace to report an ownership conflict")
			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("False"))
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("OwnershipConflict"))
			}, 3*time.Minute).Should(Succeed())

			By("confirming ownership was never established")
			Expect(namespaceOwnership(temporalNamespace)).To(BeEmpty(),
				"a conflicting namespace is neither created nor adopted")

			By("confirming Temporal was left exactly as it was")
			described := describeTemporalNamespace(temporalNamespace)
			Expect(described.Config.WorkflowExecutionRetentionTTL).To(Equal(retentionSeconds(36)),
				"a conflict must outrank retention drift")
			Expect(described.NamespaceInfo.Data).To(HaveKeyWithValue(ownerMarkerKey, foreignUID),
				"another owner's marker must not be overwritten")
		})

		It("should not let a recreated resource inherit the previous one's namespace", func() {
			applyConnectionSample()

			By("creating the namespace through a first Namespace resource")
			applyNamespace(temporalNamespace, "36h")
			Eventually(func(g Gomega) {
				g.Expect(namespaceOwnership(temporalNamespace)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			uidA := namespaceField(temporalNamespace, "{.metadata.uid}")
			Expect(describeTemporalNamespace(temporalNamespace).NamespaceInfo.Data).
				To(HaveKeyWithValue(ownerMarkerKey, uidA))

			// Taking the resource away has to happen with the operator stopped.
			// Deleting it while the operator is running is a race the operator
			// usually wins: the deletion timestamp wakes it up, it finalises the
			// namespace it legitimately owns, and there is nothing left for the
			// replacement to conflict with.
			By("stopping the operator so it cannot finalise the namespace")
			stopControllerManager()

			By("removing the first resource, leaving the Temporal namespace behind")
			removeNamespaceFinalizer(temporalNamespace)
			deleteNamespace(temporalNamespace)

			By("confirming the Temporal namespace outlived its resource")
			Expect(temporalNamespaceExists(temporalNamespace)).To(BeTrue())
			Expect(describeTemporalNamespace(temporalNamespace).NamespaceInfo.Data).
				To(HaveKeyWithValue(ownerMarkerKey, uidA))

			By("restarting the operator")
			startControllerManager()

			By("applying a replacement resource with the same name")
			applyNamespace(temporalNamespace, "36h")

			uidB := namespaceField(temporalNamespace, "{.metadata.uid}")
			Expect(uidB).NotTo(Equal(uidA), "a recreated resource must have a fresh identity")

			By("confirming the replacement reports a conflict rather than taking over")
			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("False"))
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("OwnershipConflict"))
			}, 3*time.Minute).Should(Succeed())

			Expect(namespaceOwnership(temporalNamespace)).To(BeEmpty())
			Expect(describeTemporalNamespace(temporalNamespace).NamespaceInfo.Data).
				To(HaveKeyWithValue(ownerMarkerKey, uidA), "the original owner's claim stands")

			// A normal delete here, with the operator running: the replacement
			// never established ownership, so the operator must release the
			// finalizer on its own without touching Temporal.
			By("confirming the replacement cannot delete it either")
			deleteNamespace(temporalNamespace)
			Expect(temporalNamespaceExists(temporalNamespace)).To(BeTrue())
			Expect(describeTemporalNamespace(temporalNamespace).NamespaceInfo.Data).
				To(HaveKeyWithValue(ownerMarkerKey, uidA))
		})

		It("should leave the Temporal namespace behind when the policy is Orphan", func() {
			applyConnectionSample()

			By("creating a namespace the operator owns but is told not to delete")
			applyNamespaceWithPolicy(temporalNamespace, "36h", "Orphan")

			Eventually(func(g Gomega) {
				g.Expect(namespaceOwnership(temporalNamespace)).To(Equal("Created"))
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
			}, 3*time.Minute).Should(Succeed())

			uid := namespaceField(temporalNamespace, "{.metadata.uid}")
			Expect(describeTemporalNamespace(temporalNamespace).NamespaceInfo.Data).
				To(HaveKeyWithValue(ownerMarkerKey, uid))

			By("deleting the Namespace")
			deleteNamespace(temporalNamespace)

			By("confirming the Temporal namespace and its marker survived")
			Expect(temporalNamespaceExists(temporalNamespace)).To(BeTrue())
			Expect(describeTemporalNamespace(temporalNamespace).NamespaceInfo.Data).
				To(HaveKeyWithValue(ownerMarkerKey, uid), "orphaning must not disturb the marker")
		})

		It("should release a deletion blocked on a missing Connection once orphaned", func() {
			applyConnectionSample()

			By("creating a namespace the operator owns under the default Delete policy")
			applyNamespace(temporalNamespace, "36h")

			Eventually(func(g Gomega) {
				g.Expect(namespaceOwnership(temporalNamespace)).To(Equal("Created"))
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
			}, 3*time.Minute).Should(Succeed())

			uid := namespaceField(temporalNamespace, "{.metadata.uid}")
			Expect(temporalNamespaceExists(temporalNamespace)).To(BeTrue())

			By("deleting the Connection the operator would need to finalise")
			cmd := exec.Command("kubectl", "delete", "connection", "connection-sample",
				"-n", "default", "--wait=true")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete the Connection")

			By("asking for the Namespace to be deleted")
			beginNamespaceDeletion(temporalNamespace)

			By("confirming the deletion is held open rather than orphaning silently")
			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("False"))
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("ConnectionNotFound"))
			}, 3*time.Minute).Should(Succeed())

			Expect(namespaceField(temporalNamespace, "{.metadata.deletionTimestamp}")).NotTo(BeEmpty())
			Expect(namespaceField(temporalNamespace, "{.metadata.finalizers}")).
				To(ContainSubstring("temporal.simonemms.com/namespace"))
			Expect(temporalNamespaceExists(temporalNamespace)).To(BeTrue())

			By("switching the terminating resource to Orphan")
			setNamespaceDeletionPolicy(temporalNamespace, "Orphan")

			By("confirming the resource is released")
			Eventually(func(g Gomega) {
				g.Expect(namespaceGone(temporalNamespace)).To(BeTrue())
			}, 3*time.Minute).Should(Succeed())

			By("confirming the Temporal namespace was deliberately left behind")
			Expect(temporalNamespaceExists(temporalNamespace)).To(BeTrue())
			Expect(describeTemporalNamespace(temporalNamespace).NamespaceInfo.Data).
				To(HaveKeyWithValue(ownerMarkerKey, uid))
		})

		It("should react to its Connection appearing without waiting for the retry", func() {
			By("creating a Namespace before the Connection it references exists")
			applyNamespace(temporalNamespace, "36h")

			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("False"))
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("ConnectionNotFound"))
			}, 3*time.Minute).Should(Succeed())

			By("creating the Connection it was waiting for")
			applyConnectionSample()

			// Timed from the moment the Connection reports itself Ready. The
			// window is a third of the fallback retry, so recovering inside it
			// is the watch's doing rather than the timer's.
			By("confirming the Namespace recovers promptly")
			start := time.Now()
			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
			}, watchResponseWindow).Should(Succeed())

			_, _ = fmt.Fprintf(GinkgoWriter,
				"Namespace became Ready %s after its Connection did\n", time.Since(start).Round(time.Millisecond))

			Expect(namespaceOwnership(temporalNamespace)).To(Equal("Created"))
		})

		It("should react to its Connection going away and coming back", func() {
			applyConnectionSample()
			applyNamespace(temporalNamespace, "36h")

			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
			}, 3*time.Minute).Should(Succeed())

			By("deleting the Connection")
			cmd := exec.Command("kubectl", "delete", "connection", "connection-sample",
				"-n", "default", "--wait=true")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete the Connection")

			By("confirming the Namespace notices promptly")
			start := time.Now()
			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("False"))
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("ConnectionNotFound"))
			}, watchResponseWindow).Should(Succeed())

			_, _ = fmt.Fprintf(GinkgoWriter,
				"Namespace noticed the deletion after %s\n", time.Since(start).Round(time.Millisecond))

			By("recreating the Connection under the same name")
			applyConnectionSample()

			By("confirming the Namespace recovers promptly")
			start = time.Now()
			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
			}, watchResponseWindow).Should(Succeed())

			_, _ = fmt.Fprintf(GinkgoWriter,
				"Namespace recovered %s after its Connection came back\n", time.Since(start).Round(time.Millisecond))
		})

		It("should correct retention drift on a namespace it created", func() {
			applyConnectionSample()
			applyNamespace(temporalNamespace, "36h")

			Eventually(func(g Gomega) {
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())
			Expect(describeTemporalNamespace(temporalNamespace).Config.WorkflowExecutionRetentionTTL).
				To(Equal(retentionSeconds(36)))

			By("changing spec.retention")
			applyNamespace(temporalNamespace, "96h")

			By("confirming Temporal follows and the resource reports Updated")
			Eventually(func(g Gomega) {
				g.Expect(describeTemporalNamespace(temporalNamespace).Config.WorkflowExecutionRetentionTTL).
					To(Equal(retentionSeconds(96)))
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("Updated"))
			}, 3*time.Minute).Should(Succeed())

			Expect(namespaceOwnership(temporalNamespace)).To(Equal("Created"),
				"correcting drift must not change who owns the namespace")
		})

		It("should accept a retention written in days, and keep reconciling", func() {
			// The bug this covers: "7d" was stored by a schema that only said
			// "type: string", and from then on the controller could not list
			// Namespaces at all - `unknown unit "d"` - so nothing reconciled
			// again. Reaching Ready at all proves the informer survived, and
			// the update afterwards proves it kept working.
			applyConnectionSample()
			applyNamespace(temporalNamespace, "7d")

			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			Expect(describeTemporalNamespace(temporalNamespace).Config.WorkflowExecutionRetentionTTL).
				To(Equal(retentionSeconds(168)), "7d should reach Temporal as 168h")

			By("confirming Kubernetes kept the string as written")
			Expect(namespaceRetention(temporalNamespace)).To(Equal("7d"))

			By("changing to a compound day duration")
			applyNamespace(temporalNamespace, "1d12h")

			Eventually(func(g Gomega) {
				g.Expect(describeTemporalNamespace(temporalNamespace).Config.WorkflowExecutionRetentionTTL).
					To(Equal(retentionSeconds(36)))
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("Updated"))
			}, 3*time.Minute).Should(Succeed())
		})

		It("should reconcile the minimum retention", func() {
			// The lower bound of the supported range, against a real Service.
			applyConnectionSample()
			applyNamespace(temporalNamespace, "1d")

			Eventually(func(g Gomega) {
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			Expect(describeTemporalNamespace(temporalNamespace).Config.WorkflowExecutionRetentionTTL).
				To(Equal(retentionSeconds(24)), "1d should reach Temporal as 24h")
		})

		It("should refuse a retention outside 1 to 90 days at admission", func() {
			// The range is enforced on the duration, so an out-of-range value is
			// refused whichever way it is written. None of these can reach
			// storage, which is what keeps the controller from ever seeing one.
			for _, bad := range []string{"1h", "23h59m59s", "1439m", "0", "-1d", "91d", "2161h", "90d1ns"} {
				manifest := fmt.Sprintf(`apiVersion: temporal.simonemms.com/v1beta1
kind: Namespace
metadata:
  name: %s
  namespace: default
spec:
  connectionRef:
    name: connection-sample
  retention: %q
`, temporalNamespace, bad)

				cmd := exec.Command("kubectl", "apply", "-f", "-")
				cmd.Stdin = strings.NewReader(manifest)

				output, err := utils.Run(cmd)
				Expect(err).To(HaveOccurred(), "%q must not be storable: %s", bad, output)
			}

			By("confirming the boundaries themselves are accepted")
			applyConnectionSample()
			applyNamespace(temporalNamespace, "90d")

			Eventually(func(g Gomega) {
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			Expect(describeTemporalNamespace(temporalNamespace).Config.WorkflowExecutionRetentionTTL).
				To(Equal(retentionSeconds(2160)), "90d should reach Temporal as 2160h")
		})

		It("should refuse a malformed retention at admission", func() {
			// Kubernetes must reject these outright. If any one of them could
			// be stored, it would break every subsequent list of Namespaces and
			// take the controller down with it.
			for _, bad := range []string{"7days", "7dd", "1d2d", "1.5d", "forever"} {
				manifest := fmt.Sprintf(`apiVersion: temporal.simonemms.com/v1beta1
kind: Namespace
metadata:
  name: %s
  namespace: default
spec:
  connectionRef:
    name: connection-sample
  retention: %q
`, temporalNamespace, bad)

				cmd := exec.Command("kubectl", "apply", "-f", "-")
				cmd.Stdin = strings.NewReader(manifest)

				output, err := utils.Run(cmd)
				Expect(err).To(HaveOccurred(), "%q must not be storable: %s", bad, output)
			}

			By("confirming the operator is still reconciling")
			applyConnectionSample()
			applyNamespace(temporalNamespace, "7d")

			Eventually(func(g Gomega) {
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())
		})

		It("should default retention to 72h", func() {
			applyConnectionSample()
			applyNamespace(temporalNamespace, "")

			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			described := describeTemporalNamespace(temporalNamespace)
			Expect(described.Config.WorkflowExecutionRetentionTTL).To(Equal(retentionSeconds(72)))
		})

		It("should report ConnectionNotFound when the Connection does not exist", func() {
			applyNamespace(temporalNamespace, "")

			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("False"))
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("ConnectionNotFound"))
			}, 3*time.Minute).Should(Succeed())

			By("confirming no Temporal namespace was registered")
			_, err := utils.Run(temporalCLI("operator", "namespace", "describe", "-n", temporalNamespace))
			Expect(err).To(HaveOccurred(), "The Temporal namespace should not exist")
		})

		It("should reject a Namespace without a connectionRef name", func() {
			cmd := exec.Command("kubectl", "apply", "-n", "default", "-f", "-")
			cmd.Stdin = strings.NewReader(`apiVersion: temporal.simonemms.com/v1beta1
kind: Namespace
metadata:
  name: namespace-invalid
spec:
  connectionRef: {}
`)
			_, err := utils.Run(cmd)
			Expect(err).To(HaveOccurred(), "The CRD should reject an empty connectionRef")
			Expect(err.Error()).To(ContainSubstring("connectionRef.name is required"))
		})
	})

	Context("SearchAttribute", func() {
		var (
			temporalNamespace string
			resourceName      string
			attributeName     string
		)

		BeforeEach(func() {
			suffix := fmt.Sprintf("%d-%d", GinkgoRandomSeed(), CurrentSpecReport().LineNumber())
			temporalNamespace = "e2e-sa-ns-" + suffix
			// The Kubernetes resource is named the way Kubernetes insists;
			// the Temporal attribute is named the way real ones are. Keeping
			// them different is the point of these specs.
			resourceName = "customer-id-" + suffix
			attributeName = "CustomerId" + strings.ReplaceAll(suffix, "-", "")

			By("removing anything an earlier spec left behind")
			clearOperatorResources()

			cmd := exec.Command("kubectl", "wait", "--for=condition=Available",
				"deployment/temporal", "-n", "temporal", "--timeout=5m")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Temporal did not become available")
		})

		AfterEach(func() {
			By("removing the Kubernetes resources")
			cmd := exec.Command("kubectl", "delete", "tsa", "--all",
				"-n", "default", "--wait=true", "--timeout=3m")
			if _, err := utils.Run(cmd); err != nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "SearchAttribute cleanup stalled, stripping finalizers: %s\n", err)

				cmd = exec.Command("kubectl", "patch", "tsa", "--all", "-n", "default",
					"--type=merge", "-p", `{"metadata":{"finalizers":null}}`)
				_, _ = utils.Run(cmd)
			}
			clearOperatorResources()

			// Removing the Temporal namespace takes its search attributes with
			// it, which is the tidiest way to clean up an orphaned one.
			By("removing the Temporal namespace")
			removeTemporalNamespace(temporalNamespace)
		})

		// readyNamespace brings up a Connection and Namespace for the spec, and
		// waits until search attribute calls against it actually resolve.
		//
		// A Ready Namespace is not quite enough: the Service answers describe
		// from storage but resolves search attribute calls through a namespace
		// registry that lags a few seconds behind a fresh registration, so
		// asking too early gets "namespace not found". The operator retries
		// through that; a test issuing one-shot CLI calls has to wait it out.
		readyNamespace := func() {
			applyConnectionSample()
			applyNamespace(temporalNamespace, "36h")

			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
			}, 3*time.Minute).Should(Succeed())

			Eventually(func(g Gomega) {
				_, err := utils.Run(temporalCLI("operator", "search-attribute", "list",
					"-n", temporalNamespace, "-o", "json"))
				g.Expect(err).NotTo(HaveOccurred(), "the namespace registry has not caught up yet")
			}, 2*time.Minute).Should(Succeed())
		}

		It("should register a search attribute it creates", func() {
			readyNamespace()

			Expect(resourceName).NotTo(Equal(attributeName),
				"the two names must differ for these specs to prove anything")

			applySearchAttribute(resourceName, attributeName, temporalNamespace, "Keyword", "")

			Eventually(func(g Gomega) {
				g.Expect(searchAttributeReady(resourceName)).To(Equal("True"))
				g.Expect(searchAttributeReadyReason(resourceName)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			Expect(searchAttributeOwnership(resourceName)).To(Equal("Created"))

			By("registering the Temporal name, not the Kubernetes one")
			Expect(temporalSearchAttributeType(temporalNamespace, attributeName)).
				To(Equal("INDEXED_VALUE_TYPE_KEYWORD"))
			Expect(temporalSearchAttributeType(temporalNamespace, resourceName)).To(BeEmpty(),
				"the Kubernetes resource name must never reach Temporal")

			By("refusing to retarget the resource at something else")
			cmd := exec.Command("kubectl", "patch", "tsa", resourceName, "-n", "default",
				"--type=merge", "-p", `{"spec":{"name":"SomethingElse"}}`)
			_, err := utils.Run(cmd)
			Expect(err).To(HaveOccurred(), "spec.name must be immutable")
			Expect(err.Error()).To(ContainSubstring("name is immutable"))

			By("removing it again when the resource goes")
			deleteSearchAttribute(resourceName)

			Eventually(func(g Gomega) {
				g.Expect(temporalSearchAttributeType(temporalNamespace, attributeName)).To(BeEmpty())
			}, 2*time.Minute).Should(Succeed())
		})

		It("should adopt a search attribute that already exists", func() {
			readyNamespace()

			By("registering a mixed-case attribute outside the operator")
			createTemporalSearchAttribute(temporalNamespace, attributeName, "KeywordList")
			Expect(attributeName).To(MatchRegexp(`^CustomerId`),
				"the attribute Temporal holds is PascalCase, as real ones are")

			applySearchAttribute(resourceName, attributeName, temporalNamespace, "KeywordList", "")

			Eventually(func(g Gomega) {
				g.Expect(searchAttributeReady(resourceName)).To(Equal("True"))
				g.Expect(searchAttributeReadyReason(resourceName)).To(Equal("Adopted"))
			}, 3*time.Minute).Should(Succeed())

			Expect(searchAttributeOwnership(resourceName)).To(Equal("Adopted"))

			By("leaving it behind when the resource goes")
			deleteSearchAttribute(resourceName)

			Expect(temporalSearchAttributeType(temporalNamespace, attributeName)).
				To(Equal("INDEXED_VALUE_TYPE_KEYWORD_LIST"), "an adopted attribute must survive its resource")
		})

		It("should refuse a search attribute of a different type", func() {
			readyNamespace()

			By("registering a Keyword attribute outside the operator")
			createTemporalSearchAttribute(temporalNamespace, attributeName, "Keyword")

			By("asking for the same name as Text")
			applySearchAttribute(resourceName, attributeName, temporalNamespace, "Text", "")

			Eventually(func(g Gomega) {
				g.Expect(searchAttributeReady(resourceName)).To(Equal("False"))
				g.Expect(searchAttributeReadyReason(resourceName)).To(Equal("TypeConflict"))
			}, 3*time.Minute).Should(Succeed())

			Expect(searchAttributeOwnership(resourceName)).To(BeEmpty(),
				"a conflicting attribute is neither created nor adopted")
			Expect(temporalSearchAttributeType(temporalNamespace, attributeName)).
				To(Equal("INDEXED_VALUE_TYPE_KEYWORD"), "the existing attribute must not be retyped")
		})

		It("should leave the attribute behind when the policy is Orphan", func() {
			readyNamespace()

			applySearchAttribute(resourceName, attributeName, temporalNamespace, "Int", "Orphan")

			Eventually(func(g Gomega) {
				g.Expect(searchAttributeOwnership(resourceName)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())
			Expect(temporalSearchAttributeType(temporalNamespace, attributeName)).
				To(Equal("INDEXED_VALUE_TYPE_INT"))

			deleteSearchAttribute(resourceName)

			Expect(temporalSearchAttributeType(temporalNamespace, attributeName)).
				To(Equal("INDEXED_VALUE_TYPE_INT"), "Orphan must leave the attribute alone")
		})

		It("should finalise when the Temporal namespace has already gone", func() {
			readyNamespace()

			applySearchAttribute(resourceName, attributeName, temporalNamespace, "Keyword", "")
			Eventually(func(g Gomega) {
				g.Expect(searchAttributeOwnership(resourceName)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			// Arrange the teardown order deterministically rather than racing
			// the two controllers: with the operator stopped, take the Temporal
			// namespace away underneath the search attribute.
			By("stopping the operator")
			stopControllerManager()

			By("deleting the Temporal namespace, taking its search attributes with it")
			_, err := utils.Run(temporalCLI("operator", "namespace", "delete",
				"-n", temporalNamespace, "--yes"))
			Expect(err).NotTo(HaveOccurred(), "Failed to delete the Temporal namespace")

			Eventually(func(g Gomega) {
				g.Expect(temporalNamespaceExists(temporalNamespace)).To(BeFalse())
			}, 2*time.Minute).Should(Succeed())

			By("removing the Namespace resource so the operator does not put it back")
			removeNamespaceFinalizer(temporalNamespace)
			deleteNamespace(temporalNamespace)

			By("restarting the operator")
			startControllerManager()

			By("deleting the SearchAttribute")
			deleteSearchAttribute(resourceName)

			Expect(searchAttributeGone(resourceName)).To(BeTrue(),
				"a search attribute cannot outlive the namespace holding it, so deletion is already satisfied")
		})

		It("should react to its Namespace becoming Ready without waiting for the retry", func() {
			applyConnectionSample()

			By("creating the SearchAttribute before its Namespace exists")
			applySearchAttribute(resourceName, attributeName, temporalNamespace, "Keyword", "")

			Eventually(func(g Gomega) {
				g.Expect(searchAttributeReady(resourceName)).To(Equal("False"))
				g.Expect(searchAttributeReadyReason(resourceName)).To(Equal("NamespaceNotFound"))
			}, 3*time.Minute).Should(Succeed())

			By("creating the Namespace it was waiting for")
			applyNamespace(temporalNamespace, "36h")
			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
			}, 3*time.Minute).Should(Succeed())

			// Timed from the moment the Namespace reports Ready, and measured on
			// the dependency gate rather than on Ready=True. Getting past the
			// gate is the watch's doing; what happens next depends on Temporal's
			// namespace registry catching up, which is its own few seconds and
			// nothing to do with whether the wake-up worked.
			//
			// The window is a third of the fallback retry, so clearing the gate
			// inside it cannot be the timer's doing.
			By("confirming the SearchAttribute stops waiting on its Namespace promptly")
			start := time.Now()
			Eventually(func(g Gomega) {
				g.Expect(searchAttributeReadyReason(resourceName)).NotTo(Equal("NamespaceNotFound"))
			}, watchResponseWindow).Should(Succeed())

			_, _ = fmt.Fprintf(GinkgoWriter,
				"SearchAttribute stopped waiting %s after its Namespace became Ready\n",
				time.Since(start).Round(time.Millisecond))

			By("confirming it goes on to register the attribute")
			Eventually(func(g Gomega) {
				g.Expect(searchAttributeReady(resourceName)).To(Equal("True"))
				g.Expect(searchAttributeReadyReason(resourceName)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())
		})
	})

	Context("Schedule", func() {
		var (
			temporalNamespace string
			resourceName      string
			scheduleID        string
		)

		BeforeEach(func() {
			suffix := fmt.Sprintf("%d-%d", GinkgoRandomSeed(), CurrentSpecReport().LineNumber())
			temporalNamespace = "e2e-sc-ns-" + suffix
			resourceName = "payments-nightly-" + suffix
			scheduleID = "PaymentsNightly" + strings.ReplaceAll(suffix, "-", "")

			By("removing anything an earlier spec left behind")
			clearSchedules()
			clearOperatorResources()

			cmd := exec.Command("kubectl", "wait", "--for=condition=Available",
				"deployment/temporal", "-n", "temporal", "--timeout=5m")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Temporal did not become available")
		})

		AfterEach(func() {
			By("removing the Kubernetes resources")
			clearSchedules()
			clearOperatorResources()

			By("removing anything left on the Temporal Service")
			removeTemporalSchedule(temporalNamespace, scheduleID)
			removeTemporalNamespace(temporalNamespace)
		})

		// readyNamespace gets a Temporal namespace to the point where the Service
		// will actually accept a schedule in it.
		readyNamespace := func() {
			applyConnectionSample()
			applyNamespace(temporalNamespace, "36h")

			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
			}, 3*time.Minute).Should(Succeed())

			// The Service resolves a namespace through a registry that lags a
			// few seconds behind a fresh registration, so a one-shot CLI call
			// has to wait it out. The operator retries through it.
			Eventually(func(g Gomega) {
				_, err := utils.Run(temporalCLI("schedule", "list", "-n", temporalNamespace, "-o", "json"))
				g.Expect(err).NotTo(HaveOccurred(), "the namespace registry has not caught up yet")
			}, 2*time.Minute).Should(Succeed())
		}

		It("should create a cron schedule and remove it again", func() {
			readyNamespace()

			Expect(resourceName).NotTo(Equal(scheduleID))
			applySchedule(resourceName, scheduleID, temporalNamespace,
				"    cron:\n      - \"30 2 * * *\"\n    timeZone: Europe/London\n", "")

			Eventually(func(g Gomega) {
				g.Expect(scheduleReady(resourceName)).To(Equal("True"))
				g.Expect(scheduleReadyReason(resourceName)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			Expect(scheduleOwnership(resourceName)).To(Equal("Created"))

			By("confirming the Service holds it under the Temporal ID")
			described := describeTemporalSchedule(temporalNamespace, scheduleID)
			Expect(described.Schedule.Action.StartWorkflow.WorkflowType.Name).To(Equal("ReconcilePayments"))
			Expect(described.Schedule.Action.StartWorkflow.TaskQueue.Name).To(Equal("payments"))
			Expect(described.Schedule.Spec.TimezoneName).To(Equal("Europe/London"))
			Expect(temporalScheduleExists(temporalNamespace, resourceName)).To(BeFalse(),
				"the Kubernetes resource name must never reach Temporal")

			By("confirming the Service compiled the cron expression into a calendar")
			// This is why the operator records hashes rather than comparing the
			// spec: what it sent back is not what it was given.
			Expect(described.Schedule.Spec.StructuredCalendar).NotTo(BeEmpty())
			Expect(described.Schedule.Spec.StructuredCalendar[0].Hour[0].Start).To(Equal(2))
			Expect(described.Schedule.Spec.StructuredCalendar[0].Minute[0].Start).To(Equal(30))

			By("confirming a settled schedule is not rewritten when it is looked at again")
			// The failure the two status fingerprints exist to prevent. Temporal
			// compiled the cron expression away above, so an operator comparing
			// the spec against what the Service reports would see drift here and
			// rewrite the schedule for ever.
			nudgeDependants()

			Eventually(func(g Gomega) {
				g.Expect(scheduleReadyReason(resourceName)).To(Equal("Reconciled"))
			}, 3*time.Minute).Should(Succeed())

			By("refusing to retarget the resource at a different schedule")
			cmd := exec.Command("kubectl", "patch", "tsc", resourceName, "-n", "default",
				"--type=merge", "-p", `{"spec":{"scheduleId":"SomethingElse"}}`)
			_, err := utils.Run(cmd)
			Expect(err).To(HaveOccurred(), "spec.scheduleId must be immutable")
			Expect(err.Error()).To(ContainSubstring("scheduleId is immutable"))

			By("removing it again when the resource goes")
			deleteScheduleResource(resourceName)

			Eventually(func(g Gomega) {
				g.Expect(temporalScheduleExists(temporalNamespace, scheduleID)).To(BeFalse())
			}, 2*time.Minute).Should(Succeed())
		})

		It("should reconcile a change to the timing and to the workflow", func() {
			readyNamespace()

			applySchedule(resourceName, scheduleID, temporalNamespace,
				"    cron:\n      - \"30 2 * * *\"\n", "")

			Eventually(func(g Gomega) {
				g.Expect(scheduleReady(resourceName)).To(Equal("True"))
			}, 3*time.Minute).Should(Succeed())

			By("changing the timing to an interval")
			applySchedule(resourceName, scheduleID, temporalNamespace,
				"    intervals:\n      - every: 6h\n        offset: 5h\n", "")

			Eventually(func(g Gomega) {
				described := describeTemporalSchedule(temporalNamespace, scheduleID)
				g.Expect(described.Schedule.Spec.Interval).NotTo(BeEmpty())
				g.Expect(described.Schedule.Spec.Interval[0].Interval).To(Equal("21600s"))
				g.Expect(described.Schedule.Spec.StructuredCalendar).To(BeEmpty(),
					"the old cron calendar must be replaced, not added to")
			}, 3*time.Minute).Should(Succeed())

			By("changing the workflow configuration and the policies")
			applySchedule(resourceName, scheduleID, temporalNamespace,
				"    intervals:\n      - every: 6h\n        offset: 5h\n",
				"  policies:\n    overlap: BufferAll\n")

			Eventually(func(g Gomega) {
				described := describeTemporalSchedule(temporalNamespace, scheduleID)
				g.Expect(described.Schedule.Policies.OverlapPolicy).
					To(Equal("SCHEDULE_OVERLAP_POLICY_BUFFER_ALL"))
			}, 3*time.Minute).Should(Succeed())

			Expect(scheduleOwnership(resourceName)).To(Equal("Created"))
		})

		It("should manage the paused state only while the spec declares it", func() {
			readyNamespace()

			applySchedule(resourceName, scheduleID, temporalNamespace,
				"    cron:\n      - \"30 2 * * *\"\n", "")

			Eventually(func(g Gomega) {
				g.Expect(scheduleReady(resourceName)).To(Equal("True"))
			}, 3*time.Minute).Should(Succeed())

			By("leaving a schedule paused by hand alone while pausing is unmanaged")
			// The CLI spells this "toggle --pause"; there is no "schedule pause".
			_, err := utils.Run(temporalCLI("schedule", "toggle",
				"-n", temporalNamespace, "--schedule-id", scheduleID,
				"--pause", "--reason", "paused by hand"))
			Expect(err).NotTo(HaveOccurred())

			// Wake the operator so it actually looks, then check that looking
			// changed nothing.
			nudgeDependants()

			Consistently(func(g Gomega) {
				g.Expect(describeTemporalSchedule(temporalNamespace, scheduleID).Schedule.State.Paused).
					To(BeTrue(), "an unmanaged pause must not be undone")
			}, 30*time.Second, 5*time.Second).Should(Succeed())

			By("resuming it once the spec starts managing pausing")
			applySchedule(resourceName, scheduleID, temporalNamespace,
				"    cron:\n      - \"30 2 * * *\"\n",
				"  state:\n    paused: false\n")

			Eventually(func(g Gomega) {
				g.Expect(describeTemporalSchedule(temporalNamespace, scheduleID).Schedule.State.Paused).
					To(BeFalse())
			}, 3*time.Minute).Should(Succeed())

			By("pausing it again when the spec says so")
			applySchedule(resourceName, scheduleID, temporalNamespace,
				"    cron:\n      - \"30 2 * * *\"\n",
				"  state:\n    paused: true\n")

			Eventually(func(g Gomega) {
				g.Expect(describeTemporalSchedule(temporalNamespace, scheduleID).Schedule.State.Paused).
					To(BeTrue())
			}, 3*time.Minute).Should(Succeed())
		})

		It("should adopt a schedule that already exists and leave it behind", func() {
			readyNamespace()

			By("creating a schedule outside the operator")
			createTemporalSchedule(temporalNamespace, scheduleID)
			Expect(temporalScheduleExists(temporalNamespace, scheduleID)).To(BeTrue())

			applySchedule(resourceName, scheduleID, temporalNamespace,
				"    cron:\n      - \"30 2 * * *\"\n", "")

			Eventually(func(g Gomega) {
				g.Expect(scheduleReady(resourceName)).To(Equal("True"))
			}, 3*time.Minute).Should(Succeed())

			Expect(scheduleOwnership(resourceName)).To(Equal("Adopted"))

			By("reconciling the declared fields of the adopted schedule")
			applySchedule(resourceName, scheduleID, temporalNamespace,
				"    cron:\n      - \"@hourly\"\n", "")

			Eventually(func(g Gomega) {
				described := describeTemporalSchedule(temporalNamespace, scheduleID)
				g.Expect(described.Schedule.Spec.StructuredCalendar).NotTo(BeEmpty())
				g.Expect(described.Schedule.Spec.StructuredCalendar[0].Minute[0].Start).To(Equal(0))
			}, 3*time.Minute).Should(Succeed())

			Expect(scheduleOwnership(resourceName)).To(Equal("Adopted"),
				"configuring an adopted schedule must not make it the operator's to delete")

			By("leaving it behind when the resource goes")
			deleteScheduleResource(resourceName)

			Expect(temporalScheduleExists(temporalNamespace, scheduleID)).To(BeTrue())
		})

		It("should leave a schedule behind under the Orphan policy", func() {
			readyNamespace()

			applySchedule(resourceName, scheduleID, temporalNamespace,
				"    cron:\n      - \"30 2 * * *\"\n",
				"  deletionPolicy: Orphan\n")

			Eventually(func(g Gomega) {
				g.Expect(scheduleReady(resourceName)).To(Equal("True"))
			}, 3*time.Minute).Should(Succeed())

			Expect(scheduleOwnership(resourceName)).To(Equal("Created"),
				"the operator still created it; the policy only decides what happens next")

			By("deleting the resource")
			deleteScheduleResource(resourceName)

			Expect(temporalScheduleExists(temporalNamespace, scheduleID)).To(BeTrue(),
				"Orphan must leave the schedule running")
		})

		It("should refuse an invalid schedule at the API server", func() {
			// The admission rules, proven against the real API server in the
			// cluster rather than envtest. A schedule Kubernetes will not store
			// never reaches the operator at all.
			manifest := `apiVersion: temporal.simonemms.com/v1beta1
kind: Schedule
metadata:
  name: ` + resourceName + `
spec:
  scheduleId: ` + scheduleID + `
  connectionRef:
    name: connection-sample
  namespaceRef:
    name: ` + temporalNamespace + `
  schedule:
    timeZone: UTC
  action:
    workflow:
      type: ReconcilePayments
      taskQueue: payments
`

			cmd := exec.Command("kubectl", "apply", "-n", "default", "-f", "-")
			cmd.Stdin = strings.NewReader(manifest)

			_, err := utils.Run(cmd)
			Expect(err).To(HaveOccurred(), "a schedule with no timing rules must be refused")
			Expect(err.Error()).To(ContainSubstring("at least one of calendars, intervals or cron"))
		})

		It("should report an invalid cron expression without touching Temporal", func() {
			// The CRD cannot parse cron, so this one is caught by the Go
			// validation phase - before the operator dials anything.
			readyNamespace()

			applySchedule(resourceName, scheduleID, temporalNamespace,
				"    cron:\n      - \"not a cron expression\"\n", "")

			Eventually(func(g Gomega) {
				g.Expect(scheduleReadyReason(resourceName)).To(Equal("InvalidSchedule"))
				g.Expect(scheduleReady(resourceName)).To(Equal("False"))
			}, 3*time.Minute).Should(Succeed())

			Expect(temporalScheduleExists(temporalNamespace, scheduleID)).To(BeFalse(),
				"an invalid schedule must never reach the Service")

			By("recovering once the expression is fixed")
			applySchedule(resourceName, scheduleID, temporalNamespace,
				"    cron:\n      - \"@daily\"\n", "")

			Eventually(func(g Gomega) {
				g.Expect(scheduleReady(resourceName)).To(Equal("True"))
			}, 3*time.Minute).Should(Succeed())
		})
	})

	Context("NexusEndpoint", func() {
		var (
			temporalNamespace string
			resourceName      string
			endpointName      string
			taskQueue         string
		)

		BeforeEach(func() {
			suffix := fmt.Sprintf("%d-%d", GinkgoRandomSeed(), CurrentSpecReport().LineNumber())
			temporalNamespace = "e2e-ne-ns-" + suffix
			resourceName = "payments-nexus-" + suffix
			endpointName = "PaymentsNexus" + strings.ReplaceAll(suffix, "-", "")
			taskQueue = "payments-tq-" + suffix

			By("removing anything an earlier spec left behind")
			clearNexusResources(endpointName)
			clearOperatorResources()

			cmd := exec.Command("kubectl", "wait", "--for=condition=Available",
				"deployment/temporal", "-n", "temporal", "--timeout=5m")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Temporal did not become available")
		})

		AfterEach(func() {
			// Order matters: Temporal refuses to delete a namespace while a
			// Nexus endpoint targets it, so the endpoint has to go first.
			By("removing the Kubernetes resources")
			clearNexusResources(endpointName)
			clearOperatorResources()

			By("removing the Temporal namespace")
			removeTemporalNamespace(temporalNamespace)
		})

		// readyNamespace brings up a Connection and Namespace, and waits until
		// Temporal will actually accept an endpoint targeting it.
		readyNamespace := func() {
			applyConnectionSample()
			applyNamespace(temporalNamespace, "36h")

			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
			}, 3*time.Minute).Should(Succeed())

			// The Service resolves the target namespace through a registry that
			// lags a few seconds behind a fresh registration, so a one-shot CLI
			// call has to wait it out. The operator retries through it.
			Eventually(func(g Gomega) {
				_, err := utils.Run(temporalCLI("operator", "search-attribute", "list",
					"-n", temporalNamespace, "-o", "json"))
				g.Expect(err).NotTo(HaveOccurred(), "the namespace registry has not caught up yet")
			}, 2*time.Minute).Should(Succeed())
		}

		It("should create an endpoint and remove it again", func() {
			readyNamespace()

			Expect(resourceName).NotTo(Equal(endpointName))
			applyNexusEndpoint(resourceName, endpointName, temporalNamespace, taskQueue, "")

			Eventually(func(g Gomega) {
				g.Expect(nexusEndpointReady(resourceName)).To(Equal("True"))
				g.Expect(nexusEndpointReadyReason(resourceName)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			Expect(nexusEndpointOwnership(resourceName)).To(Equal("Created"))

			By("confirming the Service holds it under the Temporal name and target")
			described, found := describeTemporalNexusEndpoint(endpointName)
			Expect(found).To(BeTrue())
			Expect(described.Spec.Target.Worker.Namespace).To(Equal(temporalNamespace))
			Expect(described.Spec.Target.Worker.TaskQueue).To(Equal(taskQueue))
			Expect(temporalNexusEndpointExists(resourceName)).To(BeFalse(),
				"the Kubernetes resource name must never reach Temporal")

			By("recording the server-assigned ID as an observation")
			Expect(nexusEndpointField(resourceName, "{.status.endpointId}")).
				To(Equal(described.ID))

			By("deleting the resource")
			deleteNexusEndpointResource(resourceName)

			Eventually(func(g Gomega) {
				g.Expect(temporalNexusEndpointExists(endpointName)).To(BeFalse())
			}, 2*time.Minute).Should(Succeed())
		})

		It("should manage the endpoint description", func() {
			// The feature this covers came from someone managing Temporal
			// resources declaratively who wanted the Markdown description -
			// what Temporal's UI renders on the endpoint page - managed too.
			const markdown = `## Payments Nexus

Handles operations for the **Payments** service.

See the internal runbook for ownership and escalation.
`

			readyNamespace()

			By("declaring a description")
			applyNexusEndpointWithDescription(
				resourceName, endpointName, temporalNamespace, taskQueue, "", new(markdown),
			)

			Eventually(func(g Gomega) {
				g.Expect(nexusEndpointReadyReason(resourceName)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			Expect(temporalNexusEndpointDescription(endpointName)).To(Equal(markdown),
				"the Markdown should reach Temporal exactly as written")

			By("rewriting it")
			applyNexusEndpointWithDescription(
				resourceName, endpointName, temporalNamespace, taskQueue, "",
				new("## Payments Nexus\n\nNow owned by the **Billing** team.\n"),
			)

			Eventually(func(g Gomega) {
				g.Expect(temporalNexusEndpointDescription(endpointName)).
					To(ContainSubstring("Billing"))
			}, 3*time.Minute).Should(Succeed())

			Expect(nexusEndpointReadyReason(resourceName)).To(Equal("Updated"))

			By("clearing it")
			applyNexusEndpointWithDescription(
				resourceName, endpointName, temporalNamespace, taskQueue, "", new(""),
			)

			Eventually(func(g Gomega) {
				g.Expect(temporalNexusEndpointDescription(endpointName)).To(BeEmpty())
			}, 3*time.Minute).Should(Succeed())
		})

		It("should leave a description it was not given alone", func() {
			// The behaviour that predates the field: a description the operator
			// was never told about survives the updates it makes.
			readyNamespace()

			applyNexusEndpointWithDescription(
				resourceName, endpointName, temporalNamespace, taskQueue, "",
				new("## Set through the operator\n"),
			)

			Eventually(func(g Gomega) {
				g.Expect(nexusEndpointReadyReason(resourceName)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			By("removing the field and retargeting the endpoint")
			applyNexusEndpoint(resourceName, endpointName, temporalNamespace, taskQueue+"-moved", "")

			Eventually(func(g Gomega) {
				g.Expect(nexusEndpointReadyReason(resourceName)).To(Equal("Updated"))
			}, 3*time.Minute).Should(Succeed())

			Expect(temporalNexusEndpointDescription(endpointName)).
				To(Equal("## Set through the operator\n"),
					"an unmanaged description should survive a retarget")
		})

		It("should adopt an existing endpoint and leave it behind", func() {
			readyNamespace()

			By("registering the endpoint outside the operator")
			createTemporalNexusEndpoint(endpointName, temporalNamespace, taskQueue)

			applyNexusEndpoint(resourceName, endpointName, temporalNamespace, taskQueue, "")

			Eventually(func(g Gomega) {
				g.Expect(nexusEndpointReady(resourceName)).To(Equal("True"))
				g.Expect(nexusEndpointReadyReason(resourceName)).To(Equal("Adopted"))
			}, 3*time.Minute).Should(Succeed())

			Expect(nexusEndpointOwnership(resourceName)).To(Equal("Adopted"))

			By("leaving it behind when the resource goes")
			deleteNexusEndpointResource(resourceName)

			Expect(temporalNexusEndpointExists(endpointName)).To(BeTrue(),
				"an adopted endpoint must survive its resource")
		})

		It("should move a drifted endpoint in place", func() {
			readyNamespace()

			applyNexusEndpoint(resourceName, endpointName, temporalNamespace, taskQueue, "")
			Eventually(func(g Gomega) {
				g.Expect(nexusEndpointOwnership(resourceName)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			before, found := describeTemporalNexusEndpoint(endpointName)
			Expect(found).To(BeTrue())

			By("changing the task queue")
			applyNexusEndpoint(resourceName, endpointName, temporalNamespace, taskQueue+"-moved", "")

			Eventually(func(g Gomega) {
				described, ok := describeTemporalNexusEndpoint(endpointName)
				g.Expect(ok).To(BeTrue())
				g.Expect(described.Spec.Target.Worker.TaskQueue).To(Equal(taskQueue + "-moved"))
			}, 3*time.Minute).Should(Succeed())

			after, _ := describeTemporalNexusEndpoint(endpointName)
			Expect(after.ID).To(Equal(before.ID),
				"the endpoint keeps its identity, so callers resolving it are undisturbed")
			Expect(nexusEndpointOwnership(resourceName)).To(Equal("Created"))
		})

		It("should put back an endpoint deleted behind its back", func() {
			readyNamespace()

			applyNexusEndpoint(resourceName, endpointName, temporalNamespace, taskQueue, "")
			Eventually(func(g Gomega) {
				g.Expect(nexusEndpointOwnership(resourceName)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			By("removing the endpoint outside the operator")
			removeTemporalNexusEndpoint(endpointName)
			Expect(temporalNexusEndpointExists(endpointName)).To(BeFalse())

			By("nudging the operator to reconcile")
			applyNexusEndpoint(resourceName, endpointName, temporalNamespace, taskQueue+"-again", "")

			Eventually(func(g Gomega) {
				g.Expect(temporalNexusEndpointExists(endpointName)).To(BeTrue())
			}, 3*time.Minute).Should(Succeed())

			Expect(nexusEndpointOwnership(resourceName)).To(Equal("Created"),
				"restoring an endpoint must not change who may delete it")
		})

		It("should leave the endpoint behind when the policy is Orphan", func() {
			readyNamespace()

			applyNexusEndpoint(resourceName, endpointName, temporalNamespace, taskQueue, "Orphan")
			Eventually(func(g Gomega) {
				g.Expect(nexusEndpointOwnership(resourceName)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			deleteNexusEndpointResource(resourceName)

			Expect(temporalNexusEndpointExists(endpointName)).To(BeTrue(),
				"Orphan must leave the endpoint alone")
		})

		It("should hold its target namespace open until it goes", func() {
			readyNamespace()

			applyNexusEndpoint(resourceName, endpointName, temporalNamespace, taskQueue, "")
			Eventually(func(g Gomega) {
				g.Expect(nexusEndpointOwnership(resourceName)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			// Temporal refuses to delete a namespace that a Nexus endpoint
			// targets, so the Namespace resource cannot finish deleting while
			// this endpoint exists. That is the Service enforcing the ordering,
			// not the operator: the Namespace holds its finalizer and retries.
			By("asking for the Namespace to be deleted")
			beginNamespaceDeletion(temporalNamespace)

			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("False"))
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("DeleteFailed"))
			}, 3*time.Minute).Should(Succeed())

			Expect(namespaceField(temporalNamespace, "{.metadata.finalizers}")).
				To(ContainSubstring("temporal.simonemms.com/namespace"))
			Expect(temporalNamespaceExists(temporalNamespace)).To(BeTrue())
			Expect(temporalNexusEndpointExists(endpointName)).To(BeTrue())

			By("deleting the endpoint, which unblocks the namespace")
			deleteNexusEndpointResource(resourceName)

			Eventually(func(g Gomega) {
				g.Expect(namespaceGone(temporalNamespace)).To(BeTrue())
			}, 3*time.Minute).Should(Succeed())

			Eventually(func(g Gomega) {
				g.Expect(temporalNamespaceExists(temporalNamespace)).To(BeFalse())
			}, 2*time.Minute).Should(Succeed())
		})

		It("should react to its Namespace becoming Ready without waiting for the retry", func() {
			applyConnectionSample()

			By("creating the NexusEndpoint before its Namespace exists")
			applyNexusEndpoint(resourceName, endpointName, temporalNamespace, taskQueue, "")

			Eventually(func(g Gomega) {
				g.Expect(nexusEndpointReady(resourceName)).To(Equal("False"))
				g.Expect(nexusEndpointReadyReason(resourceName)).To(Equal("NamespaceNotFound"))
			}, 3*time.Minute).Should(Succeed())

			By("creating the Namespace it was waiting for")
			applyNamespace(temporalNamespace, "36h")
			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
			}, 3*time.Minute).Should(Succeed())

			// Measured on the dependency gate rather than on Ready=True: getting
			// past the gate is the watch's doing, while what happens next waits
			// on Temporal's namespace registry.
			By("confirming it stops waiting on its Namespace promptly")
			start := time.Now()
			Eventually(func(g Gomega) {
				g.Expect(nexusEndpointReadyReason(resourceName)).NotTo(Equal("NamespaceNotFound"))
			}, watchResponseWindow).Should(Succeed())

			_, _ = fmt.Fprintf(GinkgoWriter,
				"NexusEndpoint stopped waiting %s after its Namespace became Ready\n",
				time.Since(start).Round(time.Millisecond))

			By("confirming it goes on to create the endpoint")
			Eventually(func(g Gomega) {
				g.Expect(nexusEndpointReady(resourceName)).To(Equal("True"))
				g.Expect(nexusEndpointReadyReason(resourceName)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())
		})
	})
})

// applyConnectionSample applies the sample Connection and waits for it to become
// Ready, so that Namespace specs start from a satisfied dependency.
func applyConnectionSample() {
	cmd := exec.Command("kubectl", "apply", "-n", "default",
		"-f", "config/samples/temporal_v1beta1_connection.yaml")
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to apply the Connection sample")

	Eventually(func(g Gomega) {
		g.Expect(connectionReady("connection-sample")).To(Equal("True"))
	}, 3*time.Minute).Should(Succeed())
}

// applyNamespace applies a Namespace referencing the sample Connection. An empty
// retention leaves the field out, so the CRD default applies.
func applyNamespace(name, retention string) {
	applyNamespaceWithPolicy(name, retention, "")
}

// applyNamespaceWithPolicy is applyNamespace with an explicit deletion policy.
// An empty policy leaves the field out, so the CRD default applies.
func applyNamespaceWithPolicy(name, retention, deletionPolicy string) {
	manifest := `apiVersion: temporal.simonemms.com/v1beta1
kind: Namespace
metadata:
  name: ` + name + `
spec:
  connectionRef:
    name: connection-sample
`
	if retention != "" {
		manifest += "  retention: " + retention + "\n"
	}

	if deletionPolicy != "" {
		manifest += "  deletionPolicy: " + deletionPolicy + "\n"
	}

	cmd := exec.Command("kubectl", "apply", "-n", "default", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)

	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to apply the Namespace")
}

// applyNamespaceWithArchival is applyNamespace with an archival block managing
// history alone. It is written out rather than built from a struct so the
// manifest reads as the YAML a user would actually apply.
func applyNamespaceWithArchival(name, retention string, historyEnabled bool) {
	manifest := fmt.Sprintf(`apiVersion: temporal.simonemms.com/v1beta1
kind: Namespace
metadata:
  name: %s
spec:
  connectionRef:
    name: connection-sample
  retention: %s
  archival:
    history:
      enabled: %t
`, name, retention, historyEnabled)

	cmd := exec.Command("kubectl", "apply", "-n", "default", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)

	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to apply the Namespace")
}

// setNamespaceDeletionPolicy patches the deletion policy on a Namespace. It is
// used on a terminating resource, which the API server permits.
func setNamespaceDeletionPolicy(name, deletionPolicy string) {
	cmd := exec.Command("kubectl", "patch", "namespace.temporal.simonemms.com", name,
		"-n", "default", "--type=merge",
		"-p", `{"spec":{"deletionPolicy":"`+deletionPolicy+`"}}`)

	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to set the deletion policy")
}

// beginNamespaceDeletion asks for deletion without waiting, so a spec can look
// at a resource the operator is refusing to let go of.
func beginNamespaceDeletion(name string) {
	cmd := exec.Command("kubectl", "delete", "namespace.temporal.simonemms.com", name,
		"-n", "default", "--wait=false")

	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to request deletion of the Namespace")
}

// namespaceGone reports whether the Namespace has left the API server.
func namespaceGone(name string) bool {
	cmd := exec.Command("kubectl", "get", "namespace.temporal.simonemms.com", name, "-n", "default")

	_, err := utils.Run(cmd)

	return err != nil
}

// deleteNamespace removes a Namespace resource and waits for the finalizer to
// let it go.
func deleteNamespace(name string) {
	cmd := exec.Command("kubectl", "delete", "namespace.temporal.simonemms.com", name,
		"-n", "default", "--wait=true", "--timeout=3m")

	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to delete the Namespace")
}

// clearOperatorResources removes every Connection and Namespace from the test
// namespace.
//
// Namespaces go first and are waited on: an owned Temporal namespace can only
// be deleted while its Connection still exists. Anything still stuck afterwards
// has its finalizer stripped, so one wedged resource cannot poison every
// following spec - the specs themselves assert the finalizer behaviour.
func clearOperatorResources() {
	cmd := exec.Command("kubectl", "delete", "namespace.temporal.simonemms.com",
		"--all", "-n", "default", "--wait=true", "--timeout=3m")
	if _, err := utils.Run(cmd); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Namespace cleanup did not complete, stripping finalizers: %s\n", err)

		cmd = exec.Command("kubectl", "patch", "namespace.temporal.simonemms.com", "--all",
			"-n", "default", "--type=merge", "-p", `{"metadata":{"finalizers":null}}`)
		_, _ = utils.Run(cmd)
	}

	cmd = exec.Command("kubectl", "delete", "connection", "--all", "-n", "default", "--wait=true")
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to remove existing Connections")
}

// createTemporalNamespace registers a namespace directly on the Temporal
// Service, standing in for one that pre-dates the operator.
func createTemporalNamespace(name, retention string) {
	_, err := utils.Run(temporalCLI("operator", "namespace", "create", "-n", name, "--retention", retention))
	Expect(err).NotTo(HaveOccurred(), "Failed to create the Temporal namespace")
}

// createTemporalNamespaceOwnedBy registers a namespace already carrying an
// ownership marker, standing in for one another Namespace resource owns.
func createTemporalNamespaceOwnedBy(name, retention, uid string) {
	_, err := utils.Run(temporalCLI("operator", "namespace", "create", "-n", name,
		"--retention", retention, "--data", fmt.Sprintf("%s=%s", ownerMarkerKey, uid)))
	Expect(err).NotTo(HaveOccurred(), "Failed to create the Temporal namespace")
}

// removeNamespaceFinalizer strips the operator's finalizer from a Namespace
// resource, standing in for someone unpicking a stuck resource by hand.
//
// Only do this with the operator stopped. Removing the finalizer from a live
// resource races whatever the operator is doing with it.
func removeNamespaceFinalizer(name string) {
	cmd := exec.Command("kubectl", "patch", "namespace.temporal.simonemms.com", name,
		"-n", "default", "--type=merge", "-p", `{"metadata":{"finalizers":null}}`)

	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to remove the Namespace finalizer")
}

// stopControllerManager scales the manager to zero and waits until its pod has
// actually gone, so a spec can rearrange resources with no reconciliation
// happening at all.
//
// The manager is scaled back up when the spec ends, however it ends, so a spec
// that fails part-way cannot leave the suite without an operator.
func stopControllerManager() {
	DeferCleanup(startControllerManager)

	cmd := exec.Command("kubectl", "scale", "deployment", controllerDeploymentName,
		"-n", namespace, "--replicas=0")
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to scale the controller-manager down")

	// Wait on the pod list, not on the Deployment's replica counts: those clear
	// the instant the scale is accepted, while the manager is still running and
	// still reconciling. The pod is the process.
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "pods", "-l", "control-plane=controller-manager",
			"-n", namespace, "-o", "jsonpath={.items[*].metadata.name}")
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(output).To(BeEmpty(), "The controller-manager pod should have gone")
	}, 3*time.Minute).Should(Succeed())
}

// startControllerManager returns the manager to its single replica and waits
// for it to be Available again. It is safe to call when the manager is already
// running.
//
// When it runs as a cleanup it can find the manager already gone: cleanup
// registered in a spec runs after the ordered container's AfterAll, which
// undeploys everything. There is nothing to restore in that case.
func startControllerManager() {
	if !controllerManagerDeployed() {
		return
	}

	cmd := exec.Command("kubectl", "scale", "deployment", controllerDeploymentName,
		"-n", namespace, "--replicas=1")
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to scale the controller-manager up")

	cmd = exec.Command("kubectl", "wait", "--for=condition=Available",
		"deployment/"+controllerDeploymentName, "-n", namespace, "--timeout=3m")
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "The controller-manager did not come back")
}

// controllerManagerDeployed reports whether the manager Deployment is still
// there to be scaled.
func controllerManagerDeployed() bool {
	cmd := exec.Command("kubectl", "get", "deployment", controllerDeploymentName, "-n", namespace)

	_, err := utils.Run(cmd)

	return err == nil
}

// removeTemporalNamespace clears up a namespace that outlived its resource.
//
// One delete attempt is not enough: the Service answers describe from storage
// but resolves deletes through a namespace registry that lags, so a namespace
// registered moments earlier can be reported missing by the delete and then
// carry on existing. Keep asking until it has really gone.
func removeTemporalNamespace(name string) {
	Eventually(func(g Gomega) {
		if !temporalNamespaceExists(name) {
			return
		}

		_, _ = utils.Run(temporalCLI("operator", "namespace", "delete", "-n", name, "--yes"))

		g.Expect(temporalNamespaceExists(name)).To(BeFalse(), "The Temporal namespace should have gone")
	}, 2*time.Minute).Should(Succeed())
}

// temporalNamespaceExists reports whether the Temporal Service still knows about
// a namespace.
func temporalNamespaceExists(name string) bool {
	_, err := utils.Run(temporalCLI("operator", "namespace", "describe", "-o", "json", "-n", name))

	return err == nil
}

// namespaceRetention reads spec.retention back as the API server stored it,
// which is the string the user wrote rather than the operator's canonical form.
func namespaceRetention(name string) string {
	cmd := exec.Command("kubectl", "get", "tns", name, "-n", "default",
		"-o", "jsonpath={.spec.retention}")

	output, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())

	return strings.TrimSpace(output)
}

// retentionSeconds renders a whole number of hours the way Temporal reports
// retention in its JSON output.
func retentionSeconds(hours int) string {
	return fmt.Sprintf("%ds", hours*3600)
}

// applySearchAttribute applies a SearchAttribute. resourceName is the
// Kubernetes resource's name and temporalName is what Temporal is asked for -
// deliberately different, because that is the distinction the API exists to
// draw. An empty deletionPolicy is left out so the CRD default applies.
func applySearchAttribute(resourceName, temporalName, namespaceRef, attrType, deletionPolicy string) {
	manifest := `apiVersion: temporal.simonemms.com/v1beta1
kind: SearchAttribute
metadata:
  name: ` + resourceName + `
spec:
  name: ` + temporalName + `
  connectionRef:
    name: connection-sample
  namespaceRef:
    name: ` + namespaceRef + `
  type: ` + attrType + `
`
	if deletionPolicy != "" {
		manifest += "  deletionPolicy: " + deletionPolicy + "\n"
	}

	cmd := exec.Command("kubectl", "apply", "-n", "default", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)

	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to apply the SearchAttribute")
}

// searchAttributeField reads a jsonpath expression from the named
// SearchAttribute.
func searchAttributeField(name, jsonPath string) string {
	cmd := exec.Command("kubectl", "get", "tsa", name, "-n", "default", "-o", "jsonpath="+jsonPath)

	output, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to read the SearchAttribute")

	return output
}

// searchAttributeReady returns the status of the named SearchAttribute's Ready
// condition.
func searchAttributeReady(name string) string {
	return searchAttributeField(name, "{.status.conditions[?(@.type=='Ready')].status}")
}

// searchAttributeReadyReason returns the reason on the Ready condition.
func searchAttributeReadyReason(name string) string {
	return searchAttributeField(name, "{.status.conditions[?(@.type=='Ready')].reason}")
}

// searchAttributeOwnership returns the persisted ownership.
func searchAttributeOwnership(name string) string {
	return searchAttributeField(name, "{.status.ownership}")
}

// searchAttributeGone reports whether the SearchAttribute has left the API
// server.
func searchAttributeGone(name string) bool {
	cmd := exec.Command("kubectl", "get", "tsa", name, "-n", "default")

	_, err := utils.Run(cmd)

	return err != nil
}

// deleteSearchAttribute removes a SearchAttribute and waits for the finalizer
// to let it go.
func deleteSearchAttribute(name string) {
	cmd := exec.Command("kubectl", "delete", "tsa", name, "-n", "default", "--wait=true", "--timeout=3m")

	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to delete the SearchAttribute")
}

// createTemporalSearchAttribute registers a search attribute directly on the
// Temporal Service, standing in for one that pre-dates the operator.
func createTemporalSearchAttribute(temporalNamespace, name, attrType string) {
	_, err := utils.Run(temporalCLI("operator", "search-attribute", "create",
		"-n", temporalNamespace, "--name", name, "--type", attrType))
	Expect(err).NotTo(HaveOccurred(), "Failed to create the Temporal search attribute")
}

// temporalSearchAttributeType returns the Temporal enum name of a registered
// custom search attribute, or an empty string when it is not registered.
func temporalSearchAttributeType(temporalNamespace, name string) string {
	output, err := utils.Run(temporalCLI("operator", "search-attribute", "list",
		"-n", temporalNamespace, "-o", "json"))
	if err != nil {
		return ""
	}

	var listed struct {
		CustomAttributes map[string]string `json:"customAttributes"`
	}
	Expect(json.Unmarshal([]byte(output), &listed)).To(Succeed(), "Failed to parse the search attribute list")

	return listed.CustomAttributes[name]
}

// applyNexusEndpoint applies a NexusEndpoint. resourceName is the Kubernetes
// resource's name and temporalName is what Temporal is asked for.
func applyNexusEndpoint(resourceName, temporalName, namespaceRef, taskQueue, deletionPolicy string) {
	applyNexusEndpointWithDescription(resourceName, temporalName, namespaceRef, taskQueue, deletionPolicy, nil)
}

// applyNexusEndpointWithDescription applies a NexusEndpoint carrying a
// description, or leaving the field out altogether when given nil - which is
// the difference between managing the description and leaving it be.
func applyNexusEndpointWithDescription(
	resourceName, temporalName, namespaceRef, taskQueue, deletionPolicy string,
	description *string,
) {
	manifest := `apiVersion: temporal.simonemms.com/v1beta1
kind: NexusEndpoint
metadata:
  name: ` + resourceName + `
spec:
  name: ` + temporalName + `
  connectionRef:
    name: connection-sample
  namespaceRef:
    name: ` + namespaceRef + `
  taskQueue: ` + taskQueue + `
`
	if deletionPolicy != "" {
		manifest += "  deletionPolicy: " + deletionPolicy + "\n"
	}

	if description != nil {
		if *description == "" {
			manifest += `  description: ""` + "\n"
		} else {
			manifest += "  description: |\n"
			for _, line := range strings.Split(strings.TrimSuffix(*description, "\n"), "\n") {
				manifest += "    " + line + "\n"
			}
		}
	}

	cmd := exec.Command("kubectl", "apply", "-n", "default", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)

	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to apply the NexusEndpoint")
}

// nexusEndpointField reads a jsonpath expression from the named NexusEndpoint.
func nexusEndpointField(name, jsonPath string) string {
	cmd := exec.Command("kubectl", "get", "tnx", name, "-n", "default", "-o", "jsonpath="+jsonPath)

	output, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to read the NexusEndpoint")

	return output
}

// nexusEndpointReady returns the status of the Ready condition.
func nexusEndpointReady(name string) string {
	return nexusEndpointField(name, "{.status.conditions[?(@.type=='Ready')].status}")
}

// nexusEndpointReadyReason returns the reason on the Ready condition.
func nexusEndpointReadyReason(name string) string {
	return nexusEndpointField(name, "{.status.conditions[?(@.type=='Ready')].reason}")
}

// nexusEndpointOwnership returns the persisted ownership.
func nexusEndpointOwnership(name string) string {
	return nexusEndpointField(name, "{.status.ownership}")
}

// deleteNexusEndpointResource removes a NexusEndpoint and waits for the
// finalizer to let it go.
func deleteNexusEndpointResource(name string) {
	cmd := exec.Command("kubectl", "delete", "tnx", name, "-n", "default", "--wait=true", "--timeout=3m")

	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to delete the NexusEndpoint")
}

// temporalNexusEndpoint is the part of `temporal operator nexus endpoint get
// -o json` output the specs assert on.
type temporalNexusEndpoint struct {
	ID   string `json:"id"`
	Spec struct {
		Name string `json:"name"`
		// The CLI decodes the description payload itself, so this arrives as
		// the Markdown string rather than as an encoded payload.
		Description string `json:"description"`
		Target      struct {
			Worker struct {
				Namespace string `json:"namespace"`
				TaskQueue string `json:"taskQueue"`
			} `json:"worker"`
		} `json:"target"`
	} `json:"spec"`
}

// describeTemporalNexusEndpoint asks the Temporal Service about an endpoint,
// reporting whether it is registered at all.
func describeTemporalNexusEndpoint(name string) (temporalNexusEndpoint, bool) {
	output, err := utils.Run(temporalCLI("operator", "nexus", "endpoint", "get", "--name", name, "-o", "json"))
	if err != nil {
		return temporalNexusEndpoint{}, false
	}

	var described temporalNexusEndpoint
	Expect(json.Unmarshal([]byte(output), &described)).To(Succeed(), "Failed to parse the endpoint")

	return described, described.Spec.Name == name
}

// temporalNexusEndpointDescription reads an endpoint's description back out of
// Temporal.
//
// Reading it through the CLI is the point: Temporal stores the description as a
// payload, and the CLI decoding it into plain Markdown is what proves the
// operator encoded it the way Temporal's own tooling expects.
func temporalNexusEndpointDescription(name string) string {
	described, found := describeTemporalNexusEndpoint(name)
	Expect(found).To(BeTrue(), "The Nexus endpoint should exist")

	return described.Spec.Description
}

// temporalNexusEndpointExists reports whether the Service still knows about an
// endpoint.
func temporalNexusEndpointExists(name string) bool {
	_, found := describeTemporalNexusEndpoint(name)

	return found
}

// createTemporalNexusEndpoint registers an endpoint directly on the Service,
// standing in for one that pre-dates the operator.
func createTemporalNexusEndpoint(name, namespace, taskQueue string) {
	Eventually(func(g Gomega) {
		_, err := utils.Run(temporalCLI("operator", "nexus", "endpoint", "create",
			"--name", name, "--target-namespace", namespace, "--target-task-queue", taskQueue))
		g.Expect(err).NotTo(HaveOccurred(), "Failed to create the Temporal Nexus endpoint")
	}, 2*time.Minute).Should(Succeed())
}

// removeTemporalNexusEndpoint clears up an endpoint that outlived its resource.
// Temporal refuses to delete a namespace while an endpoint targets it, so this
// has to happen before the namespace is cleaned up.
func removeTemporalNexusEndpoint(name string) {
	Eventually(func(g Gomega) {
		if !temporalNexusEndpointExists(name) {
			return
		}

		_, _ = utils.Run(temporalCLI("operator", "nexus", "endpoint", "delete", "--name", name))

		g.Expect(temporalNexusEndpointExists(name)).To(BeFalse(), "The Nexus endpoint should have gone")
	}, 2*time.Minute).Should(Succeed())
}

// clearNexusResources removes every NexusEndpoint in the test namespace and any
// Temporal endpoint left behind, before the namespaces they target are touched.
func clearNexusResources(endpointName string) {
	cmd := exec.Command("kubectl", "delete", "tnx", "--all",
		"-n", "default", "--wait=true", "--timeout=3m")
	if _, err := utils.Run(cmd); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "NexusEndpoint cleanup stalled, stripping finalizers: %s\n", err)

		cmd = exec.Command("kubectl", "patch", "tnx", "--all", "-n", "default",
			"--type=merge", "-p", `{"metadata":{"finalizers":null}}`)
		_, _ = utils.Run(cmd)
	}

	removeTemporalNexusEndpoint(endpointName)
}

// temporalNamespaceDescription is the part of `temporal operator namespace
// describe -o json` output the e2e specs assert on.
type temporalNamespaceDescription struct {
	NamespaceInfo struct {
		Name  string            `json:"name"`
		State string            `json:"state"`
		Data  map[string]string `json:"data"`
	} `json:"namespaceInfo"`
	Config struct {
		WorkflowExecutionRetentionTTL string `json:"workflowExecutionRetentionTtl"`
		HistoryArchivalState          string `json:"historyArchivalState"`
		HistoryArchivalURI            string `json:"historyArchivalUri"`
		VisibilityArchivalState       string `json:"visibilityArchivalState"`
		VisibilityArchivalURI         string `json:"visibilityArchivalUri"`
	} `json:"config"`
}

// Archival states as the Temporal CLI renders them in JSON. It prints the
// protobuf enum name rather than the shorthand the human-readable output shows.
const (
	archivalStateDisabled = "ARCHIVAL_STATE_DISABLED"
	archivalStateEnabled  = "ARCHIVAL_STATE_ENABLED"
)

// describeTemporalNamespace asks the Temporal Service itself about a namespace.
func describeTemporalNamespace(name string) temporalNamespaceDescription {
	output, err := utils.Run(temporalCLI("operator", "namespace", "describe", "-o", "json", "-n", name))
	Expect(err).NotTo(HaveOccurred(), "Failed to describe the Temporal namespace")

	var described temporalNamespaceDescription
	Expect(json.Unmarshal([]byte(output), &described)).To(Succeed(), "Failed to parse the description")

	return described
}

// nudgeDependants forces the operator to reconcile everything depending on the
// sample Connection, without changing any of it.
//
// A settled resource is otherwise only looked at again on its five-minute
// resync, which is far too long for a spec to wait. The Schedule controller
// watches Connection unfiltered - readiness lives in status, so a generation
// predicate would discard the events that matter - so annotating the Connection
// produces an event that enqueues every dependant immediately.
func nudgeDependants() {
	cmd := exec.Command("kubectl", "annotate", "connection", "connection-sample",
		"-n", "default", "e2e.temporal.simonemms.com/nudge="+fmt.Sprint(time.Now().UnixNano()),
		"--overwrite")

	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to nudge the Connection")
}

// applySchedule applies a Schedule. resourceName is the Kubernetes resource's
// name and scheduleID is what Temporal is asked for - deliberately different,
// because that is the distinction the API exists to draw. timing is the body of
// spec.schedule, indented to sit under it.
func applySchedule(resourceName, scheduleID, namespaceRef, timing, extra string) {
	manifest := `apiVersion: temporal.simonemms.com/v1beta1
kind: Schedule
metadata:
  name: ` + resourceName + `
spec:
  scheduleId: ` + scheduleID + `
  connectionRef:
    name: connection-sample
  namespaceRef:
    name: ` + namespaceRef + `
  schedule:
` + timing + `  action:
    workflow:
      type: ReconcilePayments
      taskQueue: payments
`
	manifest += extra

	cmd := exec.Command("kubectl", "apply", "-n", "default", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)

	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to apply the Schedule")
}

// deleteScheduleResource removes a Schedule and waits for it to go, so that a
// spec asserting on what happened afterwards is not racing the finalizer.
func deleteScheduleResource(resourceName string) {
	cmd := exec.Command("kubectl", "delete", "schedule.temporal.simonemms.com", resourceName,
		"-n", "default", "--wait=true", "--timeout=3m")

	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to delete the Schedule")
}

// clearSchedules removes every Schedule from the test namespace, stripping
// finalizers from anything that will not go quietly so one wedged resource
// cannot poison every following spec.
func clearSchedules() {
	cmd := exec.Command("kubectl", "delete", "schedule.temporal.simonemms.com",
		"--all", "-n", "default", "--wait=true", "--timeout=3m")
	if _, err := utils.Run(cmd); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Schedule cleanup did not complete, stripping finalizers: %s\n", err)

		cmd = exec.Command("kubectl", "patch", "schedule.temporal.simonemms.com", "--all",
			"-n", "default", "--type=merge", "-p", `{"metadata":{"finalizers":null}}`)
		_, _ = utils.Run(cmd)
	}
}

// scheduleField reads one field from a Schedule with a JSONPath expression.
func scheduleField(resourceName, jsonpath string) string {
	cmd := exec.Command("kubectl", "get", "tsc", resourceName, "-n", "default", "-o", "jsonpath="+jsonpath)

	output, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())

	return strings.TrimSpace(output)
}

// scheduleReady returns the status of the named Schedule's Ready condition.
func scheduleReady(resourceName string) string {
	return scheduleField(resourceName, "{.status.conditions[?(@.type=='Ready')].status}")
}

// scheduleReadyReason returns the reason on the named Schedule's Ready condition.
func scheduleReadyReason(resourceName string) string {
	return scheduleField(resourceName, "{.status.conditions[?(@.type=='Ready')].reason}")
}

// scheduleOwnership returns the persisted ownership of the named Schedule.
func scheduleOwnership(resourceName string) string {
	return scheduleField(resourceName, "{.status.ownership}")
}

// temporalScheduleDescription is the part of `temporal schedule describe -o json`
// output the e2e specs assert on.
type temporalScheduleDescription struct {
	Schedule struct {
		Spec struct {
			StructuredCalendar []struct {
				Hour []struct {
					Start int `json:"start"`
				} `json:"hour"`
				Minute []struct {
					Start int `json:"start"`
				} `json:"minute"`
			} `json:"structuredCalendar"`
			Interval []struct {
				Interval string `json:"interval"`
			} `json:"interval"`
			TimezoneName string `json:"timezoneName"`
		} `json:"spec"`
		Action struct {
			StartWorkflow struct {
				WorkflowType struct {
					Name string `json:"name"`
				} `json:"workflowType"`
				TaskQueue struct {
					Name string `json:"name"`
				} `json:"taskQueue"`
			} `json:"startWorkflow"`
		} `json:"action"`
		Policies struct {
			OverlapPolicy string `json:"overlapPolicy"`
		} `json:"policies"`
		State struct {
			Paused bool   `json:"paused"`
			Notes  string `json:"notes"`
		} `json:"state"`
	} `json:"schedule"`
}

// describeTemporalSchedule asks the Temporal Service itself about a schedule.
func describeTemporalSchedule(temporalNamespace, scheduleID string) temporalScheduleDescription {
	output, err := utils.Run(temporalCLI("schedule", "describe",
		"-n", temporalNamespace, "--schedule-id", scheduleID, "-o", "json"))
	Expect(err).NotTo(HaveOccurred(), "Failed to describe the Temporal schedule")

	var described temporalScheduleDescription
	Expect(json.Unmarshal([]byte(output), &described)).To(Succeed(), "Failed to parse the description")

	return described
}

// temporalScheduleExists reports whether the Temporal Service still knows about
// a schedule.
func temporalScheduleExists(temporalNamespace, scheduleID string) bool {
	_, err := utils.Run(temporalCLI("schedule", "describe",
		"-n", temporalNamespace, "--schedule-id", scheduleID, "-o", "json"))

	return err == nil
}

// createTemporalSchedule creates a schedule with the Temporal CLI, standing in
// for one that existed before the operator did.
func createTemporalSchedule(temporalNamespace, scheduleID string) {
	// The CLI spells the workflow type "--type", not "--workflow-type".
	_, err := utils.Run(temporalCLI("schedule", "create",
		"-n", temporalNamespace, "--schedule-id", scheduleID,
		"--cron", "30 2 * * *",
		"--workflow-id", "adopted-"+scheduleID,
		"--type", "ReconcilePayments",
		"--task-queue", "payments"))
	Expect(err).NotTo(HaveOccurred(), "Failed to create the Temporal schedule")
}

// removeTemporalSchedule deletes a schedule if it is still there, so that a
// reused cluster stays predictable.
func removeTemporalSchedule(temporalNamespace, scheduleID string) {
	if !temporalScheduleExists(temporalNamespace, scheduleID) {
		return
	}

	_, _ = utils.Run(temporalCLI("schedule", "delete",
		"-n", temporalNamespace, "--schedule-id", scheduleID))
}

// temporalCLI builds a kubectl exec running the Temporal CLI inside the Temporal
// deployment, so no extra tooling is needed on the test host.
func temporalCLI(args ...string) *exec.Cmd {
	full := append([]string{"exec", "-n", "temporal", "deploy/temporal", "--", "temporal"}, args...)

	return exec.Command("kubectl", full...)
}

// namespaceOwnership returns the persisted ownership of the named Namespace.
func namespaceOwnership(name string) string {
	return namespaceField(name, "{.status.ownership}")
}

// namespaceReady returns the status of the named Namespace's Ready condition.
func namespaceReady(name string) string {
	return namespaceReadyField(name, "status")
}

// namespaceReadyReason returns the reason on the named Namespace's Ready condition.
func namespaceReadyReason(name string) string {
	return namespaceReadyField(name, "reason")
}

// namespaceReadyField reads a single field from the named Namespace's Ready condition.
func namespaceReadyField(name, field string) string {
	return namespaceField(name, fmt.Sprintf("{.status.conditions[?(@.type=='Ready')].%s}", field))
}

// namespaceField reads a jsonpath expression from the named Namespace.
func namespaceField(name, jsonPath string) string {
	cmd := exec.Command("kubectl", "get", "namespace.temporal.simonemms.com", name,
		"-n", "default", "-o", "jsonpath="+jsonPath)

	output, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to read the Namespace")

	return output
}

// connectionReady returns the status of the named Connection's Ready condition.
func connectionReady(name string) string {
	return connectionReadyField(name, "status")
}

// connectionReadyReason returns the reason on the named Connection's Ready condition.
func connectionReadyReason(name string) string {
	return connectionReadyField(name, "reason")
}

// connectionReadyField reads a single field from the named Connection's Ready condition.
func connectionReadyField(name, field string) string {
	return connectionField(name, fmt.Sprintf("{.status.conditions[?(@.type=='Ready')].%s}", field))
}

// connectionField reads a jsonpath expression from the named Connection.
func connectionField(name, jsonPath string) string {
	cmd := exec.Command("kubectl", "get", "connection", name, "-n", "default", "-o", "jsonpath="+jsonPath)

	output, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to read the Connection")

	return output
}

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	By("creating temporary file to store the token request")
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		By("executing kubectl command to create the token")
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		By("parsing the JSON output to extract the token")
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() (string, error) {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	return utils.Run(cmd)
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
