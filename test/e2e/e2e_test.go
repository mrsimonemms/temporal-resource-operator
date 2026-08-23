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
const namespace = "app-system"

// serviceAccountName created for the project
const serviceAccountName = "app-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "app-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "app-metrics-binding"

// controllerDeploymentName is the Deployment running the manager, in the
// namespace above.
const controllerDeploymentName = "app-controller-manager"

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
				"--clusterrole=app-metrics-reader",
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
				"-f", "config/samples/temporal_v1alpha1_connection.yaml")
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
			cmd.Stdin = strings.NewReader(`apiVersion: temporal.simonemms.com/v1alpha1
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
			cmd.Stdin = strings.NewReader(`apiVersion: temporal.simonemms.com/v1alpha1
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
			cmd.Stdin = strings.NewReader(`apiVersion: temporal.simonemms.com/v1alpha1
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
})

// applyConnectionSample applies the sample Connection and waits for it to become
// Ready, so that Namespace specs start from a satisfied dependency.
func applyConnectionSample() {
	cmd := exec.Command("kubectl", "apply", "-n", "default",
		"-f", "config/samples/temporal_v1alpha1_connection.yaml")
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
	manifest := `apiVersion: temporal.simonemms.com/v1alpha1
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

// retentionSeconds renders a whole number of hours the way Temporal reports
// retention in its JSON output.
func retentionSeconds(hours int) string {
	return fmt.Sprintf("%ds", hours*3600)
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
	} `json:"config"`
}

// describeTemporalNamespace asks the Temporal Service itself about a namespace.
func describeTemporalNamespace(name string) temporalNamespaceDescription {
	output, err := utils.Run(temporalCLI("operator", "namespace", "describe", "-o", "json", "-n", name))
	Expect(err).NotTo(HaveOccurred(), "Failed to describe the Temporal namespace")

	var described temporalNamespaceDescription
	Expect(json.Unmarshal([]byte(output), &described)).To(Succeed(), "Failed to parse the description")

	return described
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
