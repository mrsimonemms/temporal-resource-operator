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
			cmd := exec.Command("kubectl", "delete", "namespace.temporal.simonemms.com,connection",
				"--all", "-n", "default", "--wait=true")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to remove existing resources")

			By("waiting for the Temporal Service to be available")
			cmd = exec.Command("kubectl", "wait", "--for=condition=Available",
				"deployment/temporal", "-n", "temporal", "--timeout=5m")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Temporal did not become available")
		})

		AfterEach(func() {
			By("removing the Kubernetes resources")
			cmd := exec.Command("kubectl", "delete", "namespace.temporal.simonemms.com,connection",
				"--all", "-n", "default", "--wait=true")
			_, _ = utils.Run(cmd)

			// The operator deliberately leaves Temporal namespaces behind, so
			// remove this one here to keep a reused cluster predictable.
			By("removing the Temporal namespace")
			_, _ = utils.Run(temporalCLI("operator", "namespace", "delete", "-n", temporalNamespace, "--yes"))
		})

		It("should register the Temporal namespace and report Ready=True", func() {
			By("applying a ready Connection")
			applyConnectionSample()

			By("applying a Namespace")
			applyNamespace(temporalNamespace, "36h")

			By("waiting for the Namespace to report Ready=True with reason Created")
			Eventually(func(g Gomega) {
				g.Expect(namespaceField(temporalNamespace, "{.status.observedGeneration}")).
					To(Equal(namespaceField(temporalNamespace, "{.metadata.generation}")))
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("Created"))
			}, 3*time.Minute).Should(Succeed())

			By("confirming the namespace exists on the Temporal Service")
			described := describeTemporalNamespace(temporalNamespace)
			Expect(described.NamespaceInfo.Name).To(Equal(temporalNamespace))
			Expect(described.Config.WorkflowExecutionRetentionTTL).To(Equal("129600s"), "36h of retention")

			By("recreating the Namespace and confirming the existing namespace is adopted")
			cmd := exec.Command("kubectl", "delete", "namespace.temporal.simonemms.com",
				temporalNamespace, "-n", "default", "--wait=true")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete the Namespace")

			applyNamespace(temporalNamespace, "36h")

			Eventually(func(g Gomega) {
				g.Expect(namespaceReady(temporalNamespace)).To(Equal("True"))
				g.Expect(namespaceReadyReason(temporalNamespace)).To(Equal("Reconciled"))
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
			Expect(described.Config.WorkflowExecutionRetentionTTL).To(Equal("259200s"), "72h of retention")
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

	cmd := exec.Command("kubectl", "apply", "-n", "default", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)

	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to apply the Namespace")
}

// temporalNamespaceDescription is the part of `temporal operator namespace
// describe -o json` output the e2e specs assert on.
type temporalNamespaceDescription struct {
	NamespaceInfo struct {
		Name  string `json:"name"`
		State string `json:"state"`
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
