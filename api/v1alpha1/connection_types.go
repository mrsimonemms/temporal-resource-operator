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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	// ConditionTypeReady is set on a Connection once the Temporal Service
	// described by the resource has been successfully contacted.
	ConditionTypeReady = "Ready"
)

// Well-known keys used when credentials are stored in a Kubernetes Secret.
// They deliberately mirror the JSON names on Credentials.
const (
	// SecretKeyAPIKey is the Secret key holding a Temporal API key.
	SecretKeyAPIKey = "apiKey"

	// SecretKeyClientCert is the Secret key holding a PEM-encoded mTLS client certificate.
	SecretKeyClientCert = "clientCert"

	// SecretKeyClientKey is the Secret key holding a PEM-encoded mTLS client private key.
	SecretKeyClientKey = "clientKey"
)

// Credentials are the secret values used to authenticate against a Temporal Service.
//
// An API key and an mTLS certificate pair may be supplied together - Temporal
// Cloud API keys and mTLS are alternative authentication mechanisms, but
// nothing prevents a Service from being fronted by a mutually-authenticated
// proxy.
type Credentials struct {
	// apiKey is a Temporal API key, sent as a bearer token on every request.
	//
	// Providing an API key causes the Temporal SDK to enable TLS if it has not
	// been explicitly configured.
	// +optional
	APIKey string `json:"apiKey,omitempty"`

	// clientCert is a PEM-encoded client certificate used for mTLS. It must be
	// supplied alongside clientKey.
	// +optional
	ClientCert string `json:"clientCert,omitempty"`

	// clientKey is the PEM-encoded private key for clientCert. It must be
	// supplied alongside clientCert.
	// +optional
	ClientKey string `json:"clientKey,omitempty"`
}

// ConnectionSpec defines the desired state of Connection
//
// +kubebuilder:validation:XValidation:rule="!(has(self.credentials) && has(self.credentialsSecretRef))",message="credentials and credentialsSecretRef are mutually exclusive"
//
//nolint:lll // kubebuilder markers cannot be wrapped
type ConnectionSpec struct {
	// address is the host:port of the Temporal Service's gRPC endpoint, for
	// example "localhost:7233" or "my-namespace.a1b2c.tmprl.cloud:7233".
	// +required
	// +kubebuilder:validation:MinLength=1
	Address string `json:"address"`

	// credentials holds the credentials inline. This is intended for
	// development and testing - use credentialsSecretRef in production.
	//
	// This is mutually exclusive with credentialsSecretRef.
	// +optional
	Credentials *Credentials `json:"credentials,omitempty"`

	// credentialsSecretRef references a Secret in the same namespace as this
	// Connection holding the credentials. The Secret may contain any of the
	// "apiKey", "clientCert" and "clientKey" keys.
	//
	// This is mutually exclusive with credentials.
	// +optional
	CredentialsSecretRef *corev1.LocalObjectReference `json:"credentialsSecretRef,omitempty"`

	// tls enables TLS on the connection to the Temporal Service.
	//
	// TLS is also implied by setting tlsServerName, by supplying an mTLS client
	// certificate, and (by the Temporal SDK itself) by supplying an API key.
	// +optional
	TLS bool `json:"tls,omitempty"`

	// tlsServerName overrides the server name used to verify the Temporal
	// Service's certificate. Setting this enables TLS.
	// +optional
	TLSServerName string `json:"tlsServerName,omitempty"`
}

// ConnectionStatus defines the observed state of Connection.
type ConnectionStatus struct {
	// conditions represent the current state of the Connection resource.
	//
	// Known condition types:
	// - "Ready": the Temporal Service is reachable and reports itself healthy
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// observedGeneration is the metadata.generation of the Connection that was
	// last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Address",type=string,JSONPath=".spec.address"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// Connection is the Schema for the connections API
type Connection struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Connection
	// +required
	Spec ConnectionSpec `json:"spec"`

	// status defines the observed state of Connection
	// +optional
	Status ConnectionStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ConnectionList contains a list of Connection
type ConnectionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Connection `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Connection{}, &ConnectionList{})
		return nil
	})
}
