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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// DefaultRetention is the workflow execution retention applied when a Namespace
// does not ask for one. It matches the CRD's default, and is applied again in
// Go so that an object built in memory behaves the same as one that has been
// through admission.
const DefaultRetention = 72 * time.Hour

// NamespaceSpec defines the desired state of Namespace
type NamespaceSpec struct {
	// connectionRef references the Connection, in the same Kubernetes
	// namespace, describing the Temporal Service that owns this namespace.
	// +required
	// +kubebuilder:validation:XValidation:rule="self.name != ''",message="connectionRef.name is required"
	ConnectionRef corev1.LocalObjectReference `json:"connectionRef"`

	// retention is how long the Temporal Service keeps a workflow execution's
	// history after it closes. Defaults to 72h.
	//
	// Only the forms accepted by Go's time.ParseDuration work here, so "72h"
	// rather than "3d".
	// +optional
	// +kubebuilder:default="72h"
	Retention *metav1.Duration `json:"retention,omitempty"`
}

// RetentionDuration returns the requested retention, falling back to
// DefaultRetention when it is unset or not positive.
func (s NamespaceSpec) RetentionDuration() time.Duration {
	if s.Retention == nil || s.Retention.Duration <= 0 {
		return DefaultRetention
	}

	return s.Retention.Duration
}

// NamespaceStatus defines the observed state of Namespace.
type NamespaceStatus struct {
	// conditions represent the current state of the Namespace resource.
	//
	// Known condition types:
	// - "Ready": the Temporal namespace exists on the referenced Service
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// observedGeneration is the metadata.generation of the Namespace that was
	// last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Connection",type=string,JSONPath=".spec.connectionRef.name"
// +kubebuilder:printcolumn:name="Retention",type=string,JSONPath=".spec.retention"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// Namespace is the Schema for the namespaces API.
//
// The resource's own metadata.name is the Temporal namespace name, so a
// Temporal namespace is named once and cannot drift from the resource
// identifying it.
type Namespace struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Namespace
	// +required
	Spec NamespaceSpec `json:"spec"`

	// status defines the observed state of Namespace
	// +optional
	Status NamespaceStatus `json:"status,omitzero"`
}

// TemporalName returns the name of the Temporal namespace this resource
// manages.
func (n *Namespace) TemporalName() string {
	return n.Name
}

// +kubebuilder:object:root=true

// NamespaceList contains a list of Namespace
type NamespaceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Namespace `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Namespace{}, &NamespaceList{})
		return nil
	})
}
