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

// NamespaceFinalizer holds a Namespace open until the operator has decided what
// to do with the Temporal namespace it manages.
const NamespaceFinalizer = "temporal.simonemms.com/namespace"

// NamespaceOwnership records how a Namespace came to manage its Temporal
// namespace, and therefore whether deleting the resource should delete the
// Temporal namespace too.
//
// The value is set once and then left alone: an established ownership never
// flips between Created and Adopted, because whether the Temporal namespace
// happens to exist right now says nothing about who put it there.
// +kubebuilder:validation:Enum=Creating;Created;Adopted
type NamespaceOwnership string

const (
	// NamespaceOwnershipCreating records that the operator found the Temporal
	// namespace missing and is about to register it. It is written before the
	// registration call, so that a reconcile interrupted between registering
	// and recording the result can still tell that the namespace it now sees is
	// its own work rather than something it should adopt.
	NamespaceOwnershipCreating NamespaceOwnership = "Creating"

	// NamespaceOwnershipCreated records that this resource caused the Temporal
	// namespace to exist. Deleting the resource deletes the namespace.
	NamespaceOwnershipCreated NamespaceOwnership = "Created"

	// NamespaceOwnershipAdopted records that the Temporal namespace pre-dated
	// this resource. The operator manages its configuration but does not own
	// its lifecycle, so deleting the resource leaves the namespace alone.
	NamespaceOwnershipAdopted NamespaceOwnership = "Adopted"
)

// OwnsTemporalNamespace reports whether deleting this resource should delete the
// Temporal namespace.
//
// Creating counts as owned. The marker is only ever written after the operator
// has seen the namespace missing, so anything under that name is either this
// resource's own registration or nothing at all - and deleting nothing is
// harmless.
func (o NamespaceOwnership) OwnsTemporalNamespace() bool {
	return o == NamespaceOwnershipCreating || o == NamespaceOwnershipCreated
}

// IsEstablished reports whether ownership has been settled one way or the other.
func (o NamespaceOwnership) IsEstablished() bool {
	return o == NamespaceOwnershipCreated || o == NamespaceOwnershipAdopted
}

// NamespaceDeletionPolicy decides what becomes of the Temporal namespace when
// the resource managing it is deleted.
//
// It governs lifecycle, not ownership: it can only ever stop the operator
// deleting a namespace, never grant it the right to delete one it does not own.
// +kubebuilder:validation:Enum=Delete;Orphan
type NamespaceDeletionPolicy string

const (
	// NamespaceDeletionPolicyDelete removes the Temporal namespace along with
	// the resource - but only where the resource owns it, and only once
	// Temporal's own metadata confirms as much. Anything the operator cannot
	// prove is its own is left alone, as it always was.
	NamespaceDeletionPolicyDelete NamespaceDeletionPolicy = "Delete"

	// NamespaceDeletionPolicyOrphan leaves the Temporal namespace exactly where
	// it is, whoever owns it. The operator asks Temporal nothing at all, so
	// deletion succeeds even when the Connection it would have needed is gone.
	NamespaceDeletionPolicyOrphan NamespaceDeletionPolicy = "Orphan"
)

// DefaultDeletionPolicy is applied when a Namespace does not ask for one. It
// matches the CRD's default, and is applied again in Go so that an object built
// in memory behaves the same as one that has been through admission.
const DefaultDeletionPolicy = NamespaceDeletionPolicyDelete

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

	// deletionPolicy decides what happens to the Temporal namespace when this
	// resource is deleted. Defaults to Delete.
	//
	// Delete removes the Temporal namespace along with the resource, where the
	// operator owns it and can prove so from Temporal's own metadata. That
	// needs a working Connection, so deletion waits rather than orphaning a
	// namespace it is responsible for.
	//
	// Orphan leaves the Temporal namespace behind and contacts Temporal not at
	// all, which is how a resource is released when the Connection it would
	// have needed no longer exists.
	//
	// This may be changed while the resource is being deleted, which is the
	// supported way out of a deletion blocked on a missing Connection.
	// +optional
	// +kubebuilder:default=Delete
	DeletionPolicy NamespaceDeletionPolicy `json:"deletionPolicy,omitempty"`
}

// RetentionDuration returns the requested retention, falling back to
// DefaultRetention when it is unset or not positive.
func (s NamespaceSpec) RetentionDuration() time.Duration {
	if s.Retention == nil || s.Retention.Duration <= 0 {
		return DefaultRetention
	}

	return s.Retention.Duration
}

// DeletionPolicyValue returns the requested deletion policy, falling back to
// DefaultDeletionPolicy when it is unset.
//
// This is the one place the policy is resolved, so an object built in memory
// and one defaulted by the API server are read the same way.
func (s NamespaceSpec) DeletionPolicyValue() NamespaceDeletionPolicy {
	if s.DeletionPolicy == "" {
		return DefaultDeletionPolicy
	}

	return s.DeletionPolicy
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

	// ownership records whether the operator created the Temporal namespace or
	// adopted one that already existed, and therefore whether deleting this
	// resource deletes the Temporal namespace.
	//
	// "Creating" is a transient marker written immediately before registering a
	// namespace; it settles to "Created" once registration is confirmed.
	// +optional
	Ownership NamespaceOwnership `json:"ownership,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=tns
// +kubebuilder:printcolumn:name="Connection",type=string,JSONPath=".spec.connectionRef.name"
// +kubebuilder:printcolumn:name="Retention",type=string,JSONPath=".spec.retention"
// +kubebuilder:printcolumn:name="Ownership",type=string,JSONPath=".status.ownership"
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
