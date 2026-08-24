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

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// NexusEndpointFinalizer holds a NexusEndpoint open until the operator has
// decided what to do with the Temporal Nexus endpoint it manages.
const NexusEndpointFinalizer = "temporal.simonemms.com/nexus-endpoint"

// NexusEndpointDeletionPolicy decides what becomes of the Temporal Nexus
// endpoint when the resource managing it is deleted.
//
// It mirrors the policy the other resources use deliberately - they should all
// behave the same way - but is its own type so that each resource's API
// describes what it actually does.
// +kubebuilder:validation:Enum=Delete;Orphan
type NexusEndpointDeletionPolicy string

const (
	// NexusEndpointDeletionPolicyDelete removes the Temporal Nexus endpoint
	// along with the resource, but only where the resource created it.
	NexusEndpointDeletionPolicyDelete NexusEndpointDeletionPolicy = "Delete"

	// NexusEndpointDeletionPolicyOrphan leaves the Temporal Nexus endpoint
	// exactly where it is. The operator asks Temporal nothing at all, so
	// deletion succeeds even when the Connection it would have needed is gone.
	NexusEndpointDeletionPolicyOrphan NexusEndpointDeletionPolicy = "Orphan"
)

// DefaultNexusEndpointDeletionPolicy is applied when a NexusEndpoint does not
// ask for one. It matches the CRD's default, and is applied again in Go so that
// an object built in memory behaves the same as one that has been through
// admission.
const DefaultNexusEndpointDeletionPolicy = NexusEndpointDeletionPolicyDelete

// NexusEndpointOwnership records how a NexusEndpoint came to manage its
// Temporal endpoint, and therefore whether deleting the resource deletes the
// endpoint.
//
// A Nexus endpoint carries a server-assigned ID but nowhere to record who
// created it - its only free-form field is a markdown description meant for
// people. This is the operator's own bookkeeping, and it cannot be checked
// against Temporal the way a namespace's owner marker can.
// +kubebuilder:validation:Enum=Creating;Created;Adopted
type NexusEndpointOwnership string

const (
	// NexusEndpointOwnershipCreating records that the operator found the
	// endpoint missing and is about to register it. It is written before the
	// create call, so that a reconcile interrupted between creating and
	// recording the result can still tell that the endpoint it now sees is most
	// likely its own work rather than something to adopt.
	NexusEndpointOwnershipCreating NexusEndpointOwnership = "Creating"

	// NexusEndpointOwnershipCreated records that this resource caused the
	// endpoint to exist. Deleting the resource deletes the endpoint.
	NexusEndpointOwnershipCreated NexusEndpointOwnership = "Created"

	// NexusEndpointOwnershipAdopted records that the endpoint pre-dated this
	// resource. The operator keeps its configuration in step but does not own
	// its lifecycle, so deleting the resource leaves the endpoint alone.
	NexusEndpointOwnershipAdopted NexusEndpointOwnership = "Adopted"
)

// OwnsEndpoint reports whether deleting this resource should delete the
// Temporal Nexus endpoint.
//
// Creating counts as owned. The marker is only ever written after the operator
// has seen the endpoint missing, so what is there now is either this resource's
// own work or nothing at all.
func (o NexusEndpointOwnership) OwnsEndpoint() bool {
	return o == NexusEndpointOwnershipCreating || o == NexusEndpointOwnershipCreated
}

// IsEstablished reports whether ownership has been settled one way or the other.
func (o NexusEndpointOwnership) IsEstablished() bool {
	return o == NexusEndpointOwnershipCreated || o == NexusEndpointOwnershipAdopted
}

// NexusEndpointSpec defines the desired state of NexusEndpoint
//
// name and connectionRef say which real endpoint, on which Temporal Service,
// this resource stands for, and are immutable: changing either would not rename
// or move anything, it would point the resource at a different endpoint and
// strand whatever it was looking after. Renaming is not something Temporal
// supports gracefully either - it breaks every workflow caller referencing the
// endpoint.
//
// The target - namespaceRef and taskQueue - is ordinary mutable configuration,
// because Temporal's update API replaces it in place.
type NexusEndpointSpec struct {
	// name is the Temporal Nexus endpoint's name, exactly as Temporal knows it.
	// Endpoint names are unique across the whole Service, not per namespace.
	//
	// This is separate from the resource's own metadata.name because Kubernetes
	// only accepts lowercase names for a resource, while endpoint names are
	// conventionally PascalCase. Nothing is derived from metadata.name.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="name is immutable"
	Name string `json:"name"`

	// connectionRef references the Connection, in the same Kubernetes
	// namespace, describing the Temporal Service holding this endpoint.
	// +required
	// +kubebuilder:validation:XValidation:rule="self.name != ''",message="connectionRef.name is required"
	// +kubebuilder:validation:XValidation:rule="self.name == oldSelf.name",message="connectionRef.name is immutable"
	ConnectionRef corev1.LocalObjectReference `json:"connectionRef"`

	// namespaceRef references the Namespace, in the same Kubernetes namespace,
	// whose Temporal namespace this endpoint routes requests to.
	//
	// A Namespace's own metadata.name is its Temporal namespace name, so this
	// is also the Temporal namespace in the endpoint's target. Temporal refuses
	// to register an endpoint whose target namespace does not exist, which is
	// why the Namespace has to be Ready before the endpoint is created.
	//
	// This is part of the endpoint's target rather than its identity, so it can
	// be changed: the operator updates the endpoint in place.
	// +required
	// +kubebuilder:validation:XValidation:rule="self.name != ''",message="namespaceRef.name is required"
	NamespaceRef corev1.LocalObjectReference `json:"namespaceRef"`

	// taskQueue is the Nexus task queue in the target namespace that requests
	// are routed to. Like the target namespace, it can be changed in place.
	// +required
	// +kubebuilder:validation:MinLength=1
	TaskQueue string `json:"taskQueue"`

	// description is the endpoint's description, as Markdown. Temporal's UI
	// renders it on the endpoint's page, which is what it is for: telling
	// whoever finds the endpoint what it is and who looks after it.
	//
	// Omitting the field leaves the description alone, so one set by hand or by
	// another tool survives. Setting it makes the description managed state
	// like anything else in the spec: the operator writes it, and puts it back
	// if it is changed behind the operator's back. Setting it to the empty
	// string removes the description.
	// +optional
	Description *string `json:"description,omitempty"`

	// deletionPolicy decides what happens to the Temporal Nexus endpoint when
	// this resource is deleted. Defaults to Delete.
	//
	// Delete removes the endpoint along with the resource, where the operator
	// created it. Orphan leaves it behind and contacts Temporal not at all,
	// which is how a resource is released when the Connection it would have
	// needed no longer exists.
	//
	// This may be changed while the resource is being deleted, which is the
	// supported way out of a deletion blocked on a missing Connection.
	// +optional
	// +kubebuilder:default=Delete
	DeletionPolicy NexusEndpointDeletionPolicy `json:"deletionPolicy,omitempty"`
}

// TemporalName returns the name of the Temporal Nexus endpoint this resource
// stands for.
//
// This is the one place the mapping lives. It is deliberately not derived from
// the resource's metadata.name, which is Kubernetes identity and nothing more.
func (s *NexusEndpointSpec) TemporalName() string {
	return s.Name
}

// TemporalNamespace returns the Temporal namespace the endpoint targets.
//
// This comes from the reference rather than from the Namespace resource itself,
// because a Namespace's Temporal name is its own metadata.name.
func (s *NexusEndpointSpec) TemporalNamespace() string {
	return s.NamespaceRef.Name
}

// DeletionPolicyValue returns the requested deletion policy, falling back to
// DefaultNexusEndpointDeletionPolicy when it is unset.
//
// Unlike the fields identifying the external endpoint, this one is mutable -
// deliberately, because switching a stuck resource to Orphan while it is
// already terminating is how a deletion blocked on a broken dependency is
// released.
func (s *NexusEndpointSpec) DeletionPolicyValue() NexusEndpointDeletionPolicy {
	if s.DeletionPolicy == "" {
		return DefaultNexusEndpointDeletionPolicy
	}

	return s.DeletionPolicy
}

// NexusEndpointStatus defines the observed state of NexusEndpoint.
type NexusEndpointStatus struct {
	// conditions represent the current state of the NexusEndpoint resource.
	//
	// Known condition types:
	// - "Ready": the endpoint exists on the referenced Service and its target
	//   matches the spec
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// observedGeneration is the metadata.generation of the NexusEndpoint that
	// was last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ownership records whether the operator created the Temporal endpoint or
	// adopted one that already existed, and therefore whether deleting this
	// resource removes it.
	//
	// "Creating" is a transient marker written immediately before creating an
	// endpoint; it settles to "Created" once creation is confirmed.
	// +optional
	Ownership NexusEndpointOwnership `json:"ownership,omitempty"`

	// endpointId is the server-assigned ID of the endpoint, recorded as an
	// observation to make the resource easier to correlate with Temporal.
	//
	// It is not proof of ownership: anyone can read it, and it says nothing
	// about who created the endpoint. The operator always re-reads the endpoint
	// before changing it rather than trusting what is recorded here.
	// +optional
	EndpointID string `json:"endpointId,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=tnx
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=".spec.name"
// +kubebuilder:printcolumn:name="Temporal NS",type=string,JSONPath=".spec.namespaceRef.name"
// +kubebuilder:printcolumn:name="Task Queue",type=string,JSONPath=".spec.taskQueue"
// +kubebuilder:printcolumn:name="Ownership",type=string,JSONPath=".status.ownership"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// NexusEndpoint is the Schema for the nexusendpoints API.
//
// One resource manages exactly one Temporal Nexus endpoint. Which one is
// spec.name; metadata.name is Kubernetes identity and is never sent to
// Temporal.
//
// Nexus endpoints belong to the Service rather than to a namespace - the
// namespace in the spec is where requests are routed, not where the endpoint
// lives.
type NexusEndpoint struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of NexusEndpoint
	// +required
	Spec NexusEndpointSpec `json:"spec"`

	// status defines the observed state of NexusEndpoint
	// +optional
	Status NexusEndpointStatus `json:"status,omitzero"`
}

// TemporalName returns the name of the Temporal Nexus endpoint this resource
// manages. See NexusEndpointSpec.TemporalName.
func (n *NexusEndpoint) TemporalName() string {
	return n.Spec.TemporalName()
}

// TemporalNamespace returns the Temporal namespace the endpoint targets. See
// NexusEndpointSpec.TemporalNamespace.
func (n *NexusEndpoint) TemporalNamespace() string {
	return n.Spec.TemporalNamespace()
}

// +kubebuilder:object:root=true

// NexusEndpointList contains a list of NexusEndpoint
type NexusEndpointList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []NexusEndpoint `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &NexusEndpoint{}, &NexusEndpointList{})
		return nil
	})
}
