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

// SearchAttributeFinalizer holds a SearchAttribute open until the operator has
// decided what to do with the Temporal search attribute it manages.
const SearchAttributeFinalizer = "temporal.simonemms.com/search-attribute"

// SearchAttributeType is the type of the values a search attribute holds.
//
// The values are Temporal's own shorthand names, so what goes in the spec is
// what the Temporal CLI and API call it - no translation table to get wrong.
// +kubebuilder:validation:Enum=Bool;Datetime;Double;Int;Keyword;KeywordList;Text
type SearchAttributeType string

const (
	// SearchAttributeTypeBool holds true/false.
	SearchAttributeTypeBool SearchAttributeType = "Bool"

	// SearchAttributeTypeDatetime holds a timestamp.
	SearchAttributeTypeDatetime SearchAttributeType = "Datetime"

	// SearchAttributeTypeDouble holds a floating point number.
	SearchAttributeTypeDouble SearchAttributeType = "Double"

	// SearchAttributeTypeInt holds a whole number.
	SearchAttributeTypeInt SearchAttributeType = "Int"

	// SearchAttributeTypeKeyword holds a single, exactly-matched token.
	SearchAttributeTypeKeyword SearchAttributeType = "Keyword"

	// SearchAttributeTypeKeywordList holds a list of exactly-matched tokens.
	SearchAttributeTypeKeywordList SearchAttributeType = "KeywordList"

	// SearchAttributeTypeText holds free text, tokenised for searching.
	SearchAttributeTypeText SearchAttributeType = "Text"
)

// SearchAttributeDeletionPolicy decides what becomes of the Temporal search
// attribute when the resource managing it is deleted.
//
// It mirrors the Namespace policy of the same name deliberately - the two
// resources should behave the same way - but is its own type so that each
// resource's API describes what it actually does.
// +kubebuilder:validation:Enum=Delete;Orphan
type SearchAttributeDeletionPolicy string

const (
	// SearchAttributeDeletionPolicyDelete removes the Temporal search attribute
	// along with the resource, but only where the resource created it.
	SearchAttributeDeletionPolicyDelete SearchAttributeDeletionPolicy = "Delete"

	// SearchAttributeDeletionPolicyOrphan leaves the Temporal search attribute
	// exactly where it is. The operator asks Temporal nothing at all, so
	// deletion succeeds even when the Connection it would have needed is gone.
	SearchAttributeDeletionPolicyOrphan SearchAttributeDeletionPolicy = "Orphan"
)

// DefaultSearchAttributeDeletionPolicy is applied when a SearchAttribute does
// not ask for one. It matches the CRD's default, and is applied again in Go so
// that an object built in memory behaves the same as one that has been through
// admission.
const DefaultSearchAttributeDeletionPolicy = SearchAttributeDeletionPolicyDelete

// SearchAttributeOwnership records how a SearchAttribute came to manage its
// Temporal search attribute, and therefore whether deleting the resource
// deletes the search attribute.
//
// Unlike a Temporal namespace, a search attribute is nothing but a name and a
// type - there is nowhere on it to record who created it. This is the
// operator's own bookkeeping, and it cannot be checked against Temporal the way
// a namespace's owner marker can.
// +kubebuilder:validation:Enum=Creating;Created;Adopted
type SearchAttributeOwnership string

const (
	// SearchAttributeOwnershipCreating records that the operator found the
	// search attribute missing and is about to register it. It is written
	// before the registration call, so that a reconcile interrupted between
	// registering and recording the result can still tell that the attribute it
	// now sees is most likely its own work rather than something to adopt.
	SearchAttributeOwnershipCreating SearchAttributeOwnership = "Creating"

	// SearchAttributeOwnershipCreated records that this resource caused the
	// search attribute to exist. Deleting the resource deletes the attribute.
	SearchAttributeOwnershipCreated SearchAttributeOwnership = "Created"

	// SearchAttributeOwnershipAdopted records that the search attribute
	// pre-dated this resource. The operator keeps it in place but does not own
	// its lifecycle, so deleting the resource leaves the attribute alone.
	SearchAttributeOwnershipAdopted SearchAttributeOwnership = "Adopted"
)

// OwnsSearchAttribute reports whether deleting this resource should delete the
// Temporal search attribute.
//
// Creating counts as owned. The marker is only ever written after the operator
// has seen the attribute missing, so what is there now is either this
// resource's own registration or nothing at all - and removing a search
// attribute that is not there is a no-op.
func (o SearchAttributeOwnership) OwnsSearchAttribute() bool {
	return o == SearchAttributeOwnershipCreating || o == SearchAttributeOwnershipCreated
}

// IsEstablished reports whether ownership has been settled one way or the other.
func (o SearchAttributeOwnership) IsEstablished() bool {
	return o == SearchAttributeOwnershipCreated || o == SearchAttributeOwnershipAdopted
}

// SearchAttributeSpec defines the desired state of SearchAttribute
//
// Between them, name, connectionRef, namespaceRef and type say which real
// search attribute, on which Temporal Service, this resource stands for. All
// four are immutable: changing one would not rename or move anything, it would
// silently point the resource at a different attribute and strand whatever it
// was looking after. Delete the resource and make a new one instead.
type SearchAttributeSpec struct {
	// name is the Temporal search attribute's name, exactly as Temporal knows
	// it - "CustomerId", not "customer-id".
	//
	// This is separate from the resource's own metadata.name because Kubernetes
	// only accepts lowercase names for a resource, while search attributes are
	// conventionally PascalCase. Nothing is derived from metadata.name: what is
	// written here is what Temporal is asked for, casing and all.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="name is immutable"
	Name string `json:"name"`

	// connectionRef references the Connection, in the same Kubernetes
	// namespace, describing the Temporal Service holding this search attribute.
	// +required
	// +kubebuilder:validation:XValidation:rule="self.name != ''",message="connectionRef.name is required"
	// +kubebuilder:validation:XValidation:rule="self.name == oldSelf.name",message="connectionRef.name is immutable"
	ConnectionRef corev1.LocalObjectReference `json:"connectionRef"`

	// namespaceRef references the Namespace, in the same Kubernetes namespace,
	// whose Temporal namespace holds this search attribute.
	//
	// A Namespace's own metadata.name is its Temporal namespace name, so this
	// is also the Temporal namespace the attribute lives in.
	// +required
	// +kubebuilder:validation:XValidation:rule="self.name != ''",message="namespaceRef.name is required"
	// +kubebuilder:validation:XValidation:rule="self.name == oldSelf.name",message="namespaceRef.name is immutable"
	NamespaceRef corev1.LocalObjectReference `json:"namespaceRef"`

	// type is the type of the values this search attribute holds.
	//
	// Temporal has no way to retype an existing search attribute - it accepts
	// the request and silently keeps the original type - so a change here could
	// never be carried out.
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="type is immutable"
	Type SearchAttributeType `json:"type"`

	// deletionPolicy decides what happens to the Temporal search attribute when
	// this resource is deleted. Defaults to Delete.
	//
	// Delete removes the search attribute along with the resource, where the
	// operator created it. Orphan leaves it behind and contacts Temporal not at
	// all, which is how a resource is released when the Connection it would
	// have needed no longer exists.
	//
	// This may be changed while the resource is being deleted, which is the
	// supported way out of a deletion blocked on a missing Connection.
	// +optional
	// +kubebuilder:default=Delete
	DeletionPolicy SearchAttributeDeletionPolicy `json:"deletionPolicy,omitempty"`
}

// TemporalName returns the name of the Temporal search attribute this resource
// stands for.
//
// This is the one place the mapping lives. It is deliberately not derived from
// the resource's metadata.name, which is Kubernetes identity and nothing more.
func (s *SearchAttributeSpec) TemporalName() string {
	return s.Name
}

// TemporalNamespace returns the Temporal namespace the search attribute lives
// in.
//
// This comes from the reference rather than from the Namespace resource itself,
// because a Namespace's Temporal name is its own metadata.name. Finalisation
// can therefore find the attribute without the Namespace resource still being
// around to ask.
func (s *SearchAttributeSpec) TemporalNamespace() string {
	return s.NamespaceRef.Name
}

// DeletionPolicyValue returns the requested deletion policy, falling back to
// DefaultSearchAttributeDeletionPolicy when it is unset.
//
// Unlike the fields identifying the external attribute, this one is mutable -
// deliberately, because switching a stuck resource to Orphan while it is
// already terminating is how a deletion blocked on a broken dependency is
// released.
func (s *SearchAttributeSpec) DeletionPolicyValue() SearchAttributeDeletionPolicy {
	if s.DeletionPolicy == "" {
		return DefaultSearchAttributeDeletionPolicy
	}

	return s.DeletionPolicy
}

// SearchAttributeStatus defines the observed state of SearchAttribute.
type SearchAttributeStatus struct {
	// conditions represent the current state of the SearchAttribute resource.
	//
	// Known condition types:
	// - "Ready": the search attribute exists on the referenced Temporal
	//   namespace with the requested type
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// observedGeneration is the metadata.generation of the SearchAttribute that
	// was last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ownership records whether the operator registered the Temporal search
	// attribute or adopted one that already existed, and therefore whether
	// deleting this resource removes it.
	//
	// "Creating" is a transient marker written immediately before registering
	// an attribute; it settles to "Created" once registration is confirmed.
	// +optional
	Ownership SearchAttributeOwnership `json:"ownership,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=tsa
// +kubebuilder:printcolumn:name="Attribute",type=string,JSONPath=".spec.name"
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=".spec.type"
// +kubebuilder:printcolumn:name="Temporal NS",type=string,JSONPath=".spec.namespaceRef.name"
// +kubebuilder:printcolumn:name="Ownership",type=string,JSONPath=".status.ownership"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// SearchAttribute is the Schema for the searchattributes API.
//
// One resource manages exactly one Temporal search attribute. Which one is
// spec.name; metadata.name is Kubernetes identity and is never sent to
// Temporal. The two are separate because Kubernetes only accepts lowercase
// names for a resource, and that is not something a CRD can relax, whereas
// search attributes are conventionally PascalCase.
type SearchAttribute struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of SearchAttribute
	// +required
	Spec SearchAttributeSpec `json:"spec"`

	// status defines the observed state of SearchAttribute
	// +optional
	Status SearchAttributeStatus `json:"status,omitzero"`
}

// TemporalName returns the name of the Temporal search attribute this resource
// manages. See SearchAttributeSpec.TemporalName.
func (s *SearchAttribute) TemporalName() string {
	return s.Spec.TemporalName()
}

// TemporalNamespace returns the Temporal namespace the search attribute lives
// in. See SearchAttributeSpec.TemporalNamespace.
func (s *SearchAttribute) TemporalNamespace() string {
	return s.Spec.TemporalNamespace()
}

// +kubebuilder:object:root=true

// SearchAttributeList contains a list of SearchAttribute
type SearchAttributeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []SearchAttribute `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &SearchAttribute{}, &SearchAttributeList{})
		return nil
	})
}
