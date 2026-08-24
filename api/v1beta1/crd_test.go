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
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/util/yaml"
)

// crd is the slice of a CustomResourceDefinition this suite cares about. The
// full schema is controller-gen's business; what matters here is that the
// operator ships exactly one API version, and that it is the one this package
// declares.
type crd struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		Group string `json:"group"`
		Scope string `json:"scope"`
		Names struct {
			Plural     string   `json:"plural"`
			Kind       string   `json:"kind"`
			ShortNames []string `json:"shortNames"`
		} `json:"names"`
		Conversion *struct {
			Strategy string `json:"strategy"`
		} `json:"conversion"`
		Versions []struct {
			Name    string `json:"name"`
			Served  bool   `json:"served"`
			Storage bool   `json:"storage"`
			Schema  struct {
				OpenAPIV3Schema struct {
					Properties struct {
						Spec struct {
							Properties map[string]struct {
								Type         string `json:"type"`
								Pattern      string `json:"pattern"`
								MaxLength    *int64 `json:"maxLength"`
								Default      any    `json:"default"`
								XValidations []struct {
									Rule    string `json:"rule"`
									Message string `json:"message"`
								} `json:"x-kubernetes-validations"`
							} `json:"properties"`
						} `json:"spec"`
					} `json:"properties"`
				} `json:"openAPIV3Schema"`
			} `json:"schema"`
		} `json:"versions"`
	} `json:"spec"`
}

// crdBases is where "make manifests" writes the generated definitions.
const crdBases = "../../config/crd/bases"

// expected maps each generated CRD file to the short names it must keep.
// Connection deliberately has none.
var expected = map[string][]string{
	"temporal.simonemms.com_connections.yaml":      nil,
	"temporal.simonemms.com_namespaces.yaml":       {"tns"},
	"temporal.simonemms.com_searchattributes.yaml": {"tsa"},
	"temporal.simonemms.com_nexusendpoints.yaml":   {"tnx"},
}

var _ = Describe("Generated CRDs", func() {
	It("should generate one file per resource and no others", func() {
		entries, err := os.ReadDir(crdBases)
		Expect(err).NotTo(HaveOccurred())

		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}

		Expect(names).To(HaveLen(len(expected)))
		for name := range expected {
			Expect(names).To(ContainElement(name))
		}
	})

	for file, shortNames := range expected {
		Context(file, func() {
			var (
				raw    []byte
				parsed crd
			)

			BeforeEach(func() {
				var err error
				raw, err = os.ReadFile(filepath.Join(crdBases, file))
				Expect(err).NotTo(HaveOccurred())
				Expect(yaml.Unmarshal(raw, &parsed)).To(Succeed())
			})

			It("should serve exactly one version, and store it", func() {
				Expect(parsed.Spec.Versions).To(HaveLen(1))
				Expect(parsed.Spec.Versions[0].Name).To(Equal(GroupVersion.Version))
				Expect(parsed.Spec.Versions[0].Name).To(Equal("v1beta1"))
				Expect(parsed.Spec.Versions[0].Served).To(BeTrue())
				Expect(parsed.Spec.Versions[0].Storage).To(BeTrue())
			})

			It("should mention no other API version anywhere", func() {
				Expect(string(raw)).NotTo(ContainSubstring("v1alpha1"))
			})

			It("should need no conversion, there being one version", func() {
				if parsed.Spec.Conversion != nil {
					Expect(parsed.Spec.Conversion.Strategy).To(Equal("None"))
				}
			})

			It("should keep its group, scope and short names", func() {
				Expect(parsed.Spec.Group).To(Equal(GroupVersion.Group))
				Expect(parsed.Spec.Scope).To(Equal("Namespaced"))
				Expect(parsed.Spec.Names.ShortNames).To(Equal(shortNames))
			})
		})
	}
})

var _ = Describe("The generated Namespace CRD", func() {
	var parsed crd

	BeforeEach(func() {
		raw, err := os.ReadFile(filepath.Join(crdBases, "temporal.simonemms.com_namespaces.yaml"))
		Expect(err).NotTo(HaveOccurred())
		Expect(yaml.Unmarshal(raw, &parsed)).To(Succeed())
	})

	// The retention field is the reason Duration exists. If the schema stops
	// carrying the pattern, or carries a different one, the API server and the
	// operator can disagree again about what a duration is - and a value only
	// the API server accepts is one that breaks the Namespace informer.
	Describe("spec.retention", func() {
		var retention struct {
			Type         string `json:"type"`
			Pattern      string `json:"pattern"`
			MaxLength    *int64 `json:"maxLength"`
			Default      any    `json:"default"`
			XValidations []struct {
				Rule    string `json:"rule"`
				Message string `json:"message"`
			} `json:"x-kubernetes-validations"`
		}

		BeforeEach(func() {
			Expect(parsed.Spec.Versions).To(HaveLen(1))

			props := parsed.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties.Spec.Properties
			Expect(props).To(HaveKey("retention"))
			retention = props["retention"]
		})

		It("should still be a plain string in the public API", func() {
			Expect(retention.Type).To(Equal("string"))
		})

		It("should carry exactly the pattern the Go parser enforces", func() {
			Expect(retention.Pattern).To(Equal(DurationPattern))
		})

		It("should carry the same length bound the Go parser enforces", func() {
			Expect(retention.MaxLength).NotTo(BeNil())
			Expect(*retention.MaxLength).To(BeNumerically("==", DurationMaxLength))
		})

		It("should keep defaulting to 72h", func() {
			Expect(retention.Default).To(Equal("72h"))
		})

		It("should accept the documented durations and reject the malformed ones", func() {
			// The schema is only as good as the regular expression in it, so
			// check that expression directly - the same one Kubernetes will
			// apply - rather than trusting that it was generated from the right
			// constant.
			schemaRE := regexp.MustCompile(retention.Pattern)

			for _, good := range []string{"7d", "3d", "1d12h", "7d30m", "72h", "168h", "2h30m", "0s"} {
				Expect(schemaRE.MatchString(good)).To(BeTrue(), "%q should be accepted", good)

				_, err := ParseDuration(good)
				Expect(err).NotTo(HaveOccurred(), "%q should also parse", good)
			}

			for _, bad := range []string{"7days", "d", "7dd", "1d2d", "1.5d", "forever", "", "1h30"} {
				Expect(schemaRE.MatchString(bad)).To(BeFalse(), "%q should be rejected", bad)

				_, err := ParseDuration(bad)
				Expect(err).To(HaveOccurred(), "%q should also fail to parse", bad)
			}
		})

		// The 1..90 day range cannot be written as a regular expression, because
		// the same duration has many spellings. It is carried as CEL instead,
		// and these specs make sure the schema still has both halves of it -
		// admission is where an out-of-range value is meant to be stopped.
		Describe("the range rules", func() {
			It("should carry one rule for each bound", func() {
				Expect(retention.XValidations).To(HaveLen(2))
				Expect(retention.XValidations[0].Message).To(ContainSubstring("at least 1 day"))
				Expect(retention.XValidations[1].Message).To(ContainSubstring("at most 90 days"))
			})

			It("should express the bounds in whole seconds", func() {
				// 86400s is MinRetention and 7776000s is MaxRetention. CEL has
				// no way to build a duration from an integer day count, so the
				// rules compare seconds; if these numbers drift from the Go
				// constants, admission and the operator would disagree.
				Expect(retention.XValidations[0].Rule).
					To(ContainSubstring(fmt.Sprintf("%d", int64(MinRetention.Seconds()))))
				Expect(retention.XValidations[1].Rule).
					To(ContainSubstring(fmt.Sprintf("%d", int64(MaxRetention.Seconds()))))
			})

			It("should scale the day count by a day's worth of seconds", func() {
				Expect(retention.XValidations[0].Rule).
					To(ContainSubstring(fmt.Sprintf("*%d", int64(Day.Seconds()))))
			})
		})
	})
})
