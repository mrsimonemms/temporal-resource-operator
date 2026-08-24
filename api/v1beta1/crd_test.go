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
	"os"
	"path/filepath"

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
