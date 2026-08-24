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

package connection_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	temporalv1beta1 "github.com/mrsimonemms/temporal-resource-operator/api/v1beta1"
	"github.com/mrsimonemms/temporal-resource-operator/internal/connection"
)

const (
	testNamespace = "temporal-test"
	testAddress   = "localhost:7233"
	testSecret    = "temporal-credentials" //nolint:gosec // the name of a Secret, not a credential
)

// clientCertPEM and clientKeyPEM are a self-signed pair generated once for the
// whole suite, so the mTLS specs exercise real certificate parsing.
var clientCertPEM, clientKeyPEM string

var _ = BeforeSuite(func() {
	clientCertPEM, clientKeyPEM = generateCertificate()
})

// newScheme builds a scheme containing the core types and the operator's own.
func newScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
	Expect(temporalv1beta1.AddToScheme(scheme)).To(Succeed())

	return scheme
}

// newResolver returns a Resolver backed by a fake Kubernetes client holding the
// given objects.
func newResolver(objects ...ctrlclient.Object) *connection.Resolver {
	builder := fake.NewClientBuilder().WithScheme(newScheme())
	if len(objects) > 0 {
		builder = builder.WithObjects(objects...)
	}

	return connection.NewResolver(builder.Build())
}

// newConnection returns a Connection in the test namespace with the given spec.
func newConnection(name string, spec temporalv1beta1.ConnectionSpec) *temporalv1beta1.Connection {
	return &temporalv1beta1.Connection{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec:       spec,
	}
}

// newCredentialsSecret returns a Secret in the test namespace with the given data.
func newCredentialsSecret(data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testSecret, Namespace: testNamespace},
		Data:       data,
	}
}

// generateCertificate creates a self-signed certificate and key, PEM-encoded.
func generateCertificate() (certPEM, keyPEM string) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	Expect(err).NotTo(HaveOccurred())

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "temporal-resource-operator-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	Expect(err).NotTo(HaveOccurred())

	keyDER, err := x509.MarshalECPrivateKey(key)
	Expect(err).NotTo(HaveOccurred())

	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))

	return certPEM, keyPEM
}

var _ = Describe("Resolver", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	Context("with no credentials", func() {
		It("should resolve an unauthenticated connection", func() {
			conn := newConnection("local", temporalv1beta1.ConnectionSpec{Address: testAddress})

			opts, err := newResolver().ResolveConnection(ctx, conn)
			Expect(err).NotTo(HaveOccurred())

			Expect(opts.HostPort).To(Equal(testAddress))
			Expect(opts.Credentials).To(BeNil())
			Expect(opts.ConnectionOptions.TLS).To(BeNil())
		})

		It("should not panic when the optional fields are absent", func() {
			conn := &temporalv1beta1.Connection{
				ObjectMeta: metav1.ObjectMeta{Name: "bare", Namespace: testNamespace},
				Spec:       temporalv1beta1.ConnectionSpec{Address: testAddress},
			}
			Expect(conn.Spec.Credentials).To(BeNil())
			Expect(conn.Spec.CredentialsSecretRef).To(BeNil())

			Expect(func() {
				_, _ = newResolver().ResolveConnection(ctx, conn)
			}).NotTo(Panic())
		})
	})

	Context("with plaintext credentials", func() {
		It("should resolve an API key", func() {
			conn := newConnection("api-key", temporalv1beta1.ConnectionSpec{
				Address:     testAddress,
				Credentials: &temporalv1beta1.Credentials{APIKey: "my-api-key"},
			})

			opts, err := newResolver().ResolveConnection(ctx, conn)
			Expect(err).NotTo(HaveOccurred())

			// The SDK's credentials are opaque, so all that can be asserted is
			// that they were configured. TLS is left alone - the SDK enables it
			// itself when an API key is given.
			Expect(opts.Credentials).NotTo(BeNil())
			Expect(opts.ConnectionOptions.TLS).To(BeNil())
		})

		It("should resolve an mTLS certificate and key", func() {
			conn := newConnection("mtls", temporalv1beta1.ConnectionSpec{
				Address: testAddress,
				Credentials: &temporalv1beta1.Credentials{
					ClientCert: clientCertPEM,
					ClientKey:  clientKeyPEM,
				},
			})

			opts, err := newResolver().ResolveConnection(ctx, conn)
			Expect(err).NotTo(HaveOccurred())

			Expect(opts.ConnectionOptions.TLS).NotTo(BeNil())
			Expect(opts.ConnectionOptions.TLS.Certificates).To(HaveLen(1))
			Expect(opts.ConnectionOptions.TLS.Certificates[0].Certificate).NotTo(BeEmpty())
		})

		It("should reject a certificate without its key", func() {
			conn := newConnection("half-mtls", temporalv1beta1.ConnectionSpec{
				Address:     testAddress,
				Credentials: &temporalv1beta1.Credentials{ClientCert: clientCertPEM},
			})

			_, err := newResolver().ResolveConnection(ctx, conn)
			Expect(err).To(MatchError(connection.ErrIncompleteClientCertificate))
		})

		It("should reject an unparseable certificate", func() {
			conn := newConnection("bad-mtls", temporalv1beta1.ConnectionSpec{
				Address: testAddress,
				Credentials: &temporalv1beta1.Credentials{
					ClientCert: "not a certificate",
					ClientKey:  clientKeyPEM,
				},
			})

			_, err := newResolver().ResolveConnection(ctx, conn)
			Expect(err).To(MatchError(ContainSubstring("parsing mTLS client certificate")))
		})
	})

	Context("with Secret-backed credentials", func() {
		var conn *temporalv1beta1.Connection

		BeforeEach(func() {
			conn = newConnection("secret", temporalv1beta1.ConnectionSpec{
				Address:              testAddress,
				CredentialsSecretRef: &corev1.LocalObjectReference{Name: testSecret},
			})
		})

		It("should resolve an API key from the Secret", func() {
			secret := newCredentialsSecret(map[string][]byte{
				temporalv1beta1.SecretKeyAPIKey: []byte("my-api-key"),
			})

			opts, err := newResolver(secret).ResolveConnection(ctx, conn)
			Expect(err).NotTo(HaveOccurred())

			Expect(opts.HostPort).To(Equal(testAddress))
			Expect(opts.Credentials).NotTo(BeNil())
		})

		It("should resolve an mTLS certificate and key from the Secret", func() {
			secret := newCredentialsSecret(map[string][]byte{
				temporalv1beta1.SecretKeyClientCert: []byte(clientCertPEM),
				temporalv1beta1.SecretKeyClientKey:  []byte(clientKeyPEM),
			})

			opts, err := newResolver(secret).ResolveConnection(ctx, conn)
			Expect(err).NotTo(HaveOccurred())

			Expect(opts.ConnectionOptions.TLS).NotTo(BeNil())
			Expect(opts.ConnectionOptions.TLS.Certificates).To(HaveLen(1))
		})

		It("should error when the Secret does not exist", func() {
			_, err := newResolver().ResolveConnection(ctx, conn)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring(testSecret))
		})

		It("should error when the Secret holds none of the well-known keys", func() {
			secret := newCredentialsSecret(map[string][]byte{"token": []byte("wrong-key")})

			_, err := newResolver(secret).ResolveConnection(ctx, conn)
			Expect(err).To(MatchError(connection.ErrEmptyCredentials))
		})

		It("should reject a Connection that also has plaintext credentials", func() {
			conn.Spec.Credentials = &temporalv1beta1.Credentials{APIKey: "my-api-key"}

			_, err := newResolver().ResolveConnection(ctx, conn)
			Expect(err).To(MatchError(connection.ErrConflictingCredentials))
		})
	})

	Context("with TLS", func() {
		It("should enable TLS when requested", func() {
			conn := newConnection("tls", temporalv1beta1.ConnectionSpec{
				Address: testAddress,
				TLS:     true,
			})

			opts, err := newResolver().ResolveConnection(ctx, conn)
			Expect(err).NotTo(HaveOccurred())

			Expect(opts.ConnectionOptions.TLS).NotTo(BeNil())
			Expect(opts.ConnectionOptions.TLS.ServerName).To(BeEmpty())
			Expect(opts.ConnectionOptions.TLS.InsecureSkipVerify).To(BeFalse())
		})

		It("should set the server name, implying TLS", func() {
			conn := newConnection("tls-server-name", temporalv1beta1.ConnectionSpec{
				Address:       testAddress,
				TLSServerName: "temporal.example.com",
			})

			opts, err := newResolver().ResolveConnection(ctx, conn)
			Expect(err).NotTo(HaveOccurred())

			Expect(opts.ConnectionOptions.TLS).NotTo(BeNil())
			Expect(opts.ConnectionOptions.TLS.ServerName).To(Equal("temporal.example.com"))
		})
	})

	Context("when resolving by reference", func() {
		It("should resolve a Connection in the given namespace", func() {
			conn := newConnection("by-ref", temporalv1beta1.ConnectionSpec{
				Address: testAddress,
				TLS:     true,
			})
			ref := corev1.LocalObjectReference{Name: conn.Name}

			opts, err := newResolver(conn).Resolve(ctx, testNamespace, ref)
			Expect(err).NotTo(HaveOccurred())

			Expect(opts.HostPort).To(Equal(testAddress))
			Expect(opts.ConnectionOptions.TLS).NotTo(BeNil())
		})

		It("should error when the Connection does not exist", func() {
			ref := corev1.LocalObjectReference{Name: "missing"}

			_, err := newResolver().Resolve(ctx, testNamespace, ref)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	Context("when the Connection is unusable", func() {
		It("should error on a nil Connection", func() {
			_, err := newResolver().ResolveConnection(ctx, nil)
			Expect(err).To(MatchError(connection.ErrNoConnection))
		})

		It("should error when the address is empty", func() {
			conn := newConnection("no-address", temporalv1beta1.ConnectionSpec{})

			_, err := newResolver().ResolveConnection(ctx, conn)
			Expect(err).To(MatchError(connection.ErrAddressRequired))
		})
	})
})
