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

// Package connection translates a Connection custom resource, and any Secret
// it refers to, into the options required to dial a Temporal Service.
//
// It is the only place that understands both Kubernetes and the Temporal SDK's
// dial options. The Temporal client wrapper deliberately knows nothing about
// Kubernetes, and the controllers deliberately know nothing about how a
// Connection maps onto client.Options.
package connection

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"

	"go.temporal.io/sdk/client"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	temporalv1alpha1 "github.com/mrsimonemms/temporal-resource-operator/api/v1alpha1"
)

var (
	// ErrNoConnection is returned when a nil Connection is resolved.
	ErrNoConnection = errors.New("no connection given")

	// ErrAddressRequired is returned when a Connection has no address. The CRD
	// schema enforces this too; this guards direct, in-process use.
	ErrAddressRequired = errors.New("connection address is required")

	// ErrConflictingCredentials is returned when both inline credentials and a
	// Secret reference are given. The CRD schema enforces this too.
	ErrConflictingCredentials = errors.New("credentials and credentialsSecretRef are mutually exclusive")

	// ErrIncompleteClientCertificate is returned when only one half of an mTLS
	// client certificate pair is supplied.
	ErrIncompleteClientCertificate = errors.New("clientCert and clientKey must be supplied together")

	// ErrEmptyCredentials is returned when a referenced Secret contains none of
	// the well-known credential keys.
	ErrEmptyCredentials = errors.New("secret contains none of the keys apiKey, clientCert or clientKey")
)

// Resolver turns Connection resources into Temporal SDK client options.
type Resolver struct {
	// client only ever reads Kubernetes objects, so the narrowest
	// controller-runtime interface is sufficient.
	client ctrlclient.Reader
}

// NewResolver returns a Resolver that reads Kubernetes objects with the given
// reader.
func NewResolver(reader ctrlclient.Reader) *Resolver {
	return &Resolver{client: reader}
}

// Resolve looks up the named Connection in the given namespace and resolves it.
func (r *Resolver) Resolve(
	ctx context.Context,
	namespace string,
	ref corev1.LocalObjectReference,
) (*client.Options, error) {
	conn := &temporalv1alpha1.Connection{}
	key := types.NamespacedName{Namespace: namespace, Name: ref.Name}

	if err := r.client.Get(ctx, key, conn); err != nil {
		return nil, fmt.Errorf("getting connection %s: %w", key, err)
	}

	return r.ResolveConnection(ctx, conn)
}

// ResolveConnection resolves an already-fetched Connection into the options
// used to dial the Temporal Service it describes.
func (r *Resolver) ResolveConnection(
	ctx context.Context,
	conn *temporalv1alpha1.Connection,
) (*client.Options, error) {
	if conn == nil {
		return nil, ErrNoConnection
	}

	if conn.Spec.Address == "" {
		return nil, ErrAddressRequired
	}

	creds, err := r.credentials(ctx, conn)
	if err != nil {
		return nil, err
	}

	opts := &client.Options{
		HostPort: conn.Spec.Address,
	}

	tlsConfig, err := tlsConfig(conn.Spec, creds)
	if err != nil {
		return nil, err
	}
	opts.ConnectionOptions.TLS = tlsConfig

	if creds.APIKey != "" {
		opts.Credentials = client.NewAPIKeyStaticCredentials(creds.APIKey)
	}

	return opts, nil
}

// credentials returns the credentials for the Connection, reading them from the
// referenced Secret where necessary. The returned value is never nil, so
// callers do not need to guard the optional fields.
func (r *Resolver) credentials(
	ctx context.Context,
	conn *temporalv1alpha1.Connection,
) (*temporalv1alpha1.Credentials, error) {
	spec := conn.Spec

	switch {
	case spec.Credentials != nil && spec.CredentialsSecretRef != nil:
		return nil, ErrConflictingCredentials
	case spec.Credentials != nil:
		return spec.Credentials, nil
	case spec.CredentialsSecretRef != nil:
		return r.credentialsFromSecret(ctx, conn.Namespace, *spec.CredentialsSecretRef)
	default:
		// An unauthenticated connection - a local dev server, typically.
		return &temporalv1alpha1.Credentials{}, nil
	}
}

// credentialsFromSecret reads the well-known credential keys from a Secret in
// the Connection's own namespace.
func (r *Resolver) credentialsFromSecret(
	ctx context.Context,
	namespace string,
	ref corev1.LocalObjectReference,
) (*temporalv1alpha1.Credentials, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: namespace, Name: ref.Name}

	if err := r.client.Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("credentials secret %s not found: %w", key, err)
		}
		return nil, fmt.Errorf("getting credentials secret %s: %w", key, err)
	}

	creds := &temporalv1alpha1.Credentials{
		APIKey:     string(secret.Data[temporalv1alpha1.SecretKeyAPIKey]),
		ClientCert: string(secret.Data[temporalv1alpha1.SecretKeyClientCert]),
		ClientKey:  string(secret.Data[temporalv1alpha1.SecretKeyClientKey]),
	}

	if creds.APIKey == "" && creds.ClientCert == "" && creds.ClientKey == "" {
		return nil, fmt.Errorf("credentials secret %s is unusable: %w", key, ErrEmptyCredentials)
	}

	return creds, nil
}

// tlsConfig builds the connection's TLS configuration, returning nil when TLS
// has not been asked for.
//
// TLS is implied - not just by spec.tls - by anything that cannot work without
// it. An API key alone is not enough: the Temporal SDK enables TLS itself when
// an API key is supplied and TLS has not been configured.
func tlsConfig(spec temporalv1alpha1.ConnectionSpec, creds *temporalv1alpha1.Credentials) (*tls.Config, error) {
	hasClientCert := creds.ClientCert != "" || creds.ClientKey != ""

	if !spec.TLS && spec.TLSServerName == "" && !hasClientCert {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: spec.TLSServerName,
	}

	if hasClientCert {
		if creds.ClientCert == "" || creds.ClientKey == "" {
			return nil, ErrIncompleteClientCertificate
		}

		cert, err := tls.X509KeyPair([]byte(creds.ClientCert), []byte(creds.ClientKey))
		if err != nil {
			return nil, fmt.Errorf("parsing mTLS client certificate: %w", err)
		}

		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}
