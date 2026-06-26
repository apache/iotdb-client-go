/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package client

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuildTLSConfigClonesBaseConfig(t *testing.T) {
	base := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	cfg, err := buildTLSConfig(&TLSConfig{
		Config: base,
	})
	if err != nil {
		t.Fatal(err)
	}

	if cfg == base {
		t.Fatal("buildTLSConfig must clone the base tls.Config")
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("MinVersion = %d, want %d", cfg.MinVersion, tls.VersionTLS12)
	}
}

func TestBuildTLSConfigLoadsFiles(t *testing.T) {
	caFile, certFile, keyFile := writeTLSFiles(t)

	cfg, err := buildTLSConfig(&TLSConfig{
		CAFile:   caFile,
		CertFile: certFile,
		KeyFile:  keyFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RootCAs == nil {
		t.Fatal("RootCAs should be set")
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("Certificates length = %d, want 1", len(cfg.Certificates))
	}
}

func TestBuildTLSConfigRequiresCertAndKey(t *testing.T) {
	_, err := buildTLSConfig(&TLSConfig{CertFile: "client.crt"})
	if err == nil {
		t.Fatal("expected error when CertFile is set without KeyFile")
	}
}

func TestNewClusterSessionReturnsTLSConfigError(t *testing.T) {
	missingCAFile := filepath.Join(t.TempDir(), "missing-ca.pem")

	_, err := newClusterSessionWithSqlDialect(&ClusterConfig{
		NodeUrls:  []string{"127.0.0.1:6667"},
		TLSConfig: &TLSConfig{CAFile: missingCAFile},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "no server can connect") {
		t.Fatalf("error = %q, want no server can connect", err)
	}
	if !strings.Contains(err.Error(), "read TLS CA file") {
		t.Fatalf("error = %q, want TLS CA file detail", err)
	}
}

func writeTLSFiles(t *testing.T) (caFile string, certFile string, keyFile string) {
	t.Helper()

	dir := t.TempDir()
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "iotdb-client-go-test-ca"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "iotdb-client-go-test-client"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	clientKeyDER, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}

	caFile = filepath.Join(dir, "ca.pem")
	certFile = filepath.Join(dir, "client.pem")
	keyFile = filepath.Join(dir, "client-key.pem")
	writePEMFile(t, caFile, "CERTIFICATE", caDER)
	writePEMFile(t, certFile, "CERTIFICATE", clientDER)
	writePEMFile(t, keyFile, "EC PRIVATE KEY", clientKeyDER)
	return caFile, certFile, keyFile
}

func writePEMFile(t *testing.T, filename string, blockType string, der []byte) {
	t.Helper()

	file, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	if err := pem.Encode(file, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		t.Fatal(err)
	}
}
