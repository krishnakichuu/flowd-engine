package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/krishnakichuu/flowd/internal/config"
)

func writeSelfSignedPair(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "flowd-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile
}

func TestServerTLSCredentialsUnconfiguredReturnsNil(t *testing.T) {
	creds, err := serverTLSCredentials(config.Config{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if creds != nil {
		t.Fatalf("expected nil credentials when TLS isn't configured, got %v", creds)
	}
}

func TestServerTLSCredentialsValidPair(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeSelfSignedPair(t, dir)

	creds, err := serverTLSCredentials(config.Config{TLSCertFile: certFile, TLSKeyFile: keyFile})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if creds == nil {
		t.Fatal("expected non-nil credentials")
	}
}

func TestServerTLSCredentialsWithClientCA(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeSelfSignedPair(t, dir)

	// Reuse the server cert file's PEM contents as a stand-in client CA —
	// only its parseability as a CA cert is under test here.
	caPEM, err := os.ReadFile(certFile) //nolint:gosec // certFile is this test's own t.TempDir() path, not external input
	if err != nil {
		t.Fatalf("read cert: %v", err)
	}
	clientCAFile := filepath.Join(dir, "client-ca.pem")
	if err := os.WriteFile(clientCAFile, caPEM, 0o600); err != nil { //nolint:gosec // clientCAFile is this test's own t.TempDir() path, not external input
		t.Fatalf("write client ca: %v", err)
	}

	creds, err := serverTLSCredentials(config.Config{TLSCertFile: certFile, TLSKeyFile: keyFile, TLSClientCAFile: clientCAFile})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if creds == nil {
		t.Fatal("expected non-nil credentials")
	}
}

func TestServerTLSCredentialsErrors(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeSelfSignedPair(t, dir)

	t.Run("missing key file", func(t *testing.T) {
		if _, err := serverTLSCredentials(config.Config{TLSCertFile: certFile, TLSKeyFile: filepath.Join(dir, "missing.pem")}); err == nil {
			t.Fatal("expected an error for a missing key file")
		}
	})

	t.Run("malformed client CA", func(t *testing.T) {
		badCA := filepath.Join(dir, "bad-ca.pem")
		if err := os.WriteFile(badCA, []byte("not a cert"), 0o600); err != nil {
			t.Fatalf("write bad ca: %v", err)
		}
		if _, err := serverTLSCredentials(config.Config{TLSCertFile: certFile, TLSKeyFile: keyFile, TLSClientCAFile: badCA}); err == nil {
			t.Fatal("expected an error for a malformed client CA file")
		}
	})

	t.Run("missing client CA file", func(t *testing.T) {
		if _, err := serverTLSCredentials(config.Config{TLSCertFile: certFile, TLSKeyFile: keyFile, TLSClientCAFile: filepath.Join(dir, "missing-ca.pem")}); err == nil {
			t.Fatal("expected an error for a missing client CA file")
		}
	})
}
