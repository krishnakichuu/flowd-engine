package client

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	flowv1 "github.com/krishnakichuu/flowd/api/gen/flow/api/v1"
	"github.com/krishnakichuu/flowd/internal/frontend"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// stubWorkflowService answers DescribeWorkflowExecution with a fixed
// response — enough to prove an RPC round-tripped through TLS and the auth
// interceptor, without standing up internal/history's full store.
type stubWorkflowService struct {
	flowv1.UnimplementedWorkflowServiceServer
}

func (stubWorkflowService) DescribeWorkflowExecution(context.Context, *flowv1.DescribeWorkflowExecutionRequest) (*flowv1.DescribeWorkflowExecutionResponse, error) {
	return &flowv1.DescribeWorkflowExecutionResponse{WorkflowType: "stub"}, nil
}

// genTLSPair generates a self-signed ECDSA cert (valid for "127.0.0.1")
// good enough for exercising TLS credential loading, and writes both the
// cert and its own PEM (acting as its own CA, since it's self-signed) to
// files under dir so tests can point CACertFile/CertFile/KeyFile at real
// paths, matching how these options are used in practice.
func genTLSPair(t *testing.T, dir string) (certFile, keyFile string, certPEMBytes []byte) {
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
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:         true,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	certPEMBytes = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEMBytes := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, certPEMBytes, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEMBytes, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile, certPEMBytes
}

// startTLSServer starts a gRPC server on 127.0.0.1 using the given
// cert/key, requiring apiKey (via internal/frontend's interceptor) on every
// RPC, and returns its address plus a cleanup func.
func startTLSServer(t *testing.T, certFile, keyFile, apiKey string) string {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("load server key pair: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}})),
		grpc.ChainUnaryInterceptor(frontend.NewAPIKeyUnaryInterceptor([]string{apiKey})),
	)
	flowv1.RegisterWorkflowServiceServer(srv, stubWorkflowService{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func TestDialTLSAndAPIKeyRoundTrip(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, certPEM := genTLSPair(t, dir)
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, certPEM, 0o600); err != nil {
		t.Fatalf("write ca file: %v", err)
	}
	addr := startTLSServer(t, certFile, keyFile, "correct-key")

	t.Run("valid TLS and API key succeeds", func(t *testing.T) {
		c, err := Dial(addr, Options{
			TLS:    &TLSOptions{CACertFile: caFile, ServerName: "localhost"},
			APIKey: "correct-key",
		})
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer func() { _ = c.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := c.DescribeWorkflow(ctx, "wf-1", "")
		if err != nil {
			t.Fatalf("describe workflow: %v", err)
		}
		if resp.WorkflowType != "stub" {
			t.Fatalf("got %q, want %q", resp.WorkflowType, "stub")
		}
	})

	t.Run("wrong API key is rejected", func(t *testing.T) {
		c, err := Dial(addr, Options{
			TLS:    &TLSOptions{CACertFile: caFile, ServerName: "localhost"},
			APIKey: "wrong-key",
		})
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer func() { _ = c.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = c.DescribeWorkflow(ctx, "wf-1", "")
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("got error %v, want Unauthenticated", err)
		}
	})

	t.Run("missing API key is rejected", func(t *testing.T) {
		c, err := Dial(addr, Options{TLS: &TLSOptions{CACertFile: caFile, ServerName: "localhost"}})
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer func() { _ = c.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = c.DescribeWorkflow(ctx, "wf-1", "")
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("got error %v, want Unauthenticated", err)
		}
	})

	t.Run("plaintext dial against a TLS server fails", func(t *testing.T) {
		c, err := Dial(addr, Options{APIKey: "correct-key"})
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer func() { _ = c.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := c.DescribeWorkflow(ctx, "wf-1", ""); err == nil {
			t.Fatal("expected an error dialing a TLS server over plaintext, got nil")
		}
	})
}

func TestTransportCredentialsErrors(t *testing.T) {
	dir := t.TempDir()

	t.Run("bad CA file", func(t *testing.T) {
		_, err := Dial("127.0.0.1:0", Options{TLS: &TLSOptions{CACertFile: filepath.Join(dir, "missing.pem")}})
		if err == nil {
			t.Fatal("expected an error for a missing CA file")
		}
	})

	t.Run("bad client key pair", func(t *testing.T) {
		_, err := Dial("127.0.0.1:0", Options{TLS: &TLSOptions{CertFile: filepath.Join(dir, "missing-cert.pem"), KeyFile: filepath.Join(dir, "missing-key.pem")}})
		if err == nil {
			t.Fatal("expected an error for a missing client key pair")
		}
	})

	t.Run("malformed CA PEM", func(t *testing.T) {
		badCA := filepath.Join(dir, "bad-ca.pem")
		if err := os.WriteFile(badCA, []byte("not a real cert"), 0o600); err != nil {
			t.Fatalf("write bad ca: %v", err)
		}
		_, err := Dial("127.0.0.1:0", Options{TLS: &TLSOptions{CACertFile: badCA}})
		if err == nil {
			t.Fatal("expected an error for a malformed CA PEM")
		}
	})
}
