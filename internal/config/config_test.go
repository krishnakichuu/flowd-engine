package config

import (
	"reflect"
	"testing"
)

func TestLoadTLSAndAuthDefaults(t *testing.T) {
	cfg := Load()
	if cfg.TLSCertFile != "" || cfg.TLSKeyFile != "" || cfg.TLSClientCAFile != "" {
		t.Fatalf("expected empty TLS config by default, got %+v", cfg)
	}
	if cfg.APIKeys != nil {
		t.Fatalf("expected nil APIKeys by default, got %v", cfg.APIKeys)
	}
}

func TestLoadTLSAndAuthFromEnv(t *testing.T) {
	t.Setenv("FLOWD_TLS_CERT_FILE", "/tmp/cert.pem")
	t.Setenv("FLOWD_TLS_KEY_FILE", "/tmp/key.pem")
	t.Setenv("FLOWD_TLS_CLIENT_CA_FILE", "/tmp/ca.pem")
	t.Setenv("FLOWD_API_KEYS", " key-a ,key-b,, key-c")

	cfg := Load()
	if cfg.TLSCertFile != "/tmp/cert.pem" || cfg.TLSKeyFile != "/tmp/key.pem" || cfg.TLSClientCAFile != "/tmp/ca.pem" {
		t.Fatalf("unexpected TLS config: %+v", cfg)
	}
	want := []string{"key-a", "key-b", "key-c"}
	if !reflect.DeepEqual(cfg.APIKeys, want) {
		t.Fatalf("got APIKeys %v, want %v", cfg.APIKeys, want)
	}
}
