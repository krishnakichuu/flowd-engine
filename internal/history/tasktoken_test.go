package history

import (
	"bytes"
	"testing"
)

func TestTaskTokenRoundTrip(t *testing.T) {
	key := []byte("test-signing-key")
	tok := newTaskToken(TaskTokenKindActivity, 1, "wf-1", "run-1", 42, "lease-abc")

	encoded, err := tok.Encode(key)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := DecodeTaskToken(encoded, key)
	if err != nil {
		t.Fatalf("DecodeTaskToken: %v", err)
	}
	if decoded != tok {
		t.Fatalf("round trip mismatch: got %+v, want %+v", decoded, tok)
	}
}

func TestTaskTokenRejectsWrongKey(t *testing.T) {
	tok := newTaskToken(TaskTokenKindWorkflow, 1, "wf-1", "run-1", 7, "lease-abc")
	encoded, err := tok.Encode([]byte("issuer-key"))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	if _, err := DecodeTaskToken(encoded, []byte("different-key")); err == nil {
		t.Fatal("expected DecodeTaskToken to reject a token signed with a different key, got nil error")
	}
}

func TestTaskTokenRejectsTamperedPayload(t *testing.T) {
	key := []byte("test-signing-key")
	// A real forgery attempt: take a validly-signed token and edit its
	// claimed workflow_id, hoping the signature check is skipped or the
	// server trusts the JSON at face value.
	tok := newTaskToken(TaskTokenKindActivity, 1, "victim-workflow", "run-1", 1, "lease-abc")
	encoded, err := tok.Encode(key)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	tampered := bytes.Replace(encoded, []byte("victim-workflow"), []byte("attacker-workfl"), 1)
	if len(tampered) != len(encoded) {
		t.Fatalf("test bug: replacement changed token length, got %d want %d", len(tampered), len(encoded))
	}

	if _, err := DecodeTaskToken(tampered, key); err == nil {
		t.Fatal("expected DecodeTaskToken to reject a tampered token, got nil error")
	}
}

func TestTaskTokenRejectsTooShort(t *testing.T) {
	if _, err := DecodeTaskToken([]byte("short"), []byte("key")); err == nil {
		t.Fatal("expected DecodeTaskToken to reject a too-short token, got nil error")
	}
}

func TestTaskTokenRejectsUnsupportedVersion(t *testing.T) {
	key := []byte("test-signing-key")
	tok := newTaskToken(TaskTokenKindQuery, 1, "wf-1", "run-1", 3, "lease-abc")
	tok.V = taskTokenVersion - 1 // simulate a token from an older server build

	encoded, err := tok.Encode(key)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if _, err := DecodeTaskToken(encoded, key); err == nil {
		t.Fatal("expected DecodeTaskToken to reject an unsupported version, got nil error")
	}
}
