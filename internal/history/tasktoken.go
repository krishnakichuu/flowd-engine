package history

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

type TaskTokenKind string

const (
	TaskTokenKindWorkflow TaskTokenKind = "workflow"
	TaskTokenKindActivity TaskTokenKind = "activity"
	TaskTokenKindQuery    TaskTokenKind = "query"
)

// TaskToken is the opaque, versioned token handed to workers by the poll
// RPCs and echoed back on respond RPCs. Real ownership enforcement is
// still the DB-side lease_token check on the referenced row (ADR-0002) —
// this token is a claim about which row that is, not the enforcement
// mechanism itself — but as of V2 it is HMAC-SHA256 signed (Phase 2
// roadmap, Track A, item 4) so a syntactically-valid forged or hand-edited
// token is rejected before it ever reaches that DB check, instead of
// merely failing to match a lease it was never issued for.
type TaskToken struct {
	V           int           `json:"v"`
	Kind        TaskTokenKind `json:"k"`
	NamespaceID int64         `json:"ns"`
	WorkflowID  string        `json:"wid"`
	RunID       string        `json:"rid"`
	TaskID      int64         `json:"tid"`
	LeaseToken  string        `json:"lt"`
}

const taskTokenVersion = 2

func newTaskToken(kind TaskTokenKind, namespaceID int64, workflowID, runID string, taskID int64, leaseToken string) TaskToken {
	return TaskToken{
		V: taskTokenVersion, Kind: kind, NamespaceID: namespaceID,
		WorkflowID: workflowID, RunID: runID, TaskID: taskID, LeaseToken: leaseToken,
	}
}

// Encode serializes t to JSON and prepends an HMAC-SHA256 signature over
// that JSON, computed with key (see Store.taskTokenSigningKey). The wire
// format is sig(32 bytes) || json — still fully opaque to callers, who
// only ever echo the bytes back, never parse them.
func (t TaskToken) Encode(key []byte) ([]byte, error) {
	payload, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}
	return append(signPayload(payload, key), payload...), nil
}

// DecodeTaskToken verifies b's HMAC signature against key before
// unmarshaling it — a token whose signature doesn't match key (forged,
// hand-edited, or signed by a different process's key) is rejected here,
// never reaching the DB-side lease check at all.
func DecodeTaskToken(b []byte, key []byte) (TaskToken, error) {
	if len(b) < sha256.Size {
		return TaskToken{}, fmt.Errorf("history: invalid task token: too short")
	}
	sig, payload := b[:sha256.Size], b[sha256.Size:]
	if !hmac.Equal(sig, signPayload(payload, key)) {
		return TaskToken{}, fmt.Errorf("history: invalid task token: signature mismatch")
	}
	var t TaskToken
	if err := json.Unmarshal(payload, &t); err != nil {
		return TaskToken{}, fmt.Errorf("history: invalid task token: %w", err)
	}
	if t.V != taskTokenVersion {
		return TaskToken{}, fmt.Errorf("history: unsupported task token version %d", t.V)
	}
	return t, nil
}

func signPayload(payload, key []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	return mac.Sum(nil)
}
