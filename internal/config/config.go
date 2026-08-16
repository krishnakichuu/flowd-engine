// Package config loads flowd server configuration from environment
// variables, with production-reasonable defaults so a bare `flowd` run
// against a local Postgres just works (see the Phase 1 quickstart).
package config

import (
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	GRPCAddr    string
	MetricsAddr string
	// WebUIAddr serves the read-only web dashboard (internal/webapi +
	// internal/webui — Phase 2 roadmap, Track D, item 3): workflow list,
	// filtering, and per-run history, all backed by the same gRPC frontend
	// every other client uses.
	WebUIAddr          string
	DatabaseDSN        string
	ReaperInterval     time.Duration
	TimerFirerInterval time.Duration

	// TLSCertFile and TLSKeyFile are PEM paths for the server's gRPC
	// listener. Leaving both empty serves plaintext gRPC, matching Phase
	// 1's behavior — deliberately opt-in so local dev and existing
	// deployments are unaffected (see Phase 2 roadmap, Track A, item 1).
	TLSCertFile string
	TLSKeyFile  string
	// TLSClientCAFile, if set, turns on mTLS: the server requires and
	// verifies a client certificate signed by this CA on every
	// connection. Requires TLSCertFile/TLSKeyFile to also be set.
	TLSClientCAFile string

	// APIKeys, if non-empty, requires every RPC to present one of these
	// keys; empty means no request authentication, Phase 1's original
	// posture (see Phase 2 roadmap, Track A, item 2). Each entry is either
	// a bare key ("admin-key", unrestricted — any namespace) or scoped to
	// specific namespaces ("ops-key:default|billing"); the scoping syntax
	// itself is parsed in internal/frontend, not here (see Track A, item
	// 3). "*" as the namespace list is equivalent to leaving it off.
	APIKeys []string

	// NumShards is the size of the fixed shard space new workflow
	// executions are assigned into (workflow_executions.shard_id) — see
	// history.PartitionFor. 1 (the default) means every execution gets
	// shard 0, Phase 1's original behavior; shard_id stays inert until a
	// future multi-node history split can actually route by it (see
	// migration 0001's doc), but computing it correctly now means that
	// split is a routing change, not a backfill (Phase 2 roadmap, Track C,
	// item 3).
	NumShards int32
	// NumTaskQueuePartitions is the size of the fixed partition space new
	// workflow/activity tasks are assigned into
	// (workflow_tasks/activity_tasks.task_queue_partition). 1 (the
	// default) means every task gets partition 0, visible to any worker —
	// Phase 1's original behavior. A worker can optionally restrict itself
	// to a subset of partitions (sdk/worker's Options.TaskQueuePartitions),
	// spreading a busy queue's dispatch load — and lock contention on the
	// dispatch index — across separate worker pools.
	NumTaskQueuePartitions int32

	// TaskTokenSigningKey HMAC-signs the opaque task tokens handed to
	// workers by the poll RPCs (Phase 2 roadmap, Track A, item 4).
	// Hex-encoded in the environment; leaving it unset means
	// history.NewStore generates a random per-process key instead — fine
	// for a single-node deployment (see StoreOptions.TaskTokenSigningKey's
	// doc), but a multi-instance deployment must set this explicitly so
	// every instance verifies tokens the others issued.
	TaskTokenSigningKey []byte
}

func Load() Config {
	return Config{
		GRPCAddr:               getEnv("FLOWD_GRPC_ADDR", ":7233"),
		MetricsAddr:            getEnv("FLOWD_METRICS_ADDR", ":9090"),
		WebUIAddr:              getEnv("FLOWD_WEBUI_ADDR", ":7234"),
		DatabaseDSN:            getEnv("FLOWD_DATABASE_DSN", "postgres://flowd:flowd@localhost:5432/flowd?sslmode=disable"),
		ReaperInterval:         getEnvDuration("FLOWD_REAPER_INTERVAL", 5*time.Second),
		TimerFirerInterval:     getEnvDuration("FLOWD_TIMER_FIRER_INTERVAL", 1*time.Second),
		TLSCertFile:            getEnv("FLOWD_TLS_CERT_FILE", ""),
		TLSKeyFile:             getEnv("FLOWD_TLS_KEY_FILE", ""),
		TLSClientCAFile:        getEnv("FLOWD_TLS_CLIENT_CA_FILE", ""),
		APIKeys:                getEnvList("FLOWD_API_KEYS", nil),
		NumShards:              getEnvInt32("FLOWD_NUM_SHARDS", 1),
		NumTaskQueuePartitions: getEnvInt32("FLOWD_NUM_TASK_QUEUE_PARTITIONS", 1),
		TaskTokenSigningKey:    getEnvHex("FLOWD_TASK_TOKEN_SIGNING_KEY", nil),
	}
}

// getEnvHex decodes a hex-encoded env var (e.g. `openssl rand -hex 32`),
// falling back on an empty/invalid value the same way the other getEnv*
// helpers do rather than failing startup outright.
func getEnvHex(key string, fallback []byte) []byte {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := hex.DecodeString(v)
	if err != nil {
		return fallback
	}
	return b
}

func getEnvInt32(key string, fallback int32) int32 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n < 1 {
		return fallback
	}
	return int32(n)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getEnvList splits a comma-separated env var into trimmed, non-empty
// entries, e.g. FLOWD_API_KEYS="key-a, key-b".
func getEnvList(key string, fallback []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	var out []string
	for part := range strings.SplitSeq(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
