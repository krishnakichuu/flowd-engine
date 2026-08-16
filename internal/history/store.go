package history

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
)

// Store is the event-sourcing core: it owns AppendHistory (the CAS
// chokepoint, see append.go) and the higher-level operations that combine
// task-queue mechanics with history writes inside a single Postgres
// transaction, per ADR-0002.
type Store struct {
	pool                   *pgxpool.Pool
	numShards              int32
	numTaskQueuePartitions int32
	taskTokenSigningKey    []byte
	notifier               *notifier
}

// StoreOptions configures the fixed shard/partition spaces new workflow
// executions and tasks are assigned into (see PartitionFor and Phase 2
// roadmap, Track C, item 3). Leaving either at its zero value means 1 —
// every execution gets shard 0 and every task gets partition 0, Phase 1's
// original behavior.
type StoreOptions struct {
	NumShards              int32
	NumTaskQueuePartitions int32
	// TaskTokenSigningKey HMAC-signs every TaskToken this Store issues
	// (Phase 2 roadmap, Track A, item 4). Leaving it empty generates a
	// random 32-byte key for this process — fine for a single-node
	// deployment, since a task token's whole lifetime is one task's lease,
	// which wouldn't survive a restart anyway — but a multi-instance
	// deployment (any two processes that might decode a token the other
	// issued) must set FLOWD_TASK_TOKEN_SIGNING_KEY explicitly so they
	// agree on the same key.
	TaskTokenSigningKey []byte
}

func NewStore(pool *pgxpool.Pool, opts StoreOptions) *Store {
	numShards := opts.NumShards
	if numShards < 1 {
		numShards = 1
	}
	numTaskQueuePartitions := opts.NumTaskQueuePartitions
	if numTaskQueuePartitions < 1 {
		numTaskQueuePartitions = 1
	}
	signingKey := opts.TaskTokenSigningKey
	if len(signingKey) == 0 {
		signingKey = make([]byte, 32)
		if _, err := rand.Read(signingKey); err != nil {
			// crypto/rand failing is a fatal platform problem, not a
			// recoverable one — every task token issued afterward would
			// otherwise silently sign with a zero key.
			panic(fmt.Errorf("history: generate task token signing key: %w", err))
		}
	}
	return &Store{
		pool: pool, numShards: numShards, numTaskQueuePartitions: numTaskQueuePartitions,
		taskTokenSigningKey: signingKey,
		notifier:            newNotifier(),
	}
}

// DecodeTaskToken verifies and unmarshals a task token issued by this
// Store — the only way to decode one from outside the history package, so
// the signing key itself never has to leave it.
func (s *Store) DecodeTaskToken(b []byte) (TaskToken, error) {
	return DecodeTaskToken(b, s.taskTokenSigningKey)
}

// withTx runs fn inside a single Postgres transaction, committing on
// success and rolling back on any error (including a panic that unwinds
// through fn).
func (s *Store) withTx(ctx context.Context, fn func(q *sqlc.Queries) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("history: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a documented no-op

	if err := fn(sqlc.New(tx)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("history: commit tx: %w", err)
	}
	return nil
}

func (s *Store) getNamespaceID(ctx context.Context, name string) (int64, error) {
	q := sqlc.New(s.pool)
	ns, err := q.GetNamespaceByName(ctx, name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrNamespaceNotFound
		}
		return 0, fmt.Errorf("history: get namespace %q: %w", name, err)
	}
	return ns.ID, nil
}
