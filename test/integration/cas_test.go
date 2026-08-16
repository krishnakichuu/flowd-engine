//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krishnakichuu/flowd/internal/history"
	postgrespkg "github.com/krishnakichuu/flowd/internal/persistence/postgres"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
)

// TestConcurrentDequeueExactlyOneWinner exercises ADR-0002 Mechanism A
// directly against real Postgres: N pollers race DequeueWorkflowTask
// against a single pending row, and FOR UPDATE SKIP LOCKED must give
// exactly one of them the row.
func TestConcurrentDequeueExactlyOneWinner(t *testing.T) {
	ctx := context.Background()
	pool, err := postgrespkg.NewPool(ctx, databaseDSN())
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer pool.Close()

	q := sqlc.New(pool)
	ns, err := q.GetNamespaceByName(ctx, "default")
	if err != nil {
		t.Fatalf("get default namespace: %v", err)
	}

	// Unique per run: this test asserts an exact winner count, so it must
	// never share a queue with leftover rows from a previous invocation
	// (this DB is a persistent, shared local instance across test runs,
	// not recreated per test).
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	taskQueue := "cas-test-queue-" + suffix
	workflowID := "cas-test-workflow-" + suffix
	runID := "cas-test-run-" + suffix

	if err := q.EnqueueWorkflowTask(ctx, sqlc.EnqueueWorkflowTaskParams{
		NamespaceID: ns.ID, TaskQueueName: taskQueue, WorkflowID: workflowID, RunID: runID, ScheduledEventID: 1,
	}); err != nil {
		t.Fatalf("enqueue workflow task: %v", err)
	}

	const pollers = 20
	var wins int64
	var wg sync.WaitGroup
	wg.Add(pollers)
	for i := 0; i < pollers; i++ {
		go func() {
			defer wg.Done()
			_, err := q.DequeueWorkflowTask(ctx, sqlc.DequeueWorkflowTaskParams{
				TaskQueueName: taskQueue, NamespaceID: ns.ID, LeaseSeconds: 10,
			})
			if err == nil {
				atomic.AddInt64(&wins, 1)
			}
		}()
	}
	wg.Wait()

	if wins != 1 {
		t.Fatalf("winners = %d, want exactly 1 (FOR UPDATE SKIP LOCKED must give exactly one poller the row)", wins)
	}
}

// TestConcurrentModificationRejectsStaleWriter exercises ADR-0002
// Mechanism B directly: two callers both read next_event_id, then both try
// to append using that same expected value in a transaction. Exactly one
// must succeed; the other must see ErrConcurrentModification, never a
// silently corrupted/forked history.
func TestConcurrentModificationRejectsStaleWriter(t *testing.T) {
	ctx := context.Background()
	pool, err := postgrespkg.NewPool(ctx, databaseDSN())
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer pool.Close()

	store := history.NewStore(pool, history.StoreOptions{})
	workflowID := fmt.Sprintf("cas-test-workflow-append-race-%d", time.Now().UnixNano())

	runID, err := store.StartWorkflowExecution(ctx, history.StartWorkflowExecutionParams{
		Namespace: "default", WorkflowID: workflowID, WorkflowType: "CASRaceTest",
		TaskQueue: "cas-test-queue", RequestID: workflowID + "-start",
	})
	if err != nil {
		t.Fatalf("start workflow: %v", err)
	}

	q := sqlc.New(pool)
	ns, err := q.GetNamespaceByName(ctx, "default")
	if err != nil {
		t.Fatalf("get namespace: %v", err)
	}
	we, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{NamespaceID: ns.ID, WorkflowID: workflowID, RunID: runID})
	if err != nil {
		t.Fatalf("get workflow_execution: %v", err)
	}
	staleExpected := we.NextEventID

	// Two concurrent transactions both attempt to advance next_event_id
	// from the same observed value.
	race := func() error {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		txq := sqlc.New(tx)
		if _, err := txq.AdvanceNextEventID(ctx, sqlc.AdvanceNextEventIDParams{
			NamespaceID: ns.ID, WorkflowID: workflowID, RunID: runID, Increment: 1, Expected: staleExpected,
		}); err != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond) // widen the race window
		return tx.Commit(ctx)
	}

	var wg sync.WaitGroup
	results := make([]error, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		i := i
		go func() { defer wg.Done(); results[i] = race() }()
	}
	wg.Wait()

	successes := 0
	for _, r := range results {
		if r == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful concurrent appends = %d, want exactly 1 (got errors: %v)", successes, results)
	}

	final, err := q.GetWorkflowExecution(ctx, sqlc.GetWorkflowExecutionParams{NamespaceID: ns.ID, WorkflowID: workflowID, RunID: runID})
	if err != nil {
		t.Fatalf("get final workflow_execution: %v", err)
	}
	if final.NextEventID != staleExpected+1 {
		t.Fatalf("next_event_id = %d, want %d (exactly one advance, not double-applied)", final.NextEventID, staleExpected+1)
	}
}
