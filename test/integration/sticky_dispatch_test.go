//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	postgrespkg "github.com/krishnakichuu/flowd/internal/persistence/postgres"
	"github.com/krishnakichuu/flowd/internal/persistence/postgres/sqlc"
)

// TestStickyDispatchPrefersRegisteredWorker exercises the sticky-caching
// dispatch gate (Phase 2 roadmap, Track C, item 1) directly against real
// Postgres: a task enqueued with a preferred worker identity must be
// invisible to every other identity's DequeueWorkflowTask call, and
// visible to the preferred one — the mechanism the whole feature's
// "stays sticky when possible" half depends on.
func TestStickyDispatchPrefersRegisteredWorker(t *testing.T) {
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

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	taskQueue := "sticky-test-queue-" + suffix
	workflowID := "sticky-test-workflow-" + suffix
	runID := "sticky-test-run-" + suffix

	if err := q.EnqueueWorkflowTask(ctx, sqlc.EnqueueWorkflowTaskParams{
		NamespaceID: ns.ID, TaskQueueName: taskQueue, WorkflowID: workflowID, RunID: runID, ScheduledEventID: 1,
		PreferredWorkerIdentity: pgtype.Text{String: "worker-A", Valid: true},
		StickyDeadline:          pgtype.Timestamptz{Time: time.Now().Add(1 * time.Minute), Valid: true},
	}); err != nil {
		t.Fatalf("enqueue sticky workflow task: %v", err)
	}

	// A different worker must not see it while the sticky window is open.
	_, err = q.DequeueWorkflowTask(ctx, sqlc.DequeueWorkflowTaskParams{
		TaskQueueName: taskQueue, NamespaceID: ns.ID, LeaseSeconds: 10,
		WorkerIdentity: pgtype.Text{String: "worker-B", Valid: true},
	})
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("dequeue as worker-B = %v, want pgx.ErrNoRows (task is sticky-locked to worker-A)", err)
	}

	// The preferred worker gets it immediately.
	row, err := q.DequeueWorkflowTask(ctx, sqlc.DequeueWorkflowTaskParams{
		TaskQueueName: taskQueue, NamespaceID: ns.ID, LeaseSeconds: 10,
		WorkerIdentity: pgtype.Text{String: "worker-A", Valid: true},
	})
	if err != nil {
		t.Fatalf("dequeue as worker-A: %v", err)
	}
	if row.WorkflowID != workflowID || row.RunID != runID {
		t.Fatalf("dequeued task = %+v, want workflow_id=%s run_id=%s", row, workflowID, runID)
	}
}

// TestStickyDispatchFallsBackAfterDeadline is the other half of the
// guarantee: once the sticky window has passed, the task is fair game for
// any worker — the fallback-to-full-replay path sticky caching depends on
// when the preferred worker doesn't come back (crashed, busy, evicted its
// cache).
func TestStickyDispatchFallsBackAfterDeadline(t *testing.T) {
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

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	taskQueue := "sticky-test-queue-" + suffix
	workflowID := "sticky-test-workflow-" + suffix
	runID := "sticky-test-run-" + suffix

	if err := q.EnqueueWorkflowTask(ctx, sqlc.EnqueueWorkflowTaskParams{
		NamespaceID: ns.ID, TaskQueueName: taskQueue, WorkflowID: workflowID, RunID: runID, ScheduledEventID: 1,
		PreferredWorkerIdentity: pgtype.Text{String: "worker-A", Valid: true},
		// Already expired.
		StickyDeadline: pgtype.Timestamptz{Time: time.Now().Add(-1 * time.Minute), Valid: true},
	}); err != nil {
		t.Fatalf("enqueue sticky workflow task: %v", err)
	}

	row, err := q.DequeueWorkflowTask(ctx, sqlc.DequeueWorkflowTaskParams{
		TaskQueueName: taskQueue, NamespaceID: ns.ID, LeaseSeconds: 10,
		WorkerIdentity: pgtype.Text{String: "worker-B", Valid: true},
	})
	if err != nil {
		t.Fatalf("dequeue as worker-B after deadline: %v", err)
	}
	if row.WorkflowID != workflowID || row.RunID != runID {
		t.Fatalf("dequeued task = %+v, want workflow_id=%s run_id=%s", row, workflowID, runID)
	}
}

// TestStickyDispatchUnscopedTaskVisibleToAnyone is the regression check: a
// task enqueued with no sticky preference at all (the zero value —
// StartWorkflowExecution's path, and every existing caller before this
// feature) must remain visible to any worker identity, including an empty
// one, exactly as before sticky caching existed.
func TestStickyDispatchUnscopedTaskVisibleToAnyone(t *testing.T) {
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

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	taskQueue := "sticky-test-queue-" + suffix
	workflowID := "sticky-test-workflow-" + suffix
	runID := "sticky-test-run-" + suffix

	if err := q.EnqueueWorkflowTask(ctx, sqlc.EnqueueWorkflowTaskParams{
		NamespaceID: ns.ID, TaskQueueName: taskQueue, WorkflowID: workflowID, RunID: runID, ScheduledEventID: 1,
	}); err != nil {
		t.Fatalf("enqueue unscoped workflow task: %v", err)
	}

	row, err := q.DequeueWorkflowTask(ctx, sqlc.DequeueWorkflowTaskParams{
		TaskQueueName: taskQueue, NamespaceID: ns.ID, LeaseSeconds: 10,
		// No worker identity supplied — matches a poll from before this
		// feature existed.
	})
	if err != nil {
		t.Fatalf("dequeue unscoped task with no worker identity: %v", err)
	}
	if row.WorkflowID != workflowID || row.RunID != runID {
		t.Fatalf("dequeued task = %+v, want workflow_id=%s run_id=%s", row, workflowID, runID)
	}
}
