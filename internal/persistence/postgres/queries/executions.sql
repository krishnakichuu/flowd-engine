-- name: InsertCurrentExecution :execrows
-- Idempotency anchor for StartWorkflowExecution: succeeds (1 row) only if no
-- run is currently open for this workflow_id.
INSERT INTO current_executions (namespace_id, workflow_id, run_id, create_request_id)
VALUES ($1, $2, $3, $4)
ON CONFLICT (namespace_id, workflow_id) DO NOTHING;

-- name: GetCurrentExecution :one
SELECT * FROM current_executions WHERE namespace_id = $1 AND workflow_id = $2;

-- name: ReplaceCurrentExecution :exec
-- Used when a prior run has closed and a fresh StartWorkflowExecution begins
-- a new run for the same workflow_id.
UPDATE current_executions
SET run_id = $3, create_request_id = $4
WHERE namespace_id = $1 AND workflow_id = $2;

-- name: CreateWorkflowExecution :exec
-- shard_id is caller-computed (see history.PartitionFor) rather than
-- defaulted here: the caller is the one who knows the workflow_id it's
-- hashing (ADR-0002 / Phase 2 roadmap, Track C, item 3).
INSERT INTO workflow_executions (
    namespace_id, workflow_id, run_id, workflow_type, task_queue, status,
    next_event_id, shard_id, workflow_execution_timeout_ns, workflow_run_timeout_ns,
    workflow_task_timeout_ns
) VALUES (
    $1, $2, $3, $4, $5, 'RUNNING', 1, $6, $7, $8, $9
);

-- name: GetWorkflowExecution :one
SELECT * FROM workflow_executions
WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3;

-- name: AdvanceNextEventID :one
-- The history-append CAS (ADR-0002, Mechanism B). Returns the new
-- next_event_id on success. Zero rows (pgx.ErrNoRows) means a concurrent
-- writer already advanced the counter past `expected` — the caller must
-- reject the request and discard any in-memory replay state.
UPDATE workflow_executions
SET next_event_id = next_event_id + sqlc.arg(increment)::bigint,
    updated_at = now()
WHERE namespace_id = sqlc.arg(namespace_id)
  AND workflow_id = sqlc.arg(workflow_id)
  AND run_id = sqlc.arg(run_id)
  AND next_event_id = sqlc.arg(expected)
RETURNING next_event_id;

-- name: CloseWorkflowExecution :exec
UPDATE workflow_executions
SET status = $4, close_time = now(), updated_at = now()
WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3;

-- name: SetStickyExecution :exec
-- Records (or, with both values NULL, clears) a run's sticky worker
-- registration — consulted by every EnqueueWorkflowTask call site to
-- decide whether the run's next workflow task should prefer a specific
-- worker (see 0002_sticky_workflow_tasks migration).
UPDATE workflow_executions
SET sticky_worker_identity = $4, sticky_expires_at = $5, updated_at = now()
WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3;

-- name: ListWorkflowExecutions :many
-- Keyset-paginated, newest first (Phase 2 roadmap, Track D, item 3: web
-- UI). All three ORDER BY columns are DESC specifically so the cursor
-- condition below can be a single row-value comparison — start_time alone
-- isn't unique, so workflow_id/run_id are tiebreakers, not meaningful
-- sort keys on their own. status_filter/task_queue are optional: a NULL
-- sqlc.narg matches everything. cursor_start_time NULL means "first
-- page" (see idx_workflow_executions_by_start_time, 0004 migration).
SELECT * FROM workflow_executions
WHERE namespace_id = sqlc.arg(namespace_id)
  AND (sqlc.narg(status_filter)::text IS NULL OR status = sqlc.narg(status_filter))
  AND (sqlc.narg(task_queue)::text IS NULL OR task_queue = sqlc.narg(task_queue))
  AND (
        sqlc.narg(cursor_start_time)::timestamptz IS NULL
        OR (start_time, workflow_id, run_id) < (
          sqlc.narg(cursor_start_time)::timestamptz,
          sqlc.narg(cursor_workflow_id)::text,
          sqlc.narg(cursor_run_id)::text
        )
      )
ORDER BY start_time DESC, workflow_id DESC, run_id DESC
LIMIT sqlc.arg(page_limit);
