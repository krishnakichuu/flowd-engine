-- name: EnqueueWorkflowTask :exec
-- preferred_worker_identity/sticky_deadline are NULL for a normal
-- (non-sticky) enqueue — every existing caller before sticky caching
-- passed the equivalent of "no preference," and still does explicitly.
-- task_queue_partition is caller-computed (see history.PartitionFor).
INSERT INTO workflow_tasks (
    namespace_id, task_queue_name, task_queue_partition, workflow_id, run_id, scheduled_event_id,
    status, visible_at, preferred_worker_identity, sticky_deadline
)
VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', now(), $7, $8);

-- name: DequeueWorkflowTask :one
-- ADR-0002, Mechanism A. FOR UPDATE SKIP LOCKED gives exactly-one-winner
-- semantics across concurrently polling workers without blocking on
-- contended rows. Zero rows (pgx.ErrNoRows) means no task is currently
-- available for this queue.
--
-- The sticky condition: a row with a preferred_worker_identity is invisible
-- to every other worker until either that worker asks (worker_identity
-- matches) or sticky_deadline passes, at which point it's fair game for
-- anyone — the fallback-to-full-replay path sticky caching depends on,
-- using the same table and the same SKIP LOCKED dispatch as always.
--
-- The partition condition (Phase 2 roadmap, Track C, item 3): an empty (or
-- SQL NULL — a nil Go slice binds as NULL, not an empty array) partitions
-- array matches every partition, via COALESCE(..., 0) = 0 — a worker that
-- never declared which partitions it serves (sdk/worker's default) sees
-- everything, exactly as before this feature existed.
UPDATE workflow_tasks
SET status = 'STARTED',
    lease_token = gen_random_uuid(),
    lease_expiry = now() + make_interval(secs => sqlc.arg(lease_seconds)::float8)
WHERE task_id = (
    SELECT t.task_id FROM workflow_tasks AS t
    WHERE t.task_queue_name = sqlc.arg(task_queue_name)
      AND t.namespace_id = sqlc.arg(namespace_id)
      AND t.status = 'PENDING'
      AND t.visible_at <= now()
      AND (
            t.preferred_worker_identity IS NULL
         OR t.preferred_worker_identity = sqlc.arg(worker_identity)
         OR t.sticky_deadline < now()
      )
      AND (
            COALESCE(cardinality(sqlc.arg(partitions)::int[]), 0) = 0
         OR t.task_queue_partition = ANY(sqlc.arg(partitions)::int[])
      )
    ORDER BY t.task_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;

-- name: CompleteWorkflowTask :execrows
-- Deletes the task row only if the caller's lease_token still matches,
-- rejecting stale completions from a worker whose lease already expired
-- and was reassigned.
DELETE FROM workflow_tasks
WHERE task_id = $1 AND lease_token = $2;

-- name: ReapExpiredWorkflowTasks :execrows
-- Background lease reaper: resets tasks whose worker crashed after
-- acquiring the lease but before responding, back to PENDING for
-- redispatch.
UPDATE workflow_tasks
SET status = 'PENDING', lease_token = NULL, lease_expiry = NULL
WHERE status = 'STARTED' AND lease_expiry < now();
