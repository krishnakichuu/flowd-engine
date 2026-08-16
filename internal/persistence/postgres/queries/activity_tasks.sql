-- name: EnqueueActivityTask :exec
-- task_queue_partition is caller-computed (see history.PartitionFor).
INSERT INTO activity_tasks (
    namespace_id, task_queue_name, task_queue_partition, workflow_id, run_id, activity_id, activity_type,
    scheduled_event_id, input, status, visible_at,
    schedule_to_start_timeout_ns, start_to_close_timeout_ns,
    retry_initial_interval_ns, retry_backoff_coefficient, retry_max_interval_ns,
    retry_max_attempts, retry_non_retryable_types
) VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, 'PENDING', now(),
    $10, $11,
    $12, $13, $14,
    $15, $16
);

-- name: DequeueActivityTask :one
-- Lease length is derived from this activity's own start_to_close_timeout,
-- not a fixed constant (ADR-0002) — a fixed lease would cause legitimately
-- long-running activities to be reaped and double-dispatched.
--
-- The partition condition (Phase 2 roadmap, Track C, item 3): an empty (or
-- SQL NULL — a nil Go slice binds as NULL, not an empty array) partitions
-- array matches every partition, via COALESCE(..., 0) = 0 — a worker that
-- never declared which partitions it serves (sdk/worker's default) sees
-- everything, exactly as before this feature existed.
UPDATE activity_tasks
SET status = 'STARTED',
    lease_token = gen_random_uuid(),
    lease_expiry = now() + make_interval(secs => (start_to_close_timeout_ns::float8 / 1e9))
WHERE task_id = (
    SELECT t.task_id FROM activity_tasks AS t
    WHERE t.task_queue_name = sqlc.arg(task_queue_name)
      AND t.namespace_id = sqlc.arg(namespace_id)
      AND t.status = 'PENDING'
      AND t.visible_at <= now()
      AND (
            COALESCE(cardinality(sqlc.arg(partitions)::int[]), 0) = 0
         OR t.task_queue_partition = ANY(sqlc.arg(partitions)::int[])
      )
    ORDER BY t.task_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;

-- name: GetActivityTaskByLease :one
-- Read the row's full details (input, retry policy, attempt count) before
-- CompleteActivityTask/RetryActivityTask consume it, validated against the
-- same lease_token so a stale caller sees the same "expired" outcome.
SELECT * FROM activity_tasks WHERE task_id = $1 AND lease_token = $2;

-- name: CompleteActivityTask :execrows
DELETE FROM activity_tasks
WHERE task_id = $1 AND lease_token = $2;

-- name: RetryActivityTask :execrows
-- Server-side retry/backoff (ADR-0002): re-queues the task without a new
-- history event per attempt, so retries survive worker crashes and history
-- stays compact.
UPDATE activity_tasks
SET status = 'PENDING',
    attempt = attempt + 1,
    visible_at = now() + make_interval(secs => sqlc.arg(backoff_seconds)::float8),
    lease_token = NULL,
    lease_expiry = NULL
WHERE task_id = $1 AND lease_token = $2;

-- name: ReapExpiredActivityTasks :execrows
UPDATE activity_tasks
SET status = 'PENDING', lease_token = NULL, lease_expiry = NULL
WHERE status = 'STARTED' AND lease_expiry < now();
