-- name: EnqueueQueryTask :one
INSERT INTO query_tasks (namespace_id, task_queue_name, workflow_id, run_id, query_type, query_args, status, visible_at)
VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', now())
RETURNING *;

-- name: DequeueQueryTask :one
-- Same FOR UPDATE SKIP LOCKED dispatch as workflow/activity tasks
-- (ADR-0002, Mechanism A).
UPDATE query_tasks
SET status = 'STARTED',
    lease_token = gen_random_uuid(),
    lease_expiry = now() + make_interval(secs => sqlc.arg(lease_seconds)::float8)
WHERE task_id = (
    SELECT t.task_id FROM query_tasks AS t
    WHERE t.task_queue_name = sqlc.arg(task_queue_name)
      AND t.namespace_id = sqlc.arg(namespace_id)
      AND t.status = 'PENDING'
      AND t.visible_at <= now()
    ORDER BY t.task_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;

-- name: CompleteQueryTask :execrows
-- Unlike CompleteWorkflowTask/CompleteActivityTask, this doesn't delete
-- the row — the result has to stay readable until QueryWorkflowExecution's
-- own polling loop reads it and deletes it itself (see DeleteQueryTask).
UPDATE query_tasks
SET status = 'COMPLETED', result = $3
WHERE task_id = $1 AND lease_token = $2;

-- name: FailQueryTask :execrows
UPDATE query_tasks
SET status = 'FAILED', failure_message = $3
WHERE task_id = $1 AND lease_token = $2;

-- name: GetQueryTask :one
SELECT * FROM query_tasks WHERE task_id = $1 AND namespace_id = $2;

-- name: DeleteQueryTask :exec
DELETE FROM query_tasks WHERE task_id = $1;

-- name: ReapExpiredQueryTasks :execrows
UPDATE query_tasks
SET status = 'PENDING', lease_token = NULL, lease_expiry = NULL
WHERE status = 'STARTED' AND lease_expiry < now();
