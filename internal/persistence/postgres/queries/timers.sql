-- name: InsertTimer :exec
INSERT INTO timers (namespace_id, workflow_id, run_id, timer_id, fire_at, scheduled_event_id)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: DequeueDueTimer :one
-- Same FOR UPDATE SKIP LOCKED pattern as task dispatch: lets multiple server
-- instances run the timer-firing scan concurrently without double-firing.
DELETE FROM timers
WHERE (namespace_id, workflow_id, run_id, timer_id) = (
    SELECT t.namespace_id, t.workflow_id, t.run_id, t.timer_id FROM timers AS t
    WHERE t.fire_at <= now()
    ORDER BY t.fire_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;
