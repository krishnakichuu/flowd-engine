-- name: InsertHistoryEvent :exec
INSERT INTO history_events (namespace_id, workflow_id, run_id, event_id, event_type, attributes)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: ListHistoryEvents :many
-- Ordered scan on the PK is exactly what the SDK replayer needs: full,
-- in-order history from the beginning of the run.
SELECT * FROM history_events
WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3
  AND event_id > sqlc.arg(after_event_id)
ORDER BY event_id
LIMIT sqlc.arg(page_limit);
