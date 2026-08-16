-- Workflow queries (Phase 2 roadmap, Track D, item 1): a synchronous,
-- read-only "what's this execution's current state" call, answered by a
-- worker's in-memory Execution (see sdk/workflow.SetQueryHandler) without
-- ever appending to history_events — deliberately a separate table and
-- lifecycle from workflow_tasks/activity_tasks, both of which DO produce
-- history.
--
-- Unlike those two, a query_tasks row is not deleted on completion: the
-- client that created it (QueryWorkflowExecution's own handler, polling
-- this row) is the one thing that reads the result, and deletes the row
-- itself once it has — see internal/history's DeleteQueryTask.
CREATE TABLE query_tasks (
    task_id         BIGSERIAL PRIMARY KEY,
    namespace_id    BIGINT NOT NULL,
    task_queue_name TEXT NOT NULL,
    workflow_id     TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    query_type      TEXT NOT NULL,
    query_args      BYTEA NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('PENDING', 'STARTED', 'COMPLETED', 'FAILED')),
    result          BYTEA,
    failure_message TEXT,
    visible_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_expiry    TIMESTAMPTZ,
    lease_token     UUID,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_query_tasks_dispatch
    ON query_tasks (task_queue_name, namespace_id, visible_at, task_id)
    WHERE status = 'PENDING';

CREATE INDEX idx_query_tasks_lease_expiry
    ON query_tasks (lease_expiry)
    WHERE status = 'STARTED';
