-- Core schema for flowd Phase 1: single-node, non-sharded event-sourced
-- workflow engine. See docs/adr/ADR-0002 for the two CAS mechanisms this
-- schema exists to support.

CREATE TABLE namespaces (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO namespaces (name) VALUES ('default');

-- current_executions enforces "at most one open run per workflow_id" and
-- backs idempotent StartWorkflowExecution: INSERT ... ON CONFLICT DO NOTHING,
-- then compare create_request_id to distinguish a retried start from a
-- genuine "workflow already running" conflict.
CREATE TABLE current_executions (
    namespace_id      BIGINT NOT NULL REFERENCES namespaces(id),
    workflow_id       TEXT NOT NULL,
    run_id            TEXT NOT NULL,
    create_request_id TEXT NOT NULL,
    PRIMARY KEY (namespace_id, workflow_id)
);

CREATE TABLE workflow_executions (
    namespace_id    BIGINT NOT NULL REFERENCES namespaces(id),
    workflow_id     TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    workflow_type   TEXT NOT NULL,
    task_queue      TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN (
                        'RUNNING', 'COMPLETED', 'FAILED', 'TERMINATED',
                        'TIMED_OUT', 'CONTINUED_AS_NEW'
                    )),
    -- Version counter for the history-append CAS (ADR-0002, Mechanism B).
    -- Starts at 1: the first event to be appended is event_id 1.
    next_event_id   BIGINT NOT NULL DEFAULT 1,
    -- Inert in Phase 1 (always 0); reserved so sharded history ownership
    -- later is a routing change, not a migration touching every row.
    shard_id        INT NOT NULL DEFAULT 0,
    workflow_execution_timeout_ns BIGINT,
    workflow_run_timeout_ns       BIGINT,
    workflow_task_timeout_ns      BIGINT,
    start_time      TIMESTAMPTZ NOT NULL DEFAULT now(),
    close_time      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, workflow_id, run_id)
);

CREATE INDEX idx_workflow_executions_open_by_queue
    ON workflow_executions (namespace_id, task_queue)
    WHERE status = 'RUNNING';

-- history_events is the immutable, ordered event history. The PK ordering
-- (namespace_id, workflow_id, run_id, event_id) is exactly the scan order
-- the SDK replayer needs.
CREATE TABLE history_events (
    namespace_id BIGINT NOT NULL,
    workflow_id  TEXT NOT NULL,
    run_id       TEXT NOT NULL,
    event_id     BIGINT NOT NULL,
    event_time   TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_type   TEXT NOT NULL,
    -- Marshaled flow.api.v1.HistoryEvent attributes oneof, protobuf bytes
    -- (not JSONB) so the wire format and storage format are the same encoding
    -- and schema evolution follows normal protobuf field-number rules.
    attributes   BYTEA NOT NULL,
    PRIMARY KEY (namespace_id, workflow_id, run_id, event_id),
    FOREIGN KEY (namespace_id, workflow_id, run_id)
        REFERENCES workflow_executions (namespace_id, workflow_id, run_id)
);

-- workflow_tasks is the physical dispatch queue for workflow tasks
-- (ADR-0002, Mechanism A: FOR UPDATE SKIP LOCKED dequeue).
CREATE TABLE workflow_tasks (
    task_id              BIGSERIAL PRIMARY KEY,
    namespace_id         BIGINT NOT NULL,
    task_queue_name      TEXT NOT NULL,
    -- Reserved for future matching partitioning; always 0 in Phase 1.
    task_queue_partition INT NOT NULL DEFAULT 0,
    workflow_id          TEXT NOT NULL,
    run_id               TEXT NOT NULL,
    scheduled_event_id   BIGINT NOT NULL,
    status               TEXT NOT NULL CHECK (status IN ('PENDING', 'STARTED')),
    visible_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_expiry         TIMESTAMPTZ,
    lease_token          UUID,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_workflow_tasks_dispatch
    ON workflow_tasks (task_queue_name, namespace_id, visible_at, task_id)
    WHERE status = 'PENDING';

CREATE INDEX idx_workflow_tasks_lease_expiry
    ON workflow_tasks (lease_expiry)
    WHERE status = 'STARTED';

-- activity_tasks: same queue pattern as workflow_tasks, plus retry/backoff
-- state enforced server-side (ADR-0002) so retries survive worker crashes.
CREATE TABLE activity_tasks (
    task_id               BIGSERIAL PRIMARY KEY,
    namespace_id          BIGINT NOT NULL,
    task_queue_name       TEXT NOT NULL,
    task_queue_partition  INT NOT NULL DEFAULT 0,
    workflow_id           TEXT NOT NULL,
    run_id                TEXT NOT NULL,
    activity_id           BIGINT NOT NULL,
    activity_type         TEXT NOT NULL,
    scheduled_event_id    BIGINT NOT NULL,
    input                 BYTEA NOT NULL,
    status                TEXT NOT NULL CHECK (status IN ('PENDING', 'STARTED')),
    attempt               INT NOT NULL DEFAULT 1,
    visible_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_expiry          TIMESTAMPTZ,
    lease_token           UUID,
    schedule_to_start_timeout_ns BIGINT NOT NULL,
    start_to_close_timeout_ns   BIGINT NOT NULL,
    retry_initial_interval_ns   BIGINT NOT NULL,
    retry_backoff_coefficient   DOUBLE PRECISION NOT NULL DEFAULT 2.0,
    retry_max_interval_ns       BIGINT NOT NULL,
    retry_max_attempts          INT NOT NULL DEFAULT 0,
    retry_non_retryable_types   TEXT[] NOT NULL DEFAULT '{}',
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_activity_tasks_dispatch
    ON activity_tasks (task_queue_name, namespace_id, visible_at, task_id)
    WHERE status = 'PENDING';

CREATE INDEX idx_activity_tasks_lease_expiry
    ON activity_tasks (lease_expiry)
    WHERE status = 'STARTED';

-- timers backs workflow.Sleep/NewTimer: a StartTimer command inserts a row
-- here (plus a TimerStarted history event); a background scanner fires due
-- timers (fire_at <= now()), appending a TimerFired event and scheduling a
-- new workflow task, the same reaper-style pattern as the task-lease reaper.
CREATE TABLE timers (
    namespace_id       BIGINT NOT NULL,
    workflow_id        TEXT NOT NULL,
    run_id             TEXT NOT NULL,
    timer_id           BIGINT NOT NULL,
    fire_at            TIMESTAMPTZ NOT NULL,
    scheduled_event_id BIGINT NOT NULL,
    PRIMARY KEY (namespace_id, workflow_id, run_id, timer_id)
);

CREATE INDEX idx_timers_fire_at ON timers (fire_at);
