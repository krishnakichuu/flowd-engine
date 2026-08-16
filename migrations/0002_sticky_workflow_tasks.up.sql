-- Sticky worker caching (Phase 2 roadmap, Track C, item 1): a worker that
-- just processed a workflow task can ask to be preferred for that run's
-- next one, so it can resume its cached in-memory Execution instead of
-- replaying the run's full history from event 1 (see ADR-0001's
-- replay-cost tradeoff, and sdk/worker's cache).
--
-- sticky_worker_identity/sticky_expires_at on workflow_executions is the
-- run's current registration, set by RespondWorkflowTaskCompleted and
-- consulted whenever something later enqueues that run's next workflow
-- task (activity completion, timer fire, signal). preferred_worker_identity
-- /sticky_deadline on workflow_tasks is that registration baked into the
-- specific dispatched row: DequeueWorkflowTask hides a preferred row from
-- every other worker until its deadline passes, at which point it falls
-- back to whichever worker is available — same table, same FOR UPDATE SKIP
-- LOCKED dispatch (ADR-0002, Mechanism A), no new queue.

ALTER TABLE workflow_executions
    ADD COLUMN sticky_worker_identity TEXT,
    ADD COLUMN sticky_expires_at      TIMESTAMPTZ;

ALTER TABLE workflow_tasks
    ADD COLUMN preferred_worker_identity TEXT,
    ADD COLUMN sticky_deadline            TIMESTAMPTZ;
