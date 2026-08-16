-- Supports ListWorkflowExecutions (Phase 2 roadmap, Track D, item 3: web
-- UI) — a general "every execution in this namespace, newest first"
-- listing. The only existing index that helps a scan like this is the
-- partial one on open executions by queue (idx_workflow_executions_open_by_queue),
-- which doesn't cover closed executions or a queue-agnostic listing.
CREATE INDEX idx_workflow_executions_by_start_time
    ON workflow_executions (namespace_id, start_time DESC, workflow_id, run_id);
