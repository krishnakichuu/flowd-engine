ALTER TABLE workflow_tasks
    DROP COLUMN IF EXISTS sticky_deadline,
    DROP COLUMN IF EXISTS preferred_worker_identity;

ALTER TABLE workflow_executions
    DROP COLUMN IF EXISTS sticky_expires_at,
    DROP COLUMN IF EXISTS sticky_worker_identity;
