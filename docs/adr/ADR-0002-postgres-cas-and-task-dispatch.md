# ADR-0002: Postgres CAS Mechanisms for Task Dispatch and History Append

## Status

Accepted

## Context

Given ADR-0001's event-sourced model, Phase 1 needs a single-node persistence layer that
guarantees two invariants under concurrent workers:

1. **No two workers are ever handed the same task.** If worker A and worker B both poll
   for work and there is one pending workflow task, exactly one of them must receive it.
2. **No two workers can ever successfully append conflicting events to the same
   workflow's history.** Even if (1) is somehow violated (e.g. a lease expires and is
   reassigned right as the original holder's response arrives), a stale writer must be
   rejected rather than corrupting or forking the history.

Phase 1 targets a single Postgres instance (HA/clustering deferred). Two mechanisms are
needed because they defend against different failure modes, and neither alone is
sufficient: (1) is about queue semantics (who gets to *attempt* work), (2) is about
write safety (whose *result* is accepted).

## Decision

### Mechanism A — task dispatch via `FOR UPDATE SKIP LOCKED`

`workflow_tasks` and `activity_tasks` are physical Postgres queue tables. Dispatch uses:

```sql
UPDATE workflow_tasks
SET status = 'STARTED', lease_token = gen_random_uuid(), lease_expiry = now() + interval '10s'
WHERE task_id = (
  SELECT task_id FROM workflow_tasks
  WHERE task_queue_name = $1 AND namespace_id = $2 AND status = 'PENDING' AND visible_at <= now()
  ORDER BY task_id
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING *;
```

`FOR UPDATE SKIP LOCKED` is a well-established "Postgres as a queue" primitive: N
concurrent pollers race this query; each row can only be locked by one transaction, and
transactions that would block on an already-locked row skip it instead of waiting. This
gives exactly-one-winner semantics per row without a separate distributed lock service.

A background **lease reaper** (fixed poll interval, default 5s) resets rows whose
`lease_expiry` has passed back to `PENDING`, handling the case where a worker acquired a
lease and then crashed before responding. Activity task leases are set from that
activity's own `start_to_close_timeout`, not a fixed constant — using a fixed constant
would cause legitimately long-running activities to be reaped and dispatched a second
time while still genuinely in progress.

### Mechanism B — history append via optimistic concurrency on `next_event_id`

`workflow_executions.next_event_id` is a per-run monotonic version counter. Every append
to that run's history (a workflow task response, an activity task response, a signal)
must, inside one transaction:

```sql
UPDATE workflow_executions
SET next_event_id = next_event_id + $n
WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3 AND next_event_id = $expected
RETURNING next_event_id;
```

Zero rows updated means another writer already advanced the version since this caller
last read it; the caller's request is rejected (distinct error type, e.g.
`ConcurrentModification` / expired task token) and it must discard any in-memory replay
state for that task rather than retry blindly. Only on success are the corresponding rows
inserted into `history_events` and the relevant task rows updated, in the same
transaction as the counter update.

This is what makes split-brain between two workers structurally impossible even in the
edge case where Mechanism A's lease semantics somehow let a task be double-dispatched
(e.g., a lease-expiry race): the last writer to successfully advance `next_event_id` wins,
and history is never forked or double-appended.

### Retry/backoff is enforced server-side, not client-side

On `RespondActivityTaskFailed`, if the attempt count is within `max_attempts` and the
error type is not in `non_retryable_error_types`, the history engine computes
`backoff = min(initial_interval * backoff_coefficient^attempt, max_interval)` and updates
the activity task row (`status = 'PENDING'`, `attempt += 1`, `visible_at = now() + backoff`)
— it does **not** append a history event for the failed attempt. Only a terminal outcome
(final success or retries exhausted) is recorded in history. This is a deliberate
divergence from "every event is a natural audit log" (ADR-0001): recording every failed
attempt would make history length scale with transient failure rate rather than business
process steps, undermining the history-growth mitigation this project already relies on
(`ContinueAsNew`, deferred, exists for the same reason). Per-attempt visibility is instead
provided via metrics (`activity_retry_total`) and logs, which is sufficient for
operational debugging without being part of the replayable, correctness-critical history.

Enforcing retry/backoff server-side (rather than the SDK sleeping and retrying locally)
is what allows retries to survive a worker crash — the pending retry is a durable queue
row, not in-memory worker state.

## Consequences

- Correctness does not depend on Postgres advisory locks, `SELECT ... FOR UPDATE` held
  across a network round-trip, or an external coordination service — both mechanisms are
  ordinary transactional SQL, keeping the Phase 1 operational footprint to "one Postgres
  instance."
- The design assumes a single Postgres primary. Sharding across multiple Postgres
  instances is out of scope for Phase 1; `shard_id`/`task_queue_partition` columns are
  added now (computed as constant/zero) specifically so that future sharding is a routing
  change rather than a schema migration touching every historical row.
- Because failed activity attempts are not recorded as history events, an operator
  reconstructing "what happened" purely from `GetWorkflowExecutionHistory` will not see
  transient failures — only the eventual outcome. This is an explicit, documented
  trade-off, not an oversight.
