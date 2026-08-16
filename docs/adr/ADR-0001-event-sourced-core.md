# ADR-0001: Event-Sourced Core with Deterministic Replay

## Status

Accepted

## Context

flowd exists to let developers write long-running, multi-step business processes
(order fulfillment, sagas, approval chains) as plain code, without hand-rolling
persistence, retries, or idempotency for every process. The central design question is:
how does a workflow survive a worker crash mid-execution and resume correctly?

Alternatives considered:

- **Declarative state-machine DSL** (AWS Step Functions / Argo Workflows style). Durability
  is trivial (the engine only ever executes one declared transition at a time), but complex
  branching/looping logic in a YAML/JSON DSL is a poor developer experience and effectively
  a second, weaker programming language.
- **Persistent actor model** (Akka/Orleans style). Good for stateful entities with
  request/response semantics, but does not give a deterministic replay guarantee for
  *exactly-once side effects* across an entire multi-step process — an actor's mailbox
  ordering is not the same guarantee as "this exact sequence of activity calls happened."
- **Job queue + manual checkpoints** (Celery/Sidekiq style). Simplest to build, but doesn't
  solve the actual problem: it pushes all durability and idempotency logic back onto the
  application developer, which is the exact burden this project exists to remove.
- **Saga-orchestration-only frameworks**. Narrower in scope — solves distributed
  transactions specifically, not general-purpose long-running process orchestration.

## Decision

flowd uses **event-sourced durable execution with deterministic replay**, the model
popularized by Temporal/Cadence. A workflow is a plain Go function. Every state
transition (workflow started, activity scheduled, activity completed, timer fired, signal
received, workflow completed) is appended to an immutable, ordered event history for that
workflow run. When a worker needs to make progress on a workflow — whether it is the
worker that started it or a fresh worker after a crash — it replays the entire history
from the beginning through a deterministic coroutine dispatcher, reconstructing in-memory
state (which Futures are resolved, which timers are pending, where execution had blocked)
before resuming forward execution.

This requires workflow code to be **deterministic**: no direct I/O, no `time.Now()`, no
raw goroutines, no unmediated randomness inside a registered workflow function. All of
that is instead exposed through SDK-provided primitives (`workflow.Now`, `workflow.Sleep`,
`workflow.ExecuteActivity`, `workflow.Go`) whose results are recorded in history and
replayed positionally rather than re-executed. This constraint is the price of the
model and is documented as the first thing any workflow author must learn.

## Consequences

- Workers are stateless with respect to any single workflow run — any worker can pick up
  any workflow task, given the history, so worker crashes are recoverable by construction
  rather than by ad hoc checkpointing.
- The event history doubles as a complete audit log of every workflow execution, with no
  additional instrumentation required (see ADR-0002 for the one deliberate exception:
  failed activity attempts are not individually recorded, to bound history growth).
- History length grows with workflow lifetime; very long-running or tight-looping
  workflows require a `ContinueAsNew` mechanism (deferred past Phase 1) to reset history.
- Determinism bugs (a workflow author accidentally calling `time.Now()` or changing
  control flow across a deploy) are a real, first-class failure mode that must be
  detected explicitly (see the replayer's non-determinism detection and the golden replay
  test strategy) rather than assumed away.
- Because only recorded activity *results* are replayed, not activity code itself,
  activities have no determinism constraint — they are plain Go functions, which keeps
  the constraint scoped to exactly the code that needs it.
