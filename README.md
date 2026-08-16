# flowd

**flowd** is a durable workflow orchestration engine for Go. It lets you write
long-running, multi-step business processes as plain Go functions and get
crash recovery, retries, and exactly-once execution semantics for free —
without hand-rolling persistence, checkpointing, or idempotency yourself.

It implements the event-sourced, deterministic-replay model popularized by
Temporal/Cadence, backed by a single Postgres instance, with a Go SDK, a gRPC
API, an operator CLI, and a read-only web dashboard.

## Table of Contents

- [Problem](#problem)
- [Why flowd](#why-flowd)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Project Layout](#project-layout)
- [Getting Started](#getting-started)
- [Installing `flow-cli`](#installing-flow-cli)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Writing a Workflow](#writing-a-workflow)
  - [Running a Worker](#running-a-worker)
  - [Starting a Workflow from Code](#starting-a-workflow-from-code)
  - [The `flow-cli` Operator CLI](#the-flow-cli-operator-cli)
  - [Web Dashboard](#web-dashboard)
- [Core Concepts](#core-concepts)
- [Testing](#testing)
- [Project Status](#project-status)
- [Contributing](#contributing)
- [License](#license)

## Problem

Business processes that span multiple steps — order fulfillment, approval
chains, sagas, payment/shipping pipelines — are simple to describe but hard
to implement correctly. A naive implementation (a job queue, a cron job, a
state column on a database row) breaks down as soon as you need:

- **Crash recovery** — a worker dies mid-process; who resumes it, and from
  where?
- **Exactly-once side effects** — a step succeeds but the confirmation is
  lost before it's recorded; does it run twice?
- **Retries with backoff** — transient failures need retrying without
  duplicating already-completed steps.
- **Long waits** — a process needs to sleep for hours or wait on an external
  signal without tying up a worker or a queue slot.
- **Auditability** — knowing exactly what happened, and when, for any given
  execution.

Most teams end up re-solving this from scratch, badly, inside every service
that needs it.

## Why flowd

flowd exists to remove that burden entirely. Instead of a declarative DSL
(Step Functions/Argo style — a poor fit for real branching logic) or a job
queue with manual checkpoints (which just pushes the durability problem back
onto the application), flowd lets you write the process as **ordinary Go
code**. The engine durably records every meaningful state transition and can
deterministically replay a workflow's history to reconstruct exactly where
it left off — on any worker, at any time, including after a crash.

See [`docs/adr/ADR-0001`](docs/adr/ADR-0001-event-sourced-core.md) for the
full design rationale and the alternatives considered.

## Key Features

- **Durable execution** — workflows survive worker crashes and resume
  exactly where they left off, via deterministic replay of an immutable
  event history.
- **Plain Go workflows** — no YAML/DSL; workflows are regular Go functions
  using SDK primitives (`workflow.ExecuteActivity`, `workflow.Sleep`,
  `workflow.Go`, `workflow.Now`) in place of direct I/O, `time.Now()`, or raw
  goroutines.
- **Automatic retries** — activity failures are retried with configurable
  backoff, enforced server-side so retries survive a worker crash, not just
  a process restart.
- **Signals & queries** — push data asynchronously into a running workflow
  (`SignalWorkflow`) or synchronously read its in-memory state
  (`QueryWorkflow`) without waiting for completion.
- **Cancellation** — cooperative cancellation via
  `RequestCancelWorkflowExecution`, observed in-workflow with
  `workflow.IsCancelRequested`.
- **Continue-As-New** — reset a workflow's history for processes that
  legitimately loop forever, keeping history length bounded.
- **No external coordination service** — task dispatch and history
  consistency are guaranteed with ordinary transactional SQL
  (`FOR UPDATE SKIP LOCKED` + optimistic concurrency), so the only
  infrastructure dependency is a single Postgres instance.
- **gRPC API + typed Go SDK** — `sdk/client` and `sdk/worker` for
  application integration; the wire protocol (`api/proto`) is
  language-agnostic.
- **Operator CLI** (`flow-cli`) — start, signal, cancel, list, describe, and
  inspect workflow history against a running server.
- **Web dashboard** — a read-only UI for browsing workflow executions and
  per-run history, served alongside the gRPC API.
- **Multi-tenancy** — namespaces isolate workflows, task queues, and
  history from each other within one deployment.
- **Observability built in** — Prometheus metrics and structured logs out
  of the box; no separate instrumentation layer required.

## Architecture

flowd runs as a single binary (`flowd`) composed of three logical
components, all backed by one Postgres database:

```
                        ┌─────────────────────────────┐
   SDK / flow-cli /  ─▶ │   Frontend (gRPC API)        │
   Web Dashboard        │   internal/frontend           │
                        └───────────────┬──────────────┘
                                        │
                        ┌───────────────▼──────────────┐
                        │   History Engine              │
                        │   internal/history             │
                        │   event-sourced execution,     │
                        │   optimistic concurrency,      │
                        │   timers, signals, queries      │
                        └───────────────┬──────────────┘
                                        │
                        ┌───────────────▼──────────────┐
                        │   Matching (task dispatch)    │
                        │   internal/matching             │
                        │   FOR UPDATE SKIP LOCKED       │
                        │   queue + lease reaper         │
                        └───────────────┬──────────────┘
                                        │
                        ┌───────────────▼──────────────┐
                        │        PostgreSQL              │
                        │  history_events, executions,   │
                        │  workflow/activity task queues  │
                        └────────────────────────────────┘
```

Workers (built with `sdk/worker`) poll task queues over gRPC, execute
workflow/activity code, and report results back — they hold no durable state
of their own, so any worker can pick up any task given the history.

Two design decisions underpin correctness at the persistence layer:

1. **Task dispatch** uses Postgres' `FOR UPDATE SKIP LOCKED` as a queue
   primitive — concurrent pollers never receive the same task, with no
   external lock service.
2. **History append** uses optimistic concurrency on a per-run version
   counter — a stale writer is rejected outright rather than forking or
   corrupting history, even under lease-expiry races.

Full rationale: [ADR-0001 — Event-Sourced Core](docs/adr/ADR-0001-event-sourced-core.md),
[ADR-0002 — Postgres CAS Mechanisms](docs/adr/ADR-0002-postgres-cas-and-task-dispatch.md).

## Project Layout

```
api/            Protobuf service definitions (api/proto) and generated code (api/gen)
cmd/flowd/      Server binary: frontend + history + matching, one process
cmd/flow-cli/   Operator CLI
internal/       Server-side implementation (frontend, history, matching,
                persistence, webapi, webui, config, metrics, log)
sdk/            Public Go SDK: client, worker, workflow, activity primitives
migrations/     Postgres schema migrations
examples/       Runnable example workflows (helloworkflow, countdown)
test/           Integration and replay test suites
docs/adr/       Architecture decision records
web/            Web dashboard frontend source (built into internal/webui/dist)
```

## Getting Started

### Prerequisites

- Go 1.25+
- Docker (for the bundled Postgres via `docker compose`), or your own
  Postgres 16+ instance

### 1. Start Postgres

```bash
make compose-up
```

This starts a local Postgres instance on `localhost:5432` (see
`docker-compose.yml`). If you're using your own Postgres instance, just make
sure `FLOWD_DATABASE_DSN` points at it (see [Configuration](#configuration)).

### 2. Run the server

```bash
make build
make run-server
```

`flowd` runs its own schema migrations on startup — no separate migrate step
is required. By default it listens on:

| Port   | Purpose                                    |
|--------|---------------------------------------------|
| `7233` | gRPC API (SDK, `flow-cli`)                  |
| `7234` | Web dashboard + read-only JSON API          |
| `9090` | Prometheus metrics (`/metrics`, `/healthz`) |

### 3. Run the example end to end

In separate terminals:

```bash
go run ./examples/helloworkflow/worker
go run ./examples/helloworkflow/starter -name "flowd"
```

The starter prints the workflow/run ID, waits for completion, and prints the
result (`Hello, flowd!`) — durably executed, retried and recoverable if the
worker crashes mid-run.

## Installing `flow-cli`

`flow-cli` is the operator CLI for starting, signaling, cancelling, and
inspecting workflows against a running `flowd` server. It's distributed
independently of the server binary, via three methods:

### Homebrew (macOS/Linux) — recommended

```bash
brew tap krishnakichuu/flowd
brew install flowd-cli
```

> The formula is named `flowd-cli`, not `flow-cli`, to avoid colliding with
> an unrelated, pre-existing `flow-cli` formula in homebrew-core — the
> installed binary itself is still called `flow-cli`.

Verify the install:

```bash
flow-cli version
```

Upgrade to a newer release:

```bash
brew upgrade flowd-cli
```

### Download a prebuilt binary

Every tagged release publishes `flow-cli` binaries for `darwin`/`linux` ×
`amd64`/`arm64`, plus a `checksums.txt`, on the
[Releases page](https://github.com/krishnakichuu/flowd-engine/releases).

```bash
# example: macOS on Apple Silicon
curl -sLO https://github.com/krishnakichuu/flowd-engine/releases/latest/download/flow-cli_<version>_darwin_arm64.tar.gz
curl -sLO https://github.com/krishnakichuu/flowd-engine/releases/latest/download/checksums.txt

# verify the checksum before running anything you downloaded
grep flow-cli_<version>_darwin_arm64.tar.gz checksums.txt | shasum -a 256 -c -

tar xzf flow-cli_<version>_darwin_arm64.tar.gz
sudo mv flow-cli /usr/local/bin/
flow-cli version
```

Substitute `linux_amd64` / `linux_arm64` for the Linux equivalents.

### Build from source

Requires Go 1.25+:

```bash
git clone https://github.com/krishnakichuu/flowd-engine.git
cd flowd-engine
make build
./bin/flow-cli version
```

`make build` produces `flow-cli` alongside the `flowd` server binary in
`bin/`; move or symlink it onto your `PATH` as needed.

## Configuration

`flowd` is configured entirely through environment variables (see
[`internal/config/config.go`](internal/config/config.go)):

| Variable                          | Default                                                    | Description                                                                 |
|------------------------------------|-------------------------------------------------------------|-------------------------------------------------------------------------------|
| `FLOWD_GRPC_ADDR`                  | `:7233`                                                     | gRPC listener address                                                        |
| `FLOWD_METRICS_ADDR`               | `:9090`                                                     | Prometheus metrics / health listener address                                 |
| `FLOWD_WEBUI_ADDR`                 | `:7234`                                                     | Web dashboard listener address                                               |
| `FLOWD_DATABASE_DSN`               | `postgres://flowd:flowd@localhost:5432/flowd?sslmode=disable` | Postgres connection string                                                   |
| `FLOWD_REAPER_INTERVAL`            | `5s`                                                         | How often expired task leases are reclaimed                                  |
| `FLOWD_TIMER_FIRER_INTERVAL`       | `1s`                                                         | How often pending workflow timers are checked                                |
| `FLOWD_TLS_CERT_FILE` / `FLOWD_TLS_KEY_FILE` | unset (plaintext)                                 | PEM paths enabling TLS on the gRPC listener                                  |
| `FLOWD_TLS_CLIENT_CA_FILE`         | unset                                                        | CA file enabling mTLS (requires the above)                                   |
| `FLOWD_API_KEYS`                   | unset (unauthenticated)                                      | Comma-separated API keys, optionally namespace-scoped (`key:ns1\|ns2`)       |
| `FLOWD_NUM_SHARDS`                 | `1`                                                          | Fixed shard space for new workflow executions                                |
| `FLOWD_NUM_TASK_QUEUE_PARTITIONS`  | `1`                                                          | Fixed partition space for task dispatch                                      |
| `FLOWD_TASK_TOKEN_SIGNING_KEY`     | random per-process                                           | Hex-encoded HMAC key for task tokens — set explicitly for multi-instance deployments |

Client-side (`flow-cli`, and any SDK client dialing a secured server):

| Variable                        | Description                                    |
|-----------------------------------|-------------------------------------------------|
| `FLOWD_ADDR`                    | Server address (default `localhost:7233`)       |
| `FLOWD_API_KEY`                 | API key to authenticate with                    |
| `FLOWD_TLS_CA_FILE`             | CA file to dial over TLS                         |
| `FLOWD_TLS_CLIENT_CERT_FILE` / `FLOWD_TLS_CLIENT_KEY_FILE` | Client certificate for mTLS |
| `FLOWD_TLS_SERVER_NAME`         | Override the server name used for cert verification |

> By default flowd runs with plaintext gRPC and no request authentication —
> fine for local development, **not** for exposing outside a trusted
> network. Set `FLOWD_TLS_CERT_FILE`/`FLOWD_TLS_KEY_FILE` and `FLOWD_API_KEYS`
> before deploying anywhere reachable.

## Usage

### Writing a Workflow

Workflows are plain Go functions using only the deterministic primitives
`sdk/workflow` provides. Activities — where actual I/O happens — are plain Go
functions with no such restriction:

```go
// workflow.go
package helloworkflow

import (
	"github.com/krishnakichuu/flowd/sdk/activity"
	"github.com/krishnakichuu/flowd/sdk/workflow"
)

const TaskQueue = "helloworkflow"

func SimpleWorkflow(ctx workflow.Context, name string) (string, error) {
	var greeting string
	err := workflow.ExecuteActivity(ctx, GreetActivity, name, workflow.ActivityOptions{}).Get(&greeting)
	if err != nil {
		return "", err
	}
	return greeting, nil
}

func GreetActivity(_ activity.Context, name string) (string, error) {
	return "Hello, " + name + "!", nil
}
```

### Running a Worker

```go
c, err := client.Dial("localhost:7233", client.Options{})
if err != nil {
	log.Fatal(err)
}
defer c.Close()

w := worker.New(c, helloworkflow.TaskQueue, worker.Options{})
w.RegisterWorkflow(helloworkflow.SimpleWorkflow)
w.RegisterActivity(helloworkflow.GreetActivity)

w.Run(ctx) // blocks, polling for work until ctx is cancelled
```

### Starting a Workflow from Code

```go
run, err := c.StartWorkflow(ctx, client.StartWorkflowOptions{
	ID:        "helloworkflow-" + time.Now().Format("20060102-150405"),
	TaskQueue: helloworkflow.TaskQueue,
}, helloworkflow.SimpleWorkflow, "flowd")
if err != nil {
	log.Fatal(err)
}

var result string
if err := run.Get(ctx, &result); err != nil {
	log.Fatal(err)
}
fmt.Println(result) // "Hello, flowd!"
```

### The `flow-cli` Operator CLI

See [Installing `flow-cli`](#installing-flow-cli) for setup. Once installed:

```bash
# Start a workflow by registered type name
flow-cli start -id order-123 -type OrderWorkflow -task-queue orders -input '{"orderId":"123"}'

# Send a signal
flow-cli signal -id order-123 -name PaymentReceived -input '{"amount":42}'

# Query current in-memory state
flow-cli query -id order-123 -type GetStatus

# Request cancellation
flow-cli cancel -id order-123 -reason "customer requested"

# List and inspect
flow-cli list -status running -task-queue orders
flow-cli describe -id order-123
flow-cli history -id order-123 -run-id <run-id>

# Namespaces
flow-cli namespace-create -name billing
flow-cli namespace-list
```

Set `FLOWD_ADDR`, `FLOWD_API_KEY`, and the `FLOWD_TLS_*` variables as needed
to target a non-default or secured server.

### Web Dashboard

With the server running, open `http://localhost:7234` for a read-only view
of running and completed workflow executions, filterable by status and task
queue, with per-run history detail — backed by the same gRPC API every other
client uses.

## Core Concepts

| Concept | Summary |
|---------|---------|
| **Workflow** | A deterministic Go function orchestrating one execution; its entire state is derived by replaying its event history. |
| **Activity** | A plain Go function performing actual side effects (I/O, external calls); retried automatically on failure per `ActivityOptions`. |
| **Task Queue** | The named queue a worker polls; connects workflow/activity tasks to the workers able to execute them. |
| **Signal** | Asynchronous, fire-and-forget data delivered into a running workflow (`SignalWorkflow` → `workflow.SetSignalHandler`). |
| **Query** | Synchronous read of a running workflow's in-memory state, answered without blocking (`QueryWorkflow` → `workflow.SetQueryHandler`). |
| **Cancellation** | Cooperative: `RequestCancelWorkflowExecution` sets a flag a workflow can observe via `workflow.IsCancelRequested` and act on. |
| **Continue-As-New** | A workflow closes its current run and atomically starts a fresh one under the same workflow ID, resetting history length. |
| **Namespace** | Isolation boundary for workflows, task queues, and history within one deployment. |

Determinism is the one constraint workflow authors must hold: no direct
I/O, `time.Now()`, raw goroutines, or unmediated randomness inside a
workflow function — everything goes through `workflow.Now`, `workflow.Sleep`,
`workflow.ExecuteActivity`, and `workflow.Go` instead. See ADR-0001 for why.

## Testing

```bash
make test-unit         # unit tests, no external dependencies
make test-integration   # requires Postgres — see make compose-up
make verify              # postgres up + migrate + full unit + integration suite
```

`test/replay` golden-replays recorded workflow histories to catch
non-determinism regressions; `test/integration/crash_recovery_test.go` kills
a worker mid-execution and asserts the workflow completes exactly once on
restart.

## Project Status

flowd is under active development. The current milestone ("Phase 1") targets
a single-node Postgres deployment with the core durable execution model,
gRPC API, Go SDK, operator CLI, and a read-only web dashboard — all present
in this repository. Multi-node history sharding, per-attempt history
visibility, and additional language SDKs are tracked as future work; see the
`docs/adr/` directory for the reasoning behind decisions made so far.

## Contributing

Issues and pull requests are welcome. Before submitting a change:

```bash
make lint
make verify
```

`buf breaking` runs as part of `make lint` to catch incompatible changes to
the protobuf API defined in `api/proto`.

## License

[MIT](LICENSE)
