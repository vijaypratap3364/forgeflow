# ForgeFlow

ForgeFlow is an incremental, primarily Go implementation of a distributed workflow execution platform. A workflow is described as a directed acyclic graph (DAG) of tasks; the eventual system will find ready tasks, dispatch independent work concurrently, persist state, retry transient failures, prevent duplicate execution, and recover work abandoned by failed workers.

## Why ForgeFlow

Reliable workflow execution is more than running functions in order. A useful engine must preserve dependency constraints while coordinating concurrency and surviving process, worker, network, and storage failures. ForgeFlow provides a focused project in which those problems can be introduced and solved one at a time, with tests and observable behavior at every stage.

## Current status

**Stage 6: optional production PostgreSQL persistence.** ForgeFlow now provides validated workflow definitions, explicit workflow/task run state machines, a scheduler, a safe task-handler registry, configurable concurrent workers, retry policy with exponential backoff, identified task attempts, worker heartbeats, expiring task leases, a versioned REST API with live Server-Sent Events (SSE), and interchangeable embedded-file and PostgreSQL stores.

Independent tasks run concurrently, successful dependencies unlock downstream work, and terminal task failure or context cancellation stops unfinished work consistently. Marked transient failures retry within policy using testable exponential backoff. Every dispatch has a stable task-run ID and attempt ID; only the worker holding that attempt's valid lease can commit its result. Heartbeats renew active leases, while expired leases make abandoned work retryable without rerunning already completed tasks. The standard-library HTTP server accepts definitions, starts and cancels asynchronous runs, exposes durable run/task status, and streams persisted transitions. The PostgreSQL adapter makes aggregate changes transactional and rejects stale concurrent writers. Remote workers, brokers, authentication, and deployment infrastructure have not been implemented. See [the roadmap](docs/roadmap.md) for the planned progression and [the architecture notes](docs/architecture.md) for the reliability and delivery guarantees.

## Local development

Local development is intentionally lightweight because the primary development machine has limited memory. ForgeFlow uses the native Go toolchain and keeps the embedded file backend as its default. It does not require Docker Desktop, WSL, Kubernetes, Kafka, a database service, or any background process. The pure-Go `pgx` dependency compiles the optional PostgreSQL adapter without installing PostgreSQL locally.

The embedded `FileStore` uses a configurable path and an append-only, checksummed JSON journal. Each mutation writes one complete aggregate snapshot, flushes it to disk, and only then exposes it in memory. On startup, complete records are replayed and an incomplete final record is truncated. Tests use isolated temporary directories, and `*.ffdb` files are ignored by Git.

This backend was chosen instead of SQLite because the local access pattern needs only a single-process durable aggregate log. It adds no CGO toolchain, native DLL, SQLite driver, daemon, or container overhead. The scheduler depends only on the `execution.Store` interface. Set `FORGEFLOW_STORE=postgres` and supply `FORGEFLOW_POSTGRES_DSN` to select the production adapter; startup verifies the pool and applies the embedded migrations. PostgreSQL integration tests run against an ephemeral service in GitHub Actions and are excluded from ordinary local tests.

## Current reliability guarantee

ForgeFlow does **not** claim universal exactly-once task execution. A completion carrying the same attempt ID is applied to workflow state at most once, and a valid lease prevents simultaneous assignment of that task attempt by the current single-process scheduler. If a worker disappears after performing a side effect but before recording completion, lease expiry can dispatch a new attempt. Handler execution is therefore at-least-once across crash recovery and retries. Handlers that affect external systems should use the stable task-run or attempt ID as an application-level idempotency key. See [the architecture notes](docs/architecture.md#delivery-and-execution-semantics) for the precise boundaries.

Prerequisite: Go 1.26 or newer within the Go 1 compatibility line.

```text
go run ./cmd/forgeflow
go fmt ./...
go vet ./...
go test ./...
go build ./...
```

On systems with `make`, `make check` runs formatting, vetting, tests, and a build.

The server listens on `127.0.0.1:8080` and writes its embedded journal to `data/forgeflow.ffdb` by default. Configuration is read from the process environment:

| Variable | Default | Purpose |
| --- | --- | --- |
| `FORGEFLOW_ADDR` | `127.0.0.1:8080` | HTTP listen address and port |
| `FORGEFLOW_STORE` | `file` | Persistence backend: `file` or `postgres` |
| `FORGEFLOW_DATA_PATH` | `data/forgeflow.ffdb` | Append-only journal path |
| `FORGEFLOW_POSTGRES_DSN` | empty | PostgreSQL connection string; required only for the `postgres` backend |
| `FORGEFLOW_WORKERS` | `4` | Worker goroutines per active run |
| `FORGEFLOW_SHUTDOWN_TIMEOUT` | `10s` | Graceful-shutdown deadline |

The checked-in [.env.example](.env.example) is a reference; ForgeFlow does not load dotenv files itself.

## API quickstart

With `go run ./cmd/forgeflow` running, submit a safe demo workflow:

```text
curl -i -H "Content-Type: application/json" --data-binary @examples/workflow.json http://127.0.0.1:8080/api/v1/workflows
```

Create an asynchronous execution with a caller-selected stable ID:

```text
curl -i -H "Content-Type: application/json" -d '{"run_id":"demo-run"}' http://127.0.0.1:8080/api/v1/workflows/forge-demo/runs
```

Stream its persisted transitions, then inspect the final aggregate and task output:

```text
curl -N http://127.0.0.1:8080/api/v1/runs/demo-run/events
curl http://127.0.0.1:8080/api/v1/runs/demo-run
curl http://127.0.0.1:8080/api/v1/runs/demo-run/tasks
```

The event stream replays transitions retained by the current process and closes when the workflow succeeds, fails, or is canceled. See [the complete curl walkthrough](examples/README.md) for cancellation and Windows PowerShell notes.

### HTTP endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/api/v1/workflows` | Validate and persist a workflow definition |
| `GET` | `/api/v1/workflows/{id}` | Read a workflow definition |
| `POST` | `/api/v1/workflows/{id}/runs` | Persist and asynchronously start a run |
| `GET` | `/api/v1/runs/{runID}` | Read aggregate run status and task counts |
| `GET` | `/api/v1/runs/{runID}/tasks` | Read task-level status, attempts, outputs, and errors |
| `POST` | `/api/v1/runs/{runID}/cancel` | Request cancellation of a nonterminal run |
| `GET` | `/api/v1/runs/{runID}/events` | Stream persisted state transitions with SSE |
| `GET` | `/healthz` | Process liveness |
| `GET` | `/readyz` | Request-serving readiness |

## Repository layout

```text
cmd/forgeflow/      executable entry point
internal/app/       bootstrap application behavior
internal/api/       versioned HTTP API, async run lifecycle, and SSE events
internal/execution/ run state, scheduler, handlers, and workers
internal/persistence/ embedded and PostgreSQL Store implementations and migrations
internal/workflow/  workflow definitions and DAG semantics
docs/               architecture and delivery roadmap
```

The module uses the canonical import path `github.com/vijaypratap3364/forgeflow`, derived from the configured `origin` remote.
