# ForgeFlow

ForgeFlow is an incremental, primarily Go implementation of a distributed workflow execution platform. A workflow is described as a directed acyclic graph (DAG) of tasks; the eventual system will find ready tasks, dispatch independent work concurrently, persist state, retry transient failures, prevent duplicate execution, and recover work abandoned by failed workers.

## Why ForgeFlow

Reliable workflow execution is more than running functions in order. A useful engine must preserve dependency constraints while coordinating concurrency and surviving process, worker, network, and storage failures. ForgeFlow provides a focused project in which those problems can be introduced and solved one at a time, with tests and observable behavior at every stage.

## Current status

**Stage 4: fault-aware local workflow execution.** ForgeFlow now provides validated workflow definitions, explicit workflow/task run state machines, a scheduler, a safe task-handler registry, configurable concurrent workers, durable local persistence, retry policy with exponential backoff, identified task attempts, worker heartbeats, and expiring task leases.

Independent tasks run concurrently, successful dependencies unlock downstream work, and terminal task failure or context cancellation stops unfinished work consistently. Marked transient failures retry within policy using testable exponential backoff. Every dispatch has a stable task-run ID and attempt ID; only the worker holding that attempt's valid lease can commit its result. Heartbeats renew active leases, while expired leases make abandoned work retryable without rerunning already completed tasks. Remote workers, APIs, brokers, PostgreSQL, authentication, and deployment infrastructure have not been implemented. See [the roadmap](docs/roadmap.md) for the planned progression and [the architecture notes](docs/architecture.md) for the reliability and delivery guarantees.

## Local development

Local development is intentionally lightweight because the primary development machine has limited memory. ForgeFlow uses the native Go toolchain and the standard library at this stage. It does not require Docker Desktop, WSL, Kubernetes, Kafka, a database service, or any background process.

The embedded `FileStore` uses a configurable path and an append-only, checksummed JSON journal. Each mutation writes one complete aggregate snapshot, flushes it to disk, and only then exposes it in memory. On startup, complete records are replayed and an incomplete final record is truncated. Tests use isolated temporary directories, and `*.ffdb` files are ignored by Git.

This backend was chosen instead of SQLite because the current access pattern needs only a single-process durable aggregate log. It adds no CGO toolchain, native DLL, pure-Go SQLite dependency, daemon, or container overhead. The scheduler depends only on the `execution.Store` interface, so a transactional PostgreSQL adapter can replace the journal when later stages require multi-process coordination and richer queries.

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

## Repository layout

```text
cmd/forgeflow/      executable entry point
internal/app/       bootstrap application behavior
internal/execution/ run state, scheduler, handlers, and workers
internal/persistence/ embedded durable Store implementation
internal/workflow/  workflow definitions and DAG semantics
docs/               architecture and delivery roadmap
```

The module uses the canonical import path `github.com/vijaypratap3364/forgeflow`, derived from the configured `origin` remote.
