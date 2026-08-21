# ForgeFlow

ForgeFlow is an incremental, primarily Go implementation of a distributed workflow execution platform. A workflow is described as a directed acyclic graph (DAG) of tasks; the eventual system will find ready tasks, dispatch independent work concurrently, persist state, retry transient failures, prevent duplicate execution, and recover work abandoned by failed workers.

## Why ForgeFlow

Reliable workflow execution is more than running functions in order. A useful engine must preserve dependency constraints while coordinating concurrency and surviving process, worker, network, and storage failures. ForgeFlow provides a focused project in which those problems can be introduced and solved one at a time, with tests and observable behavior at every stage.

## Current status

**Stage 3: durable local workflow execution.** ForgeFlow now provides validated workflow definitions, explicit workflow/task run state machines, a scheduler, a safe task-handler registry, configurable concurrent workers, and a persistence boundary backed by a lightweight local journal.

Independent tasks run concurrently, successful dependencies unlock downstream work, and task failure or context cancellation stops unfinished work consistently. Workflow definitions, run/task state, timestamps, outputs, errors, and attempt counts survive store reconstruction. A nonterminal run can be recovered without repeating completed tasks. Retries, leases, idempotency, remote workers, APIs, brokers, authentication, and deployment infrastructure have not been implemented. See [the roadmap](docs/roadmap.md) for the planned progression and [the architecture notes](docs/architecture.md) for the current/future boundary.

## Local development

Local development is intentionally lightweight because the primary development machine has limited memory. ForgeFlow uses the native Go toolchain and the standard library at this stage. It does not require Docker Desktop, WSL, Kubernetes, Kafka, a database service, or any background process.

The embedded `FileStore` uses a configurable path and an append-only, checksummed JSON journal. Each mutation writes one complete aggregate snapshot, flushes it to disk, and only then exposes it in memory. On startup, complete records are replayed and an incomplete final record is truncated. Tests use isolated temporary directories, and `*.ffdb` files are ignored by Git.

This backend was chosen instead of SQLite because the current access pattern needs only a single-process durable aggregate log. It adds no CGO toolchain, native DLL, pure-Go SQLite dependency, daemon, or container overhead. The scheduler depends only on the `execution.Store` interface, so a transactional PostgreSQL adapter can replace the journal when later stages require multi-process coordination and richer queries.

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
