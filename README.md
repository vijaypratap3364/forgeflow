# ForgeFlow

ForgeFlow is an incremental, primarily Go implementation of a distributed workflow execution platform. A workflow is described as a directed acyclic graph (DAG) of tasks; the eventual system will find ready tasks, dispatch independent work concurrently, persist state, retry transient failures, prevent duplicate execution, and recover work abandoned by failed workers.

## Why ForgeFlow

Reliable workflow execution is more than running functions in order. A useful engine must preserve dependency constraints while coordinating concurrency and surviving process, worker, network, and storage failures. ForgeFlow provides a focused project in which those problems can be introduced and solved one at a time, with tests and observable behavior at every stage.

## Current status

**Stage 1: workflow model and DAG validation.** ForgeFlow now provides in-memory workflow and task definitions, typed validation errors, cycle detection, deterministic topological ordering, root-task discovery, and ready-task discovery from a set of completed tasks.

Scheduling, task execution, workers, persistence, retries, APIs, brokers, authentication, and deployment infrastructure have not been implemented. The next milestone is bounded local execution through a registry of safe task handlers. See [the roadmap](docs/roadmap.md) for the planned progression and [the architecture notes](docs/architecture.md) for the current/future boundary.

## Local development

Local development is intentionally lightweight because the primary development machine has limited memory. ForgeFlow uses the native Go toolchain and the standard library at this stage. It does not require Docker Desktop, WSL, Kubernetes, Kafka, a database, or any background service.

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
cmd/forgeflow/   executable entry point
internal/app/    bootstrap application behavior
internal/workflow/ workflow definitions and DAG semantics
docs/            architecture and delivery roadmap
```

The module uses the canonical import path `github.com/vijaypratap3364/forgeflow`, derived from the configured `origin` remote.
