# ForgeFlow

ForgeFlow is an incremental, primarily Go implementation of a distributed workflow execution platform. A workflow is described as a directed acyclic graph (DAG) of tasks; the eventual system will find ready tasks, dispatch independent work concurrently, persist state, retry transient failures, prevent duplicate execution, and recover work abandoned by failed workers.

## Why ForgeFlow

Reliable workflow execution is more than running functions in order. A useful engine must preserve dependency constraints while coordinating concurrency and surviving process, worker, network, and storage failures. ForgeFlow provides a focused project in which those problems can be introduced and solved one at a time, with tests and observable behavior at every stage.

## Current status

**Stage 0: repository bootstrap.** The repository currently contains a minimal executable and deterministic test that prove the Go project builds and runs. Workflow models, scheduling, workers, persistence, retries, APIs, brokers, authentication, and deployment infrastructure have not been implemented yet.

The next milestone is an in-memory workflow domain model with DAG validation and deterministic ready-task discovery. See [the roadmap](docs/roadmap.md) for the planned progression and [the architecture notes](docs/architecture.md) for the current/future boundary.

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
docs/            architecture and delivery roadmap
```

The module currently uses the local name `forgeflow` because no Git remote is configured. A canonical import path should be chosen only when a real remote exists; no account or repository URL is assumed.
