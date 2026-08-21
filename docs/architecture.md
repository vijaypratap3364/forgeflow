# ForgeFlow Architecture

## Architectural intent

ForgeFlow will execute task graphs reliably across multiple workers. The eventual design separates workflow semantics from infrastructure so that scheduling rules remain testable without a broker, database, or network services.

This document separates the components that exist today from the later distributed target.

## Current implementation

Stage 5 contains:

- a Go module with no external dependencies;
- stable workflow and task identifier types plus in-memory definitions;
- typed, machine-classifiable validation errors with contextual messages;
- validation for identifiers, names, task uniqueness, dependency references, repeated edges, self-dependencies, and cycles;
- deterministic topological ordering, root-task discovery, and ready-task discovery;
- separate workflow-definition, workflow-run, and task-run models with enforced state transitions;
- a synchronous scheduler that unlocks successful dependency chains;
- per-run in-memory queues and configurable worker pools using goroutines and channels;
- a concurrency-safe registry of context-aware task handlers;
- safe built-in no-op, delay, and uppercase handlers;
- a narrow persistence interface owned by the execution layer;
- durable workflow definitions and workflow/task run snapshots with timestamps and attempt counts;
- a standard-library append-only file journal with checksums and disk flushes;
- restart recovery that preserves succeeded tasks and reschedules unfinished work;
- per-task retry policies with explicit transient-error classification and capped exponential backoff;
- stable task-run and attempt identities with duplicate/stale completion rejection;
- persisted task leases assigned to identified workers;
- persisted worker heartbeats that renew live leases;
- lease-expiry recovery and supervised replacement of disappeared local workers;
- a versioned JSON REST API built on Go's standard `net/http` stack;
- asynchronous API-managed run execution and cancellation;
- durable aggregate and task-level status reads;
- replayable live Server-Sent Events derived from persisted transitions;
- liveness, readiness, configurable process settings, and graceful shutdown;
- handler tests built with `httptest` plus deterministic domain and engine tests.

There is currently no remote worker, messaging system, authentication, cross-process recovery controller, PostgreSQL adapter, metrics/tracing stack, or browser UI. The executable runs the local HTTP service and its embedded store without a second process.

## Eventual component boundaries

| Component | Responsibility | Status |
| --- | --- | --- |
| Workflow domain | Define workflows, tasks, dependencies, and DAG invariants | Current |
| Scheduler | Identify, lease, retry, and dispatch ready tasks without violating dependencies | Current, in process |
| Dispatcher | Offer runnable tasks to workers through a queue | Current, in-memory only |
| Worker runtime | Execute registered handlers, heartbeat, and report identified attempt outcomes | Current, in process |
| State store | Persist definitions, attempts, leases, heartbeats, and aggregate run state | Current, embedded single-process journal |
| Messaging boundary | Decouple dispatch and execution through a broker adapter | Future |
| Recovery controller | Detect expired leases and make abandoned work eligible again | Current in scheduler; distributed controller future |
| API | Accept workflows; create, inspect, stream, and cancel runs | Current, HTTP/JSON and SSE |
| Observability | Provide structured logs, metrics, traces, and operational diagnostics | Future |

Messaging will receive a narrow interface when that boundary becomes real. PostgreSQL and a production message broker should arrive only after their multi-process semantics are understood and covered by contract tests.

## Current graph algorithms

Definition validation builds an adjacency list from dependency to dependent and an indegree count for every task. It rejects malformed definitions before graph traversal, allowing callers to distinguish a bad reference from a valid graph that contains a cycle.

Topological ordering uses Kahn's algorithm. A min-heap of zero-indegree task IDs makes the result lexicographically deterministic even when task definitions or Go map iteration order differ. If the algorithm cannot consume every task, the remaining dependency structure contains a cycle. Root and ready-task results use the same task-ID ordering rule.

## Current execution architecture

```text
HTTP client ---- JSON/SSE ---- API + run manager
                                  |
                    persist pending run, then recover
                                  |
                                  v
                             Scheduler loop <------- snapshot
                              |     ^     |              ^
                       lease  |     |     | persist      |
                       + job  |     |     v              |
                              | heartbeats/results  execution.Store
                              v     |                    |
                         identified workers        FileStore journal
```

`Engine.Execute` creates an isolated `WorkflowRun`, resolves every handler before accepting the run, saves its immutable definition and initial snapshot, derives a run context from the caller, and starts the configured number of workers. Each `Execute` call owns its queue and worker goroutines, so the same engine can execute multiple workflow runs concurrently without sharing mutable run state.

The scheduler is the only component that decides task transitions and dispatch. It maintains the set of succeeded task IDs and asks the workflow domain for newly ready tasks after each success. Only pending tasks are promoted to ready. An available identified worker receives a task only after the aggregate containing its attempt ID and lease has been flushed. Worker results carry the worker, task-run, and attempt identities. A matching completion moves the task to succeeded, retry-wait, or failed and may unlock downstream dependencies; the resulting aggregate snapshot is flushed before further dispatch.

Task transitions are `pending -> ready -> running -> succeeded|retry_wait|failed`, with `retry_wait -> ready` when backoff elapses. Cancellation may move any nonterminal task to `canceled`. Workflow transitions are `pending -> running -> succeeded|failed|canceled`, with pending-to-canceled allowed when the caller's context is already done.

Unmarked handler errors are terminal. A marked `RetryableTaskError` schedules another attempt only while `AttemptCount < MaxAttempts`; a zero maximum preserves the default of one total attempt. Delay starts at `InitialBackoff`, doubles after each completed attempt, and is capped by `MaxBackoff` when configured. Exhaustion or a terminal error invokes the fail-fast policy: unfinished tasks are canceled and the workflow fails. Handlers receive `context.Context` and are required to return promptly when it is canceled; Go cannot forcibly stop a handler that ignores its context.

The engine clock and timers are interfaces. Production uses the standard library clock, while tests advance controlled time to prove backoff, heartbeat, and expiry behavior without real sleeps.

## HTTP API and process lifecycle

The `internal/api` package keeps JSON request and response types separate from workflow and execution aggregates. Requests use strict JSON decoding, reject unknown fields and bodies larger than 1 MiB, and translate typed domain errors into stable error envelopes with useful HTTP status codes. Go duration strings such as `100ms` and `2s` are parsed only at the transport boundary.

`POST /api/v1/workflows/{id}/runs` validates handler availability and durably creates a pending run before returning `202 Accepted`. A server-owned run manager then calls `Engine.Recover` in a goroutine. The accepted run is deliberately independent of the request context after persistence, so closing the creation request does not cancel submitted work. The explicit cancellation endpoint cancels that server-owned run context; handlers receive the cancellation through the existing engine contract.

Run and task GET endpoints read the latest snapshot from `execution.Store`, rather than relying on an API-only cache. The event stream is also downstream of persistence: a small Store decorator observes only successful `CreateRun` and `SaveRun` calls, compares consecutive snapshots, and publishes typed SSE transitions. Its append-only in-memory event history supports `Last-Event-ID` replay for the lifetime of the current process without turning the local journal into an event-sourcing API. After a restart, durable status remains authoritative, but the previous process's SSE history is not reconstructed.

`/healthz` reports process liveness. `/readyz` reports whether the server is accepting work and changes to unavailable during shutdown; it is not yet a remote dependency probe because the embedded Store has no external service. On SIGINT or SIGTERM the process stops accepting HTTP connections, cancels active runs, waits for engine goroutines within a configurable deadline, closes SSE streams, and completes `http.Server.Shutdown`.

## Persistence boundary and local backend

The execution package owns the `Store` interface because the scheduler is its consumer. The boundary provides idempotent immutable-definition storage, create-versus-update run operations, and lookup of definitions and run snapshots. The scheduler has no dependency on the file-store package or its encoding.

`FileStore` is a single-process embedded implementation. A mutation is one checksummed journal envelope containing the complete changed aggregate. The file is opened in append mode, written fully, and synced before the in-memory index changes. Reopening replays records in order. A partial tail without the terminating newline is treated as an uncommitted write and truncated; malformed or checksum-invalid committed records stop startup with an error. This provides atomic aggregate changes for the current single-writer process without a database service. It deliberately has no compaction, concurrent-process writer coordination, or query indexes yet.

The backend uses only Go's standard library. SQLite was not selected because this stage does not need relational queries or concurrent transactions, and avoiding both CGO/native DLL requirements and a new pure-Go driver keeps the Windows development setup small. PostgreSQL can later implement the same boundary when distributed ownership requires server-side transactions.

## Leases, heartbeats, and restart behavior

Each dispatch persists a `TaskLease` containing the worker ID, stable task-run ID, stable attempt ID, and expiration. A running task cannot receive a second lease. Workers persist their latest heartbeat and renew only leases they currently own and which have not expired. A late heartbeat cannot revive an expired lease.

If a local worker disappears, its heartbeat stops and its slot is supervised with a new worker identity. The abandoned task remains protected from reassignment until its lease expires. Expiry records the attempt as abandoned, preserves its consumed attempt count, and either moves it to retry-wait/ready or fails it when the maximum is exhausted. The count increments only when the replacement attempt is actually leased. A late result from the old worker has a stale attempt or lease identity and is ignored.

`Engine.Recover` loads the run and immutable definition and validates their relationship. Succeeded tasks and outputs remain succeeded. Pending, ready, and retry-waiting tasks preserve their eligibility. A still-valid persisted lease is not stolen on restart; it is recovered only after expiry. Stage 3 snapshots without lease metadata are treated as already abandoned and recovered according to policy.

## Delivery and execution semantics

- **At-most-once** means an operation is never repeated, but loss may prevent it from happening. ForgeFlow provides at-most-once *state application per attempt ID*: after a matching attempt completion changes task state, duplicate or stale completions are no-ops. A valid lease also permits only one current assignment in the single-process scheduler.
- **At-least-once** means work may repeat so it is not silently abandoned. ForgeFlow task-handler execution is at-least-once across configured retries and expired-lease recovery. A worker might perform an external side effect and disappear before persisting completion; the replacement attempt can perform that effect again.
- **Exactly-once** would require the task's external side effect and ForgeFlow's completion record to commit atomically, or require the external system to deduplicate a stable idempotency key. ForgeFlow cannot guarantee that universally and does not claim exactly-once handler execution.

Stable task-run and attempt IDs let handlers implement application-level deduplication. Within ForgeFlow, identified completions are idempotent and completed logical tasks are not rescheduled. This mitigates duplicates but does not erase the crash window around external side effects.

The current guarantee is scoped to one engine process and its atomic aggregate `Store` writes. `FileStore` is not a distributed compare-and-swap database. PostgreSQL will later implement the same persistence boundary with transactional lease acquisition for multiple scheduler and worker processes.

## Target execution flow

For distributed execution, the existing identified-attempt and lease model will move behind transactional storage operations. Remote workers will claim attempts, heartbeat independently, invoke registered handlers, and record outcomes through a broker and PostgreSQL-backed state authority. Lease acquisition and completion will need compare-and-swap semantics across processes.

The state store will be the authority for state transitions. Broker delivery alone will not prove ownership or completion. Idempotency keys and attempt identities will make redelivery safe, while atomic state transitions will prevent two workers from committing the same logical result.

## Development constraints

Local development must remain viable on a machine with about 5.8 GB of RAM. The design therefore favors native Go processes, deterministic in-memory tests, and focused test doubles. Heavy broker, database, load, and orchestration tests should eventually run in CI or remote environments rather than a local multi-service stack.

Security, tenancy, authentication, and authorization are important future concerns, but they should be added when the API and deployment model make their requirements concrete.
