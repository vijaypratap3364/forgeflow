# ForgeFlow Architecture

## Architectural intent

ForgeFlow will execute task graphs reliably across multiple workers. The eventual design separates workflow semantics from infrastructure so that scheduling rules remain testable without a broker, database, or network services.

This document separates the components that exist today from the remaining distributed-runtime work.

## Current implementation

Stage 10 contains:

- a focused dependency surface: the Go standard library plus `golang-jwt`, native `pgx`, `nats.go`, and the official OpenTelemetry SDK/exporters for security, optional PostgreSQL/NATS access, and configurable tracing;
- stable workflow and task identifier types plus in-memory definitions;
- typed, machine-classifiable validation errors with contextual messages;
- validation for identifiers, names, task uniqueness, dependency references, repeated edges, self-dependencies, and cycles;
- deterministic topological ordering, root-task discovery, and ready-task discovery;
- separate workflow-definition, workflow-run, and task-run models with enforced state transitions;
- a synchronous scheduler that unlocks successful dependency chains;
- a task `Broker` boundary used by both scheduler and workers;
- a concurrency-safe `InMemoryBroker` for normal tests and lightweight local use;
- a NATS JetStream adapter using durable pull consumers, explicit acknowledgements, file-backed work-queue retention, and stable message IDs;
- per-run configurable worker pools using goroutines, broker deliveries, and channels for local scheduler coordination;
- a concurrency-safe registry of context-aware task handlers;
- safe built-in no-op, delay, and uppercase handlers;
- a narrow persistence interface owned by the execution layer;
- durable workflow definitions and workflow/task run snapshots with timestamps and attempt counts;
- a standard-library append-only file journal with checksums and disk flushes;
- a native `pgx` PostgreSQL adapter with embedded, checksummed migrations;
- relational constraints for definitions, runs, task state, worker heartbeats, and leases;
- optimistic run versions that reject stale cross-process aggregate writes;
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
- handler tests built with `httptest` plus deterministic domain, engine, and broker contract tests;
- deterministic contention and fault-injection tests for wide ready queues, concurrent workflows, duplicate delivery, lease recovery, cancellation, and worker-loop cleanup;
- race-enabled unit and PostgreSQL/JetStream integration suites in GitHub Actions;
- CI enforcement of formatting, module consistency, vetting, tests, builds, Staticcheck, and reachable-vulnerability scanning;
- opt-in JetStream integration tests run against a pinned standalone NATS server in GitHub Actions;
- externally issued Ed25519 JWT verification with required algorithm, signature, issuer, audience, subject, and expiration validation;
- durable users, projects, workflow ownership, project memberships, and administrative audit events;
- project-scoped member/operator/admin permissions enforced from persisted resource ownership;
- a configurable, process-local authenticated-subject rate limiter;
- structured JSON request and execution-transition logs with stable correlation identifiers;
- a Prometheus-compatible process metrics endpoint with counters, gauges, and histograms;
- explicitly injected OpenTelemetry tracing across HTTP, scheduler, broker, worker, and execution persistence operations;
- W3C trace-context propagation in durable task dispatch messages;
- dependency-aware readiness checks for PostgreSQL and the active broker.

There is currently no separately deployable worker executable, automatic cross-process recovery controller, token issuer/login service, hosted metrics/tracing backend, or browser UI. The executable runs the local HTTP service with the embedded store, in-memory broker, in-process metrics registry, and no-op trace exporter by default. It can independently select PostgreSQL, NATS, stdout tracing, and OTLP/HTTP tracing when explicitly configured. The network broker boundary is current, while extracting the worker runtime from the API/scheduler process is later work.

## Eventual component boundaries

| Component | Responsibility | Status |
| --- | --- | --- |
| Workflow domain | Define workflows, tasks, dependencies, and DAG invariants | Current |
| Scheduler | Identify, lease, retry, and dispatch ready tasks without violating dependencies | Current; one scheduler loop per active run |
| Dispatcher | Offer runnable attempts through implementation-neutral durable delivery | Current, Broker boundary |
| Worker runtime | Receive broker deliveries, execute registered handlers, heartbeat, and report identified outcomes | Current in the scheduler process; standalone process future |
| State store | Persist definitions, attempts, leases, heartbeats, and aggregate run state | Current, embedded journal or PostgreSQL |
| Messaging boundary | Decouple task dispatch through competing consumers | Current, memory or NATS JetStream |
| Recovery controller | Detect expired leases and make abandoned work eligible again | Current in scheduler; distributed controller future |
| Security boundary | Authenticate JWT subjects and authorize project-owned resources | Current; external token issuer required |
| API | Manage projects/workflows; create, inspect, stream, retry, and cancel runs | Current, HTTP/JSON and SSE |
| Observability | Provide structured logs, metrics, traces, health checks, and correlation diagnostics | Current; external telemetry backends optional |

The persistence and messaging choices are independent boundaries. Lightweight local operation uses `FileStore` plus `InMemoryBroker`; a distributed deployment should use PostgreSQL plus NATS JetStream. `FileStore` remains a single-process component and must not be shared by multiple ForgeFlow processes merely because NATS is network-accessible.

## Current graph algorithms

Definition validation builds an adjacency list from dependency to dependent and an indegree count for every task. It rejects malformed definitions before graph traversal, allowing callers to distinguish a bad reference from a valid graph that contains a cycle.

Topological ordering uses Kahn's algorithm. A min-heap of zero-indegree task IDs makes the result lexicographically deterministic even when task definitions or Go map iteration order differ. If the algorithm cannot consume every task, the remaining dependency structure contains a cycle. Root and ready-task results use the same task-ID ordering rule.

## Current execution architecture

```text
Authenticated HTTP client -- JSON/SSE --> JWT middleware + project RBAC
                                              |
                                              v
                                        API + run manager
                                  |
                    persist pending run, then recover
                                  |
                                  v
                         Scheduler loop <---------- snapshot
                           |       ^                    ^
                    publish|       |claim/result       | persist
                           v       |                    v
                       Broker boundary             execution.Store
                       /             \             /             \
              InMemoryBroker     NATS JetStream  FileStore     PostgreSQL
                       \             /
                        identified workers
                        receive/ack/progress
```

`Engine.Execute` creates an isolated `WorkflowRun`, resolves every handler before accepting the run, saves its immutable definition and initial snapshot, derives a run context from the caller, and starts the configured number of workers. Each `Execute` call owns its scheduler loop and worker goroutines. Task transport is shared only through the configured concurrency-safe `Broker`, so the same engine can execute multiple workflow runs concurrently without sharing mutable run aggregates.

The scheduler is the only component that decides task transitions and dispatch. It maintains the set of succeeded task IDs and asks the workflow domain for newly ready tasks after each success. Only pending tasks are promoted to ready, and that ready transition is persisted before a message is published. Each message ID is the stable ID of the next task attempt. Receiving a message is not authority to execute: the scheduler validates its run, task-run, and attempt identities and flushes the aggregate containing the worker's lease before authorizing the handler. Worker results carry the worker, task-run, and attempt identities. A matching completion moves the task to succeeded, retry-wait, or failed and may unlock downstream dependencies; the resulting aggregate snapshot is flushed before the delivery is acknowledged.

Task transitions are `pending -> ready -> running -> succeeded|retry_wait|failed`, with `retry_wait -> ready` when backoff elapses. Cancellation may move any nonterminal task to `canceled`. Workflow transitions are `pending -> running -> succeeded|failed|canceled`, with pending-to-canceled allowed when the caller's context is already done.

Unmarked handler errors are terminal. A marked `RetryableTaskError` schedules another attempt only while `AttemptCount < MaxAttempts`; a zero maximum preserves the default of one total attempt. Delay starts at `InitialBackoff`, doubles after each completed attempt, and is capped by `MaxBackoff` when configured. Exhaustion or a terminal error invokes the fail-fast policy: unfinished tasks are canceled and the workflow fails. Handlers receive `context.Context` and are required to return promptly when it is canceled; Go cannot forcibly stop a handler that ignores its context.

The engine clock and timers are interfaces. Production uses the standard library clock, while tests advance controlled time to prove backoff, heartbeat, and expiry behavior without real sleeps.

## Task broker boundary

`internal/broker` defines five transport operations: publish a stable task message, receive one delivery for a durable competing-consumer subscription, acknowledge completed delivery, negatively acknowledge work that should be offered again, and report progress for long-running work. Message bodies carry run, task, task-run, and attempt identities; validated lowercase message headers carry propagation metadata. Headers are excluded from stable-ID logical-content checks and the first accepted publication wins, so a changed trace does not turn a safe republish into a message conflict. Broker subjects and durable names use stable SHA-256-derived tokens so user-controlled identifiers cannot inject NATS wildcards. The scheduler and workers know only this interface.

`InMemoryBroker` keeps a process-lifetime message log, makes identical publication of a stable message ID idempotent, and divides a subscription's deliveries among competing receivers. It is deterministic and concurrency-safe, so all ordinary engine tests exercise the real broker boundary without a service. It does not pretend to survive process exit or emulate a server-side acknowledgement timer. Worker cancellation and injected disappearance explicitly nack active in-memory deliveries.

`NATSBroker` creates or reconciles one file-backed JetStream stream using work-queue retention. Each workflow run maps to a non-overlapping subject and one durable pull consumer; all workers for that run compete on the same consumer. The adapter waits for a JetStream publish acknowledgement before reporting publication success, uses `Nats-Msg-Id` with the stable attempt ID for windowed publish deduplication, requires explicit consumer acknowledgements, nacks work for immediate redelivery, and maps persisted worker heartbeats to JetStream in-progress acknowledgements. Broker redelivery is unlimited by default because ForgeFlow's persisted retry policy, rather than a broker delivery counter, decides when logical task attempts are exhausted.

JetStream's duplicate window is an optimization, not the idempotency authority. A delayed repeat can arrive after that window, and an acknowledgement can be lost after the database commit. Every delivery is therefore checked against persisted task status, attempt ID, and lease state. A completed or stale attempt is acknowledged without invoking its handler. The NATS server retains unacknowledged messages and redelivers them when `AckWait` expires; the default one-minute acknowledgement deadline is longer than the engine's default heartbeat interval, and each durable lease renewal is followed by an in-progress acknowledgement that resets that deadline.

The current executable composes the scheduler and worker goroutines in one process even when JetStream is selected. The broker protocol is network-capable and the persistence ordering is suitable for cross-process transport, but shipping a standalone worker runtime requires a separate lifecycle and narrower remote claim/completion operations. Documentation does not label that remaining extraction as already implemented.

## HTTP API and process lifecycle

The `internal/api` package keeps JSON request and response types separate from workflow and execution aggregates. Requests use strict JSON decoding, reject unknown fields and bodies larger than 1 MiB, and translate typed domain errors into stable error envelopes with useful HTTP status codes. Go duration strings such as `100ms` and `2s` are parsed only at the transport boundary.

Every `/api/v1` request passes through bearer-token authentication and the authenticated-subject rate limiter; health, readiness, and metrics remain public for trusted operational infrastructure. The verifier accepts only Ed25519/EdDSA and requires the configured issuer, audience, expiration, and a valid subject. Role claims are intentionally ignored. After authentication, handlers derive the project from durable workflow ownership (and runs through their workflow) and load the caller's persisted membership. A missing membership returns a tenant-concealing `404`; a known member without the required permission receives `403`. This separates credential validity (`401`) from resource authorization (`403`/`404`).

An authenticated subject may create a project and becomes its initial admin in the same store transaction. Members can create workflows, start runs, and inspect aggregate run state. Operators additionally inspect task failures and cancel or retry runs. A retry creates a new run rather than rewriting the source run's history. Admins manage project metadata and memberships and read administrative audit events. Project creation, project updates, and membership updates commit their audit record atomically with the changed security state.

`POST /api/v1/workflows/{id}/runs` validates handler availability and durably creates a pending run before returning `202 Accepted`. A server-owned run manager then calls `Engine.Recover` in a goroutine. The accepted run is deliberately independent of the request context after persistence, so closing the creation request does not cancel submitted work. The explicit cancellation endpoint cancels that server-owned run context; handlers receive the cancellation through the existing engine contract.

Run and task GET endpoints read the latest snapshot from `execution.Store`, rather than relying on an API-only cache. The event stream is also downstream of persistence: a small Store decorator observes only successful `CreateRun` and `SaveRun` calls, compares consecutive snapshots, and publishes typed SSE transitions. Its append-only in-memory event history supports `Last-Event-ID` replay for the lifetime of the current process without turning the local journal into an event-sourcing API. After a restart, durable status remains authoritative, but the previous process's SSE history is not reconstructed.

`/healthz` reports only process liveness. `/readyz` reports whether the server is accepting work and actively checks dependencies: PostgreSQL receives a pool ping when selected, NATS receives a protocol flush when selected, and the in-memory broker verifies that it remains open. The embedded file store has no remote dependency to probe. Readiness changes to unavailable during shutdown. On SIGINT or SIGTERM the process stops accepting HTTP connections, cancels active runs, waits for engine goroutines within a configurable deadline, closes SSE streams, flushes the configured trace provider, and completes `http.Server.Shutdown`.

## Observability and correlation

The process injects one `observability.Instrumentation` bundle instead of mutating package-global logger, meter, or tracer state. Its `slog` JSON handler writes structured events, its concurrency-safe metrics registry renders the Prometheus 0.0.4 text format, and its explicitly owned OpenTelemetry provider is shut down with the process. The default trace provider is no-op, so local development starts no background telemetry process. `stdout` is available for local trace inspection; `otlp-http` uses the standard `OTEL_EXPORTER_OTLP_*` environment variables for a remote collector.

Every request receives a new server-owned request ID returned as `X-Request-ID`. Incoming W3C `traceparent`/`tracestate` and baggage are extracted before the HTTP server span starts. Accepted workflow execution detaches client cancellation but retains request and trace values under a server-owned cancel context. A scheduler span creates a producer span for each stable attempt message and injects W3C context into that message's transport headers. The stable message body remains unchanged if a scheduler republishes the same attempt. The worker extracts the headers before its consumer/execution span. Execution-store calls are wrapped in client spans, yielding this trace chain:

```text
HTTP server span
  -> persistence.create_run
  -> scheduler.recover
       -> persistence.load/save_run
       -> broker.publish (inject trace context)
       -> broker.receive
       -> worker.execute (extract context; parent is broker.publish)
            -> persistence.save_run
```

Logs use identifiers as fields: `request_id`, `workflow_id`, `workflow_run_id`, `task_id`, `task_run_id`, `attempt_id`, `worker_id`, `trace_id`, and `span_id` appear when relevant. The implementation never logs authorization headers, JWTs, task inputs/outputs, PostgreSQL DSNs, or NATS URLs. Metrics intentionally omit resource IDs to keep label cardinality bounded; labels are limited to HTTP method/route/status and task outcome.

`/metrics` exposes workflow submission and terminal outcome counters; task execution, failure, and retry counters; active-worker, running-task, and queue gauges; and HTTP, task, and workflow duration histograms. All values are process-local and reset on restart. `forgeflow_queue_depth` counts task messages published by the current process but not yet claimed by its scheduler; it is not a JetStream-wide backlog query. A future distributed control plane should replace that gauge with backend-specific aggregate collection rather than presenting the current value as cluster-wide.

The complete metric names, example configuration, and correlation runbook are in [observability.md](observability.md).

## Persistence boundary and storage backends

The execution package owns the base `Store` interface because the scheduler is its consumer. The security package extends it with `security.Store` for users, projects, memberships, workflow ownership, and audit events. The API depends on that extended boundary; the scheduler continues to know only execution state. `CreateRun` turns an unpersisted version-zero aggregate into version one. `SaveRun` treats its supplied version as an optimistic concurrency expectation and returns the next committed version. Neither scheduler nor API depends on a backend encoding.

`FileStore` is a single-process embedded implementation. A mutation is one checksummed journal envelope containing the complete changed aggregate. The file is opened in append mode, written fully, and synced before the in-memory index changes. Reopening replays records in order. A partial tail without the terminating newline is treated as an uncommitted write and truncated; malformed or checksum-invalid committed records stop startup with an error. This provides atomic aggregate changes for the current single-writer process without a database service. It deliberately has no compaction, concurrent-process writer coordination, or query indexes yet.

The local backend uses only Go's standard library. SQLite was not selected because local development does not need relational queries or concurrent transactions, and avoiding both CGO/native DLL requirements and a SQLite driver keeps the Windows setup small. PostgreSQL is optional: the application uses the journal unless `FORGEFLOW_STORE=postgres` is selected, and ordinary tests do not connect to a database.

### PostgreSQL schema and transactions

The PostgreSQL adapter uses native, pure-Go `pgx` and a bounded connection pool. Embedded up-migrations are applied explicitly at application startup. A migration ledger stores each filename and SHA-256 checksum; a PostgreSQL advisory transaction lock serializes concurrent migrators and a changed checksum stops startup instead of silently accepting schema drift.

The relational model uses these ownership boundaries:

- `workflow_definitions` owns ordered `task_definitions`; ordered `task_dependencies` has foreign keys to both the dependent and dependency tasks.
- `workflow_runs` references one immutable definition and owns `task_runs` plus `workers`.
- `task_leases` references the task run, its deterministic task-run ID, and the worker heartbeat row. Unique `(run_id, attempt_id)` and `(run_id, worker_id)` constraints prevent duplicate attempt records and more than one active task per in-process worker identity.
- `users` supplies stable JWT subjects; `projects` and `project_memberships` define the authorization boundary and checked roles.
- `workflow_ownership` binds each immutable definition to one project and requires the creator to be a member through a composite foreign key.
- `audit_events` stores immutable administrative actions and JSON metadata with foreign keys to project and actor.
- status, identifier, attempt-count, retry-deadline, terminal-timestamp, and lease relationships are guarded by checks or foreign keys where the database can enforce them. The richer aggregate invariants are also validated by the Go domain model before writes and after reads.

Saving an API-created workflow inserts its definition, ordered tasks, dependencies, and ownership in one transaction. Existing identical definitions are idempotent; differing content or ownership under the same workflow ID is rejected. Project creation commits its user, project, initial admin membership, and audit event together; project and membership updates likewise include their audit record. Creating a run inserts the parent, all task rows, heartbeat rows, and leases in one transaction. Updating a run first executes a conditional `UPDATE workflow_runs ... WHERE run_id = ? AND version = ?`, incrementing the version only for the winner. Every task, worker, and lease change then commits in that same transaction. A stale scheduler therefore cannot publish its lease or completion state, and no job is dispatched until the winning lease snapshot has persisted. Run reads use a read-only repeatable-read transaction so parent, task, worker, and lease rows come from one consistent database snapshot.

Indexes follow current operations rather than hypothetical reporting queries. In addition to execution aggregate keys, `project_memberships (user_id, project_id)` supports authorization lookup/listing, `workflow_ownership (project_id, workflow_id)` supports tenant resource lookup, and `audit_events (project_id, occurred_at DESC, event_id DESC)` serves the audit feed. No speculative status indexes are added because the current Store has no collection scans or recovery polling query. Those indexes should accompany such queries when that API is introduced.

PostgreSQL integration tests carry the `integration` build tag and isolate themselves in temporary schemas. GitHub Actions supplies a disposable PostgreSQL service and runs those tests with the race detector. This verifies migrations, relational constraints/indexes, definition and run round trips, persisted live leases, completion, and concurrent version conflicts without requiring a local PostgreSQL installation.

## Leases, heartbeats, and restart behavior

Each dispatch persists a `TaskLease` containing the worker ID, stable task-run ID, stable attempt ID, and expiration. A running task cannot receive a second lease. Workers persist their latest heartbeat and renew only leases they currently own and which have not expired. A late heartbeat cannot revive an expired lease.

If a local worker disappears, its heartbeat stops and its slot is supervised with a new worker identity. The abandoned task remains protected from reassignment until its lease expires. Expiry records the attempt as abandoned, preserves its consumed attempt count, and either moves it to retry-wait/ready or fails it when the maximum is exhausted. The count increments only when the replacement attempt is actually leased. A late result from the old worker has a stale attempt or lease identity and is ignored.

`Engine.Recover` loads the run and immutable definition and validates their relationship. Succeeded tasks and outputs remain succeeded. Pending, ready, and retry-waiting tasks preserve their eligibility. A still-valid persisted lease is not stolen on restart; it is recovered only after expiry. Stage 3 snapshots without lease metadata are treated as already abandoned and recovered according to policy.

## Delivery and execution semantics

- **At-most-once** means an operation is never repeated, but loss may prevent it from happening. ForgeFlow provides at-most-once *state application per attempt ID*: after a matching attempt completion changes durable task state, duplicate or stale completions are no-ops. A valid persisted lease also prevents another worker assignment from being committed for the current attempt.
- **At-least-once** means work may repeat so it is not silently abandoned. JetStream delivery is at-least-once until acknowledgement, and ForgeFlow task-handler execution is at-least-once across broker redelivery, configured retries, and expired-lease recovery. A worker might perform an external side effect and disappear before persisting completion; the replacement attempt can perform that effect again.
- **Exactly-once** would require the task's external side effect and ForgeFlow's completion record to commit atomically, or require the external system to deduplicate a stable idempotency key. ForgeFlow cannot guarantee that universally and does not claim exactly-once handler execution.

Stable task-run and attempt IDs let handlers implement application-level deduplication. Within ForgeFlow, identified completions are idempotent and completed logical tasks are not rescheduled. This mitigates duplicates but does not erase the crash window around external side effects.

Broker acknowledgement is deliberately last in the success path: `handler result -> persisted completion -> broker ack`. A crash before the store commit leaves the message unacknowledged and eligible for redelivery. A crash after the store commit but before the ack also causes redelivery, but persisted terminal state makes that repeat a no-op that can be acknowledged. During execution the worker path is `delivery -> persisted lease -> handler`, so delivery alone cannot bypass lease ownership. A broker delivery count never increments `TaskRun.AttemptCount`; only a successfully persisted new lease does.

The embedded guarantee is scoped to one engine process and its atomic aggregate `Store` writes. `FileStore` is not a distributed compare-and-swap database and must not back multiple processes. PostgreSQL extends the persistence boundary with transactional, version-guarded aggregate writes: competing schedulers may race, but only one can commit a particular lease transition. JetStream adds durable at-least-once transport, not an atomic transaction with PostgreSQL or a handler's external side effect.

## Remaining distributed-runtime work

To extract workers into separately deployable processes, the existing identified-attempt and lease model will gain narrower claim, heartbeat, and completion repository operations. Remote workers will claim attempts, heartbeat independently, invoke registered handlers, and record outcomes with PostgreSQL as the state authority. The current aggregate version check already provides compare-and-swap protection, while narrower operations will avoid rewriting the complete aggregate for each remote transition.

That extraction will not change the core rule already enforced today: the state store is authoritative for transitions, while broker delivery alone proves neither ownership nor completion. Idempotency keys and attempt identities mitigate redelivery; they do not create universal exactly-once execution.

## Development constraints

Local development must remain viable on a machine with about 5.8 GB of RAM. The design therefore favors native Go processes, deterministic in-memory tests, and focused test doubles. Normal tests require no external services. PostgreSQL and NATS integration tests run in separate GitHub Actions jobs; an already-installed standalone NATS binary is optional for developers who want to invoke the JetStream suite manually. Docker, WSL, Kafka, and a local multi-service stack are not required.

The current security boundary and its explicit exclusions are documented in [security.md](security.md). The scheduler, delivery, recovery, database, and tradeoff explanations used for technical interviews are collected in [interview-notes.md](interview-notes.md). Future deployment work must add an identity-provider integration, shared rate limiting, TLS/edge controls, key rotation, and operational audit export without weakening the lightweight local path.
