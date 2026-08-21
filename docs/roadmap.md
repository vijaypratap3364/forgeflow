# ForgeFlow Roadmap

The sequence below is intentionally incremental. Each stage should leave the repository buildable, tested, and useful without requiring later infrastructure.

## Stage 0 — Repository foundation (complete)

- Establish persistent engineering and Git instructions.
- Initialize the Go module and a conventional project layout.
- Add a minimal executable, deterministic test, and lightweight developer commands.
- Document the boundary between the current code and the eventual platform.

Exit condition: formatting, vetting, tests, and build all pass with the native Go toolchain.

## Stage 1 — Workflow model and DAG semantics (complete)

- Define workflow and task identities and dependency edges.
- Validate missing references, self-dependencies, duplicate identities, and cycles.
- Determine ready tasks deterministically from workflow state.
- Cover graph and state invariants with table-driven unit tests.

Keep this stage in memory and independent of workers, databases, brokers, and HTTP.

## Stage 2 — Local execution (complete)

- Add a registry of safe, typed task handlers rather than arbitrary shell commands.
- Execute independent ready tasks concurrently with bounded parallelism.
- Propagate cancellation and record task outcomes through explicit state transitions.
- Exercise complete workflows in deterministic process-level tests.

## Stage 3 — Durable local persistence and restart recovery (complete)

- Define a persistence boundary owned by the execution engine.
- Persist workflow definitions and aggregate run/task state, timestamps, and attempt counts.
- Provide a lightweight embedded journal with atomic append-and-sync mutations.
- Reconstruct runs after restart without repeating completed tasks.
- Keep storage paths configurable and repository tests isolated.

## Stage 4 — Reliable attempts and recovery semantics (complete)

- Model attempts, retry policy, backoff, leases, and idempotency keys.
- Prevent duplicate claims and duplicate committed completion.
- Track worker heartbeats and reclaim expired work.
- Prove recovery behavior first with an in-memory implementation and controlled time.

The current implementation proves these semantics with identified in-process workers. The PostgreSQL Store now rejects competing aggregate claims across processes; independent remote-worker heartbeat operations remain part of the remote-worker stage.

## Stage 5 — REST API and live workflow status (complete)

- Add an HTTP API for workflow submission, inspection, and cancellation.
- Define validation, error, pagination, and concurrency behavior before optimizing transport.
- Stream persisted workflow and task transitions with Server-Sent Events.
- Add health/readiness probes, environment configuration, examples, and graceful shutdown.

The current API addresses individual resources by stable ID; collection listing and pagination will be added when the Store gains query operations. SSE replay is intentionally process-local, while current run status remains durable in the Store.

## Stage 6 — PostgreSQL production storage (complete)

- Add PostgreSQL behind the existing Store boundary while preserving the embedded local default.
- Apply an indexed relational schema through embedded, checksummed migrations.
- Protect concurrent aggregate transitions with transactional optimistic version checks.
- Exercise the adapter against a temporary PostgreSQL service in GitHub Actions, not on the constrained development machine.

## Stage 7 — Distributed messaging and remote workers

- Introduce a broker interface and a production-grade broker adapter.
- Run workers as separate processes and make delivery/redelivery behavior explicit.
- Add integration and failure-injection tests in CI or remote infrastructure.
- Do not require Kafka or another heavy broker on the constrained development machine.

## Stage 8 — Operations and scale evidence

- Add structured logs, metrics, traces, health checks, and operational runbooks.
- Build repeatable load and soak tests with stored configurations and raw results.
- Measure bottlenecks and tune from evidence; never invent performance figures.

## Stage 9 — Security and cloud deployment

- Add authentication, authorization, secret management, and tenant boundaries appropriate to the chosen API model.
- Package and deploy the system in remote/cloud environments.
- Add staged rollout, backup, restore, and disaster-recovery procedures.
- Keep local development lightweight even as production deployment grows.

Stage boundaries may move as evidence emerges, but new infrastructure should always follow a demonstrated requirement rather than precede it.
