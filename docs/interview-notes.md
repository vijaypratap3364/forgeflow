# ForgeFlow Interview Notes

## Thirty-second description

ForgeFlow is a Go workflow engine that validates task DAGs, persists workflow-run state, schedules dependency-ready work, and executes independent tasks concurrently through a broker boundary. The local path uses an append-only file journal and an in-memory broker; production adapters use PostgreSQL and NATS JetStream. Reliability comes from stable attempt identities, persisted leases, worker heartbeats, retry policy, idempotent state application, and an ordering rule that persists completion before acknowledging delivery.

The project deliberately does not claim universal exactly-once execution. A handler can perform an external side effect and crash before ForgeFlow records completion, so handler execution is at-least-once across that failure window. External effects need their own idempotency key or transaction boundary.

## Scheduler architecture

Each active workflow run owns one scheduler loop and a configurable worker pool. The scheduler is the sole writer of that run's in-memory aggregate; workers communicate claims, heartbeats, disappearance, and results through channels. This single-owner model keeps task transitions deterministic and avoids locks throughout the domain model. The concurrency-safe Store and Broker boundaries are shared across independent runs.

The scheduler follows this sequence:

1. Validate or restore the immutable workflow definition and run snapshot.
2. Persist pending tasks becoming ready.
3. Publish one stable message for the next attempt of each ready task.
4. Validate a received run/task/task-run/attempt identity.
5. Persist the worker lease before authorizing handler execution.
6. Apply an identified result, unlock newly ready dependents, and persist the aggregate.
7. Acknowledge the broker delivery only after the completion transition is durable.

The domain uses Kahn's algorithm with a minimum heap for deterministic topological ordering. Ready-task discovery requires every dependency to be succeeded; failed or canceled dependencies therefore never unlock downstream work.

## Concurrency model

The worker count is a per-run bound. Workers are goroutines that compete on one durable subscription for the run, execute context-aware registered handlers, and send typed results back to their scheduler. Independent `Engine.Execute` or `Engine.Recover` calls can run concurrently because they do not share workflow aggregates.

This is intentionally not a globally fair scheduler. Ten active runs configured with four workers can own up to forty worker goroutines. A hosted service would eventually add admission control, project quotas, and a global dispatch policy. The current model favors simple ownership and testable correctness over cluster-wide fairness.

Tests use barriers, channels, injected clocks, and Store/Broker decorators rather than random failures or timing sleeps. High-fan-out tests verify the worker bound, duplicate-delivery tests force two workers to present the same attempt, fake time drives lease expiry and backoff, and lifecycle counters prove worker receive loops return on cancellation.

## Delivery and execution semantics

The important ordering is:

```text
ready state persisted
  -> task message published
  -> delivery received
  -> lease persisted
  -> handler runs
  -> completion persisted
  -> delivery acknowledged
```

JetStream redelivery is at-least-once until acknowledgement. ForgeFlow state application for a matching attempt is at-most-once: once the task has moved past that attempt, a duplicate or stale completion is ignored. Handler execution remains at-least-once because a crash can occur after an external effect but before the completion commit.

Broker delivery count and logical attempt count are different. Redelivering the same message does not consume another configured attempt. `AttemptCount` increases only when ForgeFlow persists a new lease for a new stable attempt ID.

## Idempotency

`TaskRunID` is stable for one task within one workflow run. `AttemptID` is deterministically derived from that task-run identity and its attempt number. It is used as the broker message ID, carried through the worker request and result, and checked against the current persisted lease.

JetStream's duplicate window suppresses quick repeated publications, but it is only an optimization. The durable state machine is authoritative after the window expires or after reconnect/redelivery. A completion with an old attempt ID, wrong worker, missing lease, or already-terminal task cannot mutate the aggregate.

For an external payment, email, or database write, a handler should pass `TaskRunID` or `AttemptID` to the downstream system as an idempotency key. Exactly-once behavior is possible only when that system deduplicates the key or when the side effect and ForgeFlow completion share a real atomic transaction.

## Leases, heartbeats, and worker failure

A scheduler persists a lease containing worker ID, task-run ID, attempt ID, and expiration before it releases work to a handler. Another claim cannot be committed while that lease is valid. The worker periodically records a heartbeat; a successful heartbeat renews its owned lease and sends a JetStream in-progress acknowledgement to extend the broker acknowledgement deadline.

If a worker disappears, the handler result never arrives and heartbeats stop. ForgeFlow does not immediately steal the task: it waits for the persisted lease to expire. Expiry records the old attempt as abandoned and either makes the task retryable or fails it when policy is exhausted. A replacement worker receives a new identity and the next persisted attempt ID. A late result from the old worker is stale and ignored.

The current recovery controller lives inside the per-run scheduler. Automatic cross-process discovery of nonterminal runs and a separately deployable remote worker executable are future work; the code does not present those as implemented.

## PostgreSQL transaction strategy

PostgreSQL stores normalized workflow definitions, task dependencies, workflow runs, task runs, workers, and leases. Definition creation and all child rows commit atomically. Run creation inserts the aggregate in one transaction.

Run updates use optimistic concurrency. A transaction first performs a conditional parent update using the expected aggregate version. Exactly one contender can increment that version; a stale scheduler receives a typed version conflict and cannot commit its task, worker, or lease rows. All child-state replacements then commit in that same transaction. Reads use a read-only repeatable-read transaction so the parent, tasks, workers, and leases describe one consistent version.

This aggregate transaction is simple and preserves rich invariants, but it has write amplification and creates one contention point per busy run. A later remote-worker design would likely add narrow conditional claim, heartbeat, and completion statements while keeping the same attempt and version guards.

Migrations are embedded and checksummed. An advisory transaction lock serializes migrators; altered history is rejected instead of silently accepted.

## Why NATS JetStream

JetStream matches the current work-queue problem with durable messages, pull consumers, explicit acknowledgement, redelivery, publish deduplication, and progress acknowledgements. It is available as one relatively small native server and has a mature Go client, so local development can remain broker-free while CI can start a standalone binary cheaply.

Kafka was not added merely as a keyword. Its operational model, partitions, consumer-group rebalancing, and heavier local footprint would add complexity before ForgeFlow needs long-retention event streams or very high aggregate throughput. NATS does not make execution exactly-once; PostgreSQL leases and identified state transitions still protect correctness.

## Evolving the broker boundary toward Kafka

A Kafka adapter could preserve the current logical envelope and use a consumer group for competing workers, manual offset commits after durable completion, and the stable attempt ID in message headers. Partition keys would need an explicit ordering decision: keying by workflow run preserves per-run order but can restrict parallel delivery, while keying by task-run improves distribution and relies on persisted dependency/lease validation to reject premature or stale work.

The current minimal Broker interface may need to grow adapter-neutral delivery metadata, session/rebalance handling, and delayed retry or dead-letter policy. A PostgreSQL outbox could atomically record task publication intent with a state transition and a relay could publish it to Kafka, closing the current database-to-broker publication gap. Kafka transactions alone still cannot atomically include arbitrary handler side effects or a separate PostgreSQL commit, so the execution guarantee would remain honest and boundary-specific.

## Major tradeoffs and limitations

- One scheduler owner per run makes transitions understandable, but scheduler execution is still coupled to the API process.
- Full aggregate snapshots make restart and invariant validation straightforward, but increase write volume for large or highly active runs.
- `FileStore` is lightweight and crash-tolerant for one process, but has no compaction, query indexes, or multi-process writer safety.
- JetStream supplies durable transport, but there is no atomic transaction spanning PostgreSQL and message publication.
- SSE history, metrics, and rate limiting are process-local. Durable run status remains authoritative after restart, but SSE replay history does not.
- JWT verification is real, while token issuance, key discovery/rotation, and login remain external responsibilities.
- Registered handlers avoid arbitrary shell execution, but ForgeFlow is not yet a general remote-code runtime.
- No throughput, latency, or scale figure is claimed without a repeatable workload and retained raw results.

## Quality evidence

Ordinary tests require no service. The race detector covers domain, scheduler, broker, API, persistence, security, and observability tests. GitHub Actions also runs the PostgreSQL and NATS integration suites against temporary services, verifies formatting/module consistency, runs `go vet` and Staticcheck, builds the executable, and scans reachable dependencies with `govulncheck`.

The most important tests cover concurrent workflows, a wide ready queue, competing and duplicate claims, durable lease-before-execution ordering, completion-before-acknowledgement ordering, worker disappearance, lease expiry, retry exhaustion, cancellation, scheduler recovery, file-store reopen recovery, graceful shutdown, invalid transitions, and owned goroutine lifecycle completion.
