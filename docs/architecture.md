# ForgeFlow Architecture

## Architectural intent

ForgeFlow will execute task graphs reliably across multiple workers. The eventual design separates workflow semantics from infrastructure so that scheduling rules remain testable without a broker, database, or network services.

This document describes a target direction, not a claim that those components already exist.

## Current implementation

Stage 1 contains:

- a Go module with no external dependencies;
- stable workflow and task identifier types plus in-memory definitions;
- typed, machine-classifiable validation errors with contextual messages;
- validation for identifiers, names, task uniqueness, dependency references, repeated edges, self-dependencies, and cycles;
- deterministic topological ordering, root-task discovery, and ready-task discovery; and
- a small bootstrap command with deterministic unit tests.

There is currently no scheduler, task execution, worker runtime, persistence, messaging, HTTP API, authentication, recovery mechanism, or observability stack. `ReadyTasks` is a stateless domain query over a caller-provided set of completed task IDs; it does not claim, dispatch, or execute work.

## Eventual component boundaries

| Component | Responsibility | Status |
| --- | --- | --- |
| Workflow domain | Define workflows, tasks, dependencies, and DAG invariants | Current |
| Scheduler | Identify ready tasks without violating dependency constraints | Future |
| Dispatcher | Offer runnable task attempts to workers without duplicate ownership | Future |
| Worker runtime | Execute registered safe task handlers and report outcomes | Future |
| State store | Persist workflow, task, attempt, lease, and idempotency state | Future |
| Messaging boundary | Decouple dispatch and execution through a broker adapter | Future |
| Recovery controller | Detect expired leases and make abandoned work eligible again | Future |
| API | Accept workflows and expose execution state | Future |
| Observability | Provide structured logs, metrics, traces, and operational diagnostics | Future |

Persistence and messaging should be represented by narrow interfaces when those boundaries become real. Early implementations can be in memory. PostgreSQL and a production message broker should arrive only after their required semantics are understood and covered by contract tests.

## Current graph algorithms

Definition validation builds an adjacency list from dependency to dependent and an indegree count for every task. It rejects malformed definitions before graph traversal, allowing callers to distinguish a bad reference from a valid graph that contains a cycle.

Topological ordering uses Kahn's algorithm. A min-heap of zero-indegree task IDs makes the result lexicographically deterministic even when task definitions or Go map iteration order differ. If the algorithm cannot consume every task, the remaining dependency structure contains a cycle. Root and ready-task results use the same task-ID ordering rule.

## Target execution flow

Eventually, a submitted workflow will be validated before it is stored. The scheduler will select tasks whose dependencies have succeeded, and the dispatcher will create uniquely identified, leased attempts. Workers will claim attempts, invoke registered handlers, and record outcomes. Retry policy will decide whether a failed attempt becomes eligible again. Lease expiry will allow the recovery controller to reclaim work from unavailable workers without treating duplicate delivery as duplicate completion.

The state store will be the authority for state transitions. Broker delivery alone will not prove ownership or completion. Idempotency keys and attempt identities will make redelivery safe, while atomic state transitions will prevent two workers from committing the same logical result.

## Development constraints

Local development must remain viable on a machine with about 5.8 GB of RAM. The design therefore favors native Go processes, deterministic in-memory tests, and focused test doubles. Heavy broker, database, load, and orchestration tests should eventually run in CI or remote environments rather than a local multi-service stack.

Security, tenancy, authentication, and authorization are important future concerns, but they should be added when the API and deployment model make their requirements concrete.
