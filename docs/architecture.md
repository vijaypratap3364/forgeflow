# ForgeFlow Architecture

## Architectural intent

ForgeFlow will execute task graphs reliably across multiple workers. The eventual design separates workflow semantics from infrastructure so that scheduling rules remain testable without a broker, database, or network services.

This document describes a target direction, not a claim that those components already exist.

## Current implementation

Stage 2 contains:

- a Go module with no external dependencies;
- stable workflow and task identifier types plus in-memory definitions;
- typed, machine-classifiable validation errors with contextual messages;
- validation for identifiers, names, task uniqueness, dependency references, repeated edges, self-dependencies, and cycles;
- deterministic topological ordering, root-task discovery, and ready-task discovery;
- separate workflow-definition, workflow-run, and task-run models with enforced state transitions;
- a synchronous scheduler that unlocks successful dependency chains;
- per-run in-memory queues and configurable worker pools using goroutines and channels;
- a concurrency-safe registry of context-aware task handlers;
- safe built-in no-op, delay, and uppercase handlers; and
- a small bootstrap command with deterministic unit tests.

There is currently no persistence, retry or lease model, remote worker, messaging system, HTTP API, authentication, recovery controller, or observability stack. The executable remains a bootstrap status command; workflow execution is currently exposed as an internal Go API and exercised through tests.

## Eventual component boundaries

| Component | Responsibility | Status |
| --- | --- | --- |
| Workflow domain | Define workflows, tasks, dependencies, and DAG invariants | Current |
| Scheduler | Identify and dispatch ready tasks without violating dependencies | Current, in process |
| Dispatcher | Offer runnable tasks to workers through a queue | Current, in-memory only |
| Worker runtime | Execute registered safe task handlers and report outcomes | Current, in process |
| State store | Persist workflow, task, attempt, lease, and idempotency state | Future |
| Messaging boundary | Decouple dispatch and execution through a broker adapter | Future |
| Recovery controller | Detect expired leases and make abandoned work eligible again | Future |
| API | Accept workflows and expose execution state | Future |
| Observability | Provide structured logs, metrics, traces, and operational diagnostics | Future |

Persistence and messaging should be represented by narrow interfaces when those boundaries become real. Early implementations can be in memory. PostgreSQL and a production message broker should arrive only after their required semantics are understood and covered by contract tests.

## Current graph algorithms

Definition validation builds an adjacency list from dependency to dependent and an indegree count for every task. It rejects malformed definitions before graph traversal, allowing callers to distinguish a bad reference from a valid graph that contains a cycle.

Topological ordering uses Kahn's algorithm. A min-heap of zero-indegree task IDs makes the result lexicographically deterministic even when task definitions or Go map iteration order differ. If the algorithm cannot consume every task, the remaining dependency structure contains a cycle. Root and ready-task results use the same task-ID ordering rule.

## Current execution architecture

```text
WorkflowDefinition + RunID
            |
            v
       Scheduler loop
            |
            v
   unbuffered task channel
        /    |    \
   worker worker worker
        \    |    /
          results
            |
            v
   WorkflowRun state machine
```

`Engine.Execute` creates an isolated `WorkflowRun`, resolves every handler before starting, derives a run context from the caller, and starts the configured number of workers. Each `Execute` call owns its queue and worker goroutines, so the same engine can execute multiple workflow runs concurrently without sharing mutable run state.

The scheduler is the only component that decides task transitions and dispatch. It maintains the set of succeeded task IDs and asks the workflow domain for newly ready tasks after each success. Only pending tasks are promoted to ready, and a task moves to running when a worker accepts it from the unbuffered job channel. Worker results move tasks to succeeded or failed and may unlock downstream dependencies.

Task transitions are `pending -> ready -> running -> succeeded|failed`. Cancellation may move any nonterminal task to `canceled`. Workflow transitions are `pending -> running -> succeeded|failed|canceled`, with pending-to-canceled allowed when the caller's context is already done.

The current failure policy is fail-fast. The first handler failure marks that task failed, cancels the derived run context, drains results from already running tasks, cancels every unfinished task, and marks the workflow failed. Caller cancellation follows the same drain-and-stop discipline but marks the workflow canceled. A task that finishes successfully before observing cancellation remains succeeded. Handlers receive `context.Context` and are required to return promptly when it is canceled; Go cannot forcibly stop a handler that ignores its context.

This stage dispatches tasks, not durable attempts. It does not yet provide leases, retries, idempotency, crash recovery, or cross-process ownership.

## Target execution flow

Eventually, a submitted workflow will be validated before it is stored. The scheduler will select tasks whose dependencies have succeeded, and the dispatcher will create uniquely identified, leased attempts. Workers will claim attempts, invoke registered handlers, and record outcomes. Retry policy will decide whether a failed attempt becomes eligible again. Lease expiry will allow the recovery controller to reclaim work from unavailable workers without treating duplicate delivery as duplicate completion.

The state store will be the authority for state transitions. Broker delivery alone will not prove ownership or completion. Idempotency keys and attempt identities will make redelivery safe, while atomic state transitions will prevent two workers from committing the same logical result.

## Development constraints

Local development must remain viable on a machine with about 5.8 GB of RAM. The design therefore favors native Go processes, deterministic in-memory tests, and focused test doubles. Heavy broker, database, load, and orchestration tests should eventually run in CI or remote environments rather than a local multi-service stack.

Security, tenancy, authentication, and authorization are important future concerns, but they should be added when the API and deployment model make their requirements concrete.
