# ForgeFlow Agent Instructions

These instructions apply to the entire repository.

## Purpose

ForgeFlow is a distributed workflow execution platform written primarily in Go. It is built to teach and demonstrate production-minded backend and distributed-systems engineering through incremental, working stages.

## Hardware and local-development constraints

The primary development machine has about 5.8 GB of RAM. Keep local development lightweight.

- Do not install, launch, configure, or require Docker Desktop or WSL.
- Do not run Kubernetes or Kafka locally.
- Do not introduce a heavy multi-service local stack.
- Prefer native Go development and the Go standard library where reasonable.
- Run heavy integration infrastructure in GitHub Actions or a remote/cloud environment when it is eventually needed.
- Before adding a dependency or tool likely to consume significant memory, explain why it is necessary.

## Engineering rules

- Write idiomatic Go and format it with `gofmt` or `go fmt`.
- Keep packages small and responsibilities clear.
- Avoid premature abstraction and do not add technology merely for resume keywords.
- Require every external dependency to solve a concrete problem.
- Prefer interfaces at architectural boundaries such as persistence and messaging.
- Avoid global mutable state.
- Thread `context.Context` through operations that can block or be cancelled.
- Return errors with useful context and never silently ignore errors.
- Introduce structured logging when it becomes useful, but do not overbuild it initially.
- Document public APIs and important exported types.
- Keep secrets out of the repository. Add `.env.example` when environment configuration is introduced.
- Keep unit tests deterministic.
- Do not support arbitrary shell-command execution as an initial workflow task. Use a safe task-handler abstraction.
- Never fabricate benchmark results, throughput figures, user counts, latency numbers, or test counts.
- Preserve the distinction between currently implemented behavior and future architecture in code and documentation.

## Git workflow

- Keep only the `main` branch and work directly on it. Do not create stage-specific feature branches.
- Inspect the repository, status, and remotes before changing anything.
- Preserve `origin` when it exists; never invent a GitHub remote URL.
- Never force-push or rewrite/amend already-pushed history.
- Commit only coherent, meaningful units of work. Do not create artificial commits solely because a numbered stage ended.
- A stage may have one cohesive commit or several independently meaningful commits.
- Include tests with the behavior they verify in the same commit whenever practical.
- Before every commit, run the appropriate formatter, tests, and static checks, then inspect the diff.
- Commit only when the repository is in a working state.
- Use concise Conventional Commit-style messages, for example:
  - `feat(workflow): add DAG validation`
  - `feat(worker): execute ready tasks concurrently`
  - `fix(scheduler): prevent duplicate dispatch`
  - `test(recovery): cover expired task leases`
- Push each completed meaningful commit to `origin main` when a remote is configured.
- Leave the worktree clean at the end.

## Expected validation

For ordinary Go changes, run at least:

```text
go fmt ./...
go vet ./...
go test ./...
go build ./...
```

Expand validation in proportion to the risk of a change.
