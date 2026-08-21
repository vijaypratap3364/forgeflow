# ForgeFlow API example

Start ForgeFlow from the repository root:

```text
go run ./cmd/forgeflow
```

In another terminal, create the example workflow and start a run:

```text
curl -i -H "Content-Type: application/json" --data-binary @examples/workflow.json http://127.0.0.1:8080/api/v1/workflows
curl -i -H "Content-Type: application/json" -d '{"run_id":"demo-run"}' http://127.0.0.1:8080/api/v1/workflows/forge-demo/runs
```

Stream persisted transitions until the run reaches a terminal state:

```text
curl -N http://127.0.0.1:8080/api/v1/runs/demo-run/events
```

Read the aggregate and task-level results:

```text
curl http://127.0.0.1:8080/api/v1/runs/demo-run
curl http://127.0.0.1:8080/api/v1/runs/demo-run/tasks
```

For a still-running workflow, cancellation is asynchronous:

```text
curl -i -X POST http://127.0.0.1:8080/api/v1/runs/demo-run/cancel
```

On Windows PowerShell, invoke `curl.exe` if `curl` is aliased to `Invoke-WebRequest`.
