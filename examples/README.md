# ForgeFlow API example

Configure ForgeFlow with the PEM-encoded Ed25519 public key of an external JWT issuer, then start it from the repository root. The token must contain matching `iss` and `aud` claims plus `sub` and `exp`:

```text
$env:FORGEFLOW_JWT_PUBLIC_KEY = Get-Content -Raw .\issuer-public.pem
go run ./cmd/forgeflow
```

In another terminal, put a valid token in `TOKEN`, create the project, then create the example workflow and start a run:

```text
curl -i -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"id":"forge-demo-project","name":"Forge Demo"}' http://127.0.0.1:8080/api/v1/projects
curl -i -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" --data-binary @examples/workflow.json http://127.0.0.1:8080/api/v1/workflows
curl -i -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"run_id":"demo-run"}' http://127.0.0.1:8080/api/v1/workflows/forge-demo/runs
```

Stream persisted transitions until the run reaches a terminal state:

```text
curl -N -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/api/v1/runs/demo-run/events
```

Read the aggregate and task-level results:

```text
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/api/v1/runs/demo-run
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/api/v1/runs/demo-run/tasks
```

For a still-running workflow, cancellation is asynchronous:

```text
curl -i -X POST -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/api/v1/runs/demo-run/cancel
```

On Windows PowerShell, invoke `curl.exe` if `curl` is aliased to `Invoke-WebRequest`. Task-level reads and cancellation require an operator or admin role; a project's creator is an admin.
