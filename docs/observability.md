# ForgeFlow Observability

ForgeFlow is observable out of the box without requiring Prometheus, Grafana, or an OpenTelemetry Collector on the development laptop. Structured logs and a process-local metrics registry are always available. Trace recording/export is opt-in; W3C context propagation remains enabled even when the exporter is `none`.

## Structured logs and correlation

The executable emits newline-delimited JSON through Go's `log/slog`. `FORGEFLOW_LOG_LEVEL` accepts `debug`, `info`, `warn`, or `error` and defaults to `info`. Each HTTP response contains a newly generated `X-Request-ID`; ForgeFlow does not copy an untrusted caller-provided request ID.

Request logs contain the request ID, method, matched route template, status, duration, and resource IDs present in the route. Persisted workflow transitions add the stable identifiers that exist at that boundary:

- `workflow_id` and `workflow_run_id` for run lifecycle events;
- `task_id`, `task_run_id`, and `attempt_id` for task transitions;
- `worker_id` while a lease identifies the executing worker;
- `trace_id` and `span_id` whenever a recording or propagated span is present.

ForgeFlow does not log request/authorization headers, bearer tokens, JWT claims, task inputs, task outputs, PostgreSQL DSNs, NATS URLs, or OTLP headers. Metric labels never contain user, project, workflow, run, task, attempt, request, worker, or trace IDs.

Given a run ID, an operator can inspect durable truth first and then correlate telemetry:

1. Read `GET /api/v1/runs/{runID}` and `/tasks` to identify the terminal task and attempt.
2. Filter JSON logs by `workflow_run_id`; use `task_run_id`, `attempt_id`, and `worker_id` to follow lease and retry changes.
3. Copy the `trace_id` from a matching log record into the configured trace backend.
4. Compare task failure/retry counters and duration histograms around the incident window.

## Prometheus-compatible metrics

`GET /metrics` is public like the health probes and returns `text/plain; version=0.0.4`. A hosted deployment should restrict this endpoint at its network edge. No Prometheus server is needed to inspect it locally:

```text
curl http://127.0.0.1:8080/metrics
```

| Metric | Type | Meaning |
| --- | --- | --- |
| `forgeflow_workflows_submitted_total` | Counter | Runs durably created by this process |
| `forgeflow_workflows_succeeded_total` | Counter | Runs persisted as succeeded |
| `forgeflow_workflows_failed_total` | Counter | Runs persisted as failed |
| `forgeflow_workflows_canceled_total` | Counter | Runs persisted as canceled |
| `forgeflow_tasks_executed_total{status}` | Counter | Finished handler attempts by bounded outcome |
| `forgeflow_task_failures_total` | Counter | Failed attempts, including retryable failures |
| `forgeflow_task_retries_total` | Counter | Retries scheduled after a failure/expired lease |
| `forgeflow_active_workers` | Gauge | Current worker goroutines across active in-process runs |
| `forgeflow_queue_depth` | Gauge | Messages published by this process and not yet claimed locally |
| `forgeflow_running_tasks` | Gauge | Current attempts executing in handlers |
| `forgeflow_http_requests_total{method,route,status}` | Counter | Requests using matched route templates, not raw paths |
| `forgeflow_http_request_duration_seconds` | Histogram | HTTP response duration |
| `forgeflow_task_duration_seconds` | Histogram | Handler-attempt duration |
| `forgeflow_workflow_duration_seconds` | Histogram | Run creation-to-terminal duration |

Counters and gauges are process-local and reset on restart. The queue gauge is not a NATS stream/consumer backlog query; it covers task messages this process published and has not yet leased. This distinction avoids claiming a cluster-wide number before ForgeFlow has a distributed metrics aggregation design.

## Trace flow and exporters

ForgeFlow creates spans for the HTTP server, scheduler execution/recovery, execution persistence operations, broker publication and receipt, and worker handler execution. Stable workflow and execution identifiers are span attributes. Task-message transport headers carry injected W3C `traceparent`, `tracestate`, and baggage values, so the worker execution span is a child of the broker producer span even when transport redelivers the message. Trace metadata is deliberately outside the stable JSON body; republishing the same attempt under a new scheduler trace therefore does not create conflicting logical message content.

The asynchronous run manager uses a server-owned cancellation context: it deliberately ignores client disconnect cancellation after a run has been durably accepted while preserving request ID and trace values. Explicit run cancellation and process shutdown still cancel handlers.

Local no-op tracing is the default:

```powershell
$env:FORGEFLOW_TRACE_EXPORTER = "none"
go run ./cmd/forgeflow
```

For a lightweight console trace dump:

```powershell
$env:FORGEFLOW_TRACE_EXPORTER = "stdout"
$env:FORGEFLOW_SERVICE_NAME = "forgeflow-local"
go run ./cmd/forgeflow
```

For remote OTLP over HTTP, use the standard OpenTelemetry exporter variables. Real headers belong in the process environment or a secret manager, never in the repository:

```powershell
$env:FORGEFLOW_TRACE_EXPORTER = "otlp-http"
$env:OTEL_EXPORTER_OTLP_ENDPOINT = "https://collector.example:4318"
$env:OTEL_EXPORTER_OTLP_HEADERS = "authorization=Bearer <from-secret-manager>"
go run ./cmd/forgeflow
```

The OTLP provider batches spans and flushes them during graceful shutdown. ForgeFlow does not install or launch a collector, Prometheus, or Grafana process.

## Health and readiness

`GET /healthz` answers whether the HTTP process is alive; dependency failure does not change it. `GET /readyz` answers whether ForgeFlow can accept work. It returns `503` during shutdown or when a selected dependency probe fails:

- PostgreSQL: connection-pool ping;
- NATS: connection state plus a protocol flush round trip;
- in-memory broker: open-state check;
- embedded file store: no remote dependency exists after successful startup.

The readiness error response is intentionally generic and does not expose connection strings or dependency details. Structured logs record that a dependency check failed without logging its potentially sensitive error text.

## Test coverage

Ordinary unit tests need no telemetry backend. They verify Prometheus exposition, counters/gauges/histograms, structured correlation fields and secret exclusion, request IDs, readiness separation, stdout export, incoming HTTP trace extraction, persistence spans, and W3C propagation from broker publication to worker execution. PostgreSQL and NATS remain confined to their opt-in integration suites.
