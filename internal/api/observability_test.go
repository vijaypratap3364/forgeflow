package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vijaypratap3364/forgeflow/internal/execution"
	"github.com/vijaypratap3364/forgeflow/internal/observability"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestMetricsAndStructuredLogsDescribeWorkflowExecution(t *testing.T) {
	logOutput := &lockedBuffer{}
	instrumentation := observability.NewInstrumentation(
		slog.New(slog.NewJSONHandler(logOutput, nil)),
		observability.NewMetrics(),
		nil,
		nil,
	)
	server := newObservedTestServer(t, nil, instrumentation, nil)

	submitWorkflow(t, server, `{
		"id":"observed-workflow",
		"tasks":[{"id":"observed-task","name":"Observed task","handler":"noop"}]
	}`)
	created := request(
		t,
		server,
		http.MethodPost,
		"/api/v1/workflows/observed-workflow/runs",
		`{"run_id":"observed-run"}`,
		"application/json",
	)
	assertStatus(t, created, http.StatusAccepted)
	runRequestID := created.Header().Get("X-Request-ID")
	if runRequestID == "" {
		t.Fatal("run creation response is missing X-Request-ID")
	}
	waitForEventStream(t, server, "observed-run")

	metricsResponse := request(t, server, http.MethodGet, "/metrics", "", "")
	assertStatus(t, metricsResponse, http.StatusOK)
	if got := metricsResponse.Header().Get("Content-Type"); got != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("metrics Content-Type = %q", got)
	}
	metrics := metricsResponse.Body.String()
	for _, sample := range []string{
		"forgeflow_workflows_submitted_total 1",
		"forgeflow_workflows_succeeded_total 1",
		`forgeflow_tasks_executed_total{status="succeeded"} 1`,
		"forgeflow_task_failures_total 0",
		"forgeflow_task_retries_total 0",
		"forgeflow_active_workers 0",
		"forgeflow_queue_depth 0",
		"forgeflow_running_tasks 0",
		"forgeflow_task_duration_seconds_count 1",
		"forgeflow_workflow_duration_seconds_count 1",
		`forgeflow_http_requests_total{method="POST",route="/api/v1/workflows/{id}/runs",status="202"} 1`,
	} {
		if !strings.Contains(metrics, sample) {
			t.Errorf("metrics response does not contain %q\n%s", sample, metrics)
		}
	}

	logs := logOutput.String()
	for _, fragment := range []string{
		`"msg":"HTTP request completed"`,
		`"request_id":"` + runRequestID + `"`,
		`"msg":"workflow submitted"`,
		`"workflow_id":"observed-workflow"`,
		`"workflow_run_id":"observed-run"`,
		`"msg":"task started"`,
		`"task_run_id":"` + string(execution.TaskRunIDFor("observed-run", "observed-task")) + `"`,
		`"worker_id":`,
		`"msg":"task succeeded"`,
		`"msg":"workflow completed"`,
	} {
		if !strings.Contains(logs, fragment) {
			t.Errorf("structured logs do not contain %q\n%s", fragment, logs)
		}
	}
	if strings.Contains(logs, "test-token") || strings.Contains(logs, "Authorization") {
		t.Fatalf("structured logs contain authentication material: %s", logs)
	}
}

func TestReadinessChecksDependenciesWhileHealthRemainsLive(t *testing.T) {
	server := newObservedTestServer(
		t,
		nil,
		observability.NewNoopInstrumentation(),
		func(context.Context) error { return errors.New("dependency unavailable") },
	)

	health := request(t, server, http.MethodGet, "/healthz", "", "")
	assertStatus(t, health, http.StatusOK)
	ready := request(t, server, http.MethodGet, "/readyz", "", "")
	assertStatus(t, ready, http.StatusServiceUnavailable)
	assertErrorCode(t, ready, "dependency_unavailable")
}

func TestRetryInstrumentationCountsFailedAndSuccessfulAttempts(t *testing.T) {
	logOutput := &lockedBuffer{}
	instrumentation := observability.NewInstrumentation(
		slog.New(slog.NewJSONHandler(logOutput, nil)),
		observability.NewMetrics(),
		nil,
		nil,
	)
	var attempts atomic.Int32
	server := newObservedTestServer(t, func(registry *execution.HandlerRegistry) {
		if err := registry.Register("retry-once", execution.TaskHandlerFunc(func(context.Context, execution.TaskRequest) (string, error) {
			if attempts.Add(1) == 1 {
				return "", execution.Retryable(errors.New("temporary test failure"))
			}
			return "done", nil
		})); err != nil {
			t.Fatalf("Register() error = %v", err)
		}
	}, instrumentation, nil)
	submitWorkflow(t, server, `{
		"id":"retry-metrics-workflow",
		"tasks":[{"id":"retry-task","name":"Retry task","handler":"retry-once","retry":{"max_attempts":2}}]
	}`)
	created := request(
		t,
		server,
		http.MethodPost,
		"/api/v1/workflows/retry-metrics-workflow/runs",
		`{"run_id":"retry-metrics-run"}`,
		"application/json",
	)
	assertStatus(t, created, http.StatusAccepted)
	waitForEventStream(t, server, "retry-metrics-run")

	metricsResponse := request(t, server, http.MethodGet, "/metrics", "", "")
	metrics := metricsResponse.Body.String()
	for _, sample := range []string{
		`forgeflow_tasks_executed_total{status="failed"} 1`,
		`forgeflow_tasks_executed_total{status="succeeded"} 1`,
		"forgeflow_task_failures_total 1",
		"forgeflow_task_retries_total 1",
		"forgeflow_task_duration_seconds_count 2",
	} {
		if !strings.Contains(metrics, sample) {
			t.Errorf("retry metrics do not contain %q\n%s", sample, metrics)
		}
	}
	if !strings.Contains(logOutput.String(), `"msg":"task retry scheduled"`) {
		t.Fatalf("retry log is missing: %s", logOutput.String())
	}
}

func TestTraceContextConnectsHTTPToSchedulerBrokerWorkerAndPersistence(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Errorf("TracerProvider.Shutdown() error = %v", err)
		}
	})
	instrumentation := observability.NewInstrumentation(
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
		observability.NewMetrics(),
		provider,
		nil,
	)
	server := newObservedTestServer(t, nil, instrumentation, nil)
	submitWorkflow(t, server, `{
		"id":"traced-workflow",
		"tasks":[{"id":"traced-task","name":"Traced task","handler":"noop"}]
	}`)

	const traceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	createRequest := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/workflows/traced-workflow/runs",
		strings.NewReader(`{"run_id":"traced-run"}`),
	)
	createRequest.Header.Set("Authorization", "Bearer test-token")
	createRequest.Header.Set("Content-Type", "application/json")
	createRequest.Header.Set("traceparent", traceParent)
	createResponse := httptest.NewRecorder()
	server.ServeHTTP(createResponse, createRequest)
	assertStatus(t, createResponse, http.StatusAccepted)
	waitForEventStream(t, server, "traced-run")

	wantTraceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatalf("TraceIDFromHex() error = %v", err)
	}
	wantParentID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatalf("SpanIDFromHex() error = %v", err)
	}

	spans := make(map[string]tracetest.SpanStub)
	var traceSpans []tracetest.SpanStub
	for _, span := range exporter.GetSpans() {
		if span.SpanContext.TraceID() == wantTraceID {
			spans[span.Name] = span
			traceSpans = append(traceSpans, span)
		}
	}
	for _, name := range []string{
		"POST /api/v1/workflows/{id}/runs",
		"forgeflow.scheduler.recover",
		"forgeflow.persistence.create_run",
		"forgeflow.persistence.save_run",
		"forgeflow.broker.publish",
		"forgeflow.broker.receive",
		"forgeflow.worker.execute",
	} {
		if _, exists := spans[name]; !exists {
			t.Errorf("trace %s is missing span %q; spans = %v", wantTraceID, name, spanNames(spans))
		}
	}
	httpSpan, exists := spans["POST /api/v1/workflows/{id}/runs"]
	if !exists {
		t.Fatal("HTTP server span is missing")
	}
	if httpSpan.Parent.SpanID() != wantParentID || !httpSpan.Parent.IsRemote() {
		t.Fatalf("HTTP span parent = %s remote=%t, want %s remote", httpSpan.Parent.SpanID(), httpSpan.Parent.IsRemote(), wantParentID)
	}
	publishSpan, publishFound := spans["forgeflow.broker.publish"]
	workerSpan, workerFound := spans["forgeflow.worker.execute"]
	if publishFound && workerFound && workerSpan.Parent.SpanID() != publishSpan.SpanContext.SpanID() {
		t.Fatalf("worker span parent = %s, want broker publish span %s", workerSpan.Parent.SpanID(), publishSpan.SpanContext.SpanID())
	}
	if workerFound {
		persistenceChildFound := false
		for _, span := range traceSpans {
			if span.Name == "forgeflow.persistence.save_run" && span.Parent.SpanID() == workerSpan.SpanContext.SpanID() {
				persistenceChildFound = true
				break
			}
		}
		if !persistenceChildFound {
			t.Fatal("trace has no completion persistence span parented by worker execution")
		}
	}
}

func spanNames(spans map[string]tracetest.SpanStub) []string {
	names := make([]string, 0, len(spans))
	for name := range spans {
		names = append(names, name)
	}
	return names
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (buffer *lockedBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.Write(data)
}

func (buffer *lockedBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.String()
}
