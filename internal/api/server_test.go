package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/execution"
	"github.com/vijaypratap3364/forgeflow/internal/persistence"
)

func TestHealthReadinessAndStructuredRoutingErrors(t *testing.T) {
	server := newTestServer(t, nil)

	health := request(t, server, http.MethodGet, "/healthz", "", "")
	assertStatus(t, health, http.StatusOK)
	assertJSONContentType(t, health)

	ready := request(t, server, http.MethodGet, "/readyz", "", "")
	assertStatus(t, ready, http.StatusOK)
	server.SetReady(false)
	notReady := request(t, server, http.MethodGet, "/readyz", "", "")
	assertStatus(t, notReady, http.StatusServiceUnavailable)
	assertErrorCode(t, notReady, "not_ready")
	server.SetReady(true)

	wrongMethod := request(t, server, http.MethodPost, "/healthz", "", "")
	assertStatus(t, wrongMethod, http.StatusMethodNotAllowed)
	assertErrorCode(t, wrongMethod, "method_not_allowed")
	if got := wrongMethod.Header().Get("Allow"); got != http.MethodGet {
		t.Fatalf("Allow = %q, want %q", got, http.MethodGet)
	}

	notFound := request(t, server, http.MethodGet, "/missing", "", "")
	assertStatus(t, notFound, http.StatusNotFound)
	assertErrorCode(t, notFound, "route_not_found")
}

func TestWorkflowSubmissionValidationAndRetrieval(t *testing.T) {
	server := newTestServer(t, nil)

	tests := []struct {
		name        string
		body        string
		contentType string
		wantStatus  int
		wantCode    string
	}{
		{
			name:        "malformed JSON",
			body:        `{`,
			contentType: "application/json",
			wantStatus:  http.StatusBadRequest,
			wantCode:    "malformed_json",
		},
		{
			name:        "unknown field",
			body:        `{"id":"unknown-field","tasks":[],"extra":true}`,
			contentType: "application/json",
			wantStatus:  http.StatusBadRequest,
			wantCode:    "malformed_json",
		},
		{
			name:        "path-unsafe workflow ID",
			body:        `{"id":"unsafe/id","tasks":[{"id":"a","name":"A","handler":"noop"}]}`,
			contentType: "application/json",
			wantStatus:  http.StatusUnprocessableEntity,
			wantCode:    "invalid_workflow_id",
		},
		{
			name:       "missing content type",
			body:       `{"id":"missing-content-type","tasks":[]}`,
			wantStatus: http.StatusUnsupportedMediaType,
			wantCode:   "unsupported_media_type",
		},
		{
			name: "invalid dependency",
			body: `{
				"id":"invalid-dag",
				"tasks":[{"id":"b","name":"B","handler":"noop","dependencies":["a"]}]
			}`,
			contentType: "application/json",
			wantStatus:  http.StatusUnprocessableEntity,
			wantCode:    "missing_dependency",
		},
		{
			name: "duplicate task IDs",
			body: `{
				"id":"duplicate-tasks",
				"tasks":[
					{"id":"a","name":"First","handler":"noop"},
					{"id":"a","name":"Second","handler":"noop"}
				]
			}`,
			contentType: "application/json",
			wantStatus:  http.StatusUnprocessableEntity,
			wantCode:    "duplicate_task_id",
		},
		{
			name: "invalid duration",
			body: `{
				"id":"invalid-duration",
				"tasks":[{"id":"a","name":"A","handler":"noop","retry":{"initial_backoff":"later"}}]
			}`,
			contentType: "application/json",
			wantStatus:  http.StatusUnprocessableEntity,
			wantCode:    "invalid_request",
		},
		{
			name: "unknown handler",
			body: `{
				"id":"unknown-handler",
				"tasks":[{"id":"a","name":"A","handler":"shell"}]
			}`,
			contentType: "application/json",
			wantStatus:  http.StatusUnprocessableEntity,
			wantCode:    "unknown_handler",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := request(t, server, http.MethodPost, "/api/v1/workflows", test.body, test.contentType)
			assertStatus(t, response, test.wantStatus)
			assertErrorCode(t, response, test.wantCode)
		})
	}

	body := `{
		"id":"text-pipeline",
		"tasks":[
			{"id":"transform","name":"Uppercase","handler":"uppercase","input":"hello","dependencies":["start"],"retry":{"max_attempts":3,"initial_backoff":"10ms","max_backoff":"20ms"}},
			{"id":"start","name":"Start","handler":"noop"}
		]
	}`
	created := request(t, server, http.MethodPost, "/api/v1/workflows", body, "application/json")
	assertStatus(t, created, http.StatusCreated)
	if got := created.Header().Get("Location"); got != "/api/v1/workflows/text-pipeline" {
		t.Fatalf("Location = %q, want workflow URL", got)
	}
	var workflowResponse WorkflowResponse
	decodeResponse(t, created, &workflowResponse)
	if workflowResponse.ID != "text-pipeline" || len(workflowResponse.Tasks) != 2 {
		t.Fatalf("workflow response = %#v", workflowResponse)
	}
	if got := workflowResponse.Tasks[1].Retry.MaxAttempts; got != 3 {
		t.Fatalf("retry max attempts = %d, want 3", got)
	}

	loaded := request(t, server, http.MethodGet, "/api/v1/workflows/text-pipeline", "", "")
	assertStatus(t, loaded, http.StatusOK)
	duplicate := request(t, server, http.MethodPost, "/api/v1/workflows", body, "application/json")
	assertStatus(t, duplicate, http.StatusConflict)
	assertErrorCode(t, duplicate, "workflow_exists")
	missing := request(t, server, http.MethodGet, "/api/v1/workflows/missing", "", "")
	assertStatus(t, missing, http.StatusNotFound)
	assertErrorCode(t, missing, "workflow_not_found")
}

func TestWorkflowRunLifecycleTasksAndSSE(t *testing.T) {
	server := newTestServer(t, nil)
	submitWorkflow(t, server, `{
		"id":"single-task",
		"tasks":[{"id":"transform","name":"Uppercase","handler":"uppercase","input":"hello forgeflow"}]
	}`)

	created := request(
		t,
		server,
		http.MethodPost,
		"/api/v1/workflows/single-task/runs",
		`{"run_id":"api-run-1"}`,
		"application/json",
	)
	assertStatus(t, created, http.StatusAccepted)
	if got := created.Header().Get("Location"); got != "/api/v1/runs/api-run-1" {
		t.Fatalf("Location = %q, want run URL", got)
	}

	eventBody := waitForEventStream(t, server, "api-run-1")
	eventTypes := parseEventTypes(eventBody)
	wantEvents := []string{
		eventWorkflowStarted,
		eventTaskReady,
		eventTaskStarted,
		eventTaskSucceeded,
		eventWorkflowCompleted,
	}
	if !slices.Equal(eventTypes, wantEvents) {
		t.Fatalf("SSE event types = %v, want %v\nstream:\n%s", eventTypes, wantEvents, eventBody)
	}
	replayRequest := httptest.NewRequest(http.MethodGet, "/api/v1/runs/api-run-1/events", nil)
	replayRequest.Header.Set("Last-Event-ID", "3")
	replay := httptest.NewRecorder()
	server.ServeHTTP(replay, replayRequest)
	assertStatus(t, replay, http.StatusOK)
	if got, want := parseEventTypes(replay.Body.String()), []string{eventTaskSucceeded, eventWorkflowCompleted}; !slices.Equal(got, want) {
		t.Fatalf("resumed SSE event types = %v, want %v", got, want)
	}

	status := request(t, server, http.MethodGet, "/api/v1/runs/api-run-1", "", "")
	assertStatus(t, status, http.StatusOK)
	var runResponse RunResponse
	decodeResponse(t, status, &runResponse)
	if runResponse.Status != execution.WorkflowRunSucceeded || runResponse.Tasks.Succeeded != 1 {
		t.Fatalf("run response = %#v", runResponse)
	}

	tasks := request(t, server, http.MethodGet, "/api/v1/runs/api-run-1/tasks", "", "")
	assertStatus(t, tasks, http.StatusOK)
	var taskResponse TaskRunsResponse
	decodeResponse(t, tasks, &taskResponse)
	if len(taskResponse.Tasks) != 1 {
		t.Fatalf("task count = %d, want 1", len(taskResponse.Tasks))
	}
	task := taskResponse.Tasks[0]
	if task.Status != execution.TaskRunSucceeded || task.Output != "HELLO FORGEFLOW" || task.AttemptCount != 1 {
		t.Fatalf("task response = %#v", task)
	}
	if task.TaskRunID == "" || task.CurrentAttemptID == "" {
		t.Fatalf("task response lacks stable execution identities: %#v", task)
	}

	duplicate := request(
		t,
		server,
		http.MethodPost,
		"/api/v1/workflows/single-task/runs",
		`{"run_id":"api-run-1"}`,
		"application/json",
	)
	assertStatus(t, duplicate, http.StatusConflict)
	assertErrorCode(t, duplicate, "run_exists")
	autoCreated := request(t, server, http.MethodPost, "/api/v1/workflows/single-task/runs", "", "")
	assertStatus(t, autoCreated, http.StatusAccepted)
	var autoRun RunResponse
	decodeResponse(t, autoCreated, &autoRun)
	if !strings.HasPrefix(string(autoRun.ID), "run-") {
		t.Fatalf("generated run ID = %q, want run- prefix", autoRun.ID)
	}
	waitForEventStream(t, server, string(autoRun.ID))

	cancelFinished := request(t, server, http.MethodPost, "/api/v1/runs/api-run-1/cancel", "", "")
	assertStatus(t, cancelFinished, http.StatusConflict)
	assertErrorCode(t, cancelFinished, "run_finished")
}

func TestWorkflowRunCancellation(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	var once sync.Once
	server := newTestServer(t, func(registry *execution.HandlerRegistry) {
		if err := registry.Register("blocking", execution.TaskHandlerFunc(func(ctx context.Context, _ execution.TaskRequest) (string, error) {
			once.Do(func() { close(started) })
			<-ctx.Done()
			close(stopped)
			return "", ctx.Err()
		})); err != nil {
			t.Fatalf("Register() error = %v", err)
		}
	})
	submitWorkflow(t, server, `{
		"id":"cancelable",
		"tasks":[{"id":"wait","name":"Wait","handler":"blocking"}]
	}`)
	created := request(
		t,
		server,
		http.MethodPost,
		"/api/v1/workflows/cancelable/runs",
		`{"run_id":"cancel-run"}`,
		"application/json",
	)
	assertStatus(t, created, http.StatusAccepted)
	waitForSignal(t, started, "task start")

	canceled := request(t, server, http.MethodPost, "/api/v1/runs/cancel-run/cancel", "", "")
	assertStatus(t, canceled, http.StatusAccepted)
	var cancellation CancellationResponse
	decodeResponse(t, canceled, &cancellation)
	if cancellation.Status != "cancellation_requested" {
		t.Fatalf("cancellation response = %#v", cancellation)
	}
	waitForSignal(t, stopped, "handler cancellation")
	eventBody := waitForEventStream(t, server, "cancel-run")
	if !slices.Contains(parseEventTypes(eventBody), eventWorkflowCompleted) {
		t.Fatalf("cancellation event stream lacks workflow completion:\n%s", eventBody)
	}

	status := request(t, server, http.MethodGet, "/api/v1/runs/cancel-run", "", "")
	var runResponse RunResponse
	decodeResponse(t, status, &runResponse)
	if runResponse.Status != execution.WorkflowRunCanceled || runResponse.Tasks.Canceled != 1 {
		t.Fatalf("run response = %#v, want canceled", runResponse)
	}
}

func TestRunEventStreamStopsOnClientDisconnect(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	server := newTestServer(t, func(registry *execution.HandlerRegistry) {
		if err := registry.Register("disconnect-block", execution.TaskHandlerFunc(func(ctx context.Context, _ execution.TaskRequest) (string, error) {
			close(started)
			<-ctx.Done()
			close(stopped)
			return "", ctx.Err()
		})); err != nil {
			t.Fatalf("Register() error = %v", err)
		}
	})
	submitWorkflow(t, server, `{
		"id":"disconnect-workflow",
		"tasks":[{"id":"wait","name":"Wait","handler":"disconnect-block"}]
	}`)
	created := request(
		t,
		server,
		http.MethodPost,
		"/api/v1/workflows/disconnect-workflow/runs",
		`{"run_id":"disconnect-run"}`,
		"application/json",
	)
	assertStatus(t, created, http.StatusAccepted)
	waitForSignal(t, started, "task start before SSE disconnect")

	streamContext, disconnect := context.WithCancel(context.Background())
	streamRequest := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/runs/disconnect-run/events",
		nil,
	).WithContext(streamContext)
	streamWriter := newFlushSignalRecorder()
	streamReturned := make(chan struct{})
	go func() {
		server.ServeHTTP(streamWriter, streamRequest)
		close(streamReturned)
	}()
	waitForSignal(t, streamWriter.flushed, "SSE response flush")
	disconnect()
	waitForSignal(t, streamReturned, "SSE handler return after client disconnect")

	canceled := request(t, server, http.MethodPost, "/api/v1/runs/disconnect-run/cancel", "", "")
	assertStatus(t, canceled, http.StatusAccepted)
	waitForSignal(t, stopped, "handler cancellation after SSE disconnect")
}

func TestServerShutdownCancelsActiveRun(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	server := newTestServer(t, func(registry *execution.HandlerRegistry) {
		if err := registry.Register("shutdown-block", execution.TaskHandlerFunc(func(ctx context.Context, _ execution.TaskRequest) (string, error) {
			close(started)
			<-ctx.Done()
			close(stopped)
			return "", ctx.Err()
		})); err != nil {
			t.Fatalf("Register() error = %v", err)
		}
	})
	submitWorkflow(t, server, `{
		"id":"shutdown-workflow",
		"tasks":[{"id":"wait","name":"Wait","handler":"shutdown-block"}]
	}`)
	created := request(
		t,
		server,
		http.MethodPost,
		"/api/v1/workflows/shutdown-workflow/runs",
		`{"run_id":"shutdown-run"}`,
		"application/json",
	)
	assertStatus(t, created, http.StatusAccepted)
	waitForSignal(t, started, "task start before shutdown")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	waitForSignal(t, stopped, "handler stop during shutdown")

	status := request(t, server, http.MethodGet, "/api/v1/runs/shutdown-run", "", "")
	assertStatus(t, status, http.StatusOK)
	var runResponse RunResponse
	decodeResponse(t, status, &runResponse)
	if runResponse.Status != execution.WorkflowRunCanceled {
		t.Fatalf("run status = %q, want canceled", runResponse.Status)
	}
	ready := request(t, server, http.MethodGet, "/readyz", "", "")
	assertStatus(t, ready, http.StatusServiceUnavailable)
}

func TestRunRequestErrorsAndMissingResources(t *testing.T) {
	server := newTestServer(t, nil)
	submitWorkflow(t, server, `{
		"id":"known",
		"tasks":[{"id":"a","name":"A","handler":"noop"}]
	}`)

	malformed := request(
		t,
		server,
		http.MethodPost,
		"/api/v1/workflows/known/runs",
		`{"run_id":`,
		"application/json",
	)
	assertStatus(t, malformed, http.StatusBadRequest)
	assertErrorCode(t, malformed, "malformed_json")
	invalidRunID := request(
		t,
		server,
		http.MethodPost,
		"/api/v1/workflows/known/runs",
		`{"run_id":"unsafe/id"}`,
		"application/json",
	)
	assertStatus(t, invalidRunID, http.StatusBadRequest)
	assertErrorCode(t, invalidRunID, "invalid_run_id")

	unknownWorkflow := request(t, server, http.MethodPost, "/api/v1/workflows/missing/runs", "", "")
	assertStatus(t, unknownWorkflow, http.StatusNotFound)
	assertErrorCode(t, unknownWorkflow, "workflow_not_found")

	for _, path := range []string{
		"/api/v1/runs/missing",
		"/api/v1/runs/missing/tasks",
		"/api/v1/runs/missing/cancel",
		"/api/v1/runs/missing/events",
	} {
		method := http.MethodGet
		if strings.HasSuffix(path, "/cancel") {
			method = http.MethodPost
		}
		response := request(t, server, method, path, "", "")
		assertStatus(t, response, http.StatusNotFound)
		assertErrorCode(t, response, "run_not_found")
	}
}

func newTestServer(
	t *testing.T,
	configure func(*execution.HandlerRegistry),
) *Server {
	t.Helper()
	store, err := persistence.OpenFileStore(t.TempDir() + "/forgeflow.ffdb")
	if err != nil {
		t.Fatalf("OpenFileStore() error = %v", err)
	}
	registry, err := execution.NewDemoHandlerRegistry()
	if err != nil {
		t.Fatalf("NewDemoHandlerRegistry() error = %v", err)
	}
	if configure != nil {
		configure(registry)
	}
	server, err := NewServer(store, registry, 2)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})
	return server
}

func submitWorkflow(t *testing.T, server http.Handler, body string) {
	t.Helper()
	response := request(t, server, http.MethodPost, "/api/v1/workflows", body, "application/json")
	assertStatus(t, response, http.StatusCreated)
}

func request(
	t *testing.T,
	handler http.Handler,
	method string,
	path string,
	body string,
	contentType string,
) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	httpRequest := httptest.NewRequest(method, path, reader)
	if contentType != "" {
		httpRequest.Header.Set("Content-Type", contentType)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httpRequest)
	return recorder
}

func waitForEventStream(t *testing.T, handler http.Handler, runID string) string {
	t.Helper()
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		result <- request(t, handler, http.MethodGet, "/api/v1/runs/"+runID+"/events", "", "")
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case recorder := <-result:
		assertStatus(t, recorder, http.StatusOK)
		if got := recorder.Header().Get("Content-Type"); got != "text/event-stream" {
			t.Fatalf("SSE Content-Type = %q", got)
		}
		return recorder.Body.String()
	case <-timer.C:
		t.Fatal("timed out waiting for terminal workflow event")
		return ""
	}
}

func parseEventTypes(body string) []string {
	var eventTypes []string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "event: ") {
			eventTypes = append(eventTypes, strings.TrimPrefix(line, "event: "))
		}
	}
	return eventTypes
}

func waitForSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
	}
}

func assertStatus(t *testing.T, response *httptest.ResponseRecorder, want int) {
	t.Helper()
	if response.Code != want {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, want, response.Body.String())
	}
}

func assertJSONContentType(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if got := response.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}
}

func assertErrorCode(t *testing.T, response *httptest.ResponseRecorder, want string) {
	t.Helper()
	var payload ErrorResponse
	decodeResponse(t, response, &payload)
	if payload.Error.Code != want {
		t.Fatalf("error code = %q, want %q", payload.Error.Code, want)
	}
}

func decodeResponse(t *testing.T, response *httptest.ResponseRecorder, destination any) {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(response.Body.Bytes()))
	if err := decoder.Decode(destination); err != nil {
		t.Fatalf("decode response %q: %v", response.Body.String(), err)
	}
}

type flushSignalRecorder struct {
	*httptest.ResponseRecorder
	flushed chan struct{}
	once    sync.Once
}

func newFlushSignalRecorder() *flushSignalRecorder {
	return &flushSignalRecorder{
		ResponseRecorder: httptest.NewRecorder(),
		flushed:          make(chan struct{}),
	}
}

func (recorder *flushSignalRecorder) Flush() {
	recorder.ResponseRecorder.Flush()
	recorder.once.Do(func() { close(recorder.flushed) })
}
