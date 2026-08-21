// Package api exposes ForgeFlow workflow execution through a versioned HTTP API.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/vijaypratap3364/forgeflow/internal/execution"
	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

const (
	maxRequestBody = 1 << 20
	sseKeepAlive   = 15 * time.Second
)

// Server owns the HTTP handler, asynchronous run lifecycle, and live event broker.
type Server struct {
	store      execution.Store
	registry   *execution.HandlerRegistry
	manager    *runManager
	broker     *eventBroker
	mux        *http.ServeMux
	workflowMu sync.Mutex
	ready      atomic.Bool
}

// NewServer creates an HTTP API around a Store and safe task-handler registry.
func NewServer(
	store execution.Store,
	registry *execution.HandlerRegistry,
	workerCount int,
	engineOptions ...execution.EngineOption,
) (*Server, error) {
	if store == nil {
		return nil, errors.New("create API server: store must not be nil")
	}
	if registry == nil {
		return nil, errors.New("create API server: handler registry must not be nil")
	}
	broker := newEventBroker()
	observed := &observedStore{delegate: store, observe: broker.observe}
	engine, err := execution.NewEngine(workerCount, registry, observed, engineOptions...)
	if err != nil {
		return nil, fmt.Errorf("create API execution engine: %w", err)
	}
	server := &Server{
		store:    observed,
		registry: registry,
		manager:  newRunManager(engine),
		broker:   broker,
		mux:      http.NewServeMux(),
	}
	server.routes()
	server.ready.Store(true)
	return server, nil
}

// ServeHTTP dispatches one ForgeFlow API request.
func (server *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	server.mux.ServeHTTP(writer, request)
}

// SetReady changes the readiness probe state during process lifecycle changes.
func (server *Server) SetReady(ready bool) {
	server.ready.Store(ready)
}

// Shutdown rejects readiness, cancels active runs, and closes live event streams.
func (server *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("shut down API server: context must not be nil")
	}
	server.ready.Store(false)
	err := server.manager.shutdown(ctx)
	server.broker.close()
	return err
}

func (server *Server) routes() {
	server.mux.HandleFunc("/healthz", server.handleHealth)
	server.mux.HandleFunc("/readyz", server.handleReady)
	server.mux.HandleFunc("/api/v1/workflows", server.handleWorkflows)
	server.mux.HandleFunc("/api/v1/workflows/{id}", server.handleWorkflow)
	server.mux.HandleFunc("/api/v1/workflows/{id}/runs", server.handleWorkflowRuns)
	server.mux.HandleFunc("/api/v1/runs/{runID}", server.handleRun)
	server.mux.HandleFunc("/api/v1/runs/{runID}/tasks", server.handleRunTasks)
	server.mux.HandleFunc("/api/v1/runs/{runID}/cancel", server.handleRunCancellation)
	server.mux.HandleFunc("/api/v1/runs/{runID}/events", server.handleRunEvents)
	server.mux.HandleFunc("/", server.handleNotFound)
}

func (server *Server) handleHealth(writer http.ResponseWriter, request *http.Request) {
	if !requireMethod(writer, request, http.MethodGet) {
		return
	}
	writeJSON(writer, http.StatusOK, StatusResponse{Status: "ok"})
}

func (server *Server) handleReady(writer http.ResponseWriter, request *http.Request) {
	if !requireMethod(writer, request, http.MethodGet) {
		return
	}
	if !server.ready.Load() {
		writeAPIError(writer, http.StatusServiceUnavailable, "not_ready", "ForgeFlow is shutting down", "")
		return
	}
	writeJSON(writer, http.StatusOK, StatusResponse{Status: "ready"})
}

func (server *Server) handleWorkflows(writer http.ResponseWriter, request *http.Request) {
	if !requireMethod(writer, request, http.MethodPost) {
		return
	}
	if !requireJSONContentType(writer, request, false) {
		return
	}
	var payload WorkflowRequest
	if !decodeJSONBody(writer, request, &payload, false) {
		return
	}
	definition, err := payload.definition()
	if err != nil {
		writeRequestError(writer, err)
		return
	}
	if !validResourceID(string(definition.ID)) {
		writeAPIError(writer, http.StatusUnprocessableEntity, "invalid_workflow_id", "workflow ID may contain only letters, digits, dot, underscore, tilde, and hyphen", "id")
		return
	}
	if err := definition.Validate(); err != nil {
		writeRequestError(writer, err)
		return
	}
	if err := server.validateHandlers(definition); err != nil {
		writeRequestError(writer, err)
		return
	}

	server.workflowMu.Lock()
	defer server.workflowMu.Unlock()
	_, found, err := server.store.LoadWorkflow(request.Context(), definition.ID)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	if found {
		writeAPIError(writer, http.StatusConflict, "workflow_exists", "workflow already exists", "id")
		return
	}
	if err := server.store.SaveWorkflow(request.Context(), definition); err != nil {
		writeStoreError(writer, err)
		return
	}
	writer.Header().Set("Location", "/api/v1/workflows/"+string(definition.ID))
	writeJSON(writer, http.StatusCreated, workflowDTO(definition))
}

func (server *Server) handleWorkflow(writer http.ResponseWriter, request *http.Request) {
	if !requireMethod(writer, request, http.MethodGet) {
		return
	}
	workflowID := workflow.WorkflowID(request.PathValue("id"))
	if !validResourceID(string(workflowID)) {
		writeAPIError(writer, http.StatusBadRequest, "invalid_workflow_id", "workflow ID is invalid", "id")
		return
	}
	definition, found, err := server.store.LoadWorkflow(request.Context(), workflowID)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	if !found {
		writeAPIError(writer, http.StatusNotFound, "workflow_not_found", "workflow was not found", "id")
		return
	}
	writeJSON(writer, http.StatusOK, workflowDTO(definition))
}

func (server *Server) handleWorkflowRuns(writer http.ResponseWriter, request *http.Request) {
	if !requireMethod(writer, request, http.MethodPost) {
		return
	}
	workflowID := workflow.WorkflowID(request.PathValue("id"))
	if !validResourceID(string(workflowID)) {
		writeAPIError(writer, http.StatusBadRequest, "invalid_workflow_id", "workflow ID is invalid", "id")
		return
	}
	definition, found, err := server.store.LoadWorkflow(request.Context(), workflowID)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	if !found {
		writeAPIError(writer, http.StatusNotFound, "workflow_not_found", "workflow was not found", "id")
		return
	}
	if err := server.validateHandlers(definition); err != nil {
		writeRequestError(writer, err)
		return
	}

	var payload CreateRunRequest
	if !decodeOptionalJSONBody(writer, request, &payload) {
		return
	}
	runID := execution.RunID(payload.RunID)
	if runID == "" {
		runID, err = generateRunID()
		if err != nil {
			writeAPIError(writer, http.StatusInternalServerError, "internal_error", "could not generate a workflow run ID", "")
			return
		}
	}
	if !validResourceID(string(runID)) {
		writeAPIError(writer, http.StatusBadRequest, "invalid_run_id", "run ID is invalid", "run_id")
		return
	}
	run, err := execution.NewWorkflowRun(runID, definition)
	if err != nil {
		writeRequestError(writer, err)
		return
	}
	stored, err := server.store.CreateRun(request.Context(), run.Snapshot())
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	if err := server.manager.start(runID); err != nil {
		if errors.Is(err, errManagerClosed) {
			writeAPIError(writer, http.StatusServiceUnavailable, "not_ready", "ForgeFlow is shutting down", "")
			return
		}
		writeAPIError(writer, http.StatusConflict, "run_active", "workflow run is already active", "run_id")
		return
	}
	writer.Header().Set("Location", "/api/v1/runs/"+string(runID))
	writeJSON(writer, http.StatusAccepted, runDTO(stored))
}

func (server *Server) handleRun(writer http.ResponseWriter, request *http.Request) {
	if !requireMethod(writer, request, http.MethodGet) {
		return
	}
	snapshot, found := server.loadRun(writer, request)
	if !found {
		return
	}
	writeJSON(writer, http.StatusOK, runDTO(snapshot))
}

func (server *Server) handleRunTasks(writer http.ResponseWriter, request *http.Request) {
	if !requireMethod(writer, request, http.MethodGet) {
		return
	}
	snapshot, found := server.loadRun(writer, request)
	if !found {
		return
	}
	writeJSON(writer, http.StatusOK, taskRunsDTO(snapshot))
}

func (server *Server) handleRunCancellation(writer http.ResponseWriter, request *http.Request) {
	if !requireMethod(writer, request, http.MethodPost) {
		return
	}
	snapshot, found := server.loadRun(writer, request)
	if !found {
		return
	}
	if terminalWorkflowStatus(snapshot.Status) {
		writeAPIError(writer, http.StatusConflict, "run_finished", "workflow run is already finished", "runID")
		return
	}
	if !server.manager.cancel(snapshot.ID) {
		if err := server.manager.start(snapshot.ID); err != nil && !errors.Is(err, errRunAlreadyActive) {
			writeAPIError(writer, http.StatusServiceUnavailable, "not_ready", "workflow run cannot be canceled while ForgeFlow is shutting down", "")
			return
		}
		server.manager.cancel(snapshot.ID)
	}
	writeJSON(writer, http.StatusAccepted, CancellationResponse{
		RunID:  snapshot.ID,
		Status: "cancellation_requested",
	})
}

func (server *Server) handleRunEvents(writer http.ResponseWriter, request *http.Request) {
	if !requireMethod(writer, request, http.MethodGet) {
		return
	}
	snapshot, found := server.loadRun(writer, request)
	if !found {
		return
	}
	lastSequence, err := lastEventSequence(request.Header.Get("Last-Event-ID"))
	if err != nil {
		writeAPIError(writer, http.StatusBadRequest, "invalid_last_event_id", "Last-Event-ID must be a non-negative integer", "Last-Event-ID")
		return
	}
	flusher, supported := writer.(http.Flusher)
	if !supported {
		writeAPIError(writer, http.StatusInternalServerError, "streaming_unsupported", "HTTP streaming is unavailable", "")
		return
	}

	server.broker.seed(snapshot)
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.WriteHeader(http.StatusOK)
	flusher.Flush()

	keepAlive := time.NewTicker(sseKeepAlive)
	defer keepAlive.Stop()
	sequence := lastSequence
	for {
		events, notify, closed, terminal := server.broker.eventsAfter(snapshot.ID, sequence)
		for _, event := range events {
			if err := writeServerSentEvent(writer, event); err != nil {
				return
			}
			sequence = event.Sequence
		}
		if len(events) > 0 {
			flusher.Flush()
		}
		if terminal || closed {
			return
		}
		select {
		case <-request.Context().Done():
			return
		case <-notify:
		case <-keepAlive.C:
			if _, err := io.WriteString(writer, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (server *Server) handleNotFound(writer http.ResponseWriter, _ *http.Request) {
	writeAPIError(writer, http.StatusNotFound, "route_not_found", "API route was not found", "")
}

func (server *Server) loadRun(
	writer http.ResponseWriter,
	request *http.Request,
) (execution.WorkflowRunSnapshot, bool) {
	runID := execution.RunID(request.PathValue("runID"))
	if !validResourceID(string(runID)) {
		writeAPIError(writer, http.StatusBadRequest, "invalid_run_id", "run ID is invalid", "runID")
		return execution.WorkflowRunSnapshot{}, false
	}
	snapshot, found, err := server.store.LoadRun(request.Context(), runID)
	if err != nil {
		writeStoreError(writer, err)
		return execution.WorkflowRunSnapshot{}, false
	}
	if !found {
		writeAPIError(writer, http.StatusNotFound, "run_not_found", "workflow run was not found", "runID")
		return execution.WorkflowRunSnapshot{}, false
	}
	return snapshot, true
}

func (server *Server) validateHandlers(definition workflow.WorkflowDefinition) error {
	for index, task := range definition.Tasks {
		if !validIdentifier(string(task.Handler)) {
			return &requestValidationError{
				field:   fmt.Sprintf("tasks[%d].handler", index),
				message: "handler must be a non-empty identifier",
			}
		}
		if _, exists := server.registry.Handler(task.Handler); !exists {
			return &execution.UnknownHandlerError{TaskID: task.ID, HandlerName: task.Handler}
		}
	}
	return nil
}

func generateRunID() (execution.RunID, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate run ID: %w", err)
	}
	return execution.RunID("run-" + hex.EncodeToString(bytes)), nil
}

func lastEventSequence(value string) (uint64, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	return strconv.ParseUint(value, 10, 64)
}

func writeServerSentEvent(writer io.Writer, event RunEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode workflow event: %w", err)
	}
	if _, err := fmt.Fprintf(writer, "id: %d\nevent: %s\ndata: %s\n\n", event.Sequence, event.Type, payload); err != nil {
		return fmt.Errorf("write workflow event: %w", err)
	}
	return nil
}

func requireMethod(writer http.ResponseWriter, request *http.Request, method string) bool {
	if request.Method == method {
		return true
	}
	writer.Header().Set("Allow", method)
	writeAPIError(writer, http.StatusMethodNotAllowed, "method_not_allowed", "HTTP method is not allowed for this route", "")
	return false
}

func requireJSONContentType(writer http.ResponseWriter, request *http.Request, optional bool) bool {
	if optional && request.ContentLength == 0 {
		return true
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeAPIError(writer, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json", "Content-Type")
		return false
	}
	return true
}

func decodeOptionalJSONBody(writer http.ResponseWriter, request *http.Request, destination any) bool {
	if request.Body == nil || request.ContentLength == 0 {
		return true
	}
	if !requireJSONContentType(writer, request, true) {
		return false
	}
	return decodeJSONBody(writer, request, destination, false)
}

func decodeJSONBody(writer http.ResponseWriter, request *http.Request, destination any, optional bool) bool {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBody)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		if optional && errors.Is(err, io.EOF) {
			return true
		}
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writeAPIError(writer, http.StatusRequestEntityTooLarge, "payload_too_large", "request body exceeds 1 MiB", "")
			return false
		}
		writeAPIError(writer, http.StatusBadRequest, "malformed_json", "request body must contain one valid JSON object: "+err.Error(), "")
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeAPIError(writer, http.StatusBadRequest, "malformed_json", "request body must contain exactly one JSON object", "")
		return false
	}
	return true
}

func writeRequestError(writer http.ResponseWriter, err error) {
	var validationError *workflow.ValidationError
	if errors.As(err, &validationError) {
		writeAPIError(writer, http.StatusUnprocessableEntity, string(validationError.Code), validationError.Error(), "")
		return
	}
	var requestError *requestValidationError
	if errors.As(err, &requestError) {
		writeAPIError(writer, http.StatusUnprocessableEntity, "invalid_request", requestError.message, requestError.field)
		return
	}
	var handlerError *execution.UnknownHandlerError
	if errors.As(err, &handlerError) {
		writeAPIError(writer, http.StatusUnprocessableEntity, "unknown_handler", handlerError.Error(), "handler")
		return
	}
	var runIDError *execution.InvalidRunIDError
	if errors.As(err, &runIDError) {
		writeAPIError(writer, http.StatusBadRequest, "invalid_run_id", runIDError.Error(), "run_id")
		return
	}
	writeAPIError(writer, http.StatusBadRequest, "invalid_request", err.Error(), "")
}

func writeStoreError(writer http.ResponseWriter, err error) {
	var conflictError *execution.WorkflowConflictError
	var runExistsError *execution.RunAlreadyExistsError
	switch {
	case errors.As(err, &conflictError):
		writeAPIError(writer, http.StatusConflict, "workflow_conflict", conflictError.Error(), "id")
	case errors.As(err, &runExistsError):
		writeAPIError(writer, http.StatusConflict, "run_exists", runExistsError.Error(), "run_id")
	case errors.Is(err, context.DeadlineExceeded):
		writeAPIError(writer, http.StatusRequestTimeout, "request_timeout", "request deadline expired", "")
	case errors.Is(err, context.Canceled):
		writeAPIError(writer, http.StatusRequestTimeout, "request_canceled", "request was canceled", "")
	default:
		writeAPIError(writer, http.StatusInternalServerError, "internal_error", "durable state operation failed", "")
	}
}

func writeAPIError(writer http.ResponseWriter, status int, code, message, field string) {
	writeJSON(writer, status, ErrorResponse{Error: APIError{
		Code:    code,
		Message: message,
		Field:   field,
	}})
}

func writeJSON(writer http.ResponseWriter, status int, payload any) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		status = http.StatusInternalServerError
		encoded = []byte(`{"error":{"code":"internal_error","message":"response encoding failed"}}`)
	}
	encoded = append(encoded, '\n')
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.WriteHeader(status)
	if _, err := writer.Write(encoded); err != nil {
		return
	}
}

func validIdentifier(identifier string) bool {
	if identifier == "" {
		return false
	}
	for _, character := range identifier {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validResourceID(identifier string) bool {
	if identifier == "" {
		return false
	}
	for _, character := range identifier {
		switch {
		case character >= 'a' && character <= 'z':
		case character >= 'A' && character <= 'Z':
		case character >= '0' && character <= '9':
		case character == '.', character == '_', character == '~', character == '-':
		default:
			return false
		}
	}
	return true
}
