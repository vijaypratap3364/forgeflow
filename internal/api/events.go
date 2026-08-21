package api

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/execution"
	"github.com/vijaypratap3364/forgeflow/internal/observability"
	"github.com/vijaypratap3364/forgeflow/internal/workflow"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	eventWorkflowStarted   = "workflow_started"
	eventTaskReady         = "task_ready"
	eventTaskStarted       = "task_started"
	eventTaskSucceeded     = "task_succeeded"
	eventTaskFailed        = "task_failed"
	eventTaskCanceled      = "task_canceled"
	eventRetryScheduled    = "retry_scheduled"
	eventWorkflowCompleted = "workflow_completed"
	eventRunSnapshot       = "run_snapshot"
)

// RunEvent is one persisted workflow transition exposed through SSE.
type RunEvent struct {
	Sequence           uint64                      `json:"sequence"`
	Type               string                      `json:"type"`
	RunID              execution.RunID             `json:"run_id"`
	WorkflowID         workflow.WorkflowID         `json:"workflow_id"`
	TaskID             workflow.TaskID             `json:"task_id,omitempty"`
	Status             execution.WorkflowRunStatus `json:"status,omitempty"`
	TaskStatus         execution.TaskRunStatus     `json:"task_status,omitempty"`
	AttemptID          execution.AttemptID         `json:"attempt_id,omitempty"`
	OccurredAt         time.Time                   `json:"occurred_at"`
	previousTaskStatus execution.TaskRunStatus
	workerID           execution.WorkerID
}

type runEventStream struct {
	latest execution.WorkflowRunSnapshot
	events []RunEvent
	notify chan struct{}
}

type eventBroker struct {
	mu      sync.Mutex
	streams map[execution.RunID]*runEventStream
	closed  bool
}

func newEventBroker() *eventBroker {
	return &eventBroker{streams: make(map[execution.RunID]*runEventStream)}
}

func (broker *eventBroker) observe(snapshot execution.WorkflowRunSnapshot) []RunEvent {
	broker.mu.Lock()
	defer broker.mu.Unlock()

	if broker.closed {
		return nil
	}
	stream, exists := broker.streams[snapshot.ID]
	if !exists {
		broker.streams[snapshot.ID] = &runEventStream{
			latest: cloneRunSnapshot(snapshot),
			notify: make(chan struct{}),
		}
		return nil
	}

	events := deriveRunEvents(stream.latest, snapshot)
	stream.latest = cloneRunSnapshot(snapshot)
	if len(events) == 0 {
		return nil
	}
	for index := range events {
		events[index].Sequence = uint64(len(stream.events) + 1)
		stream.events = append(stream.events, events[index])
	}
	close(stream.notify)
	stream.notify = make(chan struct{})
	return append([]RunEvent(nil), events...)
}

func (broker *eventBroker) seed(snapshot execution.WorkflowRunSnapshot) {
	broker.mu.Lock()
	defer broker.mu.Unlock()

	if broker.closed {
		return
	}
	if _, exists := broker.streams[snapshot.ID]; exists {
		return
	}
	stream := &runEventStream{
		latest: cloneRunSnapshot(snapshot),
		notify: make(chan struct{}),
	}
	stream.events = append(stream.events, RunEvent{
		Sequence:   1,
		Type:       eventRunSnapshot,
		RunID:      snapshot.ID,
		WorkflowID: snapshot.WorkflowID,
		Status:     snapshot.Status,
		OccurredAt: snapshot.UpdatedAt,
	})
	broker.streams[snapshot.ID] = stream
}

func (broker *eventBroker) eventsAfter(
	runID execution.RunID,
	sequence uint64,
) ([]RunEvent, <-chan struct{}, bool, bool) {
	broker.mu.Lock()
	defer broker.mu.Unlock()

	stream, exists := broker.streams[runID]
	if !exists {
		closed := make(chan struct{})
		close(closed)
		return nil, closed, broker.closed, false
	}
	start := len(stream.events)
	if sequence < uint64(len(stream.events)) {
		start = int(sequence)
	}
	events := append([]RunEvent(nil), stream.events[start:]...)
	return events, stream.notify, broker.closed, terminalWorkflowStatus(stream.latest.Status)
}

func (broker *eventBroker) close() {
	broker.mu.Lock()
	defer broker.mu.Unlock()

	if broker.closed {
		return
	}
	broker.closed = true
	for _, stream := range broker.streams {
		close(stream.notify)
	}
}

func deriveRunEvents(
	previous execution.WorkflowRunSnapshot,
	current execution.WorkflowRunSnapshot,
) []RunEvent {
	events := make([]RunEvent, 0)
	if previous.Status != execution.WorkflowRunRunning && current.Status == execution.WorkflowRunRunning {
		events = append(events, RunEvent{
			Type:       eventWorkflowStarted,
			RunID:      current.ID,
			WorkflowID: current.WorkflowID,
			Status:     current.Status,
			OccurredAt: current.UpdatedAt,
		})
	}

	previousTasks := make(map[workflow.TaskID]execution.TaskRun, len(previous.Tasks))
	for _, task := range previous.Tasks {
		previousTasks[task.TaskID] = task
	}
	type taskEvent struct {
		priority int
		event    RunEvent
	}
	taskEvents := make([]taskEvent, 0)
	for _, task := range current.Tasks {
		old, exists := previousTasks[task.TaskID]
		if !exists || old.Status == task.Status {
			continue
		}
		eventType, priority := taskTransitionEvent(old.Status, task.Status)
		if eventType == "" {
			continue
		}
		taskEvents = append(taskEvents, taskEvent{
			priority: priority,
			event: RunEvent{
				Type:               eventType,
				RunID:              current.ID,
				WorkflowID:         current.WorkflowID,
				TaskID:             task.TaskID,
				TaskStatus:         task.Status,
				AttemptID:          task.CurrentAttemptID,
				OccurredAt:         task.UpdatedAt,
				previousTaskStatus: old.Status,
				workerID:           transitionWorkerID(old, task),
			},
		})
	}
	sort.Slice(taskEvents, func(left, right int) bool {
		if taskEvents[left].priority != taskEvents[right].priority {
			return taskEvents[left].priority < taskEvents[right].priority
		}
		return taskEvents[left].event.TaskID < taskEvents[right].event.TaskID
	})
	for _, taskEvent := range taskEvents {
		events = append(events, taskEvent.event)
	}

	if previous.Status != current.Status && terminalWorkflowStatus(current.Status) {
		events = append(events, RunEvent{
			Type:       eventWorkflowCompleted,
			RunID:      current.ID,
			WorkflowID: current.WorkflowID,
			Status:     current.Status,
			OccurredAt: current.UpdatedAt,
		})
	}
	return events
}

func transitionWorkerID(previous, current execution.TaskRun) execution.WorkerID {
	if current.Lease != nil {
		return current.Lease.WorkerID
	}
	if previous.Lease != nil {
		return previous.Lease.WorkerID
	}
	return ""
}

func taskTransitionEvent(from, to execution.TaskRunStatus) (string, int) {
	switch to {
	case execution.TaskRunSucceeded:
		return eventTaskSucceeded, 1
	case execution.TaskRunFailed:
		return eventTaskFailed, 1
	case execution.TaskRunCanceled:
		return eventTaskCanceled, 1
	case execution.TaskRunRetryWaiting:
		return eventRetryScheduled, 1
	case execution.TaskRunReady:
		if from == execution.TaskRunRunning {
			return eventRetryScheduled, 1
		}
		return eventTaskReady, 2
	case execution.TaskRunRunning:
		return eventTaskStarted, 3
	default:
		return "", 0
	}
}

func terminalWorkflowStatus(status execution.WorkflowRunStatus) bool {
	return status == execution.WorkflowRunSucceeded ||
		status == execution.WorkflowRunFailed ||
		status == execution.WorkflowRunCanceled
}

func cloneRunSnapshot(snapshot execution.WorkflowRunSnapshot) execution.WorkflowRunSnapshot {
	clone := snapshot
	clone.Tasks = append([]execution.TaskRun(nil), snapshot.Tasks...)
	for index := range clone.Tasks {
		if snapshot.Tasks[index].Lease != nil {
			lease := *snapshot.Tasks[index].Lease
			clone.Tasks[index].Lease = &lease
		}
	}
	clone.Workers = append([]execution.WorkerHeartbeat(nil), snapshot.Workers...)
	return clone
}

type observedStore struct {
	delegate        execution.Store
	observe         func(execution.WorkflowRunSnapshot) []RunEvent
	instrumentation *observability.Instrumentation
}

func (store *observedStore) SaveWorkflow(ctx context.Context, definition workflow.WorkflowDefinition) error {
	ctx, span := store.startPersistenceSpan(
		ctx,
		"save_workflow",
		attribute.String("forgeflow.workflow.id", string(definition.ID)),
	)
	defer span.End()
	err := store.delegate.SaveWorkflow(ctx, definition)
	markPersistenceError(span, err)
	return err
}

func (store *observedStore) LoadWorkflow(
	ctx context.Context,
	workflowID workflow.WorkflowID,
) (workflow.WorkflowDefinition, bool, error) {
	ctx, span := store.startPersistenceSpan(
		ctx,
		"load_workflow",
		attribute.String("forgeflow.workflow.id", string(workflowID)),
	)
	defer span.End()
	definition, found, err := store.delegate.LoadWorkflow(ctx, workflowID)
	markPersistenceError(span, err)
	return definition, found, err
}

func (store *observedStore) CreateRun(
	ctx context.Context,
	snapshot execution.WorkflowRunSnapshot,
) (execution.WorkflowRunSnapshot, error) {
	ctx, span := store.startPersistenceSpan(
		ctx,
		"create_run",
		attribute.String("forgeflow.workflow.id", string(snapshot.WorkflowID)),
		attribute.String("forgeflow.workflow_run.id", string(snapshot.ID)),
	)
	defer span.End()
	stored, err := store.delegate.CreateRun(ctx, snapshot)
	if err != nil {
		markPersistenceError(span, err)
		return execution.WorkflowRunSnapshot{}, err
	}
	store.observe(stored)
	store.instrumentation.Metrics().WorkflowSubmitted()
	store.instrumentation.Log(
		ctx,
		slog.LevelInfo,
		"workflow submitted",
		"workflow_id", stored.WorkflowID,
		"workflow_run_id", stored.ID,
	)
	return stored, nil
}

func (store *observedStore) SaveRun(
	ctx context.Context,
	snapshot execution.WorkflowRunSnapshot,
) (execution.WorkflowRunSnapshot, error) {
	ctx, span := store.startPersistenceSpan(
		ctx,
		"save_run",
		attribute.String("forgeflow.workflow.id", string(snapshot.WorkflowID)),
		attribute.String("forgeflow.workflow_run.id", string(snapshot.ID)),
	)
	defer span.End()
	stored, err := store.delegate.SaveRun(ctx, snapshot)
	if err != nil {
		markPersistenceError(span, err)
		return execution.WorkflowRunSnapshot{}, err
	}
	store.recordRunEvents(ctx, stored, store.observe(stored))
	return stored, nil
}

func (store *observedStore) LoadRun(
	ctx context.Context,
	runID execution.RunID,
) (execution.WorkflowRunSnapshot, bool, error) {
	ctx, span := store.startPersistenceSpan(
		ctx,
		"load_run",
		attribute.String("forgeflow.workflow_run.id", string(runID)),
	)
	defer span.End()
	snapshot, found, err := store.delegate.LoadRun(ctx, runID)
	markPersistenceError(span, err)
	if found {
		span.SetAttributes(attribute.String("forgeflow.workflow.id", string(snapshot.WorkflowID)))
	}
	return snapshot, found, err
}

func (store *observedStore) startPersistenceSpan(
	ctx context.Context,
	operation string,
	attributes ...attribute.KeyValue,
) (context.Context, trace.Span) {
	return store.instrumentation.Tracer("forgeflow/persistence").Start(
		ctx,
		"forgeflow.persistence."+operation,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attributes...),
	)
}

func markPersistenceError(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, "persistence operation failed")
	}
}

func (store *observedStore) recordRunEvents(
	ctx context.Context,
	snapshot execution.WorkflowRunSnapshot,
	events []RunEvent,
) {
	tasks := make(map[workflow.TaskID]execution.TaskRun, len(snapshot.Tasks))
	for _, task := range snapshot.Tasks {
		tasks[task.TaskID] = task
	}
	for _, event := range events {
		attributes := []any{
			"workflow_id", snapshot.WorkflowID,
			"workflow_run_id", snapshot.ID,
		}
		if event.TaskID != "" {
			task := tasks[event.TaskID]
			attributes = append(
				attributes,
				"task_id", task.TaskID,
				"task_run_id", task.TaskRunID,
				"attempt_id", task.CurrentAttemptID,
			)
			if event.workerID != "" {
				attributes = append(attributes, "worker_id", event.workerID)
			}
		}

		switch event.Type {
		case eventWorkflowStarted:
			store.instrumentation.Log(ctx, slog.LevelInfo, "workflow started", attributes...)
		case eventTaskReady:
			store.instrumentation.Log(ctx, slog.LevelInfo, "task ready", attributes...)
		case eventTaskStarted:
			store.instrumentation.Metrics().TaskStarted()
			store.instrumentation.Log(ctx, slog.LevelInfo, "task started", attributes...)
		case eventTaskSucceeded:
			task := tasks[event.TaskID]
			store.instrumentation.Metrics().TaskCompleted("succeeded", taskAttemptDuration(task))
			store.instrumentation.Log(ctx, slog.LevelInfo, "task succeeded", attributes...)
		case eventTaskFailed:
			task := tasks[event.TaskID]
			store.instrumentation.Metrics().TaskCompleted("failed", taskAttemptDuration(task))
			store.instrumentation.Log(ctx, slog.LevelWarn, "task failed", attributes...)
		case eventRetryScheduled:
			task := tasks[event.TaskID]
			store.instrumentation.Metrics().RetryScheduled(taskAttemptDuration(task))
			store.instrumentation.Log(ctx, slog.LevelWarn, "task retry scheduled", attributes...)
		case eventTaskCanceled:
			if event.previousTaskStatus == execution.TaskRunRunning {
				task := tasks[event.TaskID]
				store.instrumentation.Metrics().TaskCompleted("canceled", taskAttemptDuration(task))
			}
			store.instrumentation.Log(ctx, slog.LevelInfo, "task canceled", attributes...)
		case eventWorkflowCompleted:
			store.instrumentation.Metrics().WorkflowCompleted(
				string(snapshot.Status),
				snapshot.UpdatedAt.Sub(snapshot.CreatedAt),
			)
			attributes = append(attributes, "status", snapshot.Status)
			store.instrumentation.Log(ctx, slog.LevelInfo, "workflow completed", attributes...)
		}
	}
}

func taskAttemptDuration(task execution.TaskRun) time.Duration {
	if task.StartedAt.IsZero() || task.FinishedAt.IsZero() || task.FinishedAt.Before(task.StartedAt) {
		return 0
	}
	return task.FinishedAt.Sub(task.StartedAt)
}

var _ execution.Store = (*observedStore)(nil)
