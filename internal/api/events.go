package api

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/execution"
	"github.com/vijaypratap3364/forgeflow/internal/workflow"
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
	Sequence   uint64                      `json:"sequence"`
	Type       string                      `json:"type"`
	RunID      execution.RunID             `json:"run_id"`
	WorkflowID workflow.WorkflowID         `json:"workflow_id"`
	TaskID     workflow.TaskID             `json:"task_id,omitempty"`
	Status     execution.WorkflowRunStatus `json:"status,omitempty"`
	TaskStatus execution.TaskRunStatus     `json:"task_status,omitempty"`
	AttemptID  execution.AttemptID         `json:"attempt_id,omitempty"`
	OccurredAt time.Time                   `json:"occurred_at"`
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

func (broker *eventBroker) observe(snapshot execution.WorkflowRunSnapshot) {
	broker.mu.Lock()
	defer broker.mu.Unlock()

	if broker.closed {
		return
	}
	stream, exists := broker.streams[snapshot.ID]
	if !exists {
		broker.streams[snapshot.ID] = &runEventStream{
			latest: cloneRunSnapshot(snapshot),
			notify: make(chan struct{}),
		}
		return
	}

	events := deriveRunEvents(stream.latest, snapshot)
	stream.latest = cloneRunSnapshot(snapshot)
	if len(events) == 0 {
		return
	}
	for index := range events {
		events[index].Sequence = uint64(len(stream.events) + 1)
		stream.events = append(stream.events, events[index])
	}
	close(stream.notify)
	stream.notify = make(chan struct{})
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
				Type:       eventType,
				RunID:      current.ID,
				WorkflowID: current.WorkflowID,
				TaskID:     task.TaskID,
				TaskStatus: task.Status,
				AttemptID:  task.CurrentAttemptID,
				OccurredAt: task.UpdatedAt,
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
	delegate execution.Store
	observe  func(execution.WorkflowRunSnapshot)
}

func (store *observedStore) SaveWorkflow(ctx context.Context, definition workflow.WorkflowDefinition) error {
	return store.delegate.SaveWorkflow(ctx, definition)
}

func (store *observedStore) LoadWorkflow(
	ctx context.Context,
	workflowID workflow.WorkflowID,
) (workflow.WorkflowDefinition, bool, error) {
	return store.delegate.LoadWorkflow(ctx, workflowID)
}

func (store *observedStore) CreateRun(ctx context.Context, snapshot execution.WorkflowRunSnapshot) error {
	if err := store.delegate.CreateRun(ctx, snapshot); err != nil {
		return err
	}
	store.observe(snapshot)
	return nil
}

func (store *observedStore) SaveRun(ctx context.Context, snapshot execution.WorkflowRunSnapshot) error {
	if err := store.delegate.SaveRun(ctx, snapshot); err != nil {
		return err
	}
	store.observe(snapshot)
	return nil
}

func (store *observedStore) LoadRun(
	ctx context.Context,
	runID execution.RunID,
) (execution.WorkflowRunSnapshot, bool, error) {
	return store.delegate.LoadRun(ctx, runID)
}

var _ execution.Store = (*observedStore)(nil)
