// Package execution runs validated ForgeFlow workflow definitions in process.
package execution

import (
	"sort"
	"sync"
	"time"
	"unicode"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

// RunID is the stable identifier of one workflow execution instance.
type RunID string

// WorkflowRunStatus describes the lifecycle state of a workflow execution.
type WorkflowRunStatus string

const (
	// WorkflowRunPending indicates that execution has not started.
	WorkflowRunPending WorkflowRunStatus = "pending"
	// WorkflowRunRunning indicates that tasks may be executing.
	WorkflowRunRunning WorkflowRunStatus = "running"
	// WorkflowRunSucceeded indicates that every task succeeded.
	WorkflowRunSucceeded WorkflowRunStatus = "succeeded"
	// WorkflowRunFailed indicates that at least one task failed.
	WorkflowRunFailed WorkflowRunStatus = "failed"
	// WorkflowRunCanceled indicates that execution was canceled by its context.
	WorkflowRunCanceled WorkflowRunStatus = "canceled"
)

// TaskRunStatus describes the lifecycle state of one task execution.
type TaskRunStatus string

const (
	// TaskRunPending indicates that dependencies are not yet satisfied.
	TaskRunPending TaskRunStatus = "pending"
	// TaskRunReady indicates that the task can be dispatched.
	TaskRunReady TaskRunStatus = "ready"
	// TaskRunRunning indicates that a worker is executing the task.
	TaskRunRunning TaskRunStatus = "running"
	// TaskRunSucceeded indicates that the handler completed successfully.
	TaskRunSucceeded TaskRunStatus = "succeeded"
	// TaskRunFailed indicates that the handler returned an error.
	TaskRunFailed TaskRunStatus = "failed"
	// TaskRunCanceled indicates that the task did not finish because its run stopped.
	TaskRunCanceled TaskRunStatus = "canceled"
)

// TaskRun is an immutable snapshot of one task's execution state.
type TaskRun struct {
	TaskID       workflow.TaskID
	Status       TaskRunStatus
	Output       string
	Error        string
	AttemptCount int
	UpdatedAt    time.Time
	StartedAt    time.Time
	FinishedAt   time.Time
}

// WorkflowRun tracks the mutable state of one workflow execution. Its public
// methods return snapshots so callers cannot bypass the state machine.
type WorkflowRun struct {
	mu         sync.RWMutex
	id         RunID
	definition workflow.WorkflowDefinition
	status     WorkflowRunStatus
	tasks      map[workflow.TaskID]TaskRun
	createdAt  time.Time
	updatedAt  time.Time
	now        func() time.Time
}

// NewWorkflowRun validates a definition and creates a pending execution instance.
func NewWorkflowRun(runID RunID, definition workflow.WorkflowDefinition) (*WorkflowRun, error) {
	return newWorkflowRun(runID, definition, time.Now)
}

func newWorkflowRun(
	runID RunID,
	definition workflow.WorkflowDefinition,
	now func() time.Time,
) (*WorkflowRun, error) {
	if !validRunID(string(runID)) {
		return nil, &InvalidRunIDError{RunID: runID}
	}
	if err := definition.Validate(); err != nil {
		return nil, err
	}
	if now == nil {
		now = time.Now
	}

	definition = cloneDefinition(definition)
	createdAt := now().UTC()
	tasks := make(map[workflow.TaskID]TaskRun, len(definition.Tasks))
	for _, task := range definition.Tasks {
		tasks[task.ID] = TaskRun{
			TaskID:    task.ID,
			Status:    TaskRunPending,
			UpdatedAt: createdAt,
		}
	}

	return &WorkflowRun{
		id:         runID,
		definition: definition,
		status:     WorkflowRunPending,
		tasks:      tasks,
		createdAt:  createdAt,
		updatedAt:  createdAt,
		now:        now,
	}, nil
}

// ID returns the execution's stable run ID.
func (run *WorkflowRun) ID() RunID {
	return run.id
}

// WorkflowID returns the ID of the definition being executed.
func (run *WorkflowRun) WorkflowID() workflow.WorkflowID {
	return run.definition.ID
}

// Definition returns a defensive copy of the workflow definition.
func (run *WorkflowRun) Definition() workflow.WorkflowDefinition {
	run.mu.RLock()
	defer run.mu.RUnlock()

	return cloneDefinition(run.definition)
}

// Status returns the workflow execution's current status.
func (run *WorkflowRun) Status() WorkflowRunStatus {
	run.mu.RLock()
	defer run.mu.RUnlock()

	return run.status
}

// CreatedAt returns the time the workflow run was created.
func (run *WorkflowRun) CreatedAt() time.Time {
	run.mu.RLock()
	defer run.mu.RUnlock()

	return run.createdAt
}

// UpdatedAt returns the time of the most recent state change.
func (run *WorkflowRun) UpdatedAt() time.Time {
	run.mu.RLock()
	defer run.mu.RUnlock()

	return run.updatedAt
}

// Task returns an immutable snapshot of one task run.
func (run *WorkflowRun) Task(taskID workflow.TaskID) (TaskRun, bool) {
	run.mu.RLock()
	defer run.mu.RUnlock()

	task, exists := run.tasks[taskID]
	return task, exists
}

// Tasks returns task-run snapshots ordered lexicographically by task ID.
func (run *WorkflowRun) Tasks() []TaskRun {
	run.mu.RLock()
	defer run.mu.RUnlock()

	return run.sortedTasksLocked()
}

func (run *WorkflowRun) sortedTasksLocked() []TaskRun {
	tasks := make([]TaskRun, 0, len(run.tasks))
	for _, task := range run.tasks {
		tasks = append(tasks, task)
	}
	sort.Slice(tasks, func(left, right int) bool {
		return tasks[left].TaskID < tasks[right].TaskID
	})
	return tasks
}

// Snapshot returns a detached representation suitable for persistence.
func (run *WorkflowRun) Snapshot() WorkflowRunSnapshot {
	run.mu.RLock()
	defer run.mu.RUnlock()

	return WorkflowRunSnapshot{
		ID:         run.id,
		WorkflowID: run.definition.ID,
		Status:     run.status,
		CreatedAt:  run.createdAt,
		UpdatedAt:  run.updatedAt,
		Tasks:      run.sortedTasksLocked(),
	}
}

func (run *WorkflowRun) taskStatus(taskID workflow.TaskID) (TaskRunStatus, error) {
	run.mu.RLock()
	defer run.mu.RUnlock()

	task, exists := run.tasks[taskID]
	if !exists {
		return "", &UnknownTaskRunError{RunID: run.id, TaskID: taskID}
	}
	return task.Status, nil
}

func (run *WorkflowRun) transitionTask(
	taskID workflow.TaskID,
	target TaskRunStatus,
	output string,
	taskErr error,
) error {
	run.mu.Lock()
	defer run.mu.Unlock()

	task, exists := run.tasks[taskID]
	if !exists {
		return &UnknownTaskRunError{RunID: run.id, TaskID: taskID}
	}
	if !legalTaskTransition(task.Status, target) {
		return &TaskTransitionError{
			RunID:  run.id,
			TaskID: taskID,
			From:   task.Status,
			To:     target,
		}
	}

	timestamp := run.nextTimestampLocked()
	task.Status = target
	task.Output = ""
	task.Error = ""
	task.UpdatedAt = timestamp
	if target == TaskRunRunning {
		task.AttemptCount++
		task.StartedAt = timestamp
		task.FinishedAt = time.Time{}
	}
	if target == TaskRunSucceeded {
		task.Output = output
	}
	if (target == TaskRunFailed || target == TaskRunCanceled) && taskErr != nil {
		task.Error = taskErr.Error()
	}
	if target.terminal() {
		task.FinishedAt = timestamp
	}
	run.tasks[taskID] = task
	run.updatedAt = timestamp

	return nil
}

func (run *WorkflowRun) transitionWorkflow(target WorkflowRunStatus) error {
	run.mu.Lock()
	defer run.mu.Unlock()

	if !legalWorkflowTransition(run.status, target) {
		return &WorkflowTransitionError{
			RunID: run.id,
			From:  run.status,
			To:    target,
		}
	}
	run.status = target
	run.updatedAt = run.nextTimestampLocked()
	return nil
}

func (run *WorkflowRun) cancelUnfinished(cause error) {
	run.mu.Lock()
	defer run.mu.Unlock()

	timestamp := run.nextTimestampLocked()
	changed := false
	for taskID, task := range run.tasks {
		if task.Status != TaskRunPending && task.Status != TaskRunReady && task.Status != TaskRunRunning {
			continue
		}

		task.Status = TaskRunCanceled
		task.Output = ""
		task.Error = ""
		if cause != nil {
			task.Error = cause.Error()
		}
		task.UpdatedAt = timestamp
		task.FinishedAt = timestamp
		run.tasks[taskID] = task
		changed = true
	}
	if changed {
		run.updatedAt = timestamp
	}
}

func (run *WorkflowRun) nextTimestampLocked() time.Time {
	timestamp := run.now().UTC()
	if timestamp.Before(run.updatedAt) {
		return run.updatedAt
	}
	return timestamp
}

// Task transitions follow pending -> ready -> running -> succeeded|failed.
// Cancellation may move any nonterminal task directly to canceled.
func legalTaskTransition(from, to TaskRunStatus) bool {
	switch from {
	case TaskRunPending:
		return to == TaskRunReady || to == TaskRunCanceled
	case TaskRunReady:
		return to == TaskRunRunning || to == TaskRunCanceled
	case TaskRunRunning:
		return to == TaskRunSucceeded || to == TaskRunFailed || to == TaskRunCanceled
	default:
		return false
	}
}

// Workflow transitions follow pending -> running -> succeeded|failed|canceled.
// A pending workflow may also be canceled before it starts.
func legalWorkflowTransition(from, to WorkflowRunStatus) bool {
	switch from {
	case WorkflowRunPending:
		return to == WorkflowRunRunning || to == WorkflowRunCanceled
	case WorkflowRunRunning:
		return to == WorkflowRunSucceeded || to == WorkflowRunFailed || to == WorkflowRunCanceled
	default:
		return false
	}
}

func (status TaskRunStatus) terminal() bool {
	return status == TaskRunSucceeded || status == TaskRunFailed || status == TaskRunCanceled
}

func validRunID(runID string) bool {
	return validIdentifier(runID)
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

func cloneDefinition(definition workflow.WorkflowDefinition) workflow.WorkflowDefinition {
	clone := workflow.WorkflowDefinition{
		ID:    definition.ID,
		Tasks: make([]workflow.TaskDefinition, len(definition.Tasks)),
	}
	for index, task := range definition.Tasks {
		clone.Tasks[index] = task
		clone.Tasks[index].Dependencies = append([]workflow.TaskID(nil), task.Dependencies...)
	}
	return clone
}
