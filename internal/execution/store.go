package execution

import (
	"context"
	"fmt"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

// Store is the durability boundary used by the execution engine. Implementations
// must make each mutation atomic from a caller's perspective. CreateRun accepts
// an unpersisted version-zero aggregate and returns version one. SaveRun treats
// the supplied Version as an optimistic-concurrency expectation and returns the
// stored aggregate with that version advanced by one.
type Store interface {
	SaveWorkflow(context.Context, workflow.WorkflowDefinition) error
	LoadWorkflow(context.Context, workflow.WorkflowID) (workflow.WorkflowDefinition, bool, error)
	CreateRun(context.Context, WorkflowRunSnapshot) (WorkflowRunSnapshot, error)
	SaveRun(context.Context, WorkflowRunSnapshot) (WorkflowRunSnapshot, error)
	LoadRun(context.Context, RunID) (WorkflowRunSnapshot, bool, error)
}

// WorkflowRunSnapshot is the durable representation of a workflow execution.
// The workflow definition is stored separately and referenced by WorkflowID.
type WorkflowRunSnapshot struct {
	ID         RunID
	WorkflowID workflow.WorkflowID
	Version    uint64
	Status     WorkflowRunStatus
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Tasks      []TaskRun
	Workers    []WorkerHeartbeat
}

// Validate verifies the snapshot's storage-level invariants.
func (snapshot WorkflowRunSnapshot) Validate() error {
	if !validIdentifier(string(snapshot.ID)) {
		return &SnapshotValidationError{RunID: snapshot.ID, Reason: "run ID is empty or invalid"}
	}
	if !validIdentifier(string(snapshot.WorkflowID)) {
		return &SnapshotValidationError{RunID: snapshot.ID, Reason: "workflow ID is empty or invalid"}
	}
	if !snapshot.Status.valid() {
		return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("unknown workflow status %q", snapshot.Status)}
	}
	if snapshot.CreatedAt.IsZero() {
		return &SnapshotValidationError{RunID: snapshot.ID, Reason: "creation timestamp is missing"}
	}
	if snapshot.UpdatedAt.IsZero() || snapshot.UpdatedAt.Before(snapshot.CreatedAt) {
		return &SnapshotValidationError{RunID: snapshot.ID, Reason: "update timestamp is missing or precedes creation"}
	}
	if len(snapshot.Tasks) == 0 {
		return &SnapshotValidationError{RunID: snapshot.ID, Reason: "task runs are missing"}
	}

	seen := make(map[workflow.TaskID]struct{}, len(snapshot.Tasks))
	statusCounts := make(map[TaskRunStatus]int)
	leasedWorkers := make(map[WorkerID]struct{})
	for _, task := range snapshot.Tasks {
		if !validIdentifier(string(task.TaskID)) {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: "task run has an empty or invalid task ID"}
		}
		if _, exists := seen[task.TaskID]; exists {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("duplicate task run %q", task.TaskID)}
		}
		seen[task.TaskID] = struct{}{}
		expectedTaskRunID := TaskRunIDFor(snapshot.ID, task.TaskID)
		if task.TaskRunID != "" && task.TaskRunID != expectedTaskRunID {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("task %q has an invalid task-run ID", task.TaskID)}
		}

		if !task.Status.valid() {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("task %q has unknown status %q", task.TaskID, task.Status)}
		}
		if task.AttemptCount < 0 {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("task %q has a negative attempt count", task.TaskID)}
		}
		if task.AttemptCount == 0 && task.CurrentAttemptID != "" {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("task %q has an attempt ID without an attempt", task.TaskID)}
		}
		if task.CurrentAttemptID != "" && task.CurrentAttemptID != AttemptIDFor(expectedTaskRunID, task.AttemptCount) {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("task %q has an invalid current attempt ID", task.TaskID)}
		}
		if task.UpdatedAt.IsZero() || task.UpdatedAt.Before(snapshot.CreatedAt) {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("task %q has an invalid update timestamp", task.TaskID)}
		}
		if task.Status == TaskRunRunning && (task.AttemptCount == 0 || task.StartedAt.IsZero()) {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("running task %q has no recorded attempt", task.TaskID)}
		}
		if (task.Status == TaskRunSucceeded || task.Status == TaskRunFailed) &&
			(task.AttemptCount == 0 || task.FinishedAt.IsZero()) {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("completed task %q has no recorded attempt", task.TaskID)}
		}
		if task.Status == TaskRunCanceled && task.FinishedAt.IsZero() {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("canceled task %q has no finish timestamp", task.TaskID)}
		}
		if task.Status == TaskRunRetryWaiting && task.NextAttemptAt.IsZero() {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("retry-waiting task %q has no retry deadline", task.TaskID)}
		}
		if task.Status == TaskRunRetryWaiting && (task.AttemptCount == 0 || task.CurrentAttemptID == "") {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("retry-waiting task %q has no completed attempt", task.TaskID)}
		}
		if task.Status != TaskRunRetryWaiting && !task.NextAttemptAt.IsZero() {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("task %q has a retry deadline outside retry-wait state", task.TaskID)}
		}
		if task.Lease != nil {
			if task.Status != TaskRunRunning || !validIdentifier(string(task.Lease.WorkerID)) ||
				task.TaskRunID != expectedTaskRunID || !validIdentifier(string(task.Lease.AttemptID)) ||
				task.Lease.TaskRunID != expectedTaskRunID ||
				task.Lease.AttemptID != task.CurrentAttemptID || task.Lease.ExpiresAt.IsZero() {
				return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("task %q has an invalid lease", task.TaskID)}
			}
			leasedWorkers[task.Lease.WorkerID] = struct{}{}
		}
		statusCounts[task.Status]++
	}
	seenWorkers := make(map[WorkerID]struct{}, len(snapshot.Workers))
	for _, worker := range snapshot.Workers {
		if !validIdentifier(string(worker.WorkerID)) || worker.LastHeartbeatAt.IsZero() || worker.LastHeartbeatAt.Before(snapshot.CreatedAt) {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("worker %q has invalid heartbeat state", worker.WorkerID)}
		}
		if _, exists := seenWorkers[worker.WorkerID]; exists {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("duplicate worker heartbeat %q", worker.WorkerID)}
		}
		seenWorkers[worker.WorkerID] = struct{}{}
	}
	for workerID := range leasedWorkers {
		if _, exists := seenWorkers[workerID]; !exists {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: fmt.Sprintf("lease references worker %q without heartbeat state", workerID)}
		}
	}
	if err := validateAggregateStatus(snapshot, statusCounts); err != nil {
		return err
	}

	return nil
}

func validateAggregateStatus(snapshot WorkflowRunSnapshot, counts map[TaskRunStatus]int) error {
	unfinished := counts[TaskRunPending] + counts[TaskRunReady] + counts[TaskRunRunning] + counts[TaskRunRetryWaiting]
	switch snapshot.Status {
	case WorkflowRunPending:
		if counts[TaskRunPending] != len(snapshot.Tasks) {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: "pending workflow contains a task that has started"}
		}
	case WorkflowRunRunning:
		// A task result and the workflow's terminal transition are separate
		// durable mutations. Recovery finishes a partially recorded failure or
		// cancellation before dispatching any more work.
	case WorkflowRunSucceeded:
		if counts[TaskRunSucceeded] != len(snapshot.Tasks) {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: "succeeded workflow contains a task that did not succeed"}
		}
	case WorkflowRunFailed:
		if unfinished > 0 || counts[TaskRunFailed]+counts[TaskRunCanceled] == 0 {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: "failed workflow has inconsistent task states"}
		}
	case WorkflowRunCanceled:
		if unfinished > 0 || counts[TaskRunFailed] > 0 {
			return &SnapshotValidationError{RunID: snapshot.ID, Reason: "canceled workflow has inconsistent task states"}
		}
	}
	return nil
}

// RestoreWorkflowRun reconstructs a mutable aggregate from its durable snapshot
// and immutable workflow definition.
func RestoreWorkflowRun(
	snapshot WorkflowRunSnapshot,
	definition workflow.WorkflowDefinition,
) (*WorkflowRun, error) {
	return restoreWorkflowRun(snapshot, definition, time.Now)
}

func restoreWorkflowRun(
	snapshot WorkflowRunSnapshot,
	definition workflow.WorkflowDefinition,
	now func() time.Time,
) (*WorkflowRun, error) {
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	if err := definition.Validate(); err != nil {
		return nil, err
	}
	if snapshot.WorkflowID != definition.ID {
		return nil, &SnapshotValidationError{
			RunID:  snapshot.ID,
			Reason: fmt.Sprintf("references workflow %q, not %q", snapshot.WorkflowID, definition.ID),
		}
	}
	if len(snapshot.Tasks) != len(definition.Tasks) {
		return nil, &SnapshotValidationError{RunID: snapshot.ID, Reason: "task runs do not match workflow definition"}
	}

	tasks := make(map[workflow.TaskID]TaskRun, len(snapshot.Tasks))
	for _, task := range snapshot.Tasks {
		if task.TaskRunID == "" {
			task.TaskRunID = TaskRunIDFor(snapshot.ID, task.TaskID)
		}
		if task.CurrentAttemptID == "" && task.AttemptCount > 0 {
			task.CurrentAttemptID = AttemptIDFor(task.TaskRunID, task.AttemptCount)
		}
		tasks[task.TaskID] = cloneTaskRun(task)
	}
	for _, task := range definition.Tasks {
		if _, exists := tasks[task.ID]; !exists {
			return nil, &SnapshotValidationError{
				RunID:  snapshot.ID,
				Reason: fmt.Sprintf("task run %q is missing", task.ID),
			}
		}
	}
	if now == nil {
		now = time.Now
	}
	workers := make(map[WorkerID]WorkerHeartbeat, len(snapshot.Workers))
	for _, worker := range snapshot.Workers {
		workers[worker.WorkerID] = worker
	}

	return &WorkflowRun{
		id:         snapshot.ID,
		definition: cloneDefinition(definition),
		version:    snapshot.Version,
		status:     snapshot.Status,
		tasks:      tasks,
		workers:    workers,
		createdAt:  snapshot.CreatedAt,
		updatedAt:  snapshot.UpdatedAt,
		now:        now,
	}, nil
}

func (status WorkflowRunStatus) valid() bool {
	switch status {
	case WorkflowRunPending,
		WorkflowRunRunning,
		WorkflowRunSucceeded,
		WorkflowRunFailed,
		WorkflowRunCanceled:
		return true
	default:
		return false
	}
}

func (status TaskRunStatus) valid() bool {
	switch status {
	case TaskRunPending,
		TaskRunReady,
		TaskRunRunning,
		TaskRunRetryWaiting,
		TaskRunSucceeded,
		TaskRunFailed,
		TaskRunCanceled:
		return true
	default:
		return false
	}
}
