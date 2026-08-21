package execution

import (
	"fmt"
	"sort"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

// TaskRunID identifies one logical task within one workflow run.
type TaskRunID string

// AttemptID identifies one dispatch attempt of a task run.
type AttemptID string

// WorkerID identifies one worker participating in a workflow run.
type WorkerID string

// TaskLease records the worker currently allowed to complete an attempt.
type TaskLease struct {
	WorkerID  WorkerID
	TaskRunID TaskRunID
	AttemptID AttemptID
	ExpiresAt time.Time
}

// WorkerHeartbeat is the latest persisted liveness observation for a worker.
type WorkerHeartbeat struct {
	WorkerID        WorkerID
	LastHeartbeatAt time.Time
}

// CompletionOutcome describes how an identified completion affected task state.
type CompletionOutcome string

const (
	// CompletionIgnored indicates a duplicate or stale completion.
	CompletionIgnored CompletionOutcome = "ignored"
	// CompletionSucceeded indicates that the attempt completed the task.
	CompletionSucceeded CompletionOutcome = "succeeded"
	// CompletionRetryScheduled indicates that another attempt is eligible later.
	CompletionRetryScheduled CompletionOutcome = "retry_scheduled"
	// CompletionFailed indicates a terminal failure or exhausted retry policy.
	CompletionFailed CompletionOutcome = "failed"
)

// LeaseRecovery describes one task whose expired or missing lease was handled.
type LeaseRecovery struct {
	TaskID    workflow.TaskID
	AttemptID AttemptID
	Outcome   CompletionOutcome
}

// TaskRunIDFor deterministically derives a task-run ID without delimiter
// ambiguity, even when user-selected IDs contain punctuation.
func TaskRunIDFor(runID RunID, taskID workflow.TaskID) TaskRunID {
	return TaskRunID(fmt.Sprintf("%d:%s:%s", len([]byte(runID)), runID, taskID))
}

// AttemptIDFor deterministically derives an ID for a numbered task attempt.
func AttemptIDFor(taskRunID TaskRunID, attempt int) AttemptID {
	return AttemptID(fmt.Sprintf("%s:attempt:%d", taskRunID, attempt))
}

// Workers returns heartbeat snapshots ordered lexicographically by worker ID.
func (run *WorkflowRun) Workers() []WorkerHeartbeat {
	run.mu.RLock()
	defer run.mu.RUnlock()

	return run.sortedWorkersLocked()
}

func (run *WorkflowRun) sortedWorkersLocked() []WorkerHeartbeat {
	var workers []WorkerHeartbeat
	for _, worker := range run.workers {
		workers = append(workers, worker)
	}
	sort.Slice(workers, func(left, right int) bool {
		return workers[left].WorkerID < workers[right].WorkerID
	})
	return workers
}

func (run *WorkflowRun) recordWorkerHeartbeat(workerID WorkerID, leaseDuration time.Duration) error {
	if !validIdentifier(string(workerID)) {
		return fmt.Errorf("worker ID %q is invalid", workerID)
	}
	if leaseDuration <= 0 {
		return fmt.Errorf("lease duration must be positive")
	}

	run.mu.Lock()
	defer run.mu.Unlock()
	timestamp := run.nextTimestampLocked()
	run.workers[workerID] = WorkerHeartbeat{
		WorkerID:        workerID,
		LastHeartbeatAt: timestamp,
	}
	for taskID, task := range run.tasks {
		if task.Status != TaskRunRunning || task.Lease == nil || task.Lease.WorkerID != workerID {
			continue
		}
		if !timestamp.Before(task.Lease.ExpiresAt) {
			continue
		}
		lease := *task.Lease
		lease.ExpiresAt = timestamp.Add(leaseDuration)
		task.Lease = &lease
		task.UpdatedAt = timestamp
		run.tasks[taskID] = task
	}
	run.updatedAt = timestamp
	return nil
}

func (run *WorkflowRun) startTaskAttempt(
	taskID workflow.TaskID,
	workerID WorkerID,
	leaseDuration time.Duration,
) (AttemptID, error) {
	if !validIdentifier(string(workerID)) {
		return "", fmt.Errorf("worker ID %q is invalid", workerID)
	}
	if leaseDuration <= 0 {
		return "", fmt.Errorf("lease duration must be positive")
	}

	run.mu.Lock()
	defer run.mu.Unlock()
	task, exists := run.tasks[taskID]
	if !exists {
		return "", &UnknownTaskRunError{RunID: run.id, TaskID: taskID}
	}
	if task.Status != TaskRunReady {
		return "", &TaskTransitionError{RunID: run.id, TaskID: taskID, From: task.Status, To: TaskRunRunning}
	}

	timestamp := run.nextTimestampLocked()
	task.TaskRunID = TaskRunIDFor(run.id, taskID)
	task.AttemptCount++
	task.CurrentAttemptID = AttemptIDFor(task.TaskRunID, task.AttemptCount)
	task.Status = TaskRunRunning
	task.Output = ""
	task.Error = ""
	task.NextAttemptAt = time.Time{}
	task.StartedAt = timestamp
	task.FinishedAt = time.Time{}
	task.UpdatedAt = timestamp
	task.Lease = &TaskLease{
		WorkerID:  workerID,
		TaskRunID: task.TaskRunID,
		AttemptID: task.CurrentAttemptID,
		ExpiresAt: timestamp.Add(leaseDuration),
	}
	run.tasks[taskID] = task
	run.workers[workerID] = WorkerHeartbeat{WorkerID: workerID, LastHeartbeatAt: timestamp}
	run.updatedAt = timestamp
	return task.CurrentAttemptID, nil
}

func (run *WorkflowRun) completeTaskAttempt(
	taskID workflow.TaskID,
	workerID WorkerID,
	attemptID AttemptID,
	output string,
	taskErr error,
) (CompletionOutcome, error) {
	run.mu.Lock()
	defer run.mu.Unlock()
	task, exists := run.tasks[taskID]
	if !exists {
		return "", &UnknownTaskRunError{RunID: run.id, TaskID: taskID}
	}
	if task.Status != TaskRunRunning || task.CurrentAttemptID != attemptID || task.Lease == nil ||
		task.Lease.AttemptID != attemptID || task.Lease.WorkerID != workerID {
		return CompletionIgnored, nil
	}

	timestamp := run.nextTimestampLocked()
	task.Lease = nil
	task.UpdatedAt = timestamp
	task.FinishedAt = timestamp
	task.NextAttemptAt = time.Time{}
	if taskErr == nil {
		task.Status = TaskRunSucceeded
		task.Output = output
		task.Error = ""
		run.tasks[taskID] = task
		run.updatedAt = timestamp
		return CompletionSucceeded, nil
	}

	task.Output = ""
	task.Error = taskErr.Error()
	policy := run.retryPolicyLocked(taskID)
	if IsRetryable(taskErr) && task.AttemptCount < policy.EffectiveMaxAttempts() {
		delay := policy.RetryDelay(task.AttemptCount)
		if delay == 0 {
			task.Status = TaskRunReady
		} else {
			task.Status = TaskRunRetryWaiting
			task.NextAttemptAt = timestamp.Add(delay)
		}
		run.tasks[taskID] = task
		run.updatedAt = timestamp
		return CompletionRetryScheduled, nil
	}

	task.Status = TaskRunFailed
	run.tasks[taskID] = task
	run.updatedAt = timestamp
	return CompletionFailed, nil
}

func (run *WorkflowRun) recoverExpiredLeases() []LeaseRecovery {
	run.mu.Lock()
	defer run.mu.Unlock()

	timestamp := run.nextTimestampLocked()
	recoveries := make([]LeaseRecovery, 0)
	for taskID, task := range run.tasks {
		if task.Status != TaskRunRunning {
			continue
		}
		if task.Lease != nil && timestamp.Before(task.Lease.ExpiresAt) {
			continue
		}

		attemptID := task.CurrentAttemptID
		if attemptID == "" && task.AttemptCount > 0 {
			task.TaskRunID = TaskRunIDFor(run.id, taskID)
			attemptID = AttemptIDFor(task.TaskRunID, task.AttemptCount)
			task.CurrentAttemptID = attemptID
		}
		leaseError := &LeaseExpiredError{RunID: run.id, TaskID: taskID, AttemptID: attemptID}
		task.Lease = nil
		task.Output = ""
		task.Error = leaseError.Error()
		task.UpdatedAt = timestamp
		task.FinishedAt = timestamp
		task.NextAttemptAt = time.Time{}
		policy := run.retryPolicyLocked(taskID)
		outcome := CompletionFailed
		if task.AttemptCount < policy.EffectiveMaxAttempts() {
			delay := policy.RetryDelay(task.AttemptCount)
			if delay == 0 {
				task.Status = TaskRunReady
			} else {
				task.Status = TaskRunRetryWaiting
				task.NextAttemptAt = timestamp.Add(delay)
			}
			outcome = CompletionRetryScheduled
		} else {
			task.Status = TaskRunFailed
		}
		run.tasks[taskID] = task
		recoveries = append(recoveries, LeaseRecovery{TaskID: taskID, AttemptID: attemptID, Outcome: outcome})
	}
	if len(recoveries) > 0 {
		run.updatedAt = timestamp
		sort.Slice(recoveries, func(left, right int) bool {
			return recoveries[left].TaskID < recoveries[right].TaskID
		})
	}
	return recoveries
}

func (run *WorkflowRun) promoteDueRetries() []workflow.TaskID {
	run.mu.Lock()
	defer run.mu.Unlock()

	timestamp := run.nextTimestampLocked()
	promoted := make([]workflow.TaskID, 0)
	for taskID, task := range run.tasks {
		if task.Status != TaskRunRetryWaiting || timestamp.Before(task.NextAttemptAt) {
			continue
		}
		task.Status = TaskRunReady
		task.NextAttemptAt = time.Time{}
		task.UpdatedAt = timestamp
		run.tasks[taskID] = task
		promoted = append(promoted, taskID)
	}
	if len(promoted) > 0 {
		run.updatedAt = timestamp
		sort.Slice(promoted, func(left, right int) bool { return promoted[left] < promoted[right] })
	}
	return promoted
}

func (run *WorkflowRun) nextReliabilityDeadline() (time.Time, bool) {
	run.mu.RLock()
	defer run.mu.RUnlock()

	var deadline time.Time
	for _, task := range run.tasks {
		var candidate time.Time
		switch {
		case task.Status == TaskRunRunning && task.Lease == nil:
			return run.updatedAt, true
		case task.Status == TaskRunRunning:
			candidate = task.Lease.ExpiresAt
		case task.Status == TaskRunRetryWaiting:
			candidate = task.NextAttemptAt
		default:
			continue
		}
		if deadline.IsZero() || candidate.Before(deadline) {
			deadline = candidate
		}
	}
	return deadline, !deadline.IsZero()
}

func (run *WorkflowRun) retryPolicyLocked(taskID workflow.TaskID) workflow.RetryPolicy {
	for _, task := range run.definition.Tasks {
		if task.ID == taskID {
			return task.Retry
		}
	}
	return workflow.RetryPolicy{}
}
