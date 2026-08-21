package execution

import (
	"errors"
	"fmt"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

// RetryableTaskError marks a handler failure as transient. Unmarked handler
// errors are terminal and are not retried.
type RetryableTaskError struct {
	Cause error
}

// Error returns the underlying transient failure message.
func (e *RetryableTaskError) Error() string {
	return e.Cause.Error()
}

// Unwrap exposes the transient failure for errors.Is and errors.As.
func (e *RetryableTaskError) Unwrap() error {
	return e.Cause
}

// Retryable marks a non-nil handler failure as eligible for retry.
func Retryable(cause error) error {
	if cause == nil {
		return nil
	}
	return &RetryableTaskError{Cause: cause}
}

// IsRetryable reports whether an error chain contains a retryable task marker.
func IsRetryable(err error) bool {
	var retryable *RetryableTaskError
	return errors.As(err, &retryable)
}

// InvalidEngineConfigError reports configuration that cannot create an engine.
type InvalidEngineConfigError struct {
	WorkerCount int
	Reason      string
}

// Error returns a contextual description of the invalid engine configuration.
func (e *InvalidEngineConfigError) Error() string {
	return fmt.Sprintf("invalid execution engine configuration with %d workers: %s", e.WorkerCount, e.Reason)
}

// HandlerRegistrationError reports an invalid or duplicate handler registration.
type HandlerRegistrationError struct {
	HandlerName workflow.HandlerName
	Reason      string
}

// Error returns a contextual description of the failed registration.
func (e *HandlerRegistrationError) Error() string {
	return fmt.Sprintf("cannot register task handler %q: %s", e.HandlerName, e.Reason)
}

// UnknownHandlerError reports a task that refers to an unregistered handler.
type UnknownHandlerError struct {
	TaskID      workflow.TaskID
	HandlerName workflow.HandlerName
}

// Error returns a contextual description of the unresolved handler.
func (e *UnknownHandlerError) Error() string {
	return fmt.Sprintf("task %q references unregistered handler %q", e.TaskID, e.HandlerName)
}

// TaskExecutionError reports the handler failure that stopped a workflow run.
type TaskExecutionError struct {
	RunID  RunID
	TaskID workflow.TaskID
	Cause  error
}

// LeaseExpiredError reports an attempt abandoned after its worker stopped
// renewing the persisted lease.
type LeaseExpiredError struct {
	RunID     RunID
	TaskID    workflow.TaskID
	AttemptID AttemptID
}

// Error returns a contextual description of the abandoned attempt.
func (e *LeaseExpiredError) Error() string {
	return fmt.Sprintf("lease expired for attempt %q of task %q in workflow run %q", e.AttemptID, e.TaskID, e.RunID)
}

// Error returns a contextual description of the handler failure.
func (e *TaskExecutionError) Error() string {
	return fmt.Sprintf("task %q in workflow run %q failed: %v", e.TaskID, e.RunID, e.Cause)
}

// Unwrap exposes the handler error for errors.Is and errors.As.
func (e *TaskExecutionError) Unwrap() error {
	return e.Cause
}

// RunCanceledError reports a workflow run stopped by context cancellation.
type RunCanceledError struct {
	RunID RunID
	Cause error
}

// Error returns a contextual description of the canceled workflow run.
func (e *RunCanceledError) Error() string {
	return fmt.Sprintf("workflow run %q canceled: %v", e.RunID, e.Cause)
}

// Unwrap exposes the context error for errors.Is and errors.As.
func (e *RunCanceledError) Unwrap() error {
	return e.Cause
}

// InvalidRunIDError reports an empty or malformed workflow run ID.
type InvalidRunIDError struct {
	RunID RunID
}

// Error returns a contextual description of the invalid run ID.
func (e *InvalidRunIDError) Error() string {
	return fmt.Sprintf("run ID %q is invalid: identifiers must be non-empty and contain no whitespace or control characters", e.RunID)
}

// SnapshotValidationError identifies malformed durable execution state.
type SnapshotValidationError struct {
	RunID  RunID
	Reason string
}

// Error returns a contextual description of the malformed snapshot.
func (e *SnapshotValidationError) Error() string {
	return fmt.Sprintf("invalid snapshot for workflow run %q: %s", e.RunID, e.Reason)
}

// WorkflowConflictError reports an attempt to replace an immutable definition.
type WorkflowConflictError struct {
	WorkflowID workflow.WorkflowID
}

// Error returns a contextual description of the definition conflict.
func (e *WorkflowConflictError) Error() string {
	return fmt.Sprintf("workflow definition %q already exists with different content", e.WorkflowID)
}

// WorkflowNotFoundError reports a run referencing an unknown definition.
type WorkflowNotFoundError struct {
	WorkflowID workflow.WorkflowID
}

// Error returns a contextual description of the missing workflow.
func (e *WorkflowNotFoundError) Error() string {
	return fmt.Sprintf("workflow definition %q was not found", e.WorkflowID)
}

// RunAlreadyExistsError reports a duplicate workflow run ID.
type RunAlreadyExistsError struct {
	RunID RunID
}

// Error returns a contextual description of the duplicate run.
func (e *RunAlreadyExistsError) Error() string {
	return fmt.Sprintf("workflow run %q already exists", e.RunID)
}

// RunNotFoundError reports a mutation targeting an unknown workflow run.
type RunNotFoundError struct {
	RunID RunID
}

// Error returns a contextual description of the missing run.
func (e *RunNotFoundError) Error() string {
	return fmt.Sprintf("workflow run %q was not found", e.RunID)
}

// PersistenceError reports a store operation that prevented durable progress.
type PersistenceError struct {
	Operation string
	RunID     RunID
	Cause     error
}

// Error returns a contextual description of the failed durable operation.
func (e *PersistenceError) Error() string {
	if e.RunID == "" {
		return fmt.Sprintf("persistence operation %q failed: %v", e.Operation, e.Cause)
	}
	return fmt.Sprintf("persistence operation %q failed for workflow run %q: %v", e.Operation, e.RunID, e.Cause)
}

// Unwrap exposes the repository error for errors.Is and errors.As.
func (e *PersistenceError) Unwrap() error {
	return e.Cause
}

// UnknownTaskRunError reports a task ID that is not part of an execution.
type UnknownTaskRunError struct {
	RunID  RunID
	TaskID workflow.TaskID
}

// Error returns a contextual description of the unknown task.
func (e *UnknownTaskRunError) Error() string {
	return fmt.Sprintf("task %q is not part of workflow run %q", e.TaskID, e.RunID)
}

// TaskTransitionError reports an illegal task-run state transition.
type TaskTransitionError struct {
	RunID  RunID
	TaskID workflow.TaskID
	From   TaskRunStatus
	To     TaskRunStatus
}

// Error returns a contextual description of the illegal transition.
func (e *TaskTransitionError) Error() string {
	return fmt.Sprintf("task %q in run %q cannot transition from %q to %q", e.TaskID, e.RunID, e.From, e.To)
}

// WorkflowTransitionError reports an illegal workflow-run state transition.
type WorkflowTransitionError struct {
	RunID RunID
	From  WorkflowRunStatus
	To    WorkflowRunStatus
}

// Error returns a contextual description of the illegal transition.
func (e *WorkflowTransitionError) Error() string {
	return fmt.Sprintf("workflow run %q cannot transition from %q to %q", e.RunID, e.From, e.To)
}
