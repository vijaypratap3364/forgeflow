package execution

import (
	"fmt"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

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
