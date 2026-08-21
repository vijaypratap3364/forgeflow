package execution

import (
	"fmt"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

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
