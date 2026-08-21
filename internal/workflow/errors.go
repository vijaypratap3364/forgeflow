package workflow

import "fmt"

// ValidationCode identifies a category of workflow validation failure.
type ValidationCode string

const (
	// ValidationInvalidWorkflowID identifies an empty or malformed workflow ID.
	ValidationInvalidWorkflowID ValidationCode = "invalid_workflow_id"
	// ValidationEmptyWorkflow identifies a workflow with no tasks.
	ValidationEmptyWorkflow ValidationCode = "empty_workflow"
	// ValidationInvalidTaskID identifies an empty or malformed task ID.
	ValidationInvalidTaskID ValidationCode = "invalid_task_id"
	// ValidationInvalidTaskName identifies a task without a human-readable name.
	ValidationInvalidTaskName ValidationCode = "invalid_task_name"
	// ValidationDuplicateTaskID identifies two definitions with the same task ID.
	ValidationDuplicateTaskID ValidationCode = "duplicate_task_id"
	// ValidationInvalidDependencyID identifies an empty or malformed dependency ID.
	ValidationInvalidDependencyID ValidationCode = "invalid_dependency_id"
	// ValidationMissingDependency identifies a dependency not defined by the workflow.
	ValidationMissingDependency ValidationCode = "missing_dependency"
	// ValidationSelfDependency identifies a task that directly depends on itself.
	ValidationSelfDependency ValidationCode = "self_dependency"
	// ValidationDuplicateDependency identifies a repeated dependency edge.
	ValidationDuplicateDependency ValidationCode = "duplicate_dependency"
	// ValidationCycle identifies a dependency cycle involving multiple tasks.
	ValidationCycle ValidationCode = "cycle"
	// ValidationUnknownCompletedTask identifies completed state for an undefined task.
	ValidationUnknownCompletedTask ValidationCode = "unknown_completed_task"
	// ValidationInvalidRetryPolicy identifies invalid retry limits or delays.
	ValidationInvalidRetryPolicy ValidationCode = "invalid_retry_policy"
)

// ValidationError describes a workflow validation failure in a form that later
// transport layers can classify without parsing its message.
type ValidationError struct {
	Code         ValidationCode
	WorkflowID   WorkflowID
	TaskID       TaskID
	DependencyID TaskID
	Reason       string
}

// Error returns a contextual description of the validation failure.
func (e *ValidationError) Error() string {
	switch e.Code {
	case ValidationInvalidWorkflowID:
		return fmt.Sprintf("workflow ID %q is invalid: identifiers must be non-empty and contain no whitespace or control characters", e.WorkflowID)
	case ValidationEmptyWorkflow:
		return fmt.Sprintf("workflow %q must contain at least one task", e.WorkflowID)
	case ValidationInvalidTaskID:
		return fmt.Sprintf("workflow %q has invalid task ID %q: identifiers must be non-empty and contain no whitespace or control characters", e.WorkflowID, e.TaskID)
	case ValidationInvalidTaskName:
		return fmt.Sprintf("task %q in workflow %q must have a non-empty name", e.TaskID, e.WorkflowID)
	case ValidationDuplicateTaskID:
		return fmt.Sprintf("workflow %q contains duplicate task ID %q", e.WorkflowID, e.TaskID)
	case ValidationInvalidDependencyID:
		return fmt.Sprintf("task %q in workflow %q has invalid dependency ID %q", e.TaskID, e.WorkflowID, e.DependencyID)
	case ValidationMissingDependency:
		return fmt.Sprintf("task %q in workflow %q depends on undefined task %q", e.TaskID, e.WorkflowID, e.DependencyID)
	case ValidationSelfDependency:
		return fmt.Sprintf("task %q in workflow %q depends on itself", e.TaskID, e.WorkflowID)
	case ValidationDuplicateDependency:
		return fmt.Sprintf("task %q in workflow %q repeats dependency %q", e.TaskID, e.WorkflowID, e.DependencyID)
	case ValidationCycle:
		return fmt.Sprintf("workflow %q contains a dependency cycle", e.WorkflowID)
	case ValidationUnknownCompletedTask:
		return fmt.Sprintf("completed task %q is not defined in workflow %q", e.TaskID, e.WorkflowID)
	case ValidationInvalidRetryPolicy:
		return fmt.Sprintf("task %q in workflow %q has an invalid retry policy: %s", e.TaskID, e.WorkflowID, e.Reason)
	default:
		return fmt.Sprintf("workflow %q is invalid", e.WorkflowID)
	}
}
