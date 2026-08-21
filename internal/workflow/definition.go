// Package workflow defines ForgeFlow workflows and their dependency semantics.
package workflow

// WorkflowID is the stable identifier of a workflow definition.
type WorkflowID string

// TaskID is the stable identifier of a task within a workflow definition.
type TaskID string

// TaskDefinition describes a task and the tasks that must complete before it.
type TaskDefinition struct {
	ID           TaskID
	Name         string
	Dependencies []TaskID
}

// WorkflowDefinition describes a workflow as a collection of task definitions.
type WorkflowDefinition struct {
	ID    WorkflowID
	Tasks []TaskDefinition
}
