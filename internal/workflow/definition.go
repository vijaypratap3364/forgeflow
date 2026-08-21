// Package workflow defines ForgeFlow workflows and their dependency semantics.
package workflow

// WorkflowID is the stable identifier of a workflow definition.
type WorkflowID string

// TaskID is the stable identifier of a task within a workflow definition.
type TaskID string

// HandlerName identifies a safe task handler registered with the execution engine.
type HandlerName string

// TaskDefinition describes a task, its dependencies, and its safe handler input.
type TaskDefinition struct {
	ID           TaskID
	Name         string
	Dependencies []TaskID
	Handler      HandlerName
	Input        string
}

// WorkflowDefinition describes a workflow as a collection of task definitions.
type WorkflowDefinition struct {
	ID    WorkflowID
	Tasks []TaskDefinition
}
