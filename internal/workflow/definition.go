// Package workflow defines ForgeFlow workflows and their dependency semantics.
package workflow

import "time"

// WorkflowID is the stable identifier of a workflow definition.
type WorkflowID string

// TaskID is the stable identifier of a task within a workflow definition.
type TaskID string

// HandlerName identifies a safe task handler registered with the execution engine.
type HandlerName string

// RetryPolicy controls how a retryable task failure is attempted again. A zero
// MaxAttempts value means one total attempt, preserving the pre-retry default.
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// EffectiveMaxAttempts returns the configured total-attempt limit, applying the
// default of one attempt when MaxAttempts is zero.
func (policy RetryPolicy) EffectiveMaxAttempts() int {
	if policy.MaxAttempts == 0 {
		return 1
	}
	return policy.MaxAttempts
}

// RetryDelay returns the exponential delay after completedAttempt. Attempt one
// uses InitialBackoff; later attempts double up to MaxBackoff when it is set.
func (policy RetryPolicy) RetryDelay(completedAttempt int) time.Duration {
	if completedAttempt < 1 || policy.InitialBackoff <= 0 {
		return 0
	}

	delay := policy.InitialBackoff
	for attempt := 1; attempt < completedAttempt; attempt++ {
		if policy.MaxBackoff > 0 && delay >= policy.MaxBackoff {
			return policy.MaxBackoff
		}
		if delay > time.Duration(1<<63-1)/2 {
			delay = time.Duration(1<<63 - 1)
		} else {
			delay *= 2
		}
	}
	if policy.MaxBackoff > 0 && delay > policy.MaxBackoff {
		return policy.MaxBackoff
	}
	return delay
}

// TaskDefinition describes a task, its dependencies, and its safe handler input.
type TaskDefinition struct {
	ID           TaskID
	Name         string
	Dependencies []TaskID
	Handler      HandlerName
	Input        string
	Retry        RetryPolicy
}

// WorkflowDefinition describes a workflow as a collection of task definitions.
type WorkflowDefinition struct {
	ID    WorkflowID
	Tasks []TaskDefinition
}
