package workflow

import (
	"errors"
	"testing"
	"time"
)

func TestRetryPolicyEffectiveMaxAttempts(t *testing.T) {
	t.Parallel()

	if got := (RetryPolicy{}).EffectiveMaxAttempts(); got != 1 {
		t.Fatalf("zero policy EffectiveMaxAttempts() = %d, want 1", got)
	}
	if got := (RetryPolicy{MaxAttempts: 4}).EffectiveMaxAttempts(); got != 4 {
		t.Fatalf("configured EffectiveMaxAttempts() = %d, want 4", got)
	}
}

func TestRetryPolicyRetryDelay(t *testing.T) {
	t.Parallel()

	policy := RetryPolicy{
		MaxAttempts:    6,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     350 * time.Millisecond,
	}
	tests := []struct {
		completedAttempt int
		want             time.Duration
	}{
		{completedAttempt: 0, want: 0},
		{completedAttempt: 1, want: 100 * time.Millisecond},
		{completedAttempt: 2, want: 200 * time.Millisecond},
		{completedAttempt: 3, want: 350 * time.Millisecond},
		{completedAttempt: 4, want: 350 * time.Millisecond},
	}
	for _, test := range tests {
		if got := policy.RetryDelay(test.completedAttempt); got != test.want {
			t.Fatalf("RetryDelay(%d) = %v, want %v", test.completedAttempt, got, test.want)
		}
	}
}

func TestWorkflowDefinitionRejectsInvalidRetryPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy RetryPolicy
	}{
		{name: "negative attempts", policy: RetryPolicy{MaxAttempts: -1}},
		{name: "negative initial backoff", policy: RetryPolicy{InitialBackoff: -time.Second}},
		{name: "negative maximum backoff", policy: RetryPolicy{MaxBackoff: -time.Second}},
		{name: "maximum below initial", policy: RetryPolicy{InitialBackoff: time.Second, MaxBackoff: time.Millisecond}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			definition := WorkflowDefinition{
				ID: "workflow",
				Tasks: []TaskDefinition{{
					ID:    "task",
					Name:  "Task",
					Retry: test.policy,
				}},
			}
			var validationError *ValidationError
			if err := definition.Validate(); !errors.As(err, &validationError) {
				t.Fatalf("Validate() error = %v, want *ValidationError", err)
			}
			if validationError.Code != ValidationInvalidRetryPolicy {
				t.Fatalf("Validate() code = %q, want %q", validationError.Code, ValidationInvalidRetryPolicy)
			}
		})
	}
}
