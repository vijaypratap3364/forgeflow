package execution

import (
	"errors"
	"testing"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

func TestTaskAttemptRetriesTransientFailureThenSucceeds(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	run := newReliabilityRun(t, &now, workflow.RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: time.Second,
		MaxBackoff:     2 * time.Second,
	})
	attemptOne := startReliabilityAttempt(t, run, "worker-1", 10*time.Second)
	temporary := errors.New("temporary failure")
	outcome, err := run.completeTaskAttempt("task", "worker-1", attemptOne, "", Retryable(temporary))
	if err != nil {
		t.Fatalf("completeTaskAttempt() error = %v", err)
	}
	if outcome != CompletionRetryScheduled {
		t.Fatalf("completion outcome = %q, want %q", outcome, CompletionRetryScheduled)
	}
	task := requireTaskRun(t, run, "task")
	if task.Status != TaskRunRetryWaiting || task.NextAttemptAt != now.Add(time.Second) {
		t.Fatalf("task after transient failure = %#v", task)
	}

	duplicate, err := run.completeTaskAttempt("task", "worker-1", attemptOne, "duplicate", nil)
	if err != nil || duplicate != CompletionIgnored {
		t.Fatalf("duplicate completion = %q, error %v, want ignored", duplicate, err)
	}
	if got := requireTaskRun(t, run, "task"); got.Status != TaskRunRetryWaiting || got.Output != "" {
		t.Fatalf("duplicate completion corrupted retry state: %#v", got)
	}

	now = now.Add(time.Second)
	if promoted := run.promoteDueRetries(); len(promoted) != 1 || promoted[0] != "task" {
		t.Fatalf("promoteDueRetries() = %v, want [task]", promoted)
	}
	attemptTwo := startReliabilityAttempt(t, run, "worker-2", 10*time.Second)
	if attemptTwo == attemptOne {
		t.Fatalf("attempt IDs are not unique: %q", attemptTwo)
	}
	outcome, err = run.completeTaskAttempt("task", "worker-2", attemptTwo, "success", nil)
	if err != nil || outcome != CompletionSucceeded {
		t.Fatalf("successful completion = %q, error %v", outcome, err)
	}
	task = requireTaskRun(t, run, "task")
	if task.Status != TaskRunSucceeded || task.AttemptCount != 2 || task.Output != "success" || task.Lease != nil {
		t.Fatalf("final task state = %#v", task)
	}
}

func TestStableExecutionIdentifiersAreDeterministicAndUnambiguous(t *testing.T) {
	t.Parallel()

	first := TaskRunIDFor("run:one", "task")
	if got := TaskRunIDFor("run:one", "task"); got != first {
		t.Fatalf("TaskRunIDFor() = %q, want stable %q", got, first)
	}
	if collision := TaskRunIDFor("run", "one:task"); collision == first {
		t.Fatalf("TaskRunIDFor() collided for distinct logical tasks: %q", first)
	}
	if AttemptIDFor(first, 1) == AttemptIDFor(first, 2) {
		t.Fatal("AttemptIDFor() returned the same ID for distinct attempts")
	}
}

func TestTaskAttemptDistinguishesTerminalFailureAndRetryExhaustion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		firstError    func(error) error
		wantAttempts  int
		wantFirst     CompletionOutcome
		completeAgain bool
	}{
		{
			name:         "unmarked failure is terminal",
			firstError:   func(err error) error { return err },
			wantAttempts: 1,
			wantFirst:    CompletionFailed,
		},
		{
			name:          "retryable failure exhausts limit",
			firstError:    Retryable,
			wantAttempts:  2,
			wantFirst:     CompletionRetryScheduled,
			completeAgain: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
			run := newReliabilityRun(t, &now, workflow.RetryPolicy{MaxAttempts: 2})
			attempt := startReliabilityAttempt(t, run, "worker-1", time.Minute)
			outcome, err := run.completeTaskAttempt("task", "worker-1", attempt, "", test.firstError(errors.New("failure")))
			if err != nil || outcome != test.wantFirst {
				t.Fatalf("first completion = %q, error %v, want %q", outcome, err, test.wantFirst)
			}
			if test.completeAgain {
				attempt = startReliabilityAttempt(t, run, "worker-2", time.Minute)
				outcome, err = run.completeTaskAttempt("task", "worker-2", attempt, "", Retryable(errors.New("still failing")))
				if err != nil || outcome != CompletionFailed {
					t.Fatalf("exhausting completion = %q, error %v, want failed", outcome, err)
				}
			}
			task := requireTaskRun(t, run, "task")
			if task.Status != TaskRunFailed || task.AttemptCount != test.wantAttempts {
				t.Fatalf("terminal task = %#v, want failed after %d attempts", task, test.wantAttempts)
			}
		})
	}
}

func TestTaskLeaseHeartbeatAndExpiryRecovery(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	run := newReliabilityRun(t, &now, workflow.RetryPolicy{MaxAttempts: 3})
	attemptOne := startReliabilityAttempt(t, run, "worker-1", 10*time.Second)
	initialExpiry := requireTaskRun(t, run, "task").Lease.ExpiresAt

	now = now.Add(5 * time.Second)
	if recoveries := run.recoverExpiredLeases(); len(recoveries) != 0 {
		t.Fatalf("active lease was stolen: %v", recoveries)
	}
	if err := run.recordWorkerHeartbeat("worker-1", 10*time.Second); err != nil {
		t.Fatalf("recordWorkerHeartbeat() error = %v", err)
	}
	renewed := requireTaskRun(t, run, "task")
	if !renewed.Lease.ExpiresAt.After(initialExpiry) {
		t.Fatalf("heartbeat expiry = %v, want after %v", renewed.Lease.ExpiresAt, initialExpiry)
	}
	workers := run.Workers()
	if len(workers) != 1 || workers[0].WorkerID != "worker-1" || workers[0].LastHeartbeatAt != now {
		t.Fatalf("Workers() = %#v", workers)
	}
	originalExpiry := renewed.Lease.ExpiresAt
	callerLease := renewed.Lease
	callerLease.ExpiresAt = now.Add(24 * time.Hour)
	if got := requireTaskRun(t, run, "task").Lease.ExpiresAt; got != originalExpiry {
		t.Fatalf("Task() exposed mutable lease storage: got %v, want %v", got, originalExpiry)
	}

	now = originalExpiry
	recoveries := run.recoverExpiredLeases()
	if len(recoveries) != 1 || recoveries[0].AttemptID != attemptOne || recoveries[0].Outcome != CompletionRetryScheduled {
		t.Fatalf("recoverExpiredLeases() = %#v", recoveries)
	}
	task := requireTaskRun(t, run, "task")
	if task.Status != TaskRunReady || task.AttemptCount != 1 || task.Lease != nil {
		t.Fatalf("recovered task = %#v", task)
	}
	attemptTwo := startReliabilityAttempt(t, run, "worker-2", 10*time.Second)
	if attemptTwo == attemptOne || requireTaskRun(t, run, "task").AttemptCount != 2 {
		t.Fatal("recovered dispatch did not create exactly one new attempt")
	}
}

func TestExpiredLeaseDoesNotCorruptCompletedTask(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	run := newReliabilityRun(t, &now, workflow.RetryPolicy{MaxAttempts: 2})
	attempt := startReliabilityAttempt(t, run, "worker-1", time.Second)
	if outcome, err := run.completeTaskAttempt("task", "worker-1", attempt, "done", nil); err != nil || outcome != CompletionSucceeded {
		t.Fatalf("completeTaskAttempt() = %q, error %v", outcome, err)
	}
	now = now.Add(time.Hour)
	if recoveries := run.recoverExpiredLeases(); len(recoveries) != 0 {
		t.Fatalf("completed task was recovered: %v", recoveries)
	}
	task := requireTaskRun(t, run, "task")
	if task.Status != TaskRunSucceeded || task.Output != "done" || task.AttemptCount != 1 {
		t.Fatalf("completed task was corrupted: %#v", task)
	}
}

func newReliabilityRun(t *testing.T, now *time.Time, policy workflow.RetryPolicy) *WorkflowRun {
	t.Helper()

	definition := testWorkflow(testTask("task"))
	definition.Tasks[0].Retry = policy
	run, err := newWorkflowRun("run-1", definition, func() time.Time { return *now })
	if err != nil {
		t.Fatalf("newWorkflowRun() error = %v", err)
	}
	if err := run.transitionWorkflow(WorkflowRunRunning); err != nil {
		t.Fatalf("transitionWorkflow() error = %v", err)
	}
	if err := run.transitionTask("task", TaskRunReady, "", nil); err != nil {
		t.Fatalf("transitionTask(ready) error = %v", err)
	}
	return run
}

func startReliabilityAttempt(t *testing.T, run *WorkflowRun, workerID WorkerID, lease time.Duration) AttemptID {
	t.Helper()

	attemptID, err := run.startTaskAttempt("task", workerID, lease)
	if err != nil {
		t.Fatalf("startTaskAttempt() error = %v", err)
	}
	return attemptID
}
