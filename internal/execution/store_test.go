package execution

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

func TestWorkflowRunSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	definition := testWorkflow(testTask("a"), testTask("b", "a"))
	baseTime := time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)
	run, err := newWorkflowRun("run-1", definition, func() time.Time { return baseTime })
	if err != nil {
		t.Fatalf("newWorkflowRun() error = %v", err)
	}
	if err := run.transitionWorkflow(WorkflowRunRunning); err != nil {
		t.Fatalf("transitionWorkflow() error = %v", err)
	}
	mustTransitionTask(t, run, "a", TaskRunReady, TaskRunRunning, TaskRunSucceeded)

	snapshot := run.Snapshot()
	restored, err := restoreWorkflowRun(snapshot, definition, func() time.Time { return baseTime.Add(time.Hour) })
	if err != nil {
		t.Fatalf("restoreWorkflowRun() error = %v", err)
	}
	if got := restored.Snapshot(); !reflect.DeepEqual(got, snapshot) {
		t.Fatalf("restored Snapshot() = %#v, want %#v", got, snapshot)
	}

	snapshot.Tasks[0].Output = "mutated"
	task, exists := restored.Task("a")
	if !exists {
		t.Fatal("Task(a) was not found")
	}
	if task.Output == "mutated" {
		t.Fatal("RestoreWorkflowRun() retained caller-owned task storage")
	}
}

func TestWorkflowRunSnapshotValidation(t *testing.T) {
	t.Parallel()

	definition := testWorkflow(testTask("a"))
	run := mustWorkflowRun(t, definition)
	valid := run.Snapshot()

	tests := []struct {
		name   string
		mutate func(*WorkflowRunSnapshot)
	}{
		{name: "empty run ID", mutate: func(snapshot *WorkflowRunSnapshot) { snapshot.ID = "" }},
		{name: "unknown status", mutate: func(snapshot *WorkflowRunSnapshot) { snapshot.Status = "unknown" }},
		{name: "missing timestamp", mutate: func(snapshot *WorkflowRunSnapshot) { snapshot.CreatedAt = time.Time{} }},
		{name: "missing tasks", mutate: func(snapshot *WorkflowRunSnapshot) { snapshot.Tasks = nil }},
		{name: "duplicate task", mutate: func(snapshot *WorkflowRunSnapshot) { snapshot.Tasks = append(snapshot.Tasks, snapshot.Tasks[0]) }},
		{name: "negative attempts", mutate: func(snapshot *WorkflowRunSnapshot) { snapshot.Tasks[0].AttemptCount = -1 }},
		{name: "running without attempt", mutate: func(snapshot *WorkflowRunSnapshot) { snapshot.Tasks[0].Status = TaskRunRunning }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			snapshot := valid
			snapshot.Tasks = append([]TaskRun(nil), valid.Tasks...)
			test.mutate(&snapshot)
			var validationError *SnapshotValidationError
			if err := snapshot.Validate(); !errors.As(err, &validationError) {
				t.Fatalf("Validate() error = %v, want *SnapshotValidationError", err)
			}
		})
	}
}

func TestWorkflowRunSnapshotRejectsMultipleLeasesForOneWorker(t *testing.T) {
	t.Parallel()

	run := mustWorkflowRun(t, testWorkflow(testTask("a"), testTask("b")))
	snapshot := run.Snapshot()
	snapshot.Status = WorkflowRunRunning
	workerID := WorkerID("worker-1")
	for index := range snapshot.Tasks {
		task := &snapshot.Tasks[index]
		task.Status = TaskRunRunning
		task.AttemptCount = 1
		task.CurrentAttemptID = AttemptIDFor(task.TaskRunID, task.AttemptCount)
		task.StartedAt = task.UpdatedAt
		task.Lease = &TaskLease{
			WorkerID:  workerID,
			TaskRunID: task.TaskRunID,
			AttemptID: task.CurrentAttemptID,
			ExpiresAt: task.UpdatedAt.Add(time.Minute),
		}
	}
	snapshot.Workers = []WorkerHeartbeat{{
		WorkerID:        workerID,
		LastHeartbeatAt: snapshot.UpdatedAt,
	}}

	var validationError *SnapshotValidationError
	if err := snapshot.Validate(); !errors.As(err, &validationError) {
		t.Fatalf("Validate() error = %v, want *SnapshotValidationError", err)
	}
}

func TestRestoreWorkflowRunRejectsDefinitionMismatch(t *testing.T) {
	t.Parallel()

	run := mustWorkflowRun(t, testWorkflow(testTask("a")))
	definition := workflow.WorkflowDefinition{
		ID:    "another-workflow",
		Tasks: []workflow.TaskDefinition{testTask("a")},
	}

	var validationError *SnapshotValidationError
	if _, err := RestoreWorkflowRun(run.Snapshot(), definition); !errors.As(err, &validationError) {
		t.Fatalf("RestoreWorkflowRun() error = %v, want *SnapshotValidationError", err)
	}
}
