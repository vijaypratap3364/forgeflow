package execution

import (
	"errors"
	"reflect"
	"testing"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

func TestNewWorkflowRunCreatesIndependentPendingState(t *testing.T) {
	t.Parallel()

	definition := testWorkflow(
		testTask("b", "a"),
		testTask("a"),
	)

	run, err := NewWorkflowRun("run-1", definition)
	if err != nil {
		t.Fatalf("NewWorkflowRun() error = %v", err)
	}

	if got := run.ID(); got != "run-1" {
		t.Fatalf("ID() = %q, want %q", got, RunID("run-1"))
	}
	if got := run.WorkflowID(); got != definition.ID {
		t.Fatalf("WorkflowID() = %q, want %q", got, definition.ID)
	}
	if got := run.Status(); got != WorkflowRunPending {
		t.Fatalf("Status() = %q, want %q", got, WorkflowRunPending)
	}

	wantTasks := []TaskRun{
		{TaskID: "a", Status: TaskRunPending, UpdatedAt: run.CreatedAt()},
		{TaskID: "b", Status: TaskRunPending, UpdatedAt: run.CreatedAt()},
	}
	if got := run.Tasks(); !reflect.DeepEqual(got, wantTasks) {
		t.Fatalf("Tasks() = %#v, want %#v", got, wantTasks)
	}

	definition.Tasks[0].Name = "mutated"
	definition.Tasks[0].Dependencies[0] = "mutated"
	copyFromRun := run.Definition()
	if copyFromRun.Tasks[0].Name == "mutated" || copyFromRun.Tasks[0].Dependencies[0] == "mutated" {
		t.Fatal("NewWorkflowRun() retained mutable workflow definition storage")
	}

	copyFromRun.Tasks[0].Dependencies[0] = "changed-again"
	if got := run.Definition().Tasks[0].Dependencies[0]; got == "changed-again" {
		t.Fatal("Definition() returned mutable workflow definition storage")
	}
}

func TestNewWorkflowRunRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		runID      RunID
		definition workflow.WorkflowDefinition
		wantType   any
	}{
		{
			name:       "empty run ID",
			definition: testWorkflow(testTask("a")),
			wantType:   &InvalidRunIDError{},
		},
		{
			name:       "run ID with whitespace",
			runID:      "bad run",
			definition: testWorkflow(testTask("a")),
			wantType:   &InvalidRunIDError{},
		},
		{
			name:       "invalid workflow definition",
			runID:      "run-1",
			definition: workflow.WorkflowDefinition{ID: "workflow"},
			wantType:   &workflow.ValidationError{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewWorkflowRun(test.runID, test.definition)
			if err == nil {
				t.Fatal("NewWorkflowRun() error = nil, want error")
			}

			switch test.wantType.(type) {
			case *InvalidRunIDError:
				var target *InvalidRunIDError
				if !errors.As(err, &target) {
					t.Fatalf("NewWorkflowRun() error type = %T, want *InvalidRunIDError", err)
				}
			case *workflow.ValidationError:
				var target *workflow.ValidationError
				if !errors.As(err, &target) {
					t.Fatalf("NewWorkflowRun() error type = %T, want *workflow.ValidationError", err)
				}
			default:
				t.Fatalf("unsupported expected error type %T", test.wantType)
			}
		})
	}
}

func TestTaskRunLegalTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		steps      []taskTransition
		wantStatus TaskRunStatus
		wantOutput string
		wantError  string
	}{
		{
			name: "successful task",
			steps: []taskTransition{
				{target: TaskRunReady},
				{target: TaskRunRunning},
				{target: TaskRunSucceeded, output: "result"},
			},
			wantStatus: TaskRunSucceeded,
			wantOutput: "result",
		},
		{
			name: "failed task",
			steps: []taskTransition{
				{target: TaskRunReady},
				{target: TaskRunRunning},
				{target: TaskRunFailed, err: errors.New("handler failed")},
			},
			wantStatus: TaskRunFailed,
			wantError:  "handler failed",
		},
		{
			name:       "pending task canceled",
			steps:      []taskTransition{{target: TaskRunCanceled, err: errors.New("stopped")}},
			wantStatus: TaskRunCanceled,
			wantError:  "stopped",
		},
		{
			name: "ready task canceled",
			steps: []taskTransition{
				{target: TaskRunReady},
				{target: TaskRunCanceled, err: errors.New("stopped")},
			},
			wantStatus: TaskRunCanceled,
			wantError:  "stopped",
		},
		{
			name: "running task canceled",
			steps: []taskTransition{
				{target: TaskRunReady},
				{target: TaskRunRunning},
				{target: TaskRunCanceled, err: errors.New("stopped")},
			},
			wantStatus: TaskRunCanceled,
			wantError:  "stopped",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			run := mustWorkflowRun(t, testWorkflow(testTask("a")))
			for _, step := range test.steps {
				if err := run.transitionTask("a", step.target, step.output, step.err); err != nil {
					t.Fatalf("transitionTask(%q) error = %v", step.target, err)
				}
			}

			task, exists := run.Task("a")
			if !exists {
				t.Fatal("Task() did not find task a")
			}
			if task.Status != test.wantStatus || task.Output != test.wantOutput || task.Error != test.wantError {
				t.Fatalf("Task() = %#v, want status=%q output=%q error=%q", task, test.wantStatus, test.wantOutput, test.wantError)
			}
			wantAttempts := 0
			if slicesContainTransition(test.steps, TaskRunRunning) {
				wantAttempts = 1
			}
			if task.AttemptCount != wantAttempts {
				t.Fatalf("Task().AttemptCount = %d, want %d", task.AttemptCount, wantAttempts)
			}
			if task.UpdatedAt.IsZero() || task.FinishedAt.IsZero() {
				t.Fatalf("Task() timestamps were not recorded: %#v", task)
			}
			if wantAttempts == 1 && task.StartedAt.IsZero() {
				t.Fatalf("Task().StartedAt is zero after execution: %#v", task)
			}
		})
	}
}

func TestTaskRunRejectsIllegalTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		setup  []TaskRunStatus
		target TaskRunStatus
	}{
		{name: "pending to running", target: TaskRunRunning},
		{name: "pending to succeeded", target: TaskRunSucceeded},
		{name: "ready to succeeded", setup: []TaskRunStatus{TaskRunReady}, target: TaskRunSucceeded},
		{name: "running to ready", setup: []TaskRunStatus{TaskRunReady, TaskRunRunning}, target: TaskRunReady},
		{name: "succeeded to failed", setup: []TaskRunStatus{TaskRunReady, TaskRunRunning, TaskRunSucceeded}, target: TaskRunFailed},
		{name: "failed to ready", setup: []TaskRunStatus{TaskRunReady, TaskRunRunning, TaskRunFailed}, target: TaskRunReady},
		{name: "canceled to ready", setup: []TaskRunStatus{TaskRunCanceled}, target: TaskRunReady},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			run := mustWorkflowRun(t, testWorkflow(testTask("a")))
			for _, status := range test.setup {
				if err := run.transitionTask("a", status, "", nil); err != nil {
					t.Fatalf("setup transitionTask(%q) error = %v", status, err)
				}
			}

			err := run.transitionTask("a", test.target, "", nil)
			var transitionError *TaskTransitionError
			if !errors.As(err, &transitionError) {
				t.Fatalf("transitionTask() error = %v, want *TaskTransitionError", err)
			}
		})
	}
}

func TestWorkflowRunTransitions(t *testing.T) {
	t.Parallel()

	terminalStatuses := []WorkflowRunStatus{
		WorkflowRunSucceeded,
		WorkflowRunFailed,
		WorkflowRunCanceled,
	}
	for _, terminalStatus := range terminalStatuses {
		t.Run(string(terminalStatus), func(t *testing.T) {
			t.Parallel()

			run := mustWorkflowRun(t, testWorkflow(testTask("a")))
			if err := run.transitionWorkflow(WorkflowRunRunning); err != nil {
				t.Fatalf("transitionWorkflow(running) error = %v", err)
			}
			if err := run.transitionWorkflow(terminalStatus); err != nil {
				t.Fatalf("transitionWorkflow(%q) error = %v", terminalStatus, err)
			}
			if got := run.Status(); got != terminalStatus {
				t.Fatalf("Status() = %q, want %q", got, terminalStatus)
			}

			err := run.transitionWorkflow(WorkflowRunRunning)
			var transitionError *WorkflowTransitionError
			if !errors.As(err, &transitionError) {
				t.Fatalf("terminal transition error = %v, want *WorkflowTransitionError", err)
			}
		})
	}

	t.Run("pending cancellation", func(t *testing.T) {
		t.Parallel()

		run := mustWorkflowRun(t, testWorkflow(testTask("a")))
		if err := run.transitionWorkflow(WorkflowRunCanceled); err != nil {
			t.Fatalf("transitionWorkflow(canceled) error = %v", err)
		}
	})
}

func TestWorkflowRunCancelsOnlyUnfinishedTasks(t *testing.T) {
	t.Parallel()

	run := mustWorkflowRun(t, testWorkflow(
		testTask("pending"),
		testTask("ready"),
		testTask("running"),
		testTask("succeeded"),
		testTask("failed"),
	))

	mustTransitionTask(t, run, "ready", TaskRunReady)
	mustTransitionTask(t, run, "running", TaskRunReady, TaskRunRunning)
	mustTransitionTask(t, run, "succeeded", TaskRunReady, TaskRunRunning, TaskRunSucceeded)
	mustTransitionTask(t, run, "failed", TaskRunReady, TaskRunRunning, TaskRunFailed)

	run.cancelUnfinished(errors.New("workflow stopped"))

	wantStatuses := map[workflow.TaskID]TaskRunStatus{
		"pending":   TaskRunCanceled,
		"ready":     TaskRunCanceled,
		"running":   TaskRunCanceled,
		"succeeded": TaskRunSucceeded,
		"failed":    TaskRunFailed,
	}
	for taskID, want := range wantStatuses {
		task, exists := run.Task(taskID)
		if !exists {
			t.Fatalf("Task(%q) was not found", taskID)
		}
		if task.Status != want {
			t.Fatalf("Task(%q).Status = %q, want %q", taskID, task.Status, want)
		}
	}
}

func TestWorkflowRunRejectsUnknownTask(t *testing.T) {
	t.Parallel()

	run := mustWorkflowRun(t, testWorkflow(testTask("a")))
	err := run.transitionTask("missing", TaskRunReady, "", nil)
	var unknownTaskError *UnknownTaskRunError
	if !errors.As(err, &unknownTaskError) {
		t.Fatalf("transitionTask() error = %v, want *UnknownTaskRunError", err)
	}
}

type taskTransition struct {
	target TaskRunStatus
	output string
	err    error
}

func slicesContainTransition(steps []taskTransition, target TaskRunStatus) bool {
	for _, step := range steps {
		if step.target == target {
			return true
		}
	}
	return false
}

func mustWorkflowRun(t *testing.T, definition workflow.WorkflowDefinition) *WorkflowRun {
	t.Helper()

	run, err := NewWorkflowRun("run-1", definition)
	if err != nil {
		t.Fatalf("NewWorkflowRun() error = %v", err)
	}
	return run
}

func mustTransitionTask(t *testing.T, run *WorkflowRun, taskID workflow.TaskID, statuses ...TaskRunStatus) {
	t.Helper()

	for _, status := range statuses {
		if err := run.transitionTask(taskID, status, "", nil); err != nil {
			t.Fatalf("transitionTask(%q, %q) error = %v", taskID, status, err)
		}
	}
}

func testWorkflow(tasks ...workflow.TaskDefinition) workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		ID:    "workflow",
		Tasks: tasks,
	}
}

func testTask(id string, dependencies ...string) workflow.TaskDefinition {
	dependencyIDs := make([]workflow.TaskID, len(dependencies))
	for index, dependency := range dependencies {
		dependencyIDs[index] = workflow.TaskID(dependency)
	}
	return workflow.TaskDefinition{
		ID:           workflow.TaskID(id),
		Name:         "Task " + id,
		Dependencies: dependencyIDs,
	}
}
