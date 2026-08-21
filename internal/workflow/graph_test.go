package workflow

import (
	"errors"
	"reflect"
	"testing"
)

func TestWorkflowDefinitionGraphShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tasks     []TaskDefinition
		wantOrder []TaskID
		wantRoots []TaskID
	}{
		{
			name: "linear DAG",
			tasks: []TaskDefinition{
				newTask("c", "b"),
				newTask("a"),
				newTask("b", "a"),
			},
			wantOrder: taskIDs("a", "b", "c"),
			wantRoots: taskIDs("a"),
		},
		{
			name: "fan-out",
			tasks: []TaskDefinition{
				newTask("c", "a"),
				newTask("a"),
				newTask("b", "a"),
			},
			wantOrder: taskIDs("a", "b", "c"),
			wantRoots: taskIDs("a"),
		},
		{
			name: "fan-in",
			tasks: []TaskDefinition{
				newTask("c", "b", "a"),
				newTask("b"),
				newTask("a"),
			},
			wantOrder: taskIDs("a", "b", "c"),
			wantRoots: taskIDs("a", "b"),
		},
		{
			name: "diamond dependency",
			tasks: []TaskDefinition{
				newTask("d", "c", "b"),
				newTask("c", "a"),
				newTask("a"),
				newTask("b", "a"),
			},
			wantOrder: taskIDs("a", "b", "c", "d"),
			wantRoots: taskIDs("a"),
		},
		{
			name: "disconnected components",
			tasks: []TaskDefinition{
				newTask("d", "c"),
				newTask("b", "a"),
				newTask("c"),
				newTask("a"),
			},
			wantOrder: taskIDs("a", "b", "c", "d"),
			wantRoots: taskIDs("a", "c"),
		},
		{
			name: "branch and merge",
			tasks: []TaskDefinition{
				newTask("e", "d", "c"),
				newTask("d", "b"),
				newTask("c", "b"),
				newTask("b", "a"),
				newTask("a"),
			},
			wantOrder: taskIDs("a", "b", "c", "d", "e"),
			wantRoots: taskIDs("a"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			definition := WorkflowDefinition{
				ID:    "workflow",
				Tasks: test.tasks,
			}

			if err := definition.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}

			acyclic, err := definition.IsAcyclic()
			if err != nil {
				t.Fatalf("IsAcyclic() error = %v", err)
			}
			if !acyclic {
				t.Fatal("IsAcyclic() = false, want true")
			}

			order, err := definition.TopologicalOrder()
			if err != nil {
				t.Fatalf("TopologicalOrder() error = %v", err)
			}
			assertTaskIDs(t, order, test.wantOrder)

			roots, err := definition.RootTasks()
			if err != nil {
				t.Fatalf("RootTasks() error = %v", err)
			}
			assertTaskIDs(t, idsOf(roots), test.wantRoots)
		})
	}
}

func TestWorkflowDefinitionReadyTasks(t *testing.T) {
	t.Parallel()

	definition := WorkflowDefinition{
		ID: "workflow",
		Tasks: []TaskDefinition{
			newTask("e", "d", "c"),
			newTask("d", "b"),
			newTask("c", "b"),
			newTask("b", "a"),
			newTask("a"),
		},
	}

	tests := []struct {
		name      string
		completed map[TaskID]struct{}
		want      []TaskID
	}{
		{name: "initial root", want: taskIDs("a")},
		{name: "linear successor", completed: completedTasks("a"), want: taskIDs("b")},
		{name: "fan-out successors", completed: completedTasks("a", "b"), want: taskIDs("c", "d")},
		{name: "partially completed fan-out", completed: completedTasks("a", "b", "c"), want: taskIDs("d")},
		{name: "fan-in successor", completed: completedTasks("a", "b", "c", "d"), want: taskIDs("e")},
		{name: "workflow completed", completed: completedTasks("a", "b", "c", "d", "e")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ready, err := definition.ReadyTasks(test.completed)
			if err != nil {
				t.Fatalf("ReadyTasks() error = %v", err)
			}
			assertTaskIDs(t, idsOf(ready), test.want)
		})
	}
}

func TestWorkflowDefinitionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		definition     WorkflowDefinition
		wantCode       ValidationCode
		wantTask       TaskID
		wantDependency TaskID
	}{
		{
			name:       "empty workflow ID",
			definition: WorkflowDefinition{Tasks: []TaskDefinition{newTask("a")}},
			wantCode:   ValidationInvalidWorkflowID,
		},
		{
			name:       "workflow ID contains whitespace",
			definition: WorkflowDefinition{ID: "invalid ID", Tasks: []TaskDefinition{newTask("a")}},
			wantCode:   ValidationInvalidWorkflowID,
		},
		{
			name:       "empty workflow",
			definition: WorkflowDefinition{ID: "workflow"},
			wantCode:   ValidationEmptyWorkflow,
		},
		{
			name: "empty task ID",
			definition: WorkflowDefinition{
				ID: "workflow",
				Tasks: []TaskDefinition{
					{ID: "", Name: "Empty ID"},
				},
			},
			wantCode: ValidationInvalidTaskID,
		},
		{
			name: "task ID contains whitespace",
			definition: WorkflowDefinition{
				ID: "workflow",
				Tasks: []TaskDefinition{
					{ID: "bad id", Name: "Bad ID"},
				},
			},
			wantCode: ValidationInvalidTaskID,
			wantTask: "bad id",
		},
		{
			name: "empty task name",
			definition: WorkflowDefinition{
				ID: "workflow",
				Tasks: []TaskDefinition{
					{ID: "a", Name: "  "},
				},
			},
			wantCode: ValidationInvalidTaskName,
			wantTask: "a",
		},
		{
			name: "duplicate task IDs",
			definition: WorkflowDefinition{
				ID: "workflow",
				Tasks: []TaskDefinition{
					newTask("a"),
					newTask("a"),
				},
			},
			wantCode: ValidationDuplicateTaskID,
			wantTask: "a",
		},
		{
			name: "invalid dependency ID",
			definition: WorkflowDefinition{
				ID: "workflow",
				Tasks: []TaskDefinition{
					newTask("a"),
					newTask("b", ""),
				},
			},
			wantCode:       ValidationInvalidDependencyID,
			wantTask:       "b",
			wantDependency: "",
		},
		{
			name: "missing dependency",
			definition: WorkflowDefinition{
				ID: "workflow",
				Tasks: []TaskDefinition{
					newTask("a", "missing"),
				},
			},
			wantCode:       ValidationMissingDependency,
			wantTask:       "a",
			wantDependency: "missing",
		},
		{
			name: "self-cycle",
			definition: WorkflowDefinition{
				ID: "workflow",
				Tasks: []TaskDefinition{
					newTask("a", "a"),
				},
			},
			wantCode:       ValidationSelfDependency,
			wantTask:       "a",
			wantDependency: "a",
		},
		{
			name: "duplicate dependency",
			definition: WorkflowDefinition{
				ID: "workflow",
				Tasks: []TaskDefinition{
					newTask("a"),
					newTask("b", "a", "a"),
				},
			},
			wantCode:       ValidationDuplicateDependency,
			wantTask:       "b",
			wantDependency: "a",
		},
		{
			name: "multi-node cycle",
			definition: WorkflowDefinition{
				ID: "workflow",
				Tasks: []TaskDefinition{
					newTask("a", "c"),
					newTask("b", "a"),
					newTask("c", "b"),
				},
			},
			wantCode: ValidationCycle,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			validationError := requireValidationCode(t, test.definition.Validate(), test.wantCode)
			if validationError.TaskID != test.wantTask {
				t.Fatalf("ValidationError.TaskID = %q, want %q", validationError.TaskID, test.wantTask)
			}
			if validationError.DependencyID != test.wantDependency {
				t.Fatalf("ValidationError.DependencyID = %q, want %q", validationError.DependencyID, test.wantDependency)
			}
			if validationError.Error() == "" {
				t.Fatal("ValidationError.Error() returned an empty message")
			}
		})
	}
}

func TestWorkflowDefinitionIsAcyclic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition WorkflowDefinition
		want       bool
		wantCode   ValidationCode
	}{
		{
			name: "acyclic",
			definition: WorkflowDefinition{
				ID:    "workflow",
				Tasks: []TaskDefinition{newTask("b", "a"), newTask("a")},
			},
			want: true,
		},
		{
			name: "cyclic",
			definition: WorkflowDefinition{
				ID:    "workflow",
				Tasks: []TaskDefinition{newTask("a", "b"), newTask("b", "a")},
			},
			want: false,
		},
		{
			name: "structurally invalid",
			definition: WorkflowDefinition{
				ID:    "workflow",
				Tasks: []TaskDefinition{newTask("a", "missing")},
			},
			wantCode: ValidationMissingDependency,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := test.definition.IsAcyclic()
			if test.wantCode != "" {
				requireValidationCode(t, err, test.wantCode)
				return
			}
			if err != nil {
				t.Fatalf("IsAcyclic() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("IsAcyclic() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestReadyTasksRejectsUnknownCompletedTaskDeterministically(t *testing.T) {
	t.Parallel()

	definition := WorkflowDefinition{
		ID:    "workflow",
		Tasks: []TaskDefinition{newTask("a")},
	}

	_, err := definition.ReadyTasks(completedTasks("z-unknown", "b-unknown"))
	validationError := requireValidationCode(t, err, ValidationUnknownCompletedTask)
	if validationError.TaskID != "b-unknown" {
		t.Fatalf("ValidationError.TaskID = %q, want %q", validationError.TaskID, TaskID("b-unknown"))
	}
}

func newTask(id string, dependencies ...string) TaskDefinition {
	dependencyIDs := make([]TaskID, len(dependencies))
	for index, dependency := range dependencies {
		dependencyIDs[index] = TaskID(dependency)
	}

	return TaskDefinition{
		ID:           TaskID(id),
		Name:         "Task " + id,
		Dependencies: dependencyIDs,
	}
}

func taskIDs(ids ...string) []TaskID {
	result := make([]TaskID, len(ids))
	for index, id := range ids {
		result[index] = TaskID(id)
	}
	return result
}

func completedTasks(ids ...string) map[TaskID]struct{} {
	completed := make(map[TaskID]struct{}, len(ids))
	for _, id := range ids {
		completed[TaskID(id)] = struct{}{}
	}
	return completed
}

func idsOf(tasks []TaskDefinition) []TaskID {
	if len(tasks) == 0 {
		return nil
	}

	ids := make([]TaskID, len(tasks))
	for index, task := range tasks {
		ids[index] = task.ID
	}
	return ids
}

func assertTaskIDs(t *testing.T, got, want []TaskID) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("task IDs = %v, want %v", got, want)
	}
}

func requireValidationCode(t *testing.T, err error, want ValidationCode) *ValidationError {
	t.Helper()

	if err == nil {
		t.Fatalf("error = nil, want validation code %q", want)
	}

	var validationError *ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("error type = %T, want *ValidationError: %v", err, err)
	}
	if validationError.Code != want {
		t.Fatalf("ValidationError.Code = %q, want %q: %v", validationError.Code, want, err)
	}

	return validationError
}
