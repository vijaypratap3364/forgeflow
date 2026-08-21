package persistence

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/execution"
	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

func TestFileStorePersistsDefinitionsAndRunsAcrossReopen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "nested", "forgeflow.ffdb")
	store, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("OpenFileStore() error = %v", err)
	}
	definition := repositoryWorkflow()
	if err := store.SaveWorkflow(context.Background(), definition); err != nil {
		t.Fatalf("SaveWorkflow() error = %v", err)
	}

	run, err := execution.NewWorkflowRun("run-1", definition)
	if err != nil {
		t.Fatalf("NewWorkflowRun() error = %v", err)
	}
	created := run.Snapshot()
	if err := store.CreateRun(context.Background(), created); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	updated := created
	updated.Tasks = append([]execution.TaskRun(nil), created.Tasks...)
	updated.Status = execution.WorkflowRunRunning
	updated.UpdatedAt = created.UpdatedAt.Add(time.Second)
	updated.Tasks[0].Status = execution.TaskRunReady
	updated.Tasks[0].UpdatedAt = updated.UpdatedAt
	if err := store.SaveRun(context.Background(), updated); err != nil {
		t.Fatalf("SaveRun() error = %v", err)
	}

	loadedDefinition, found, err := store.LoadWorkflow(context.Background(), definition.ID)
	if err != nil || !found {
		t.Fatalf("LoadWorkflow() = found %v, error %v", found, err)
	}
	loadedDefinition.Tasks[1].Dependencies[0] = "mutated"
	loadedRun, found, err := store.LoadRun(context.Background(), updated.ID)
	if err != nil || !found {
		t.Fatalf("LoadRun() = found %v, error %v", found, err)
	}
	loadedRun.Tasks[0].Status = execution.TaskRunFailed

	reopened, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("reopen OpenFileStore() error = %v", err)
	}
	gotDefinition, found, err := reopened.LoadWorkflow(context.Background(), definition.ID)
	if err != nil || !found {
		t.Fatalf("reopened LoadWorkflow() = found %v, error %v", found, err)
	}
	if !reflect.DeepEqual(gotDefinition, definition) {
		t.Fatalf("reopened definition = %#v, want %#v", gotDefinition, definition)
	}
	gotRun, found, err := reopened.LoadRun(context.Background(), updated.ID)
	if err != nil || !found {
		t.Fatalf("reopened LoadRun() = found %v, error %v", found, err)
	}
	if !reflect.DeepEqual(gotRun, updated) {
		t.Fatalf("reopened run = %#v, want %#v", gotRun, updated)
	}
}

func TestFileStoreRepositoryErrors(t *testing.T) {
	t.Parallel()

	store, err := OpenFileStore(filepath.Join(t.TempDir(), "forgeflow.ffdb"))
	if err != nil {
		t.Fatalf("OpenFileStore() error = %v", err)
	}
	definition := repositoryWorkflow()
	if err := store.SaveWorkflow(context.Background(), definition); err != nil {
		t.Fatalf("SaveWorkflow() error = %v", err)
	}
	run, err := execution.NewWorkflowRun("run-1", definition)
	if err != nil {
		t.Fatalf("NewWorkflowRun() error = %v", err)
	}
	snapshot := run.Snapshot()
	if err := store.CreateRun(context.Background(), snapshot); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	t.Run("immutable workflow conflict", func(t *testing.T) {
		conflicting := repositoryWorkflow()
		conflicting.Tasks[0].Name = "different"
		var target *execution.WorkflowConflictError
		if err := store.SaveWorkflow(context.Background(), conflicting); !errors.As(err, &target) {
			t.Fatalf("SaveWorkflow() error = %v, want *WorkflowConflictError", err)
		}
	})

	t.Run("duplicate run", func(t *testing.T) {
		var target *execution.RunAlreadyExistsError
		if err := store.CreateRun(context.Background(), snapshot); !errors.As(err, &target) {
			t.Fatalf("CreateRun() error = %v, want *RunAlreadyExistsError", err)
		}
	})

	t.Run("missing run", func(t *testing.T) {
		missing := snapshot
		missing.ID = "missing-run"
		var target *execution.RunNotFoundError
		if err := store.SaveRun(context.Background(), missing); !errors.As(err, &target) {
			t.Fatalf("SaveRun() error = %v, want *RunNotFoundError", err)
		}
	})

	t.Run("missing workflow", func(t *testing.T) {
		missing := snapshot
		missing.ID = "another-run"
		missing.WorkflowID = "missing-workflow"
		var target *execution.WorkflowNotFoundError
		if err := store.CreateRun(context.Background(), missing); !errors.As(err, &target) {
			t.Fatalf("CreateRun() error = %v, want *WorkflowNotFoundError", err)
		}
	})

	t.Run("canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, err := store.LoadRun(ctx, snapshot.ID); !errors.Is(err, context.Canceled) {
			t.Fatalf("LoadRun() error = %v, want context.Canceled", err)
		}
	})
}

func TestFileStoreSaveWorkflowIsIdempotent(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "forgeflow.ffdb")
	store, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("OpenFileStore() error = %v", err)
	}
	definition := repositoryWorkflow()
	if err := store.SaveWorkflow(context.Background(), definition); err != nil {
		t.Fatalf("SaveWorkflow() error = %v", err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if err := store.SaveWorkflow(context.Background(), definition); err != nil {
		t.Fatalf("second SaveWorkflow() error = %v", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("second Stat() error = %v", err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("idempotent SaveWorkflow() changed journal size from %d to %d", before.Size(), after.Size())
	}
}

func TestFileStoreTruncatesIncompleteFinalRecord(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "forgeflow.ffdb")
	store, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("OpenFileStore() error = %v", err)
	}
	definition := repositoryWorkflow()
	if err := store.SaveWorkflow(context.Background(), definition); err != nil {
		t.Fatalf("SaveWorkflow() error = %v", err)
	}
	committed, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	if _, err := file.WriteString(`{"Checksum":"torn`); err != nil {
		t.Fatalf("WriteString() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("reopen OpenFileStore() error = %v", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("second Stat() error = %v", err)
	}
	if after.Size() != committed.Size() {
		t.Fatalf("journal size after recovery = %d, want %d", after.Size(), committed.Size())
	}
	if _, found, err := reopened.LoadWorkflow(context.Background(), definition.ID); err != nil || !found {
		t.Fatalf("LoadWorkflow() after torn-tail recovery = found %v, error %v", found, err)
	}
}

func TestFileStoreRejectsCorruptCommittedRecord(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "forgeflow.ffdb")
	if err := os.WriteFile(path, []byte("{\"Checksum\":\"bad\",\"Payload\":{}}\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := OpenFileStore(path); err == nil {
		t.Fatal("OpenFileStore() error = nil, want corruption error")
	}
}

func repositoryWorkflow() workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		ID: "workflow",
		Tasks: []workflow.TaskDefinition{
			{ID: "a", Name: "Task a", Handler: "noop"},
			{ID: "b", Name: "Task b", Dependencies: []workflow.TaskID{"a"}, Handler: "uppercase", Input: "hello"},
		},
	}
}
