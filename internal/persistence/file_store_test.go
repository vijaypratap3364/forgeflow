package persistence

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
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
	created, err = store.CreateRun(context.Background(), created)
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	updated := created
	updated.Tasks = append([]execution.TaskRun(nil), created.Tasks...)
	updated.Status = execution.WorkflowRunRunning
	updated.UpdatedAt = created.UpdatedAt.Add(time.Second)
	updated.Tasks[0].Status = execution.TaskRunReady
	updated.Tasks[0].UpdatedAt = updated.UpdatedAt
	updated, err = store.SaveRun(context.Background(), updated)
	if err != nil {
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
	snapshot, err = store.CreateRun(context.Background(), snapshot)
	if err != nil {
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
		if _, err := store.CreateRun(context.Background(), snapshot); !errors.As(err, &target) {
			t.Fatalf("CreateRun() error = %v, want *RunAlreadyExistsError", err)
		}
	})

	t.Run("missing run", func(t *testing.T) {
		missing := snapshot
		missing.ID = "missing-run"
		for index := range missing.Tasks {
			missing.Tasks[index].TaskRunID = execution.TaskRunIDFor(missing.ID, missing.Tasks[index].TaskID)
		}
		var target *execution.RunNotFoundError
		if _, err := store.SaveRun(context.Background(), missing); !errors.As(err, &target) {
			t.Fatalf("SaveRun() error = %v, want *RunNotFoundError", err)
		}
	})

	t.Run("missing workflow", func(t *testing.T) {
		missing := snapshot
		missing.ID = "another-run"
		missing.WorkflowID = "missing-workflow"
		missing.Version = 0
		for index := range missing.Tasks {
			missing.Tasks[index].TaskRunID = execution.TaskRunIDFor(missing.ID, missing.Tasks[index].TaskID)
		}
		var target *execution.WorkflowNotFoundError
		if _, err := store.CreateRun(context.Background(), missing); !errors.As(err, &target) {
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

func TestFileStoreRejectsStaleRunSnapshot(t *testing.T) {
	t.Parallel()

	store, err := OpenFileStore(filepath.Join(t.TempDir(), "forgeflow.ffdb"))
	if err != nil {
		t.Fatalf("OpenFileStore() error = %v", err)
	}
	definition := repositoryWorkflow()
	if err := store.SaveWorkflow(context.Background(), definition); err != nil {
		t.Fatalf("SaveWorkflow() error = %v", err)
	}
	run, err := execution.NewWorkflowRun("versioned-run", definition)
	if err != nil {
		t.Fatalf("NewWorkflowRun() error = %v", err)
	}
	created, err := store.CreateRun(context.Background(), run.Snapshot())
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	if created.Version != 1 {
		t.Fatalf("created version = %d, want 1", created.Version)
	}

	updated := created
	updated.Tasks = append([]execution.TaskRun(nil), created.Tasks...)
	updated.Status = execution.WorkflowRunRunning
	updated.UpdatedAt = created.UpdatedAt.Add(time.Second)
	updated.Tasks[0].Status = execution.TaskRunReady
	updated.Tasks[0].UpdatedAt = updated.UpdatedAt
	stored, err := store.SaveRun(context.Background(), updated)
	if err != nil {
		t.Fatalf("SaveRun() error = %v", err)
	}
	if stored.Version != 2 {
		t.Fatalf("stored version = %d, want 2", stored.Version)
	}

	stale := updated
	stale.UpdatedAt = updated.UpdatedAt.Add(time.Second)
	var conflict *execution.RunVersionConflictError
	if _, err := store.SaveRun(context.Background(), stale); !errors.As(err, &conflict) {
		t.Fatalf("stale SaveRun() error = %v, want *RunVersionConflictError", err)
	}
	if conflict.ExpectedVersion != 1 || conflict.ActualVersion != 2 {
		t.Fatalf("version conflict = %#v, want expected 1 actual 2", conflict)
	}
	loaded, found, err := store.LoadRun(context.Background(), created.ID)
	if err != nil || !found {
		t.Fatalf("LoadRun() = found %v, error %v", found, err)
	}
	if !reflect.DeepEqual(loaded, stored) {
		t.Fatalf("loaded snapshot changed after stale save: got %#v, want %#v", loaded, stored)
	}
}

func TestFileStoreReplaysLegacyUnversionedRunRecords(t *testing.T) {
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
	run, err := execution.NewWorkflowRun("legacy-run", definition)
	if err != nil {
		t.Fatalf("NewWorkflowRun() error = %v", err)
	}
	created := run.Snapshot()
	if err := store.appendRecord(journalRecord{
		Version:   journalVersion,
		Operation: operationCreateRun,
		Run:       &created,
	}); err != nil {
		t.Fatalf("append legacy create record: %v", err)
	}
	updated := created
	updated.Tasks = append([]execution.TaskRun(nil), created.Tasks...)
	updated.Status = execution.WorkflowRunRunning
	updated.UpdatedAt = created.UpdatedAt.Add(time.Second)
	updated.Tasks[0].Status = execution.TaskRunReady
	updated.Tasks[0].UpdatedAt = updated.UpdatedAt
	if err := store.appendRecord(journalRecord{
		Version:   journalVersion,
		Operation: operationSaveRun,
		Run:       &updated,
	}); err != nil {
		t.Fatalf("append legacy save record: %v", err)
	}

	reopened, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("reopen OpenFileStore() error = %v", err)
	}
	loaded, found, err := reopened.LoadRun(context.Background(), created.ID)
	if err != nil || !found {
		t.Fatalf("LoadRun() = found %v, error %v", found, err)
	}
	if loaded.Version != 2 {
		t.Fatalf("legacy replay version = %d, want 2", loaded.Version)
	}
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

func TestEngineRecoversPartiallyExecutedWorkflowAfterStoreReopen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "forgeflow.ffdb")
	durableStore, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("OpenFileStore() error = %v", err)
	}
	processStopped := errors.New("simulated process stop after durable partial progress")
	interruptingStore := &interruptAfterPartialStore{
		Store: durableStore,
		cause: processStopped,
	}

	definition := repositoryWorkflow()
	var (
		callsMu sync.Mutex
		calls   = make(map[workflow.TaskID]int)
	)
	registry := execution.NewHandlerRegistry()
	if err := registry.Register("noop", execution.TaskHandlerFunc(func(_ context.Context, request execution.TaskRequest) (string, error) {
		callsMu.Lock()
		calls[request.Task.ID]++
		callsMu.Unlock()
		return "a-complete", nil
	})); err != nil {
		t.Fatalf("Register(noop) error = %v", err)
	}
	if err := registry.Register("uppercase", execution.UppercaseHandler{}); err != nil {
		t.Fatalf("Register(uppercase) error = %v", err)
	}
	engine, err := execution.NewEngine(1, registry, interruptingStore)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	firstRun, err := engine.Execute(context.Background(), "restart-run", definition)
	if !errors.Is(err, processStopped) {
		t.Fatalf("Execute() error = %v, want simulated process stop", err)
	}
	if firstRun == nil {
		t.Fatal("Execute() run = nil after partial execution")
	}

	reopened, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("reopen OpenFileStore() error = %v", err)
	}
	partial, found, err := reopened.LoadRun(context.Background(), "restart-run")
	if err != nil || !found {
		t.Fatalf("LoadRun(partial) = found %v, error %v", found, err)
	}
	assertStoredTask(t, partial, "a", execution.TaskRunSucceeded, 1)
	assertStoredTask(t, partial, "b", execution.TaskRunReady, 0)

	recoveryRegistry := execution.NewHandlerRegistry()
	if err := recoveryRegistry.Register("noop", execution.TaskHandlerFunc(func(_ context.Context, request execution.TaskRequest) (string, error) {
		callsMu.Lock()
		calls[request.Task.ID]++
		callsMu.Unlock()
		return "unexpected", nil
	})); err != nil {
		t.Fatalf("recovery Register(noop) error = %v", err)
	}
	if err := recoveryRegistry.Register("uppercase", execution.TaskHandlerFunc(func(_ context.Context, request execution.TaskRequest) (string, error) {
		callsMu.Lock()
		calls[request.Task.ID]++
		callsMu.Unlock()
		return "HELLO", nil
	})); err != nil {
		t.Fatalf("recovery Register(uppercase) error = %v", err)
	}
	recoveryEngine, err := execution.NewEngine(1, recoveryRegistry, reopened)
	if err != nil {
		t.Fatalf("recovery NewEngine() error = %v", err)
	}
	recovered, err := recoveryEngine.Recover(context.Background(), "restart-run")
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if recovered.Status() != execution.WorkflowRunSucceeded {
		t.Fatalf("recovered status = %q, want %q", recovered.Status(), execution.WorkflowRunSucceeded)
	}
	callsMu.Lock()
	aCalls := calls["a"]
	bCalls := calls["b"]
	callsMu.Unlock()
	if aCalls != 1 || bCalls != 1 {
		t.Fatalf("handler calls after recovery = a:%d b:%d, want a:1 b:1", aCalls, bCalls)
	}

	secondReopen, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("second reopen OpenFileStore() error = %v", err)
	}
	persisted, found, err := secondReopen.LoadRun(context.Background(), "restart-run")
	if err != nil || !found {
		t.Fatalf("LoadRun(completed) = found %v, error %v", found, err)
	}
	if persisted.Status != execution.WorkflowRunSucceeded {
		t.Fatalf("persisted status after recovery = %q, want %q", persisted.Status, execution.WorkflowRunSucceeded)
	}
	assertStoredTask(t, persisted, "a", execution.TaskRunSucceeded, 1)
	assertStoredTask(t, persisted, "b", execution.TaskRunSucceeded, 1)
}

type interruptAfterPartialStore struct {
	execution.Store
	cause       error
	interrupted atomic.Bool
}

func (store *interruptAfterPartialStore) SaveRun(
	ctx context.Context,
	snapshot execution.WorkflowRunSnapshot,
) (execution.WorkflowRunSnapshot, error) {
	if store.interrupted.Load() {
		return execution.WorkflowRunSnapshot{}, store.cause
	}
	if storedTaskStatus(snapshot, "a") == execution.TaskRunSucceeded &&
		storedTaskStatus(snapshot, "b") == execution.TaskRunReady {
		stored, err := store.Store.SaveRun(ctx, snapshot)
		if err != nil {
			return execution.WorkflowRunSnapshot{}, err
		}
		store.interrupted.Store(true)
		return stored, store.cause
	}
	return store.Store.SaveRun(ctx, snapshot)
}

func storedTaskStatus(snapshot execution.WorkflowRunSnapshot, taskID workflow.TaskID) execution.TaskRunStatus {
	for _, task := range snapshot.Tasks {
		if task.TaskID == taskID {
			return task.Status
		}
	}
	return ""
}

func assertStoredTask(
	t *testing.T,
	snapshot execution.WorkflowRunSnapshot,
	taskID workflow.TaskID,
	wantStatus execution.TaskRunStatus,
	wantAttempts int,
) {
	t.Helper()

	for _, task := range snapshot.Tasks {
		if task.TaskID != taskID {
			continue
		}
		if task.Status != wantStatus || task.AttemptCount != wantAttempts {
			t.Fatalf("stored task %q = status %q attempts %d, want status %q attempts %d", taskID, task.Status, task.AttemptCount, wantStatus, wantAttempts)
		}
		return
	}
	t.Fatalf("stored task %q was not found", taskID)
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
