package execution

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

func TestEngineExecutesLinearWorkflow(t *testing.T) {
	t.Parallel()

	registry := NewHandlerRegistry()
	var (
		orderMu sync.Mutex
		order   []workflow.TaskID
	)
	if err := registry.Register("record", TaskHandlerFunc(func(_ context.Context, request TaskRequest) (string, error) {
		orderMu.Lock()
		order = append(order, request.Task.ID)
		orderMu.Unlock()
		return request.Task.Input, nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	engine := mustEngine(t, 3, registry)
	definition := executionWorkflow(
		executionTask("c", "record", "output-c", "b"),
		executionTask("a", "record", "output-a"),
		executionTask("b", "record", "output-b", "a"),
	)

	run, err := engine.Execute(context.Background(), "linear-run", definition)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := run.Status(); got != WorkflowRunSucceeded {
		t.Fatalf("Status() = %q, want %q", got, WorkflowRunSucceeded)
	}
	if !reflect.DeepEqual(order, []workflow.TaskID{"a", "b", "c"}) {
		t.Fatalf("handler order = %v, want [a b c]", order)
	}
	for _, taskID := range []workflow.TaskID{"a", "b", "c"} {
		task := requireTaskRun(t, run, taskID)
		if task.Status != TaskRunSucceeded {
			t.Fatalf("task %q status = %q, want %q", taskID, task.Status, TaskRunSucceeded)
		}
		if task.Output != "output-"+string(taskID) {
			t.Fatalf("task %q output = %q, want %q", taskID, task.Output, "output-"+string(taskID))
		}
	}
}

func TestEngineExecutesFanOutConcurrently(t *testing.T) {
	t.Parallel()

	started := make(chan workflow.TaskID, 2)
	release := make(chan struct{})
	registry := NewHandlerRegistry()
	if err := registry.Register("barrier", TaskHandlerFunc(func(ctx context.Context, request TaskRequest) (string, error) {
		if request.Task.ID == "a" {
			return "", nil
		}
		started <- request.Task.ID
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-release:
			return "", nil
		}
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	engine := mustEngine(t, 2, registry)
	definition := executionWorkflow(
		executionTask("c", "barrier", "", "a"),
		executionTask("a", "barrier", ""),
		executionTask("b", "barrier", "", "a"),
	)
	ctx, cancel := guardedContext(t)
	defer cancel()

	result := executeAsync(engine, ctx, "fan-out-run", definition)
	first := receive(t, ctx, started, "first fan-out task")
	second := receive(t, ctx, started, "second fan-out task")
	if first == second {
		t.Fatalf("fan-out started task %q twice", first)
	}
	close(release)

	executionResult := receive(t, ctx, result, "fan-out workflow result")
	if executionResult.err != nil {
		t.Fatalf("Execute() error = %v", executionResult.err)
	}
	if executionResult.run.Status() != WorkflowRunSucceeded {
		t.Fatalf("Status() = %q, want %q", executionResult.run.Status(), WorkflowRunSucceeded)
	}
}

func TestEngineWaitsForAllFanInDependencies(t *testing.T) {
	t.Parallel()

	var aCompleted, bCompleted atomic.Bool
	bStarted := make(chan struct{})
	releaseB := make(chan struct{})
	aDependentStarted := make(chan struct{})
	registry := NewHandlerRegistry()
	if err := registry.Register("fan-in", TaskHandlerFunc(func(ctx context.Context, request TaskRequest) (string, error) {
		switch request.Task.ID {
		case "a":
			aCompleted.Store(true)
		case "b":
			close(bStarted)
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-releaseB:
			}
			bCompleted.Store(true)
		case "join":
			if !aCompleted.Load() || !bCompleted.Load() {
				return "", errors.New("fan-in task started before both dependencies completed")
			}
		case "probe":
			close(aDependentStarted)
		}
		return "", nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	engine := mustEngine(t, 3, registry)
	definition := executionWorkflow(
		executionTask("join", "fan-in", "", "a", "b"),
		executionTask("probe", "fan-in", "", "a"),
		executionTask("b", "fan-in", ""),
		executionTask("a", "fan-in", ""),
	)
	ctx, cancel := guardedContext(t)
	defer cancel()
	result := executeAsync(engine, ctx, "fan-in-run", definition)

	receive(t, ctx, bStarted, "blocked fan-in dependency start")
	receive(t, ctx, aDependentStarted, "task depending only on a")
	close(releaseB)

	executionResult := receive(t, ctx, result, "fan-in workflow result")
	if executionResult.err != nil {
		t.Fatalf("Execute() error = %v", executionResult.err)
	}
	if executionResult.run.Status() != WorkflowRunSucceeded {
		t.Fatalf("Status() = %q, want %q", executionResult.run.Status(), WorkflowRunSucceeded)
	}
}

func TestEngineFailsFastAndCancelsRemainingWork(t *testing.T) {
	t.Parallel()

	handlerFailure := errors.New("deterministic failure")
	peerStarted := make(chan struct{})
	var downstreamCalls atomic.Int32
	registry := NewHandlerRegistry()
	if err := registry.Register("failure", TaskHandlerFunc(func(ctx context.Context, request TaskRequest) (string, error) {
		switch request.Task.ID {
		case "fail":
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-peerStarted:
				return "", handlerFailure
			}
		case "peer":
			close(peerStarted)
			<-ctx.Done()
			return "", ctx.Err()
		case "downstream":
			downstreamCalls.Add(1)
			return "", nil
		default:
			return "", errors.New("unexpected task")
		}
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	engine := mustEngine(t, 2, registry)
	definition := executionWorkflow(
		executionTask("downstream", "failure", "", "fail"),
		executionTask("peer", "failure", ""),
		executionTask("fail", "failure", ""),
	)
	ctx, cancel := guardedContext(t)
	defer cancel()

	run, err := engine.Execute(ctx, "failure-run", definition)
	if !errors.Is(err, handlerFailure) {
		t.Fatalf("Execute() error = %v, want wrapped handler failure", err)
	}
	var executionError *TaskExecutionError
	if !errors.As(err, &executionError) || executionError.TaskID != "fail" {
		t.Fatalf("Execute() error = %#v, want failure for task fail", err)
	}
	if run.Status() != WorkflowRunFailed {
		t.Fatalf("Status() = %q, want %q", run.Status(), WorkflowRunFailed)
	}
	assertTaskStatus(t, run, "fail", TaskRunFailed)
	assertTaskStatus(t, run, "peer", TaskRunCanceled)
	assertTaskStatus(t, run, "downstream", TaskRunCanceled)
	if got := downstreamCalls.Load(); got != 0 {
		t.Fatalf("downstream handler calls = %d, want 0", got)
	}
}

func TestEngineCancellationStopsWorkersAndKeepsStateConsistent(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	registry := NewHandlerRegistry()
	if err := registry.Register("blocking", TaskHandlerFunc(func(ctx context.Context, _ TaskRequest) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	engine := mustEngine(t, 2, registry)
	definition := executionWorkflow(
		executionTask("a", "blocking", ""),
		executionTask("b", "blocking", "", "a"),
	)
	guard, stopGuard := guardedContext(t)
	defer stopGuard()
	ctx, cancel := context.WithCancel(guard)
	result := executeAsync(engine, ctx, "canceled-run", definition)

	receive(t, guard, started, "blocking handler start")
	cancel()
	executionResult := receive(t, guard, result, "canceled workflow result")
	if !errors.Is(executionResult.err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context.Canceled", executionResult.err)
	}
	if executionResult.run.Status() != WorkflowRunCanceled {
		t.Fatalf("Status() = %q, want %q", executionResult.run.Status(), WorkflowRunCanceled)
	}
	assertTaskStatus(t, executionResult.run, "a", TaskRunCanceled)
	assertTaskStatus(t, executionResult.run, "b", TaskRunCanceled)
}

func TestEngineSupportsSimultaneousWorkflowRuns(t *testing.T) {
	t.Parallel()

	started := make(chan RunID, 2)
	releases := map[RunID]chan struct{}{
		"run-one": make(chan struct{}),
		"run-two": make(chan struct{}),
	}
	registry := NewHandlerRegistry()
	if err := registry.Register("multi-run", TaskHandlerFunc(func(ctx context.Context, request TaskRequest) (string, error) {
		started <- request.RunID
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-releases[request.RunID]:
			return string(request.RunID), nil
		}
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	engine := mustEngine(t, 1, registry)
	definition := executionWorkflow(executionTask("a", "multi-run", ""))
	ctx, cancel := guardedContext(t)
	defer cancel()

	resultOne := executeAsync(engine, ctx, "run-one", definition)
	resultTwo := executeAsync(engine, ctx, "run-two", definition)
	first := receive(t, ctx, started, "first workflow start")
	second := receive(t, ctx, started, "second workflow start")
	if first == second {
		t.Fatalf("workflow run %q started twice", first)
	}
	close(releases["run-one"])
	close(releases["run-two"])

	for name, result := range map[string]<-chan asyncExecutionResult{
		"run-one": resultOne,
		"run-two": resultTwo,
	} {
		executionResult := receive(t, ctx, result, name+" result")
		if executionResult.err != nil {
			t.Fatalf("%s Execute() error = %v", name, executionResult.err)
		}
		if executionResult.run.Status() != WorkflowRunSucceeded {
			t.Fatalf("%s status = %q, want %q", name, executionResult.run.Status(), WorkflowRunSucceeded)
		}
	}
}

func TestEngineRejectsInvalidConfigurationAndUnknownHandler(t *testing.T) {
	t.Parallel()

	for _, workerCount := range []int{-1, 0} {
		_, err := NewEngine(workerCount, NewHandlerRegistry(), newMemoryTestStore())
		var configError *InvalidEngineConfigError
		if !errors.As(err, &configError) {
			t.Fatalf("NewEngine(%d) error = %v, want *InvalidEngineConfigError", workerCount, err)
		}
	}
	_, err := NewEngine(1, nil, newMemoryTestStore())
	var configError *InvalidEngineConfigError
	if !errors.As(err, &configError) {
		t.Fatalf("NewEngine(nil registry) error = %v, want *InvalidEngineConfigError", err)
	}
	_, err = NewEngine(1, NewHandlerRegistry(), nil)
	if !errors.As(err, &configError) {
		t.Fatalf("NewEngine(nil store) error = %v, want *InvalidEngineConfigError", err)
	}
	invalidOptions := []EngineOption{
		nil,
		WithClock(nil),
		WithLeaseTiming(0, time.Second),
		WithLeaseTiming(time.Second, time.Second),
		WithWorkerNamespace("bad namespace"),
	}
	for index, option := range invalidOptions {
		_, err := NewEngine(1, NewHandlerRegistry(), newMemoryTestStore(), option)
		if !errors.As(err, &configError) {
			t.Fatalf("NewEngine(invalid option %d) error = %v, want *InvalidEngineConfigError", index, err)
		}
	}

	engine := mustEngine(t, 1, NewHandlerRegistry())
	run, err := engine.Execute(
		context.Background(),
		"unknown-handler-run",
		executionWorkflow(executionTask("a", "missing", "")),
	)
	if run != nil {
		t.Fatalf("Execute() run = %#v, want nil for rejected submission", run)
	}
	var unknownHandlerError *UnknownHandlerError
	if !errors.As(err, &unknownHandlerError) {
		t.Fatalf("Execute() error = %v, want *UnknownHandlerError", err)
	}
}

func TestEngineHandlesContextCanceledBeforeExecution(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	registry := NewHandlerRegistry()
	if err := registry.Register("count", TaskHandlerFunc(func(context.Context, TaskRequest) (string, error) {
		calls.Add(1)
		return "", nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine := mustEngine(t, 1, registry)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	run, err := engine.Execute(ctx, "pre-canceled-run", executionWorkflow(executionTask("a", "count", "")))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context.Canceled", err)
	}
	if run.Status() != WorkflowRunCanceled {
		t.Fatalf("Status() = %q, want %q", run.Status(), WorkflowRunCanceled)
	}
	assertTaskStatus(t, run, "a", TaskRunCanceled)
	if got := calls.Load(); got != 0 {
		t.Fatalf("handler calls = %d, want 0", got)
	}
}

func TestEnginePersistsCompletedRun(t *testing.T) {
	t.Parallel()

	store := newMemoryTestStore()
	registry := NewHandlerRegistry()
	if err := registry.Register("complete", TaskHandlerFunc(func(_ context.Context, request TaskRequest) (string, error) {
		return "output-" + string(request.Task.ID), nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine := mustEngineWithStore(t, 2, registry, store)
	definition := executionWorkflow(
		executionTask("a", "complete", ""),
		executionTask("b", "complete", "", "a"),
	)

	run, err := engine.Execute(context.Background(), "persisted-run", definition)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	snapshot, found, err := store.LoadRun(context.Background(), run.ID())
	if err != nil || !found {
		t.Fatalf("LoadRun() = found %v, error %v", found, err)
	}
	if !reflect.DeepEqual(snapshot, run.Snapshot()) {
		t.Fatalf("persisted snapshot = %#v, want %#v", snapshot, run.Snapshot())
	}
	if snapshot.Status != WorkflowRunSucceeded {
		t.Fatalf("persisted status = %q, want %q", snapshot.Status, WorkflowRunSucceeded)
	}
	for _, task := range snapshot.Tasks {
		if task.AttemptCount != 1 || task.StartedAt.IsZero() || task.FinishedAt.IsZero() {
			t.Fatalf("persisted task attempt metadata is incomplete: %#v", task)
		}
	}
}

func TestEngineRecoverReexecutesInterruptedRunningTask(t *testing.T) {
	t.Parallel()

	store := newMemoryTestStore()
	registry := NewHandlerRegistry()
	var calls atomic.Int32
	if err := registry.Register("recover", TaskHandlerFunc(func(context.Context, TaskRequest) (string, error) {
		calls.Add(1)
		return "recovered", nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	definition := executionWorkflow(executionTask("a", "recover", ""))
	definition.Tasks[0].Retry.MaxAttempts = 2
	if err := store.SaveWorkflow(context.Background(), definition); err != nil {
		t.Fatalf("SaveWorkflow() error = %v", err)
	}
	run := mustWorkflowRun(t, definition)
	snapshot := run.Snapshot()
	interruptedAt := snapshot.CreatedAt.Add(time.Second)
	snapshot.Status = WorkflowRunRunning
	snapshot.UpdatedAt = interruptedAt
	snapshot.Tasks[0].Status = TaskRunRunning
	snapshot.Tasks[0].AttemptCount = 1
	snapshot.Tasks[0].UpdatedAt = interruptedAt
	snapshot.Tasks[0].StartedAt = interruptedAt
	if err := store.CreateRun(context.Background(), snapshot); err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}

	engine := mustEngineWithStore(t, 1, registry, store)
	recovered, err := engine.Recover(context.Background(), snapshot.ID)
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if recovered.Status() != WorkflowRunSucceeded {
		t.Fatalf("Status() = %q, want %q", recovered.Status(), WorkflowRunSucceeded)
	}
	task := requireTaskRun(t, recovered, "a")
	if task.AttemptCount != 2 {
		t.Fatalf("AttemptCount = %d, want 2", task.AttemptCount)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("recovery handler calls = %d, want 1", got)
	}
}

func TestEngineRecoverReturnsTerminalRunWithoutResolvingHandlers(t *testing.T) {
	t.Parallel()

	store := newMemoryTestStore()
	registry := NewHandlerRegistry()
	if err := registry.Register("complete", NoopHandler{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	definition := executionWorkflow(executionTask("a", "complete", ""))
	engine := mustEngineWithStore(t, 1, registry, store)
	completed, err := engine.Execute(context.Background(), "terminal-run", definition)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	recoveryEngine := mustEngineWithStore(t, 1, NewHandlerRegistry(), store)
	recovered, err := recoveryEngine.Recover(context.Background(), completed.ID())
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if !reflect.DeepEqual(recovered.Snapshot(), completed.Snapshot()) {
		t.Fatalf("recovered terminal snapshot = %#v, want %#v", recovered.Snapshot(), completed.Snapshot())
	}
}

func TestEngineRecoverRejectsUnknownRun(t *testing.T) {
	t.Parallel()

	engine := mustEngineWithStore(t, 1, NewHandlerRegistry(), newMemoryTestStore())
	var notFound *RunNotFoundError
	if _, err := engine.Recover(context.Background(), "missing-run"); !errors.As(err, &notFound) {
		t.Fatalf("Recover() error = %v, want *RunNotFoundError", err)
	}
}

type asyncExecutionResult struct {
	run *WorkflowRun
	err error
}

func executeAsync(
	engine *Engine,
	ctx context.Context,
	runID RunID,
	definition workflow.WorkflowDefinition,
) <-chan asyncExecutionResult {
	result := make(chan asyncExecutionResult, 1)
	go func() {
		run, err := engine.Execute(ctx, runID, definition)
		result <- asyncExecutionResult{run: run, err: err}
	}()
	return result
}

func mustEngine(t *testing.T, workerCount int, registry *HandlerRegistry) *Engine {
	t.Helper()

	return mustEngineWithStore(t, workerCount, registry, newMemoryTestStore())
}

func mustEngineWithStore(t *testing.T, workerCount int, registry *HandlerRegistry, store Store) *Engine {
	t.Helper()

	engine, err := NewEngine(workerCount, registry, store)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	return engine
}

type memoryTestStore struct {
	mu          sync.RWMutex
	definitions map[workflow.WorkflowID]workflow.WorkflowDefinition
	runs        map[RunID]WorkflowRunSnapshot
}

func newMemoryTestStore() *memoryTestStore {
	return &memoryTestStore{
		definitions: make(map[workflow.WorkflowID]workflow.WorkflowDefinition),
		runs:        make(map[RunID]WorkflowRunSnapshot),
	}
}

func (store *memoryTestStore) SaveWorkflow(_ context.Context, definition workflow.WorkflowDefinition) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if existing, exists := store.definitions[definition.ID]; exists && !reflect.DeepEqual(existing, definition) {
		return &WorkflowConflictError{WorkflowID: definition.ID}
	}
	store.definitions[definition.ID] = cloneDefinition(definition)
	return nil
}

func (store *memoryTestStore) LoadWorkflow(
	_ context.Context,
	workflowID workflow.WorkflowID,
) (workflow.WorkflowDefinition, bool, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	definition, found := store.definitions[workflowID]
	return cloneDefinition(definition), found, nil
}

func (store *memoryTestStore) CreateRun(_ context.Context, snapshot WorkflowRunSnapshot) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if _, exists := store.runs[snapshot.ID]; exists {
		return &RunAlreadyExistsError{RunID: snapshot.ID}
	}
	store.runs[snapshot.ID] = cloneTestSnapshot(snapshot)
	return nil
}

func (store *memoryTestStore) SaveRun(_ context.Context, snapshot WorkflowRunSnapshot) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if _, exists := store.runs[snapshot.ID]; !exists {
		return &RunNotFoundError{RunID: snapshot.ID}
	}
	store.runs[snapshot.ID] = cloneTestSnapshot(snapshot)
	return nil
}

func (store *memoryTestStore) LoadRun(
	_ context.Context,
	runID RunID,
) (WorkflowRunSnapshot, bool, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	snapshot, found := store.runs[runID]
	return cloneTestSnapshot(snapshot), found, nil
}

func cloneTestSnapshot(snapshot WorkflowRunSnapshot) WorkflowRunSnapshot {
	clone := snapshot
	clone.Tasks = make([]TaskRun, len(snapshot.Tasks))
	for index, task := range snapshot.Tasks {
		clone.Tasks[index] = cloneTaskRun(task)
	}
	clone.Workers = append([]WorkerHeartbeat(nil), snapshot.Workers...)
	return clone
}

func executionWorkflow(tasks ...workflow.TaskDefinition) workflow.WorkflowDefinition {
	return workflow.WorkflowDefinition{
		ID:    "workflow",
		Tasks: tasks,
	}
}

func executionTask(
	id string,
	handler workflow.HandlerName,
	input string,
	dependencies ...string,
) workflow.TaskDefinition {
	dependencyIDs := make([]workflow.TaskID, len(dependencies))
	for index, dependency := range dependencies {
		dependencyIDs[index] = workflow.TaskID(dependency)
	}
	return workflow.TaskDefinition{
		ID:           workflow.TaskID(id),
		Name:         "Task " + id,
		Dependencies: dependencyIDs,
		Handler:      handler,
		Input:        input,
	}
}

func guardedContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()

	return context.WithTimeout(context.Background(), 5*time.Second)
}

func receive[T any](t *testing.T, ctx context.Context, channel <-chan T, description string) T {
	t.Helper()

	select {
	case value := <-channel:
		return value
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s: %v", description, ctx.Err())
		var zero T
		return zero
	}
}

func requireTaskRun(t *testing.T, run *WorkflowRun, taskID workflow.TaskID) TaskRun {
	t.Helper()

	task, exists := run.Task(taskID)
	if !exists {
		t.Fatalf("Task(%q) was not found", taskID)
	}
	return task
}

func assertTaskStatus(t *testing.T, run *WorkflowRun, taskID workflow.TaskID, want TaskRunStatus) {
	t.Helper()

	task := requireTaskRun(t, run, taskID)
	if task.Status != want {
		t.Fatalf("task %q status = %q, want %q", taskID, task.Status, want)
	}
}
