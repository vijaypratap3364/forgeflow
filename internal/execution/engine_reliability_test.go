package execution

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

func TestEngineRetriesTransientFailureAfterBackoff(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC))
	store := newWatchingStore(newMemoryTestStore())
	registry := NewHandlerRegistry()
	temporary := errors.New("dependency temporarily unavailable")
	calls := make(chan TaskRequest, 2)
	var attempt atomic.Int32
	if err := registry.Register("retry", TaskHandlerFunc(func(_ context.Context, request TaskRequest) (string, error) {
		calls <- request
		if attempt.Add(1) == 1 {
			return "", Retryable(temporary)
		}
		return "recovered", nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine := mustReliabilityEngine(t, 1, registry, store, clock)
	definition := executionWorkflow(executionTask("task", "retry", ""))
	definition.Tasks[0].Retry = workflow.RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: time.Second,
		MaxBackoff:     4 * time.Second,
	}
	ctx, cancel := guardedContext(t)
	defer cancel()
	result := executeAsync(engine, ctx, "retry-run", definition)

	firstRequest := receive(t, ctx, calls, "first retry attempt")
	if firstRequest.TaskRunID == "" || firstRequest.AttemptID == "" {
		t.Fatalf("first request lacks stable execution IDs: %#v", firstRequest)
	}
	waitForStoredTask(t, ctx, store.updates, "task", TaskRunRetryWaiting, 1)
	select {
	case unexpected := <-calls:
		t.Fatalf("retry ran before fake backoff elapsed: %#v", unexpected)
	default:
	}

	if !clock.WaitForTimers(ctx, 2) {
		t.Fatal("timed out waiting for retry and heartbeat timers")
	}
	clock.Advance(time.Second)
	secondRequest := receive(t, ctx, calls, "second retry attempt")
	if secondRequest.AttemptID == firstRequest.AttemptID || secondRequest.TaskRunID != firstRequest.TaskRunID {
		t.Fatalf("retry IDs = first %#v, second %#v", firstRequest, secondRequest)
	}
	executionResult := receive(t, ctx, result, "retried workflow result")
	if executionResult.err != nil {
		t.Fatalf("Execute() error = %v", executionResult.err)
	}
	task := requireTaskRun(t, executionResult.run, "task")
	if task.Status != TaskRunSucceeded || task.AttemptCount != 2 || task.Output != "recovered" {
		t.Fatalf("retried task = %#v", task)
	}
}

func TestEngineStopsAfterRetryExhaustion(t *testing.T) {
	t.Parallel()

	registry := NewHandlerRegistry()
	failure := errors.New("still transient")
	var calls atomic.Int32
	if err := registry.Register("exhaust", TaskHandlerFunc(func(context.Context, TaskRequest) (string, error) {
		calls.Add(1)
		return "", Retryable(failure)
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	definition := executionWorkflow(executionTask("task", "exhaust", ""))
	definition.Tasks[0].Retry.MaxAttempts = 3
	run, err := mustEngine(t, 1, registry).Execute(context.Background(), "exhausted-run", definition)
	if !errors.Is(err, failure) {
		t.Fatalf("Execute() error = %v, want retry failure", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("handler calls = %d, want 3", got)
	}
	task := requireTaskRun(t, run, "task")
	if task.Status != TaskRunFailed || task.AttemptCount != 3 {
		t.Fatalf("exhausted task = %#v", task)
	}
}

func TestEngineRecoversExpiredLeaseAfterWorkerDisappears(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC))
	store := newWatchingStore(newMemoryTestStore())
	registry := NewHandlerRegistry()
	calls := make(chan TaskRequest, 2)
	var attempt atomic.Int32
	if err := registry.Register("worker-loss", TaskHandlerFunc(func(_ context.Context, request TaskRequest) (string, error) {
		calls <- request
		if attempt.Add(1) == 1 {
			return "", errWorkerDisappeared
		}
		return "replacement worker completed", nil
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine := mustReliabilityEngine(t, 1, registry, store, clock)
	definition := executionWorkflow(executionTask("task", "worker-loss", ""))
	definition.Tasks[0].Retry.MaxAttempts = 2
	ctx, cancel := guardedContext(t)
	defer cancel()
	result := executeAsync(engine, ctx, "worker-loss-run", definition)

	first := receive(t, ctx, calls, "attempt on disappearing worker")
	waitForStoredTask(t, ctx, store.updates, "task", TaskRunRunning, 1)
	if !clock.WaitForTimers(ctx, 2) {
		t.Fatal("timed out waiting for replacement heartbeat and lease timers")
	}
	clock.Advance(10 * time.Second)
	second := receive(t, ctx, calls, "attempt on replacement worker")
	if second.AttemptID == first.AttemptID {
		t.Fatalf("replacement reused attempt ID %q", second.AttemptID)
	}
	executionResult := receive(t, ctx, result, "worker-loss workflow result")
	if executionResult.err != nil {
		t.Fatalf("Execute() error = %v", executionResult.err)
	}
	task := requireTaskRun(t, executionResult.run, "task")
	if task.Status != TaskRunSucceeded || task.AttemptCount != 2 {
		t.Fatalf("recovered task = %#v", task)
	}
	if len(executionResult.run.Workers()) < 2 {
		t.Fatalf("worker replacement was not registered: %#v", executionResult.run.Workers())
	}
}

func TestEngineHeartbeatRenewsActiveLeaseWithoutRedispatch(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC))
	store := newWatchingStore(newMemoryTestStore())
	registry := NewHandlerRegistry()
	started := make(chan TaskRequest, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	if err := registry.Register("heartbeat", TaskHandlerFunc(func(ctx context.Context, request TaskRequest) (string, error) {
		calls.Add(1)
		started <- request
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-release:
			return "done", nil
		}
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine := mustReliabilityEngine(t, 1, registry, store, clock)
	definition := executionWorkflow(executionTask("task", "heartbeat", ""))
	definition.Tasks[0].Retry.MaxAttempts = 2
	ctx, cancel := guardedContext(t)
	defer cancel()
	result := executeAsync(engine, ctx, "heartbeat-run", definition)

	receive(t, ctx, started, "heartbeat task start")
	initial := waitForStoredTask(t, ctx, store.updates, "task", TaskRunRunning, 1)
	initialExpiry := initial.Lease.ExpiresAt
	if !clock.WaitForTimers(ctx, 2) {
		t.Fatal("timed out waiting for heartbeat and lease timers")
	}
	clock.Advance(3 * time.Second)
	renewed := waitForLeaseAfter(t, ctx, store.updates, "task", initialExpiry)
	if renewed.AttemptCount != 1 || calls.Load() != 1 {
		t.Fatalf("heartbeat caused redispatch: task %#v, calls %d", renewed, calls.Load())
	}
	close(release)
	executionResult := receive(t, ctx, result, "heartbeat workflow result")
	if executionResult.err != nil {
		t.Fatalf("Execute() error = %v", executionResult.err)
	}
}

func mustReliabilityEngine(
	t *testing.T,
	workers int,
	registry *HandlerRegistry,
	store Store,
	clock Clock,
) *Engine {
	t.Helper()

	engine, err := NewEngine(
		workers,
		registry,
		store,
		WithClock(clock),
		WithLeaseTiming(10*time.Second, 3*time.Second),
		WithWorkerNamespace("test-engine"),
	)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	return engine
}

type watchingStore struct {
	Store
	updates chan WorkflowRunSnapshot
}

func newWatchingStore(store Store) *watchingStore {
	return &watchingStore{Store: store, updates: make(chan WorkflowRunSnapshot, 256)}
}

func (store *watchingStore) SaveRun(ctx context.Context, snapshot WorkflowRunSnapshot) error {
	if err := store.Store.SaveRun(ctx, snapshot); err != nil {
		return err
	}
	store.updates <- cloneTestSnapshot(snapshot)
	return nil
}

func waitForStoredTask(
	t *testing.T,
	ctx context.Context,
	updates <-chan WorkflowRunSnapshot,
	taskID workflow.TaskID,
	status TaskRunStatus,
	attempts int,
) TaskRun {
	t.Helper()

	for {
		snapshot := receive(t, ctx, updates, "persisted task state")
		for _, task := range snapshot.Tasks {
			if task.TaskID == taskID && task.Status == status && task.AttemptCount == attempts {
				return task
			}
		}
	}
}

func waitForLeaseAfter(
	t *testing.T,
	ctx context.Context,
	updates <-chan WorkflowRunSnapshot,
	taskID workflow.TaskID,
	previous time.Time,
) TaskRun {
	t.Helper()

	for {
		snapshot := receive(t, ctx, updates, "renewed task lease")
		for _, task := range snapshot.Tasks {
			if task.TaskID == taskID && task.Lease != nil && task.Lease.ExpiresAt.After(previous) {
				return task
			}
		}
	}
}
