package execution

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/broker"
	"github.com/vijaypratap3364/forgeflow/internal/workflow"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Engine executes workflows with identified in-process workers and durable
// reliability state. Execute is safe to call concurrently; each run owns its
// worker pool while the Store serializes durable aggregate changes.
type Engine struct {
	workerCount int
	registry    *HandlerRegistry
	store       Store
	config      engineConfig
}

// NewEngine creates an in-process workflow execution engine.
func NewEngine(
	workerCount int,
	registry *HandlerRegistry,
	store Store,
	options ...EngineOption,
) (*Engine, error) {
	if workerCount < 1 {
		return nil, &InvalidEngineConfigError{
			WorkerCount: workerCount,
			Reason:      "worker count must be at least one",
		}
	}
	if registry == nil {
		return nil, &InvalidEngineConfigError{
			WorkerCount: workerCount,
			Reason:      "handler registry must not be nil",
		}
	}
	if store == nil {
		return nil, &InvalidEngineConfigError{
			WorkerCount: workerCount,
			Reason:      "store must not be nil",
		}
	}

	config, err := defaultEngineConfig()
	if err != nil {
		return nil, &InvalidEngineConfigError{WorkerCount: workerCount, Reason: err.Error()}
	}
	for _, option := range options {
		if option == nil {
			return nil, &InvalidEngineConfigError{WorkerCount: workerCount, Reason: "engine option must not be nil"}
		}
		if err := option(&config); err != nil {
			return nil, &InvalidEngineConfigError{WorkerCount: workerCount, Reason: err.Error()}
		}
	}
	if err := config.validate(); err != nil {
		return nil, &InvalidEngineConfigError{WorkerCount: workerCount, Reason: err.Error()}
	}

	return &Engine{
		workerCount: workerCount,
		registry:    registry,
		store:       store,
		config:      config,
	}, nil
}

// WorkerCount returns the configured number of workers per workflow run.
func (engine *Engine) WorkerCount() int {
	return engine.workerCount
}

// Execute validates, durably creates, and runs a new workflow execution.
func (engine *Engine) Execute(
	ctx context.Context,
	runID RunID,
	definition workflow.WorkflowDefinition,
) (result *WorkflowRun, resultErr error) {
	if ctx == nil {
		return nil, errors.New("execute workflow: context is nil")
	}
	ctx, schedulerSpan := engine.config.instrumentation.Tracer("forgeflow/execution").Start(
		ctx,
		"forgeflow.scheduler.execute",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("forgeflow.workflow.id", string(definition.ID)),
			attribute.String("forgeflow.workflow_run.id", string(runID)),
		),
	)
	defer func() {
		if resultErr != nil {
			schedulerSpan.SetStatus(codes.Error, "workflow execution failed")
		}
		schedulerSpan.End()
	}()

	run, err := newWorkflowRun(runID, definition, engine.config.clock.Now)
	if err != nil {
		return nil, err
	}
	definition = run.definition
	resolvedHandlers, err := engine.resolveHandlers(definition)
	if err != nil {
		return nil, err
	}

	durabilityContext := context.WithoutCancel(ctx)
	if err := engine.store.SaveWorkflow(durabilityContext, definition); err != nil {
		return nil, persistenceError("save workflow definition", runID, err)
	}
	createdSnapshot, err := engine.store.CreateRun(durabilityContext, run.Snapshot())
	if err != nil {
		return nil, persistenceError("create workflow run", runID, err)
	}
	if err := applyPersistedVersion(run, run.Snapshot(), createdSnapshot); err != nil {
		return nil, persistenceError("create workflow run", runID, err)
	}

	if contextErr := ctx.Err(); contextErr != nil {
		return engine.cancelRun(durabilityContext, run, contextErr)
	}
	if err := run.transitionWorkflow(WorkflowRunRunning); err != nil {
		return run, fmt.Errorf("start workflow run: %w", err)
	}
	if err := engine.saveRun(durabilityContext, run, "start workflow run"); err != nil {
		return run, err
	}
	return engine.executeBrokerRun(ctx, durabilityContext, run, resolvedHandlers)
}

// Recover reconstructs a persisted workflow run. Terminal runs are returned
// unchanged. A valid lease is allowed to expire rather than being stolen; an
// expired or legacy lease is recovered according to the task retry policy.
func (engine *Engine) Recover(ctx context.Context, runID RunID) (result *WorkflowRun, resultErr error) {
	if ctx == nil {
		return nil, errors.New("recover workflow: context is nil")
	}
	if !validRunID(string(runID)) {
		return nil, &InvalidRunIDError{RunID: runID}
	}
	ctx, schedulerSpan := engine.config.instrumentation.Tracer("forgeflow/execution").Start(
		ctx,
		"forgeflow.scheduler.recover",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("forgeflow.workflow_run.id", string(runID))),
	)
	defer func() {
		if resultErr != nil {
			schedulerSpan.SetStatus(codes.Error, "workflow recovery failed")
		}
		schedulerSpan.End()
	}()

	snapshot, found, err := engine.store.LoadRun(ctx, runID)
	if err != nil {
		return nil, persistenceError("load workflow run", runID, err)
	}
	if !found {
		return nil, &RunNotFoundError{RunID: runID}
	}
	definition, found, err := engine.store.LoadWorkflow(ctx, snapshot.WorkflowID)
	if err != nil {
		return nil, persistenceError("load workflow definition", runID, err)
	}
	if !found {
		return nil, &WorkflowNotFoundError{WorkflowID: snapshot.WorkflowID}
	}
	schedulerSpan.SetAttributes(attribute.String("forgeflow.workflow.id", string(snapshot.WorkflowID)))
	run, err := restoreWorkflowRun(snapshot, definition, engine.config.clock.Now)
	if err != nil {
		return nil, fmt.Errorf("restore workflow run %q: %w", runID, err)
	}
	if run.Status() == WorkflowRunSucceeded ||
		run.Status() == WorkflowRunFailed ||
		run.Status() == WorkflowRunCanceled {
		return run, nil
	}

	durabilityContext := context.WithoutCancel(ctx)
	if status, cause := recoveredTerminalTaskState(run); status != "" {
		run.cancelUnfinished(cause)
		if err := run.transitionWorkflow(status); err != nil {
			return run, fmt.Errorf("finish recovered workflow run: %w", err)
		}
		if err := engine.saveRun(durabilityContext, run, "finish recovered workflow run"); err != nil {
			return run, err
		}
		return run, nil
	}
	if run.Status() == WorkflowRunPending {
		if err := run.transitionWorkflow(WorkflowRunRunning); err != nil {
			return run, fmt.Errorf("start recovered workflow run: %w", err)
		}
		if err := engine.saveRun(durabilityContext, run, "start recovered workflow run"); err != nil {
			return run, err
		}
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return engine.cancelRun(durabilityContext, run, contextErr)
	}
	resolvedHandlers, err := engine.resolveHandlers(definition)
	if err != nil {
		return run, err
	}
	return engine.executeBrokerRun(ctx, durabilityContext, run, resolvedHandlers)
}

func recoveredTerminalTaskState(run *WorkflowRun) (WorkflowRunStatus, error) {
	var canceled bool
	for _, task := range run.Tasks() {
		switch task.Status {
		case TaskRunFailed:
			return WorkflowRunFailed, fmt.Errorf("recovered failed task %q: %s", task.TaskID, task.Error)
		case TaskRunCanceled:
			canceled = true
		}
	}
	if canceled {
		return WorkflowRunCanceled, errors.New("recovered an interrupted workflow cancellation")
	}
	return "", nil
}

type localWorker struct {
	id     WorkerID
	slot   int
	cancel context.CancelFunc
}

type workerIdentity struct {
	workerID WorkerID
	slot     int
}

type workerHeartbeatEvent struct {
	workerID WorkerID
}

type workerLostEvent struct {
	workerID WorkerID
	slot     int
}

type workerClaimEvent struct {
	workerID WorkerID
	slot     int
	delivery broker.Delivery
	response chan workerClaimResponse
}

type workerClaimResponse struct {
	job *taskJob
}

type workerBrokerError struct {
	workerID WorkerID
	slot     int
	err      error
}

func (engine *Engine) reliabilityTimer(run *WorkflowRun) (Timer, <-chan time.Time) {
	deadline, exists := run.nextReliabilityDeadline()
	if !exists {
		return nil, nil
	}
	duration := deadline.Sub(engine.config.clock.Now().UTC())
	if duration < 0 {
		duration = 0
	}
	timer := engine.config.clock.NewTimer(duration)
	return timer, timer.C()
}

func (engine *Engine) failRun(
	ctx context.Context,
	run *WorkflowRun,
	cause error,
) (*WorkflowRun, error) {
	run.cancelUnfinished(cause)
	if err := run.transitionWorkflow(WorkflowRunFailed); err != nil {
		return run, errors.Join(cause, fmt.Errorf("mark workflow failed: %w", err))
	}
	if err := engine.saveRun(ctx, run, "fail workflow run"); err != nil {
		return run, errors.Join(cause, err)
	}
	return run, cause
}

func (engine *Engine) cancelRun(
	ctx context.Context,
	run *WorkflowRun,
	cause error,
) (*WorkflowRun, error) {
	run.cancelUnfinished(cause)
	if err := run.transitionWorkflow(WorkflowRunCanceled); err != nil {
		return run, fmt.Errorf("cancel workflow run: %w", err)
	}
	canceledError := &RunCanceledError{RunID: run.id, Cause: cause}
	if err := engine.saveRun(ctx, run, "cancel workflow run"); err != nil {
		return run, errors.Join(canceledError, err)
	}
	return run, canceledError
}

func (engine *Engine) saveRun(ctx context.Context, run *WorkflowRun, operation string) error {
	candidate := run.Snapshot()
	stored, err := engine.store.SaveRun(ctx, candidate)
	if err != nil {
		return persistenceError(operation, run.id, err)
	}
	if err := applyPersistedVersion(run, candidate, stored); err != nil {
		return persistenceError(operation, run.id, err)
	}
	return nil
}

func applyPersistedVersion(
	run *WorkflowRun,
	candidate WorkflowRunSnapshot,
	stored WorkflowRunSnapshot,
) error {
	if stored.ID != candidate.ID || stored.WorkflowID != candidate.WorkflowID {
		return errors.New("store returned a different workflow run after persistence")
	}
	if stored.Version != candidate.Version+1 {
		return fmt.Errorf(
			"store returned workflow run version %d after version %d",
			stored.Version,
			candidate.Version,
		)
	}
	run.setPersistedVersion(stored.Version)
	return nil
}

func persistenceError(operation string, runID RunID, cause error) error {
	return &PersistenceError{Operation: operation, RunID: runID, Cause: cause}
}

func (engine *Engine) resolveHandlers(
	definition workflow.WorkflowDefinition,
) (map[workflow.TaskID]TaskHandler, error) {
	tasks := append([]workflow.TaskDefinition(nil), definition.Tasks...)
	sortTasks(tasks)
	resolved := make(map[workflow.TaskID]TaskHandler, len(tasks))
	for _, task := range tasks {
		handler, exists := engine.registry.Handler(task.Handler)
		if !exists {
			return nil, &UnknownHandlerError{
				TaskID:      task.ID,
				HandlerName: task.Handler,
			}
		}
		resolved[task.ID] = handler
	}
	return resolved, nil
}

func markNewReady(
	run *WorkflowRun,
	definition workflow.WorkflowDefinition,
	completed map[workflow.TaskID]struct{},
) ([]workflow.TaskDefinition, error) {
	candidates, err := definition.ReadyTasks(completed)
	if err != nil {
		return nil, err
	}
	ready := make([]workflow.TaskDefinition, 0, len(candidates))
	for _, task := range candidates {
		status, err := run.taskStatus(task.ID)
		if err != nil {
			return nil, err
		}
		if status != TaskRunPending {
			continue
		}
		if err := run.transitionTask(task.ID, TaskRunReady, "", nil); err != nil {
			return nil, err
		}
		ready = append(ready, task)
	}
	return ready, nil
}

func completedTaskIDs(run *WorkflowRun) map[workflow.TaskID]struct{} {
	completed := make(map[workflow.TaskID]struct{})
	for _, task := range run.Tasks() {
		if task.Status == TaskRunSucceeded {
			completed[task.TaskID] = struct{}{}
		}
	}
	return completed
}

func existingReadyTasks(
	run *WorkflowRun,
	definition workflow.WorkflowDefinition,
) []workflow.TaskDefinition {
	definitions := taskDefinitionsByID(definition)
	ready := make([]workflow.TaskDefinition, 0)
	for _, task := range run.Tasks() {
		if task.Status == TaskRunReady {
			ready = append(ready, definitions[task.TaskID])
		}
	}
	sortTasks(ready)
	return ready
}

func taskDefinitionsByID(definition workflow.WorkflowDefinition) map[workflow.TaskID]workflow.TaskDefinition {
	tasks := make(map[workflow.TaskID]workflow.TaskDefinition, len(definition.Tasks))
	for _, task := range definition.Tasks {
		tasks[task.ID] = task
	}
	return tasks
}

func requireDefinition(
	definitions map[workflow.TaskID]workflow.TaskDefinition,
	taskID workflow.TaskID,
) workflow.TaskDefinition {
	return definitions[taskID]
}

func popReadyTask(ready map[workflow.TaskID]workflow.TaskDefinition) workflow.TaskDefinition {
	taskIDs := make([]workflow.TaskID, 0, len(ready))
	for taskID := range ready {
		taskIDs = append(taskIDs, taskID)
	}
	sort.Slice(taskIDs, func(left, right int) bool { return taskIDs[left] < taskIDs[right] })
	task := ready[taskIDs[0]]
	delete(ready, task.ID)
	return task
}

func sortTasks(tasks []workflow.TaskDefinition) {
	sort.Slice(tasks, func(left, right int) bool {
		return tasks[left].ID < tasks[right].ID
	})
}

type taskJob struct {
	context context.Context
	request TaskRequest
	handler TaskHandler
}

var errWorkerDisappeared = errors.New("worker disappeared")

func emitHeartbeats(
	ctx context.Context,
	clock Clock,
	interval time.Duration,
	workerID WorkerID,
	events chan<- workerHeartbeatEvent,
	workers *sync.WaitGroup,
) {
	defer workers.Done()
	for {
		timer := clock.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C():
			timer.Stop()
		}
		select {
		case <-ctx.Done():
			return
		case events <- workerHeartbeatEvent{workerID: workerID}:
		}
	}
}

func invokeHandler(ctx context.Context, job taskJob) (output string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			output = ""
			err = fmt.Errorf("%w: handler panic: %v", errWorkerDisappeared, recovered)
		}
	}()
	return job.handler.Execute(ctx, job.request)
}

func cloneTaskDefinition(task workflow.TaskDefinition) workflow.TaskDefinition {
	clone := task
	clone.Dependencies = append([]workflow.TaskID(nil), task.Dependencies...)
	return clone
}
