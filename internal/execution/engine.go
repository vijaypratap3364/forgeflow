package execution

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

// Engine executes workflows with a configurable in-process worker pool and a
// durable Store. Execute is safe to call concurrently; each call owns its queue
// and workers while the Store serializes durable state changes.
type Engine struct {
	workerCount int
	registry    *HandlerRegistry
	store       Store
}

// NewEngine creates an in-process workflow execution engine.
func NewEngine(workerCount int, registry *HandlerRegistry, store Store) (*Engine, error) {
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

	return &Engine{
		workerCount: workerCount,
		registry:    registry,
		store:       store,
	}, nil
}

// WorkerCount returns the configured number of workers per workflow run.
func (engine *Engine) WorkerCount() int {
	return engine.workerCount
}

// Execute validates, durably creates, and runs a new workflow execution. A
// handler failure fails the workflow and cancels all remaining work. Context
// cancellation marks the workflow and every unfinished task canceled.
func (engine *Engine) Execute(
	ctx context.Context,
	runID RunID,
	definition workflow.WorkflowDefinition,
) (*WorkflowRun, error) {
	if ctx == nil {
		return nil, errors.New("execute workflow: context is nil")
	}

	run, err := NewWorkflowRun(runID, definition)
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
	if err := engine.store.CreateRun(durabilityContext, run.Snapshot()); err != nil {
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

	return engine.executeActiveRun(ctx, durabilityContext, run, resolvedHandlers)
}

// Recover reconstructs a persisted workflow run. Terminal runs are returned
// unchanged. A nonterminal run resumes from completed tasks; a task that was
// running at process loss is made ready and may execute again.
func (engine *Engine) Recover(ctx context.Context, runID RunID) (*WorkflowRun, error) {
	if ctx == nil {
		return nil, errors.New("recover workflow: context is nil")
	}
	if !validRunID(string(runID)) {
		return nil, &InvalidRunIDError{RunID: runID}
	}

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
	run, err := RestoreWorkflowRun(snapshot, definition)
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
	if run.recoverInterruptedTasks() {
		if err := engine.saveRun(durabilityContext, run, "release interrupted tasks"); err != nil {
			return run, err
		}
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

	return engine.executeActiveRun(ctx, durabilityContext, run, resolvedHandlers)
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

func (engine *Engine) executeActiveRun(
	ctx context.Context,
	durabilityContext context.Context,
	run *WorkflowRun,
	resolvedHandlers map[workflow.TaskID]TaskHandler,
) (*WorkflowRun, error) {
	definition := run.definition
	completed := completedTaskIDs(run)
	ready := existingReadyTasks(run, definition)
	newReady, err := markNewReady(run, definition, completed)
	if err != nil {
		return engine.failRun(durabilityContext, run, fmt.Errorf("prepare workflow run: %w", err))
	}
	ready = append(ready, newReady...)
	sortTasks(ready)
	if len(newReady) > 0 {
		if err := engine.saveRun(durabilityContext, run, "make dependency-ready tasks schedulable"); err != nil {
			return engine.failRun(durabilityContext, run, err)
		}
	}

	runContext, cancelRun := context.WithCancel(ctx)
	jobs := make(chan taskJob)
	results := make(chan taskResult)

	var workers sync.WaitGroup
	workers.Add(engine.workerCount)
	for range engine.workerCount {
		go executeTasks(runContext, jobs, results, &workers)
	}

	shutdownWorkers := func() {
		close(jobs)
		cancelRun()
		workers.Wait()
	}

	var (
		inFlight          int
		preparedJob       *taskJob
		taskFailure       *TaskExecutionError
		cancellationCause error
		schedulerFailure  error
	)

	for {
		stopping := taskFailure != nil || cancellationCause != nil || schedulerFailure != nil
		if stopping {
			preparedJob = nil
		}
		if !stopping && len(completed) == len(definition.Tasks) {
			if err := run.transitionWorkflow(WorkflowRunSucceeded); err != nil {
				schedulerFailure = fmt.Errorf("complete workflow run: %w", err)
				cancelRun()
				continue
			}
			if err := engine.saveRun(durabilityContext, run, "complete workflow run"); err != nil {
				shutdownWorkers()
				return run, err
			}
			shutdownWorkers()
			return run, nil
		}
		if stopping && inFlight == 0 {
			break
		}

		if !stopping && preparedJob == nil && len(ready) > 0 {
			nextTask := ready[0]
			ready = ready[1:]
			if err := run.transitionTask(nextTask.ID, TaskRunRunning, "", nil); err != nil {
				schedulerFailure = fmt.Errorf("mark task running: %w", err)
				cancelRun()
				continue
			}
			if err := engine.saveRun(durabilityContext, run, "dispatch task"); err != nil {
				schedulerFailure = err
				cancelRun()
				continue
			}
			preparedJob = &taskJob{
				request: TaskRequest{
					RunID: run.id,
					Task:  cloneTaskDefinition(nextTask),
				},
				handler: resolvedHandlers[nextTask.ID],
			}
		}
		if !stopping && preparedJob == nil && len(ready) == 0 && inFlight == 0 {
			schedulerFailure = fmt.Errorf("workflow run %q stalled with unfinished tasks", run.id)
			cancelRun()
			continue
		}

		var (
			dispatch   chan<- taskJob
			nextJob    taskJob
			parentDone <-chan struct{}
		)
		if !stopping && preparedJob != nil {
			dispatch = jobs
			nextJob = *preparedJob
		}
		if !stopping {
			parentDone = ctx.Done()
		}

		select {
		case dispatch <- nextJob:
			preparedJob = nil
			inFlight++

		case result := <-results:
			inFlight--
			stopping = taskFailure != nil || cancellationCause != nil || schedulerFailure != nil
			if !stopping {
				if contextErr := ctx.Err(); contextErr != nil {
					cancellationCause = contextErr
					cancelRun()
					stopping = true
				}
			}
			if stopping {
				cause := cancellationCause
				if cause == nil {
					cause = combinedRunFailure(schedulerFailure, taskFailure)
				}
				if result.err == nil {
					err = run.transitionTask(result.taskID, TaskRunSucceeded, result.output, nil)
				} else {
					err = run.transitionTask(result.taskID, TaskRunCanceled, "", cause)
				}
				if err != nil && schedulerFailure == nil {
					schedulerFailure = fmt.Errorf("record task result while stopping: %w", err)
				}
				if err == nil {
					if saveErr := engine.saveRun(durabilityContext, run, "record task result while stopping"); saveErr != nil && schedulerFailure == nil {
						schedulerFailure = saveErr
					}
				}
				continue
			}

			if result.err != nil {
				if err := run.transitionTask(result.taskID, TaskRunFailed, "", result.err); err != nil {
					schedulerFailure = fmt.Errorf("record task failure: %w", err)
				} else {
					taskFailure = &TaskExecutionError{
						RunID:  run.id,
						TaskID: result.taskID,
						Cause:  result.err,
					}
					if saveErr := engine.saveRun(durabilityContext, run, "record task failure"); saveErr != nil {
						schedulerFailure = saveErr
					}
				}
				cancelRun()
				continue
			}

			if err := run.transitionTask(result.taskID, TaskRunSucceeded, result.output, nil); err != nil {
				schedulerFailure = fmt.Errorf("record task success: %w", err)
				cancelRun()
				continue
			}
			completed[result.taskID] = struct{}{}
			newReady, err := markNewReady(run, definition, completed)
			if err != nil {
				schedulerFailure = fmt.Errorf("unlock dependent tasks: %w", err)
				cancelRun()
				continue
			}
			ready = append(ready, newReady...)
			sortTasks(ready)
			if err := engine.saveRun(durabilityContext, run, "record task success and unlock dependencies"); err != nil {
				schedulerFailure = err
				cancelRun()
			}

		case <-parentDone:
			cancellationCause = ctx.Err()
			cancelRun()
		}
	}

	shutdownWorkers()
	if schedulerFailure != nil {
		return engine.failRun(durabilityContext, run, combinedRunFailure(schedulerFailure, taskFailure))
	}
	if taskFailure != nil {
		return engine.failRun(durabilityContext, run, taskFailure)
	}
	return engine.cancelRun(durabilityContext, run, cancellationCause)
}

func combinedRunFailure(schedulerFailure error, taskFailure *TaskExecutionError) error {
	if schedulerFailure == nil {
		return taskFailure
	}
	if taskFailure == nil {
		return schedulerFailure
	}
	return errors.Join(schedulerFailure, taskFailure)
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
	if err := engine.store.SaveRun(ctx, run.Snapshot()); err != nil {
		return persistenceError(operation, run.id, err)
	}
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
	definitions := make(map[workflow.TaskID]workflow.TaskDefinition, len(definition.Tasks))
	for _, task := range definition.Tasks {
		definitions[task.ID] = task
	}
	ready := make([]workflow.TaskDefinition, 0)
	for _, task := range run.Tasks() {
		if task.Status == TaskRunReady {
			ready = append(ready, definitions[task.TaskID])
		}
	}
	sortTasks(ready)
	return ready
}

func sortTasks(tasks []workflow.TaskDefinition) {
	sort.Slice(tasks, func(left, right int) bool {
		return tasks[left].ID < tasks[right].ID
	})
}

type taskJob struct {
	request TaskRequest
	handler TaskHandler
}

type taskResult struct {
	taskID workflow.TaskID
	output string
	err    error
}

func executeTasks(
	ctx context.Context,
	jobs <-chan taskJob,
	results chan<- taskResult,
	workers *sync.WaitGroup,
) {
	defer workers.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case job, open := <-jobs:
			if !open {
				return
			}

			output, err := job.handler.Execute(ctx, job.request)
			results <- taskResult{
				taskID: job.request.Task.ID,
				output: output,
				err:    err,
			}
		}
	}
}

func cloneTaskDefinition(task workflow.TaskDefinition) workflow.TaskDefinition {
	clone := task
	clone.Dependencies = append([]workflow.TaskID(nil), task.Dependencies...)
	return clone
}
