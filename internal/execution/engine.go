package execution

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

// Engine executes workflows with a configurable in-process worker pool.
// Execute is safe to call concurrently; each call owns its queue and workers.
type Engine struct {
	workerCount int
	registry    *HandlerRegistry
}

// NewEngine creates an in-process workflow execution engine.
func NewEngine(workerCount int, registry *HandlerRegistry) (*Engine, error) {
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

	return &Engine{
		workerCount: workerCount,
		registry:    registry,
	}, nil
}

// WorkerCount returns the configured number of workers per workflow run.
func (engine *Engine) WorkerCount() int {
	return engine.workerCount
}

// Execute validates and runs a workflow to a terminal state. A handler failure
// fails the workflow and cancels all remaining work. Context cancellation marks
// the workflow and every unfinished task canceled.
func (engine *Engine) Execute(
	ctx context.Context,
	runID RunID,
	definition workflow.WorkflowDefinition,
) (*WorkflowRun, error) {
	run, err := NewWorkflowRun(runID, definition)
	if err != nil {
		return nil, err
	}

	definition = run.definition
	resolvedHandlers, err := engine.resolveHandlers(definition)
	if err != nil {
		return nil, err
	}

	if contextErr := ctx.Err(); contextErr != nil {
		run.cancelUnfinished(contextErr)
		if err := run.transitionWorkflow(WorkflowRunCanceled); err != nil {
			return run, fmt.Errorf("cancel workflow before execution: %w", err)
		}
		return run, &RunCanceledError{RunID: runID, Cause: contextErr}
	}

	if err := run.transitionWorkflow(WorkflowRunRunning); err != nil {
		return run, fmt.Errorf("start workflow run: %w", err)
	}

	completed := make(map[workflow.TaskID]struct{}, len(definition.Tasks))
	ready, err := markNewReady(run, definition, completed)
	if err != nil {
		run.cancelUnfinished(err)
		if transitionErr := run.transitionWorkflow(WorkflowRunFailed); transitionErr != nil {
			return run, fmt.Errorf("prepare workflow run: %w; mark failed: %v", err, transitionErr)
		}
		return run, fmt.Errorf("prepare workflow run: %w", err)
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
		taskFailure       *TaskExecutionError
		cancellationCause error
		schedulerFailure  error
	)

	for {
		stopping := taskFailure != nil || cancellationCause != nil || schedulerFailure != nil
		if !stopping && len(completed) == len(definition.Tasks) {
			if err := run.transitionWorkflow(WorkflowRunSucceeded); err != nil {
				schedulerFailure = fmt.Errorf("complete workflow run: %w", err)
				cancelRun()
				continue
			}
			shutdownWorkers()
			return run, nil
		}
		if stopping && inFlight == 0 {
			break
		}
		if !stopping && len(ready) == 0 && inFlight == 0 {
			schedulerFailure = fmt.Errorf("workflow run %q stalled with unfinished tasks", runID)
			cancelRun()
			continue
		}

		var (
			dispatch   chan<- taskJob
			nextJob    taskJob
			parentDone <-chan struct{}
		)
		if !stopping && len(ready) > 0 {
			nextTask := ready[0]
			dispatch = jobs
			nextJob = taskJob{
				request: TaskRequest{
					RunID: runID,
					Task:  cloneTaskDefinition(nextTask),
				},
				handler: resolvedHandlers[nextTask.ID],
			}
		}
		if !stopping {
			parentDone = ctx.Done()
		}

		select {
		case dispatch <- nextJob:
			ready = ready[1:]
			inFlight++
			if err := run.transitionTask(nextJob.request.Task.ID, TaskRunRunning, "", nil); err != nil {
				schedulerFailure = fmt.Errorf("mark task running: %w", err)
				cancelRun()
			}

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
				if result.err == nil {
					if err := run.transitionTask(result.taskID, TaskRunSucceeded, result.output, nil); err != nil && schedulerFailure == nil {
						schedulerFailure = fmt.Errorf("record task completion while stopping: %w", err)
					}
				} else {
					cause := cancellationCause
					if cause == nil {
						cause = taskFailure
					}
					if cause == nil {
						cause = schedulerFailure
					}
					if err := run.transitionTask(result.taskID, TaskRunCanceled, "", cause); err != nil && schedulerFailure == nil {
						schedulerFailure = fmt.Errorf("record task cancellation while stopping: %w", err)
					}
				}
				continue
			}

			if result.err != nil {
				if err := run.transitionTask(result.taskID, TaskRunFailed, "", result.err); err != nil {
					schedulerFailure = fmt.Errorf("record task failure: %w", err)
				} else {
					taskFailure = &TaskExecutionError{
						RunID:  runID,
						TaskID: result.taskID,
						Cause:  result.err,
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
			sort.Slice(ready, func(left, right int) bool {
				return ready[left].ID < ready[right].ID
			})

		case <-parentDone:
			cancellationCause = ctx.Err()
			cancelRun()
		}
	}

	shutdownWorkers()

	if schedulerFailure != nil {
		run.cancelUnfinished(schedulerFailure)
		if err := run.transitionWorkflow(WorkflowRunFailed); err != nil {
			return run, fmt.Errorf("scheduler failed: %w; mark workflow failed: %v", schedulerFailure, err)
		}
		return run, schedulerFailure
	}
	if taskFailure != nil {
		run.cancelUnfinished(taskFailure)
		if err := run.transitionWorkflow(WorkflowRunFailed); err != nil {
			return run, fmt.Errorf("task failed: %w; mark workflow failed: %v", taskFailure, err)
		}
		return run, taskFailure
	}

	run.cancelUnfinished(cancellationCause)
	if err := run.transitionWorkflow(WorkflowRunCanceled); err != nil {
		return run, fmt.Errorf("cancel workflow run: %w", err)
	}
	return run, &RunCanceledError{RunID: runID, Cause: cancellationCause}
}

func (engine *Engine) resolveHandlers(
	definition workflow.WorkflowDefinition,
) (map[workflow.TaskID]TaskHandler, error) {
	tasks := append([]workflow.TaskDefinition(nil), definition.Tasks...)
	sort.Slice(tasks, func(left, right int) bool {
		return tasks[left].ID < tasks[right].ID
	})

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
