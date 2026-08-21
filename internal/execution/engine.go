package execution

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
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
) (*WorkflowRun, error) {
	if ctx == nil {
		return nil, errors.New("execute workflow: context is nil")
	}

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
// unchanged. A valid lease is allowed to expire rather than being stolen; an
// expired or legacy lease is recovered according to the task retry policy.
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

type localWorker struct {
	id     WorkerID
	slot   int
	jobs   chan taskJob
	cancel context.CancelFunc
}

type workerAvailable struct {
	workerID WorkerID
	slot     int
	jobs     chan taskJob
}

type workerHeartbeatEvent struct {
	workerID WorkerID
}

type workerLostEvent struct {
	workerID WorkerID
	slot     int
}

func (engine *Engine) executeActiveRun(
	ctx context.Context,
	durabilityContext context.Context,
	run *WorkflowRun,
	resolvedHandlers map[workflow.TaskID]TaskHandler,
) (*WorkflowRun, error) {
	definition := run.definition
	definitions := taskDefinitionsByID(definition)
	completed := completedTaskIDs(run)
	ready := make(map[workflow.TaskID]workflow.TaskDefinition)
	for _, task := range existingReadyTasks(run, definition) {
		ready[task.ID] = task
	}
	newReady, err := markNewReady(run, definition, completed)
	if err != nil {
		return engine.failRun(durabilityContext, run, fmt.Errorf("prepare workflow run: %w", err))
	}
	for _, task := range newReady {
		ready[task.ID] = task
	}
	if len(newReady) > 0 {
		if err := engine.saveRun(durabilityContext, run, "make dependency-ready tasks schedulable"); err != nil {
			return engine.failRun(durabilityContext, run, err)
		}
	}

	runContext, cancelRunWorkers := context.WithCancel(ctx)
	availableEvents := make(chan workerAvailable, engine.workerCount*2)
	heartbeatEvents := make(chan workerHeartbeatEvent, engine.workerCount*2)
	resultEvents := make(chan taskResult, engine.workerCount*2)
	lostEvents := make(chan workerLostEvent, engine.workerCount*2)

	var workerGroup sync.WaitGroup
	workers := make(map[WorkerID]*localWorker)
	currentBySlot := make(map[int]WorkerID)
	generations := make(map[int]int)
	available := make(map[WorkerID]workerAvailable)

	spawnWorker := func(slot int) {
		generations[slot]++
		workerID := WorkerID(fmt.Sprintf(
			"%s/%s/slot-%d/generation-%d",
			engine.config.workerNamespace,
			run.id,
			slot,
			generations[slot],
		))
		workerContext, cancelWorker := context.WithCancel(runContext)
		worker := &localWorker{
			id:     workerID,
			slot:   slot,
			jobs:   make(chan taskJob, 1),
			cancel: cancelWorker,
		}
		workers[workerID] = worker
		currentBySlot[slot] = workerID
		workerGroup.Add(1)
		go executeWorker(
			workerContext,
			engine.config.clock,
			engine.config.heartbeatInterval,
			workerAvailable{workerID: workerID, slot: slot, jobs: worker.jobs},
			availableEvents,
			heartbeatEvents,
			resultEvents,
			lostEvents,
			&workerGroup,
		)
	}

	for slot := 1; slot <= engine.workerCount; slot++ {
		spawnWorker(slot)
	}
	shutdownWorkers := func() {
		cancelRunWorkers()
		for _, worker := range workers {
			worker.cancel()
		}
		workerGroup.Wait()
	}

	inFlight := make(map[AttemptID]struct{})
	for _, task := range run.Tasks() {
		if task.Status == TaskRunRunning && task.CurrentAttemptID != "" {
			inFlight[task.CurrentAttemptID] = struct{}{}
		}
	}

	replaceWorker := func(workerID WorkerID) {
		worker, exists := workers[workerID]
		if !exists || currentBySlot[worker.slot] != workerID {
			return
		}
		worker.cancel()
		delete(available, workerID)
		delete(workers, workerID)
		delete(currentBySlot, worker.slot)
		spawnWorker(worker.slot)
	}

	for {
		recoveries := run.recoverExpiredLeases()
		promoted := run.promoteDueRetries()
		var leaseFailure *TaskExecutionError
		for _, recovery := range recoveries {
			delete(inFlight, recovery.AttemptID)
			if recovery.WorkerID != "" {
				replaceWorker(recovery.WorkerID)
			}
			if recovery.Outcome == CompletionRetryScheduled {
				task := requireDefinition(definitions, recovery.TaskID)
				if current, exists := run.Task(recovery.TaskID); exists && current.Status == TaskRunReady {
					ready[task.ID] = task
				}
				continue
			}
			leaseFailure = &TaskExecutionError{
				RunID:  run.id,
				TaskID: recovery.TaskID,
				Cause: &LeaseExpiredError{
					RunID:     run.id,
					TaskID:    recovery.TaskID,
					AttemptID: recovery.AttemptID,
				},
			}
		}
		for _, taskID := range promoted {
			ready[taskID] = requireDefinition(definitions, taskID)
		}
		if len(recoveries) > 0 || len(promoted) > 0 {
			if err := engine.saveRun(durabilityContext, run, "recover expired leases and due retries"); err != nil {
				shutdownWorkers()
				return engine.failRun(durabilityContext, run, err)
			}
		}
		if leaseFailure != nil {
			shutdownWorkers()
			return engine.failRun(durabilityContext, run, leaseFailure)
		}

		if len(completed) == len(definition.Tasks) {
			shutdownWorkers()
			if err := run.transitionWorkflow(WorkflowRunSucceeded); err != nil {
				return engine.failRun(durabilityContext, run, fmt.Errorf("complete workflow run: %w", err))
			}
			if err := engine.saveRun(durabilityContext, run, "complete workflow run"); err != nil {
				return run, err
			}
			return run, nil
		}

		if len(ready) > 0 && len(available) > 0 {
			task := popReadyTask(ready)
			workerSlot := popAvailableWorker(available)
			attemptID, err := run.startTaskAttempt(task.ID, workerSlot.workerID, engine.config.leaseDuration)
			if err != nil {
				shutdownWorkers()
				return engine.failRun(durabilityContext, run, fmt.Errorf("lease task to worker: %w", err))
			}
			if err := engine.saveRun(durabilityContext, run, "lease and dispatch task"); err != nil {
				shutdownWorkers()
				return engine.failRun(durabilityContext, run, err)
			}
			inFlight[attemptID] = struct{}{}
			job := taskJob{
				request: TaskRequest{
					RunID:     run.id,
					TaskRunID: TaskRunIDFor(run.id, task.ID),
					AttemptID: attemptID,
					Task:      cloneTaskDefinition(task),
				},
				handler: resolvedHandlers[task.ID],
			}
			select {
			case workerSlot.jobs <- job:
				continue
			case <-ctx.Done():
				shutdownWorkers()
				return engine.cancelRun(durabilityContext, run, ctx.Err())
			}
		}

		if len(ready) == 0 && len(inFlight) == 0 {
			if _, waiting := run.nextReliabilityDeadline(); !waiting {
				shutdownWorkers()
				return engine.failRun(
					durabilityContext,
					run,
					fmt.Errorf("workflow run %q stalled with unfinished tasks", run.id),
				)
			}
		}

		timer, timerChannel := engine.reliabilityTimer(run)
		select {
		case event := <-availableEvents:
			if currentBySlot[event.slot] != event.workerID {
				break
			}
			if err := run.recordWorkerHeartbeat(event.workerID, engine.config.leaseDuration); err != nil {
				if timer != nil {
					timer.Stop()
				}
				shutdownWorkers()
				return engine.failRun(durabilityContext, run, fmt.Errorf("register worker: %w", err))
			}
			if err := engine.saveRun(durabilityContext, run, "register available worker"); err != nil {
				if timer != nil {
					timer.Stop()
				}
				shutdownWorkers()
				return engine.failRun(durabilityContext, run, err)
			}
			available[event.workerID] = event

		case event := <-heartbeatEvents:
			worker, alive := workers[event.workerID]
			if !alive || currentBySlot[worker.slot] != event.workerID {
				break
			}
			if err := run.recordWorkerHeartbeat(event.workerID, engine.config.leaseDuration); err != nil {
				if timer != nil {
					timer.Stop()
				}
				shutdownWorkers()
				return engine.failRun(durabilityContext, run, fmt.Errorf("record worker heartbeat: %w", err))
			}
			if err := engine.saveRun(durabilityContext, run, "record worker heartbeat"); err != nil {
				if timer != nil {
					timer.Stop()
				}
				shutdownWorkers()
				return engine.failRun(durabilityContext, run, err)
			}

		case event := <-lostEvents:
			if currentBySlot[event.slot] == event.workerID {
				replaceWorker(event.workerID)
			}

		case result := <-resultEvents:
			if contextErr := ctx.Err(); contextErr != nil {
				if timer != nil {
					timer.Stop()
				}
				shutdownWorkers()
				return engine.cancelRun(durabilityContext, run, contextErr)
			}
			delete(inFlight, result.attemptID)
			outcome, err := run.completeTaskAttempt(
				result.taskID,
				result.taskRunID,
				result.workerID,
				result.attemptID,
				result.output,
				result.err,
			)
			if err != nil {
				if timer != nil {
					timer.Stop()
				}
				shutdownWorkers()
				return engine.failRun(durabilityContext, run, fmt.Errorf("record identified task completion: %w", err))
			}
			switch outcome {
			case CompletionIgnored:
				// Duplicate and stale completion events are deliberately no-ops.
			case CompletionSucceeded:
				completed[result.taskID] = struct{}{}
				newReady, err := markNewReady(run, definition, completed)
				if err != nil {
					if timer != nil {
						timer.Stop()
					}
					shutdownWorkers()
					return engine.failRun(durabilityContext, run, fmt.Errorf("unlock dependent tasks: %w", err))
				}
				for _, task := range newReady {
					ready[task.ID] = task
				}
				if err := engine.saveRun(durabilityContext, run, "record completion and unlock dependencies"); err != nil {
					if timer != nil {
						timer.Stop()
					}
					shutdownWorkers()
					return engine.failRun(durabilityContext, run, err)
				}
			case CompletionRetryScheduled:
				if task, exists := run.Task(result.taskID); exists && task.Status == TaskRunReady {
					ready[result.taskID] = requireDefinition(definitions, result.taskID)
				}
				if err := engine.saveRun(durabilityContext, run, "schedule task retry"); err != nil {
					if timer != nil {
						timer.Stop()
					}
					shutdownWorkers()
					return engine.failRun(durabilityContext, run, err)
				}
			case CompletionFailed:
				if err := engine.saveRun(durabilityContext, run, "record terminal task failure"); err != nil {
					if timer != nil {
						timer.Stop()
					}
					shutdownWorkers()
					return engine.failRun(durabilityContext, run, err)
				}
				if timer != nil {
					timer.Stop()
				}
				shutdownWorkers()
				return engine.failRun(durabilityContext, run, &TaskExecutionError{
					RunID:  run.id,
					TaskID: result.taskID,
					Cause:  result.err,
				})
			}

		case <-timerChannel:
			// The loop applies due retries and expired leases against Clock.Now.

		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			shutdownWorkers()
			return engine.cancelRun(durabilityContext, run, ctx.Err())
		}
		if timer != nil {
			timer.Stop()
		}
	}
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

func popAvailableWorker(available map[WorkerID]workerAvailable) workerAvailable {
	workerIDs := make([]WorkerID, 0, len(available))
	for workerID := range available {
		workerIDs = append(workerIDs, workerID)
	}
	sort.Slice(workerIDs, func(left, right int) bool { return workerIDs[left] < workerIDs[right] })
	worker := available[workerIDs[0]]
	delete(available, worker.workerID)
	return worker
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
	taskID    workflow.TaskID
	taskRunID TaskRunID
	workerID  WorkerID
	attemptID AttemptID
	output    string
	err       error
}

var errWorkerDisappeared = errors.New("worker disappeared")

func executeWorker(
	ctx context.Context,
	clock Clock,
	heartbeatInterval time.Duration,
	identity workerAvailable,
	available chan<- workerAvailable,
	heartbeats chan<- workerHeartbeatEvent,
	results chan<- taskResult,
	lost chan<- workerLostEvent,
	workers *sync.WaitGroup,
) {
	defer workers.Done()

	heartbeatContext, cancelHeartbeat := context.WithCancel(ctx)
	var heartbeatGroup sync.WaitGroup
	heartbeatGroup.Add(1)
	go emitHeartbeats(heartbeatContext, clock, heartbeatInterval, identity.workerID, heartbeats, &heartbeatGroup)
	defer func() {
		cancelHeartbeat()
		heartbeatGroup.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case available <- identity:
		}

		var job taskJob
		select {
		case <-ctx.Done():
			return
		case job = <-identity.jobs:
		}

		output, err := invokeHandler(ctx, job)
		if errors.Is(err, errWorkerDisappeared) {
			cancelHeartbeat()
			heartbeatGroup.Wait()
			select {
			case <-ctx.Done():
			case lost <- workerLostEvent{workerID: identity.workerID, slot: identity.slot}:
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case results <- taskResult{
			taskID:    job.request.Task.ID,
			taskRunID: job.request.TaskRunID,
			workerID:  identity.workerID,
			attemptID: job.request.AttemptID,
			output:    output,
			err:       err,
		}:
		}
	}
}

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
