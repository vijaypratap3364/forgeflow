package execution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/broker"
	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

type taskDispatch struct {
	RunID     RunID           `json:"run_id"`
	TaskID    workflow.TaskID `json:"task_id"`
	TaskRunID TaskRunID       `json:"task_run_id"`
	AttemptID AttemptID       `json:"attempt_id"`
}

type brokerTaskResult struct {
	taskID    workflow.TaskID
	taskRunID TaskRunID
	workerID  WorkerID
	attemptID AttemptID
	output    string
	err       error
	delivery  broker.Delivery
}

func (engine *Engine) executeBrokerRun(
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
	heartbeatEvents := make(chan workerHeartbeatEvent, engine.workerCount*2)
	claimEvents := make(chan workerClaimEvent, engine.workerCount*2)
	resultEvents := make(chan brokerTaskResult, engine.workerCount*2)
	lostEvents := make(chan workerLostEvent, engine.workerCount*2)
	brokerErrors := make(chan workerBrokerError, engine.workerCount*2)
	subscription := taskSubscription(run.id)

	var workerGroup sync.WaitGroup
	workers := make(map[WorkerID]*localWorker)
	currentBySlot := make(map[int]WorkerID)
	generations := make(map[int]int)

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
		worker := &localWorker{id: workerID, slot: slot, cancel: cancelWorker}
		workers[workerID] = worker
		currentBySlot[slot] = workerID
		workerGroup.Add(1)
		go executeBrokerWorker(
			workerContext,
			engine.config.clock,
			engine.config.heartbeatInterval,
			workerIdentity{workerID: workerID, slot: slot},
			engine.config.taskBroker,
			subscription,
			claimEvents,
			heartbeatEvents,
			resultEvents,
			lostEvents,
			brokerErrors,
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
	activeDeliveries := make(map[WorkerID]broker.Delivery)
	pendingClaims := make(map[AttemptID][]workerClaimEvent)
	published := make(map[AttemptID]struct{})

	replaceWorker := func(workerID WorkerID) {
		worker, exists := workers[workerID]
		if !exists || currentBySlot[worker.slot] != workerID {
			return
		}
		worker.cancel()
		delete(activeDeliveries, workerID)
		delete(workers, workerID)
		delete(currentBySlot, worker.slot)
		spawnWorker(worker.slot)
	}

	for {
		if contextErr := ctx.Err(); contextErr != nil {
			shutdownWorkers()
			return engine.cancelRun(durabilityContext, run, contextErr)
		}

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
			for _, recovery := range recoveries {
				claims := pendingClaims[recovery.AttemptID]
				if err := acknowledgePendingClaims(durabilityContext, run.id, claims); err != nil {
					shutdownWorkers()
					return run, err
				}
				delete(pendingClaims, recovery.AttemptID)
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

		if err := engine.publishReadyTasks(ctx, run, ready, published); err != nil {
			shutdownWorkers()
			return run, err
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
		case claim := <-claimEvents:
			if currentBySlot[claim.slot] != claim.workerID {
				if err := claim.delivery.Nack(durabilityContext); err != nil && !errors.Is(err, broker.ErrDeliverySettled) {
					if timer != nil {
						timer.Stop()
					}
					shutdownWorkers()
					return run, brokerOperationError("nack stale worker delivery", run.id, claim.delivery.Message().ID, err)
				}
				claim.response <- workerClaimResponse{}
				break
			}

			message := claim.delivery.Message()
			dispatch, err := decodeTaskDispatch(message)
			if err != nil || dispatch.RunID != run.id || message.Topic != subscription.Topic ||
				message.ID != broker.MessageID(dispatch.AttemptID) {
				ackErr := acknowledgeDelivery(durabilityContext, run.id, claim.delivery)
				claim.response <- workerClaimResponse{}
				if timer != nil {
					timer.Stop()
				}
				shutdownWorkers()
				return run, brokerOperationError(
					"decode task delivery",
					run.id,
					message.ID,
					errors.Join(err, ackErr),
				)
			}
			task, exists := run.Task(dispatch.TaskID)
			if !exists || dispatch.TaskRunID != TaskRunIDFor(run.id, dispatch.TaskID) {
				ackErr := acknowledgeDelivery(durabilityContext, run.id, claim.delivery)
				claim.response <- workerClaimResponse{}
				if timer != nil {
					timer.Stop()
				}
				shutdownWorkers()
				return run, brokerOperationError(
					"validate task delivery",
					run.id,
					message.ID,
					errors.Join(fmt.Errorf("delivery references unknown task identity"), ackErr),
				)
			}

			switch {
			case task.Status == TaskRunReady && dispatch.AttemptID == AttemptIDFor(task.TaskRunID, task.AttemptCount+1):
				attemptID, err := run.startTaskAttempt(dispatch.TaskID, claim.workerID, engine.config.leaseDuration)
				if err != nil {
					if timer != nil {
						timer.Stop()
					}
					shutdownWorkers()
					return engine.failRun(durabilityContext, run, fmt.Errorf("lease delivered task: %w", err))
				}
				if err := engine.saveRun(durabilityContext, run, "lease delivered task"); err != nil {
					if timer != nil {
						timer.Stop()
					}
					shutdownWorkers()
					return run, err
				}
				delete(ready, dispatch.TaskID)
				inFlight[attemptID] = struct{}{}
				activeDeliveries[claim.workerID] = claim.delivery
				taskDefinition := requireDefinition(definitions, dispatch.TaskID)
				claim.response <- workerClaimResponse{job: &taskJob{
					request: TaskRequest{
						RunID:     run.id,
						TaskRunID: task.TaskRunID,
						AttemptID: attemptID,
						Task:      cloneTaskDefinition(taskDefinition),
					},
					handler: resolvedHandlers[dispatch.TaskID],
				}}

			case task.Status == TaskRunRunning && dispatch.AttemptID == task.CurrentAttemptID:
				pendingClaims[dispatch.AttemptID] = append(pendingClaims[dispatch.AttemptID], claim)

			default:
				if err := acknowledgeDelivery(durabilityContext, run.id, claim.delivery); err != nil {
					if timer != nil {
						timer.Stop()
					}
					shutdownWorkers()
					return run, err
				}
				claim.response <- workerClaimResponse{}
			}

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
				return run, err
			}
			if delivery := activeDeliveries[event.workerID]; delivery != nil {
				if err := delivery.Progress(durabilityContext); err != nil {
					if timer != nil {
						timer.Stop()
					}
					shutdownWorkers()
					return run, brokerOperationError("extend task delivery", run.id, delivery.Message().ID, err)
				}
			}

		case event := <-lostEvents:
			if currentBySlot[event.slot] == event.workerID {
				delete(activeDeliveries, event.workerID)
				replaceWorker(event.workerID)
			}

		case event := <-brokerErrors:
			if currentBySlot[event.slot] != event.workerID {
				break
			}
			if timer != nil {
				timer.Stop()
			}
			shutdownWorkers()
			return run, brokerOperationError("receive task", run.id, "", event.err)

		case result := <-resultEvents:
			if contextErr := ctx.Err(); contextErr != nil {
				if timer != nil {
					timer.Stop()
				}
				shutdownWorkers()
				return engine.cancelRun(durabilityContext, run, contextErr)
			}
			delete(activeDeliveries, result.workerID)
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

			var terminalFailure error
			switch outcome {
			case CompletionIgnored:
				// Persisted state already makes this stale delivery harmless.
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
					return run, err
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
					return run, err
				}
			case CompletionFailed:
				if err := engine.saveRun(durabilityContext, run, "record terminal task failure"); err != nil {
					if timer != nil {
						timer.Stop()
					}
					shutdownWorkers()
					return run, err
				}
				terminalFailure = &TaskExecutionError{RunID: run.id, TaskID: result.taskID, Cause: result.err}
			}

			if err := acknowledgeDelivery(durabilityContext, run.id, result.delivery); err != nil {
				if timer != nil {
					timer.Stop()
				}
				shutdownWorkers()
				return run, err
			}
			claims := pendingClaims[result.attemptID]
			if err := acknowledgePendingClaims(durabilityContext, run.id, claims); err != nil {
				if timer != nil {
					timer.Stop()
				}
				shutdownWorkers()
				return run, err
			}
			delete(pendingClaims, result.attemptID)
			if terminalFailure != nil {
				if timer != nil {
					timer.Stop()
				}
				shutdownWorkers()
				return engine.failRun(durabilityContext, run, terminalFailure)
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

func (engine *Engine) publishReadyTasks(
	ctx context.Context,
	run *WorkflowRun,
	ready map[workflow.TaskID]workflow.TaskDefinition,
	published map[AttemptID]struct{},
) error {
	taskIDs := make([]workflow.TaskID, 0, len(ready))
	for taskID := range ready {
		taskIDs = append(taskIDs, taskID)
	}
	sort.Slice(taskIDs, func(left, right int) bool { return taskIDs[left] < taskIDs[right] })
	for _, taskID := range taskIDs {
		task, exists := run.Task(taskID)
		if !exists {
			return fmt.Errorf("publish ready task %q: task run is missing", taskID)
		}
		attemptID := AttemptIDFor(task.TaskRunID, task.AttemptCount+1)
		if _, exists := published[attemptID]; exists {
			continue
		}
		dispatch := taskDispatch{
			RunID:     run.id,
			TaskID:    taskID,
			TaskRunID: task.TaskRunID,
			AttemptID: attemptID,
		}
		body, err := json.Marshal(dispatch)
		if err != nil {
			return brokerOperationError("encode task", run.id, broker.MessageID(attemptID), err)
		}
		message := broker.TaskMessage{
			ID:    broker.MessageID(attemptID),
			Topic: taskTopic(run.id),
			Body:  body,
		}
		if err := engine.config.taskBroker.Publish(ctx, message); err != nil {
			return brokerOperationError("publish task", run.id, message.ID, err)
		}
		published[attemptID] = struct{}{}
	}
	return nil
}

func decodeTaskDispatch(message broker.TaskMessage) (taskDispatch, error) {
	if err := message.Validate(); err != nil {
		return taskDispatch{}, err
	}
	var dispatch taskDispatch
	if err := json.Unmarshal(message.Body, &dispatch); err != nil {
		return taskDispatch{}, fmt.Errorf("decode task dispatch: %w", err)
	}
	if !validIdentifier(string(dispatch.RunID)) || !validIdentifier(string(dispatch.TaskID)) ||
		!validIdentifier(string(dispatch.TaskRunID)) || !validIdentifier(string(dispatch.AttemptID)) {
		return taskDispatch{}, errors.New("decode task dispatch: identity is invalid")
	}
	return dispatch, nil
}

func taskSubscription(runID RunID) broker.Subscription {
	digest := sha256.Sum256([]byte(runID))
	suffix := hex.EncodeToString(digest[:])
	return broker.Subscription{
		ConsumerID: broker.ConsumerID("forgeflow-run-" + suffix),
		Topic:      broker.Topic("run-" + suffix),
	}
}

func taskTopic(runID RunID) broker.Topic {
	return taskSubscription(runID).Topic
}

func acknowledgeDelivery(ctx context.Context, runID RunID, delivery broker.Delivery) error {
	if delivery == nil {
		return nil
	}
	if err := delivery.Ack(ctx); err != nil {
		nackErr := delivery.Nack(context.Background())
		if errors.Is(nackErr, broker.ErrDeliverySettled) {
			nackErr = nil
		}
		return brokerOperationError("acknowledge task", runID, delivery.Message().ID, errors.Join(err, nackErr))
	}
	return nil
}

func acknowledgePendingClaims(ctx context.Context, runID RunID, claims []workerClaimEvent) error {
	for _, claim := range claims {
		if err := acknowledgeDelivery(ctx, runID, claim.delivery); err != nil {
			return err
		}
		claim.response <- workerClaimResponse{}
	}
	return nil
}

func brokerOperationError(operation string, runID RunID, messageID broker.MessageID, cause error) error {
	if cause == nil {
		cause = errors.New("unknown task broker failure")
	}
	return &BrokerOperationError{Operation: operation, RunID: runID, MessageID: messageID, Cause: cause}
}

func executeBrokerWorker(
	ctx context.Context,
	clock Clock,
	heartbeatInterval time.Duration,
	identity workerIdentity,
	taskBroker broker.Broker,
	subscription broker.Subscription,
	claims chan<- workerClaimEvent,
	heartbeats chan<- workerHeartbeatEvent,
	results chan<- brokerTaskResult,
	lost chan<- workerLostEvent,
	brokerErrors chan<- workerBrokerError,
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

	select {
	case <-ctx.Done():
		return
	case heartbeats <- workerHeartbeatEvent{workerID: identity.workerID}:
	}

	for {
		delivery, err := taskBroker.Receive(ctx, subscription)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			select {
			case <-ctx.Done():
			case brokerErrors <- workerBrokerError{workerID: identity.workerID, slot: identity.slot, err: err}:
			}
			return
		}

		response := make(chan workerClaimResponse, 1)
		claim := workerClaimEvent{
			workerID: identity.workerID,
			slot:     identity.slot,
			delivery: delivery,
			response: response,
		}
		select {
		case <-ctx.Done():
			nackWorkerDelivery(delivery)
			return
		case claims <- claim:
		}

		var assignment workerClaimResponse
		select {
		case <-ctx.Done():
			nackWorkerDelivery(delivery)
			return
		case assignment = <-response:
		}
		if assignment.job == nil {
			continue
		}

		output, err := invokeHandler(ctx, *assignment.job)
		if errors.Is(err, errWorkerDisappeared) {
			nackWorkerDelivery(delivery)
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
			nackWorkerDelivery(delivery)
			return
		case results <- brokerTaskResult{
			taskID:    assignment.job.request.Task.ID,
			taskRunID: assignment.job.request.TaskRunID,
			workerID:  identity.workerID,
			attemptID: assignment.job.request.AttemptID,
			output:    output,
			err:       err,
			delivery:  delivery,
		}:
		}
	}
}

func nackWorkerDelivery(delivery broker.Delivery) {
	if delivery == nil {
		return
	}
	err := delivery.Nack(context.Background())
	if err != nil && !errors.Is(err, broker.ErrDeliverySettled) {
		// NATS will still redeliver after AckWait. The in-memory broker cannot
		// fail this operation while open.
		return
	}
}
