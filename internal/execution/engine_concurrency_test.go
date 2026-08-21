package execution

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vijaypratap3364/forgeflow/internal/broker"
	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

func TestEngineExecutesManyReadyTasksWithBoundedConcurrency(t *testing.T) {
	const (
		taskCount   = 64
		workerCount = 8
	)

	started := make(chan workflow.TaskID, taskCount)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	calls := make(map[workflow.TaskID]*atomic.Int32, taskCount)
	tasks := make([]workflow.TaskDefinition, 0, taskCount)
	for index := 0; index < taskCount; index++ {
		taskID := workflow.TaskID(fmt.Sprintf("task-%02d", index))
		calls[taskID] = &atomic.Int32{}
		tasks = append(tasks, executionTask(string(taskID), "wide-barrier", ""))
	}

	registry := NewHandlerRegistry()
	if err := registry.Register("wide-barrier", TaskHandlerFunc(func(ctx context.Context, request TaskRequest) (string, error) {
		calls[request.Task.ID].Add(1)
		current := active.Add(1)
		defer active.Add(-1)
		updateMaximum(&maximum, current)
		started <- request.Task.ID
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-release:
			return string(request.Task.ID), nil
		}
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	engine := mustEngine(t, workerCount, registry)
	ctx, cancel := guardedContext(t)
	defer cancel()
	result := executeAsync(engine, ctx, "wide-run", executionWorkflow(tasks...))

	firstWave := make(map[workflow.TaskID]struct{}, workerCount)
	for index := 0; index < workerCount; index++ {
		taskID := receive(t, ctx, started, "ready task in first worker wave")
		if _, duplicate := firstWave[taskID]; duplicate {
			t.Fatalf("task %q started twice in first worker wave", taskID)
		}
		firstWave[taskID] = struct{}{}
	}
	if got := maximum.Load(); got != workerCount {
		t.Fatalf("maximum concurrent handlers = %d, want %d", got, workerCount)
	}
	close(release)

	executionResult := receive(t, ctx, result, "wide workflow result")
	if executionResult.err != nil {
		t.Fatalf("Execute() error = %v", executionResult.err)
	}
	if executionResult.run.Status() != WorkflowRunSucceeded {
		t.Fatalf("run status = %q, want succeeded", executionResult.run.Status())
	}
	if got := maximum.Load(); got > workerCount {
		t.Fatalf("maximum concurrent handlers = %d, exceeds worker count %d", got, workerCount)
	}
	for taskID, count := range calls {
		if got := count.Load(); got != 1 {
			t.Errorf("handler calls for %q = %d, want 1", taskID, got)
		}
		task := requireTaskRun(t, executionResult.run, taskID)
		if task.Status != TaskRunSucceeded || task.AttemptCount != 1 {
			t.Errorf("task %q = status %q attempts %d, want succeeded once", taskID, task.Status, task.AttemptCount)
		}
	}
}

func TestEngineIgnoresConcurrentDuplicateDelivery(t *testing.T) {
	transport := newDuplicateDeliveryBroker()
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int32
	registry := NewHandlerRegistry()
	if err := registry.Register("duplicate-probe", TaskHandlerFunc(func(ctx context.Context, _ TaskRequest) (string, error) {
		calls.Add(1)
		started <- struct{}{}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-release:
			return "completed once", nil
		}
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine, err := NewEngine(2, registry, newMemoryTestStore(), WithBroker(transport))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	ctx, cancel := guardedContext(t)
	defer cancel()
	result := executeAsync(
		engine,
		ctx,
		"duplicate-delivery-run",
		executionWorkflow(executionTask("task", "duplicate-probe", "")),
	)

	receive(t, ctx, started, "handler start for duplicated delivery")
	for index := 0; index < 4; index++ {
		receive(t, ctx, transport.messageReads, "worker and scheduler delivery inspection")
	}
	close(release)

	executionResult := receive(t, ctx, result, "duplicate-delivery workflow result")
	if executionResult.err != nil {
		t.Fatalf("Execute() error = %v", executionResult.err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want exactly 1", got)
	}
	for index := 0; index < 2; index++ {
		receive(t, ctx, transport.acknowledged, "acknowledgement for duplicate delivery")
	}
	task := requireTaskRun(t, executionResult.run, "task")
	if task.Status != TaskRunSucceeded || task.AttemptCount != 1 || task.Output != "completed once" {
		t.Fatalf("task after duplicate delivery = %#v", task)
	}
}

func TestEngineCancellationReleasesWorkerReceiveLoops(t *testing.T) {
	const workerCount = 4

	transport := newReceiveTrackingBroker()
	started := make(chan struct{})
	stopped := make(chan struct{})
	registry := NewHandlerRegistry()
	if err := registry.Register("cancel-probe", TaskHandlerFunc(func(ctx context.Context, _ TaskRequest) (string, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return "", ctx.Err()
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine, err := NewEngine(workerCount, registry, newMemoryTestStore(), WithBroker(transport))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	guard, stopGuard := guardedContext(t)
	defer stopGuard()
	ctx, cancel := context.WithCancel(guard)
	result := executeAsync(
		engine,
		ctx,
		"worker-lifecycle-run",
		executionWorkflow(executionTask("task", "cancel-probe", "")),
	)

	receive(t, guard, started, "handler start before cancellation")
	for index := 0; index < workerCount; index++ {
		receive(t, guard, transport.receiveStarted, "worker broker receive loop")
	}
	cancel()
	receive(t, guard, stopped, "handler exit after cancellation")
	executionResult := receive(t, guard, result, "canceled worker lifecycle result")
	if !errors.Is(executionResult.err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context.Canceled", executionResult.err)
	}
	if got := transport.activeReceives.Load(); got != 0 {
		t.Fatalf("active broker Receive calls after Execute returned = %d, want 0", got)
	}
	if started, finished := transport.receiveCalls.Load(), transport.receiveReturns.Load(); started != finished {
		t.Fatalf("broker Receive lifecycle = %d started, %d returned", started, finished)
	}
}

func updateMaximum(maximum *atomic.Int32, candidate int32) {
	for current := maximum.Load(); candidate > current; current = maximum.Load() {
		if maximum.CompareAndSwap(current, candidate) {
			return
		}
	}
}

type duplicateDeliveryBroker struct {
	publishOnce  sync.Once
	deliveries   chan broker.Delivery
	messageReads chan broker.MessageID
	acknowledged chan broker.MessageID
}

func newDuplicateDeliveryBroker() *duplicateDeliveryBroker {
	return &duplicateDeliveryBroker{
		deliveries:   make(chan broker.Delivery, 2),
		messageReads: make(chan broker.MessageID, 8),
		acknowledged: make(chan broker.MessageID, 2),
	}
}

func (taskBroker *duplicateDeliveryBroker) Publish(ctx context.Context, message broker.TaskMessage) error {
	if ctx == nil {
		return errors.New("publish duplicate test delivery: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := message.Validate(); err != nil {
		return err
	}
	published := false
	taskBroker.publishOnce.Do(func() {
		published = true
		for index := 0; index < 2; index++ {
			taskBroker.deliveries <- &injectedDelivery{
				message:      copyTaskMessage(message),
				messageReads: taskBroker.messageReads,
				acknowledged: taskBroker.acknowledged,
			}
		}
	})
	if !published {
		return errors.New("duplicate test broker received an unexpected second publish")
	}
	return nil
}

func (taskBroker *duplicateDeliveryBroker) Receive(
	ctx context.Context,
	subscription broker.Subscription,
) (broker.Delivery, error) {
	if ctx == nil {
		return nil, errors.New("receive duplicate test delivery: context is nil")
	}
	if err := subscription.Validate(); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case delivery := <-taskBroker.deliveries:
		return delivery, nil
	}
}

type injectedDelivery struct {
	message      broker.TaskMessage
	messageReads chan<- broker.MessageID
	acknowledged chan<- broker.MessageID
	settled      atomic.Bool
}

func (delivery *injectedDelivery) Message() broker.TaskMessage {
	delivery.messageReads <- delivery.message.ID
	return copyTaskMessage(delivery.message)
}

func (delivery *injectedDelivery) DeliveryCount() uint64 {
	return 1
}

func (delivery *injectedDelivery) Ack(ctx context.Context) error {
	if err := injectedDeliveryContextError(ctx); err != nil {
		return err
	}
	if !delivery.settled.CompareAndSwap(false, true) {
		return broker.ErrDeliverySettled
	}
	delivery.acknowledged <- delivery.message.ID
	return nil
}

func (delivery *injectedDelivery) Nack(ctx context.Context) error {
	if err := injectedDeliveryContextError(ctx); err != nil {
		return err
	}
	if !delivery.settled.CompareAndSwap(false, true) {
		return broker.ErrDeliverySettled
	}
	return nil
}

func (delivery *injectedDelivery) Progress(ctx context.Context) error {
	if err := injectedDeliveryContextError(ctx); err != nil {
		return err
	}
	if delivery.settled.Load() {
		return broker.ErrDeliverySettled
	}
	return nil
}

func injectedDeliveryContextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("injected delivery context is nil")
	}
	return ctx.Err()
}

func copyTaskMessage(message broker.TaskMessage) broker.TaskMessage {
	clone := message
	clone.Body = append([]byte(nil), message.Body...)
	if message.Headers != nil {
		clone.Headers = make(map[string]string, len(message.Headers))
		for name, value := range message.Headers {
			clone.Headers[name] = value
		}
	}
	return clone
}

type receiveTrackingBroker struct {
	delegate       *broker.InMemoryBroker
	receiveStarted chan struct{}
	activeReceives atomic.Int32
	receiveCalls   atomic.Int32
	receiveReturns atomic.Int32
}

func newReceiveTrackingBroker() *receiveTrackingBroker {
	return &receiveTrackingBroker{
		delegate:       broker.NewInMemoryBroker(),
		receiveStarted: make(chan struct{}, 16),
	}
}

func (taskBroker *receiveTrackingBroker) Publish(ctx context.Context, message broker.TaskMessage) error {
	return taskBroker.delegate.Publish(ctx, message)
}

func (taskBroker *receiveTrackingBroker) Receive(
	ctx context.Context,
	subscription broker.Subscription,
) (broker.Delivery, error) {
	taskBroker.receiveCalls.Add(1)
	taskBroker.activeReceives.Add(1)
	taskBroker.receiveStarted <- struct{}{}
	defer func() {
		taskBroker.activeReceives.Add(-1)
		taskBroker.receiveReturns.Add(1)
	}()
	return taskBroker.delegate.Receive(ctx, subscription)
}

var _ broker.Broker = (*duplicateDeliveryBroker)(nil)
var _ broker.Broker = (*receiveTrackingBroker)(nil)
