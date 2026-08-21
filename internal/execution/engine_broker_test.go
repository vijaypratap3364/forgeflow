package execution

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/vijaypratap3364/forgeflow/internal/broker"
)

func TestEngineAcknowledgesTaskOnlyAfterCompletionIsDurable(t *testing.T) {
	t.Parallel()

	ctx, cancel := guardedContext(t)
	defer cancel()
	baseStore := newMemoryTestStore()
	store := &completionBlockingStore{
		Store:   baseStore,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	transport := newObservedTaskBroker()
	registry, err := NewDemoHandlerRegistry()
	if err != nil {
		t.Fatalf("NewDemoHandlerRegistry() error = %v", err)
	}
	engine, err := NewEngine(1, registry, store, WithBroker(transport))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}

	result := executeAsync(
		engine,
		ctx,
		"broker-order-run",
		executionWorkflow(executionTask("task", NoopHandlerName, "")),
	)
	published := receive(t, ctx, transport.published, "published task message")
	expectedAttemptID := AttemptIDFor(TaskRunIDFor("broker-order-run", "task"), 1)
	if published.ID != broker.MessageID(expectedAttemptID) {
		t.Fatalf("published message ID = %q, want %q", published.ID, expectedAttemptID)
	}
	receive(t, ctx, store.entered, "completion persistence boundary")
	select {
	case messageID := <-transport.acked:
		t.Fatalf("message %q was acknowledged before completion persisted", messageID)
	default:
	}
	close(store.release)

	executionResult := receive(t, ctx, result, "broker-backed execution")
	if executionResult.err != nil {
		t.Fatalf("Engine.Execute() error = %v", executionResult.err)
	}
	if executionResult.run.Status() != WorkflowRunSucceeded {
		t.Fatalf("run status = %q, want succeeded", executionResult.run.Status())
	}
	if messageID := receive(t, ctx, transport.acked, "task acknowledgement"); messageID != published.ID {
		t.Fatalf("acknowledged message ID = %q, want %q", messageID, published.ID)
	}
}

func TestEngineLeavesReadyTaskRecoverableWhenPublishFails(t *testing.T) {
	t.Parallel()

	transportFailure := errors.New("transport unavailable")
	registry, err := NewDemoHandlerRegistry()
	if err != nil {
		t.Fatalf("NewDemoHandlerRegistry() error = %v", err)
	}
	store := newMemoryTestStore()
	engine, err := NewEngine(1, registry, store, WithBroker(&publishFailingBroker{err: transportFailure}))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}

	run, err := engine.Execute(
		context.Background(),
		"broker-failure-run",
		executionWorkflow(executionTask("task", NoopHandlerName, "")),
	)
	var brokerError *BrokerOperationError
	if !errors.As(err, &brokerError) || !errors.Is(err, transportFailure) {
		t.Fatalf("Engine.Execute() error = %T %v, want BrokerOperationError wrapping transport failure", err, err)
	}
	if run.Status() != WorkflowRunRunning {
		t.Fatalf("run status = %q, want running for later recovery", run.Status())
	}
	task := requireTaskRun(t, run, "task")
	if task.Status != TaskRunReady || task.AttemptCount != 0 || task.Lease != nil {
		t.Fatalf("task after publish failure = %#v, want unleased ready task", task)
	}
	persisted, found, loadErr := store.LoadRun(context.Background(), run.ID())
	if loadErr != nil || !found || persisted.Status != WorkflowRunRunning || persisted.Tasks[0].Status != TaskRunReady {
		t.Fatalf("persisted run after publish failure = %#v, found %t, error %v", persisted, found, loadErr)
	}
}

type completionBlockingStore struct {
	Store
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (store *completionBlockingStore) SaveRun(
	ctx context.Context,
	snapshot WorkflowRunSnapshot,
) (WorkflowRunSnapshot, error) {
	for _, task := range snapshot.Tasks {
		if task.Status != TaskRunSucceeded || snapshot.Status != WorkflowRunRunning {
			continue
		}
		store.once.Do(func() { close(store.entered) })
		select {
		case <-store.release:
		case <-ctx.Done():
			return WorkflowRunSnapshot{}, ctx.Err()
		}
		break
	}
	return store.Store.SaveRun(ctx, snapshot)
}

type observedTaskBroker struct {
	delegate  *broker.InMemoryBroker
	published chan broker.TaskMessage
	acked     chan broker.MessageID
}

func newObservedTaskBroker() *observedTaskBroker {
	return &observedTaskBroker{
		delegate:  broker.NewInMemoryBroker(),
		published: make(chan broker.TaskMessage, 4),
		acked:     make(chan broker.MessageID, 4),
	}
}

func (taskBroker *observedTaskBroker) Publish(ctx context.Context, message broker.TaskMessage) error {
	if err := taskBroker.delegate.Publish(ctx, message); err != nil {
		return err
	}
	taskBroker.published <- message
	return nil
}

func (taskBroker *observedTaskBroker) Receive(
	ctx context.Context,
	subscription broker.Subscription,
) (broker.Delivery, error) {
	delivery, err := taskBroker.delegate.Receive(ctx, subscription)
	if err != nil {
		return nil, err
	}
	return &observedDelivery{Delivery: delivery, acked: taskBroker.acked}, nil
}

type observedDelivery struct {
	broker.Delivery
	acked chan<- broker.MessageID
}

func (delivery *observedDelivery) Ack(ctx context.Context) error {
	if err := delivery.Delivery.Ack(ctx); err != nil {
		return err
	}
	delivery.acked <- delivery.Message().ID
	return nil
}

type publishFailingBroker struct {
	err error
}

func (taskBroker *publishFailingBroker) Publish(context.Context, broker.TaskMessage) error {
	return taskBroker.err
}

func (taskBroker *publishFailingBroker) Receive(
	ctx context.Context,
	_ broker.Subscription,
) (broker.Delivery, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

var _ broker.Broker = (*observedTaskBroker)(nil)
var _ broker.Broker = (*publishFailingBroker)(nil)
