package broker

import (
	"context"
	"errors"
	"testing"
)

func TestInMemoryBrokerContract(t *testing.T) {
	t.Parallel()

	runBrokerContract(t, NewInMemoryBroker())
}

func TestInMemoryBrokerRejectsConflictingStableMessage(t *testing.T) {
	t.Parallel()

	taskBroker := NewInMemoryBroker()
	message := TaskMessage{ID: "attempt-1", Topic: "run-1", Body: []byte("first")}
	if err := taskBroker.Publish(context.Background(), message); err != nil {
		t.Fatalf("first Publish() error = %v", err)
	}
	message.Body = []byte("changed")
	var conflict *MessageConflictError
	if err := taskBroker.Publish(context.Background(), message); !errors.As(err, &conflict) {
		t.Fatalf("conflicting Publish() error = %T %v, want *MessageConflictError", err, err)
	}
}

func TestInMemoryBrokerDefensivelyCopiesMessages(t *testing.T) {
	t.Parallel()

	taskBroker := NewInMemoryBroker()
	message := TaskMessage{
		ID: "attempt-1", Topic: "run-1", Body: []byte("original"),
		Headers: map[string]string{"traceparent": "original-trace"},
	}
	if err := taskBroker.Publish(context.Background(), message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	message.Body[0] = 'X'
	message.Headers["traceparent"] = "mutated-before-receive"
	delivery, err := taskBroker.Receive(context.Background(), Subscription{ConsumerID: "worker", Topic: "run-1"})
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	received := delivery.Message()
	received.Body[0] = 'Y'
	received.Headers["traceparent"] = "mutated-after-receive"
	if got := string(delivery.Message().Body); got != "original" {
		t.Fatalf("Delivery.Message().Body = %q, want original", got)
	}
	if got := delivery.Message().Headers["traceparent"]; got != "original-trace" {
		t.Fatalf("Delivery.Message().Headers[traceparent] = %q, want original-trace", got)
	}
}

func TestTaskMessageRejectsUnsafePropagationHeaders(t *testing.T) {
	t.Parallel()

	tests := []TaskMessage{
		{ID: "attempt", Topic: "run", Body: []byte("body"), Headers: map[string]string{"Traceparent": "value"}},
		{ID: "attempt", Topic: "run", Body: []byte("body"), Headers: map[string]string{"traceparent": "value\r\ninjected: true"}},
	}
	for _, message := range tests {
		var validationError *ValidationError
		if err := message.Validate(); !errors.As(err, &validationError) {
			t.Fatalf("TaskMessage.Validate() error = %T %v, want ValidationError", err, err)
		}
	}
}

func TestInMemoryBrokerCloseReleasesReceivers(t *testing.T) {
	t.Parallel()

	taskBroker := NewInMemoryBroker()
	result := make(chan error, 1)
	go func() {
		_, err := taskBroker.Receive(context.Background(), Subscription{ConsumerID: "worker", Topic: "run-1"})
		result <- err
	}()
	if err := taskBroker.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("Receive() error = %v, want ErrClosed", err)
	}
}

func TestInMemoryBrokerReadinessTracksLifecycle(t *testing.T) {
	t.Parallel()

	taskBroker := NewInMemoryBroker()
	if err := taskBroker.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	if err := taskBroker.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := taskBroker.Ping(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Ping() after Close() error = %v, want ErrClosed", err)
	}
}
