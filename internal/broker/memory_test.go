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
	message := TaskMessage{ID: "attempt-1", Topic: "run-1", Body: []byte("original")}
	if err := taskBroker.Publish(context.Background(), message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	message.Body[0] = 'X'
	delivery, err := taskBroker.Receive(context.Background(), Subscription{ConsumerID: "worker", Topic: "run-1"})
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	received := delivery.Message()
	received.Body[0] = 'Y'
	if got := string(delivery.Message().Body); got != "original" {
		t.Fatalf("Delivery.Message().Body = %q, want original", got)
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
