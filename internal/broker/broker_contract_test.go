package broker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

var contractSequence atomic.Uint64

func runBrokerContract(t *testing.T, taskBroker Broker) {
	t.Helper()

	t.Run("publish receive and acknowledge", func(t *testing.T) {
		message, subscription := contractMessage("ack")
		if err := taskBroker.Publish(context.Background(), message); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		delivery, err := taskBroker.Receive(context.Background(), subscription)
		if err != nil {
			t.Fatalf("Receive() error = %v", err)
		}
		if got := delivery.Message(); !reflect.DeepEqual(got, message) {
			t.Fatalf("Delivery.Message() = %#v, want %#v", got, message)
		}
		if delivery.DeliveryCount() != 1 {
			t.Fatalf("DeliveryCount() = %d, want 1", delivery.DeliveryCount())
		}
		if err := delivery.Progress(context.Background()); err != nil {
			t.Fatalf("Progress() error = %v", err)
		}
		if err := delivery.Ack(context.Background()); err != nil {
			t.Fatalf("Ack() error = %v", err)
		}
		assertNoBrokerDelivery(t, taskBroker, subscription)
	})

	t.Run("negative acknowledgement redelivers", func(t *testing.T) {
		message, subscription := contractMessage("nack")
		if err := taskBroker.Publish(context.Background(), message); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		first, err := taskBroker.Receive(context.Background(), subscription)
		if err != nil {
			t.Fatalf("first Receive() error = %v", err)
		}
		if err := first.Nack(context.Background()); err != nil {
			t.Fatalf("Nack() error = %v", err)
		}
		second, err := taskBroker.Receive(context.Background(), subscription)
		if err != nil {
			t.Fatalf("second Receive() error = %v", err)
		}
		if second.Message().ID != message.ID || second.DeliveryCount() < 2 {
			t.Fatalf("redelivery = %#v count %d", second.Message(), second.DeliveryCount())
		}
		if err := second.Ack(context.Background()); err != nil {
			t.Fatalf("redelivery Ack() error = %v", err)
		}
	})

	t.Run("stable publish is idempotent", func(t *testing.T) {
		message, subscription := contractMessage("duplicate")
		if err := taskBroker.Publish(context.Background(), message); err != nil {
			t.Fatalf("first Publish() error = %v", err)
		}
		if err := taskBroker.Publish(context.Background(), message); err != nil {
			t.Fatalf("duplicate Publish() error = %v", err)
		}
		delivery, err := taskBroker.Receive(context.Background(), subscription)
		if err != nil {
			t.Fatalf("Receive() error = %v", err)
		}
		if err := delivery.Ack(context.Background()); err != nil {
			t.Fatalf("Ack() error = %v", err)
		}
		assertNoBrokerDelivery(t, taskBroker, subscription)
	})

	t.Run("competing receivers claim once", func(t *testing.T) {
		message, subscription := contractMessage("competing")
		if err := taskBroker.Publish(context.Background(), message); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		type receiveResult struct {
			delivery Delivery
			err      error
		}
		results := make(chan receiveResult, 2)
		for index := 0; index < 2; index++ {
			go func() {
				delivery, err := taskBroker.Receive(ctx, subscription)
				results <- receiveResult{delivery: delivery, err: err}
			}()
		}
		first := <-results
		if first.err != nil {
			t.Fatalf("first competing Receive() error = %v", first.err)
		}
		cancel()
		second := <-results
		if !errors.Is(second.err, context.Canceled) {
			t.Fatalf("second competing Receive() error = %v, want context.Canceled", second.err)
		}
		if err := first.delivery.Ack(context.Background()); err != nil {
			t.Fatalf("competing delivery Ack() error = %v", err)
		}
	})
}

func contractMessage(label string) (TaskMessage, Subscription) {
	sequence := contractSequence.Add(1)
	topic := Topic(fmt.Sprintf("contract-%s-%d", label, sequence))
	return TaskMessage{
			ID:    MessageID(fmt.Sprintf("message-%s-%d", label, sequence)),
			Topic: topic,
			Body:  []byte("payload-" + label),
		}, Subscription{
			ConsumerID: ConsumerID(fmt.Sprintf("consumer-%s-%d", label, sequence)),
			Topic:      topic,
		}
}

func assertNoBrokerDelivery(t *testing.T, taskBroker Broker, subscription Subscription) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if delivery, err := taskBroker.Receive(ctx, subscription); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Receive() after ack = %#v, error %v; want context deadline", delivery, err)
	}
}
