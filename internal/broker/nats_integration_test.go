//go:build integration

package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

const natsTestURLEnvironment = "FORGEFLOW_NATS_TEST_URL"

var natsTestSequence atomic.Uint64

func TestNATSBrokerContract(t *testing.T) {
	taskBroker := openTestNATSBroker(t, time.Second)
	runBrokerContract(t, taskBroker)
}

func TestNATSBrokerRedeliversAfterAcknowledgementDeadline(t *testing.T) {
	ackWait := 75 * time.Millisecond
	taskBroker := openTestNATSBroker(t, ackWait)
	message, subscription := contractMessage("ack-timeout")

	if err := taskBroker.Publish(context.Background(), message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	first, err := taskBroker.Receive(context.Background(), subscription)
	if err != nil {
		t.Fatalf("first Receive() error = %v", err)
	}
	if first.DeliveryCount() != 1 {
		t.Fatalf("first DeliveryCount() = %d, want 1", first.DeliveryCount())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	second, err := taskBroker.Receive(ctx, subscription)
	if err != nil {
		t.Fatalf("redelivery Receive() error = %v", err)
	}
	if second.Message().ID != message.ID {
		t.Fatalf("redelivery ID = %q, want %q", second.Message().ID, message.ID)
	}
	if second.DeliveryCount() < 2 {
		t.Fatalf("redelivery DeliveryCount() = %d, want at least 2", second.DeliveryCount())
	}
	if err := second.Ack(context.Background()); err != nil {
		t.Fatalf("redelivery Ack() error = %v", err)
	}
}

func TestNATSBrokerPreservesUnacknowledgedMessageAcrossReconnect(t *testing.T) {
	config := testNATSConfig(t, time.Second)
	first, err := OpenNATSBroker(context.Background(), config)
	if err != nil {
		t.Fatalf("first OpenNATSBroker() error = %v", err)
	}

	message, subscription := contractMessage("reconnect")
	if err := first.Publish(context.Background(), message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	second, err := OpenNATSBroker(context.Background(), config)
	if err != nil {
		t.Fatalf("second OpenNATSBroker() error = %v", err)
	}
	t.Cleanup(func() {
		deleteCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := second.jetStream.DeleteStream(deleteCtx, config.StreamName); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("DeleteStream() error = %v", err)
		}
		if err := second.Close(); err != nil {
			t.Errorf("second Close() error = %v", err)
		}
	})
	delivery, err := second.Receive(context.Background(), subscription)
	if err != nil {
		t.Fatalf("Receive() after reconnect error = %v", err)
	}
	if delivery.Message().ID != message.ID {
		t.Fatalf("delivery ID = %q, want %q", delivery.Message().ID, message.ID)
	}
	if err := delivery.Ack(context.Background()); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
}

func openTestNATSBroker(t *testing.T, ackWait time.Duration) *NATSBroker {
	t.Helper()
	config := testNATSConfig(t, ackWait)
	taskBroker, err := OpenNATSBroker(context.Background(), config)
	if err != nil {
		t.Fatalf("OpenNATSBroker() error = %v", err)
	}
	t.Cleanup(func() {
		deleteCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := taskBroker.jetStream.DeleteStream(deleteCtx, config.StreamName); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("DeleteStream() error = %v", err)
		}
		if err := taskBroker.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return taskBroker
}

func testNATSConfig(t *testing.T, ackWait time.Duration) NATSConfig {
	t.Helper()
	url := os.Getenv(natsTestURLEnvironment)
	if url == "" {
		t.Skipf("set %s to run NATS JetStream integration tests", natsTestURLEnvironment)
	}
	sequence := natsTestSequence.Add(1)
	suffix := fmt.Sprintf("%d_%d", os.Getpid(), sequence)
	config := DefaultNATSConfig()
	config.URL = url
	config.StreamName = "FF_TEST_" + suffix
	config.SubjectPrefix = "forgeflow.test." + suffix
	config.AckWait = ackWait
	config.MaxAge = 10 * time.Minute
	return config
}
