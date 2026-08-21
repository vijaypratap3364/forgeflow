package broker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDefaultNATSConfigIsValid(t *testing.T) {
	if err := DefaultNATSConfig().Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestNATSConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*NATSConfig)
		field  string
	}{
		{name: "empty URL", mutate: func(c *NATSConfig) { c.URL = " " }, field: "url"},
		{name: "empty stream", mutate: func(c *NATSConfig) { c.StreamName = "" }, field: "stream_name"},
		{name: "invalid stream", mutate: func(c *NATSConfig) { c.StreamName = "forge.flow" }, field: "stream_name"},
		{name: "stream wildcard", mutate: func(c *NATSConfig) { c.StreamName = "FORGEFLOW>" }, field: "stream_name"},
		{name: "empty prefix", mutate: func(c *NATSConfig) { c.SubjectPrefix = "" }, field: "subject_prefix"},
		{name: "empty prefix token", mutate: func(c *NATSConfig) { c.SubjectPrefix = "forgeflow..tasks" }, field: "subject_prefix"},
		{name: "wildcard prefix", mutate: func(c *NATSConfig) { c.SubjectPrefix = "forgeflow.*" }, field: "subject_prefix"},
		{name: "partial wildcard prefix", mutate: func(c *NATSConfig) { c.SubjectPrefix = "forgeflow.task*" }, field: "subject_prefix"},
		{name: "zero ack wait", mutate: func(c *NATSConfig) { c.AckWait = 0 }, field: "ack_wait"},
		{name: "zero duplicate window", mutate: func(c *NATSConfig) { c.DuplicateWindow = 0 }, field: "duplicate_window"},
		{name: "zero max age", mutate: func(c *NATSConfig) { c.MaxAge = 0 }, field: "max_age"},
		{name: "invalid max deliver", mutate: func(c *NATSConfig) { c.MaxDeliver = -2 }, field: "max_deliver"},
		{name: "zero connect timeout", mutate: func(c *NATSConfig) { c.ConnectTimeout = 0 }, field: "connect_timeout"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := DefaultNATSConfig()
			test.mutate(&config)
			err := config.Validate()
			var validationErr *ValidationError
			if !errors.As(err, &validationErr) {
				t.Fatalf("Validate() error = %v, want ValidationError", err)
			}
			if validationErr.Field != test.field {
				t.Fatalf("ValidationError.Field = %q, want %q", validationErr.Field, test.field)
			}
		})
	}
}

func TestOpenNATSBrokerRejectsNilOrCancelledContextBeforeConnecting(t *testing.T) {
	if _, err := OpenNATSBroker(nil, DefaultNATSConfig()); err == nil {
		t.Fatal("OpenNATSBroker(nil) error = nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	_, err := OpenNATSBroker(ctx, DefaultNATSConfig())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenNATSBroker(cancelled) error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("OpenNATSBroker(cancelled) took %s; it should not attempt a connection", elapsed)
	}
}

func TestHashedNameIsStableAndNATSSafe(t *testing.T) {
	first := hashedName("FF", "consumer with unsafe.* characters")
	second := hashedName("FF", "consumer with unsafe.* characters")
	if first != second {
		t.Fatalf("hashedName() = %q then %q", first, second)
	}
	if len(first) != 67 {
		t.Fatalf("len(hashedName()) = %d, want 67", len(first))
	}
}

func TestNATSControlHeadersAreNotApplicationMetadata(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{name: "Nats-Msg-Id", want: true},
		{name: "Nats-Expected-Stream", want: true},
		{name: "nats-custom-control", want: true},
		{name: "traceparent", want: false},
		{name: "tracestate", want: false},
		{name: "baggage", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isNATSControlHeader(test.name); got != test.want {
				t.Fatalf("isNATSControlHeader(%q) = %t, want %t", test.name, got, test.want)
			}
		})
	}
}
