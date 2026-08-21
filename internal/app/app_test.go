package app

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("FORGEFLOW_ADDR", "127.0.0.1:9090")
	t.Setenv("FORGEFLOW_STORE", "postgres")
	t.Setenv("FORGEFLOW_DATA_PATH", "custom.ffdb")
	t.Setenv("FORGEFLOW_POSTGRES_DSN", "postgres://forgeflow.example/forgeflow")
	t.Setenv("FORGEFLOW_BROKER", "nats")
	t.Setenv("FORGEFLOW_NATS_URL", "nats://forgeflow.example:4222")
	t.Setenv("FORGEFLOW_NATS_STREAM", "FORGEFLOW_TEST_TASKS")
	t.Setenv("FORGEFLOW_NATS_SUBJECT_PREFIX", "forgeflow.test.tasks")
	t.Setenv("FORGEFLOW_WORKERS", "2")
	t.Setenv("FORGEFLOW_SHUTDOWN_TIMEOUT", "3s")

	config, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if config.Address != "127.0.0.1:9090" || config.StoreBackend != "postgres" ||
		config.DataPath != "custom.ffdb" || config.PostgresDSN != "postgres://forgeflow.example/forgeflow" ||
		config.BrokerBackend != "nats" || config.NATSURL != "nats://forgeflow.example:4222" ||
		config.NATSStreamName != "FORGEFLOW_TEST_TASKS" || config.NATSSubjectPrefix != "forgeflow.test.tasks" ||
		config.WorkerCount != 2 || config.ShutdownTimeout != 3*time.Second {
		t.Fatalf("ConfigFromEnv() = %#v", config)
	}
}

func TestConfigValidateStoreRequirements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{
			name: "unknown backend",
			mutate: func(config *Config) {
				config.StoreBackend = "sqlite"
			},
		},
		{
			name: "file path missing",
			mutate: func(config *Config) {
				config.DataPath = ""
			},
		},
		{
			name: "PostgreSQL DSN missing",
			mutate: func(config *Config) {
				config.StoreBackend = "postgres"
			},
		},
		{
			name: "unknown broker",
			mutate: func(config *Config) {
				config.BrokerBackend = "kafka"
			},
		},
		{
			name: "invalid NATS stream",
			mutate: func(config *Config) {
				config.BrokerBackend = "nats"
				config.NATSStreamName = "forgeflow.tasks"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := DefaultConfig()
			test.mutate(&config)
			if err := config.Validate(); err == nil {
				t.Fatal("Config.Validate() error = nil")
			}
		})
	}
}

func TestConfigFromEnvRejectsInvalidWorkerCount(t *testing.T) {
	t.Setenv("FORGEFLOW_WORKERS", "many")

	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("ConfigFromEnv() error = nil, want invalid worker count")
	}
}

func TestRunStartsAndGracefullyStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	output := newSignalWriter()
	result := make(chan error, 1)
	config := DefaultConfig()
	config.Address = "127.0.0.1:0"
	config.DataPath = t.TempDir() + "/forgeflow.ffdb"
	config.ShutdownTimeout = 3 * time.Second

	go func() {
		result <- Run(ctx, config, output)
	}()
	waitForChannel(t, output.written, "server startup")
	if got := output.String(); !strings.HasPrefix(got, "ForgeFlow listening on http://127.0.0.1:") {
		t.Fatalf("Run() output = %q", got)
	}
	cancel()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-timer.C:
		t.Fatal("Run() did not stop after context cancellation")
	}
}

type signalWriter struct {
	mu      sync.Mutex
	buffer  bytes.Buffer
	written chan struct{}
	once    sync.Once
}

func newSignalWriter() *signalWriter {
	return &signalWriter{written: make(chan struct{})}
}

func (writer *signalWriter) Write(data []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	count, err := writer.buffer.Write(data)
	writer.once.Do(func() { close(writer.written) })
	return count, err
}

func (writer *signalWriter) String() string {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.buffer.String()
}

func waitForChannel(t *testing.T, channel <-chan struct{}, description string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-channel:
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
	}
}
