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
	t.Setenv("FORGEFLOW_DATA_PATH", "custom.ffdb")
	t.Setenv("FORGEFLOW_WORKERS", "2")
	t.Setenv("FORGEFLOW_SHUTDOWN_TIMEOUT", "3s")

	config, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if config.Address != "127.0.0.1:9090" || config.DataPath != "custom.ffdb" ||
		config.WorkerCount != 2 || config.ShutdownTimeout != 3*time.Second {
		t.Fatalf("ConfigFromEnv() = %#v", config)
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
