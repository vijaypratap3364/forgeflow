package app

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/broker"
	"github.com/vijaypratap3364/forgeflow/internal/persistence"
)

func TestConfigFromEnv(t *testing.T) {
	publicKey := testPublicKeyPEM(t)
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
	t.Setenv("FORGEFLOW_JWT_PUBLIC_KEY", publicKey)
	t.Setenv("FORGEFLOW_JWT_ISSUER", "https://issuer.example")
	t.Setenv("FORGEFLOW_JWT_AUDIENCE", "forgeflow-test")
	t.Setenv("FORGEFLOW_JWT_LEEWAY", "5s")
	t.Setenv("FORGEFLOW_RATE_LIMIT", "25")
	t.Setenv("FORGEFLOW_RATE_LIMIT_WINDOW", "30s")
	t.Setenv("FORGEFLOW_LOG_LEVEL", "debug")
	t.Setenv("FORGEFLOW_TRACE_EXPORTER", "stdout")
	t.Setenv("FORGEFLOW_SERVICE_NAME", "forgeflow-test")

	config, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if config.Address != "127.0.0.1:9090" || config.StoreBackend != "postgres" ||
		config.DataPath != "custom.ffdb" || config.PostgresDSN != "postgres://forgeflow.example/forgeflow" ||
		config.BrokerBackend != "nats" || config.NATSURL != "nats://forgeflow.example:4222" ||
		config.NATSStreamName != "FORGEFLOW_TEST_TASKS" || config.NATSSubjectPrefix != "forgeflow.test.tasks" ||
		config.WorkerCount != 2 || config.ShutdownTimeout != 3*time.Second || config.JWTPublicKeyPEM != strings.TrimSpace(publicKey) ||
		config.JWTIssuer != "https://issuer.example" || config.JWTAudience != "forgeflow-test" ||
		config.JWTLeeway != 5*time.Second || config.RateLimit != 25 || config.RateLimitWindow != 30*time.Second ||
		config.LogLevel != "debug" || config.TraceExporter != "stdout" || config.ServiceName != "forgeflow-test" {
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
		{
			name: "JWT public key missing",
			mutate: func(config *Config) {
				config.JWTPublicKeyPEM = ""
			},
		},
		{
			name: "invalid log level",
			mutate: func(config *Config) {
				config.LogLevel = "verbose"
			},
		},
		{
			name: "invalid trace exporter",
			mutate: func(config *Config) {
				config.TraceExporter = "jaeger"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := DefaultConfig()
			config.JWTPublicKeyPEM = testPublicKeyPEM(t)
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

func TestRedactSensitiveErrorRemovesConfiguredSecrets(t *testing.T) {
	t.Parallel()

	err := errors.New("connect postgres://user:password@example.test/db with authorization=Bearer secret-token")
	redacted := redactSensitiveError(
		err,
		"postgres://user:password@example.test/db",
		"authorization=Bearer secret-token",
	)
	if strings.Contains(redacted.Error(), "password") || strings.Contains(redacted.Error(), "secret-token") {
		t.Fatalf("redactSensitiveError() = %q", redacted)
	}
	if !strings.Contains(redacted.Error(), "[REDACTED]") {
		t.Fatalf("redactSensitiveError() = %q, want redaction marker", redacted)
	}
}

func TestDependencyReadinessChecksConfiguredBroker(t *testing.T) {
	t.Parallel()

	store, err := persistence.OpenFileStore(t.TempDir() + "/forgeflow.ffdb")
	if err != nil {
		t.Fatalf("OpenFileStore() error = %v", err)
	}
	taskBroker := broker.NewInMemoryBroker()
	check := dependencyReadiness(store, taskBroker)
	if err := check(context.Background()); err != nil {
		t.Fatalf("dependencyReadiness() error = %v", err)
	}
	if err := taskBroker.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := check(context.Background()); !errors.Is(err, broker.ErrClosed) {
		t.Fatalf("dependencyReadiness() error = %v, want ErrClosed", err)
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
	config.JWTPublicKeyPEM = testPublicKeyPEM(t)

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

func testPublicKeyPEM(t *testing.T) string {
	t.Helper()
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	encoded, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey() error = %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded}))
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
