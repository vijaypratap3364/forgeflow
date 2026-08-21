// Package app composes the ForgeFlow HTTP process from lightweight components.
package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/api"
	"github.com/vijaypratap3364/forgeflow/internal/broker"
	"github.com/vijaypratap3364/forgeflow/internal/execution"
	"github.com/vijaypratap3364/forgeflow/internal/observability"
	"github.com/vijaypratap3364/forgeflow/internal/persistence"
	"github.com/vijaypratap3364/forgeflow/internal/security"
)

const (
	defaultAddress         = "127.0.0.1:8080"
	defaultDataPath        = "data/forgeflow.ffdb"
	defaultStoreBackend    = "file"
	defaultBrokerBackend   = "memory"
	defaultWorkerCount     = 4
	defaultShutdownTimeout = 10 * time.Second
	defaultJWTIssuer       = "forgeflow"
	defaultJWTAudience     = "forgeflow-api"
	defaultJWTLeeway       = 30 * time.Second
	defaultRateLimit       = 120
	defaultRateWindow      = time.Minute
	defaultLogLevel        = "info"
	defaultTraceExporter   = observability.TraceExporterNone
	defaultServiceName     = "forgeflow"
)

// Config contains process settings for the ForgeFlow API server. PostgresDSN
// and NATSURL may contain credentials and must not be logged.
type Config struct {
	Address           string
	StoreBackend      string
	DataPath          string
	PostgresDSN       string
	BrokerBackend     string
	NATSURL           string
	NATSStreamName    string
	NATSSubjectPrefix string
	WorkerCount       int
	ShutdownTimeout   time.Duration
	JWTPublicKeyPEM   string
	JWTIssuer         string
	JWTAudience       string
	JWTLeeway         time.Duration
	RateLimit         int
	RateLimitWindow   time.Duration
	LogLevel          string
	TraceExporter     string
	ServiceName       string
}

// DefaultConfig returns laptop-friendly local process defaults.
func DefaultConfig() Config {
	natsConfig := broker.DefaultNATSConfig()
	return Config{
		Address:           defaultAddress,
		StoreBackend:      defaultStoreBackend,
		DataPath:          defaultDataPath,
		BrokerBackend:     defaultBrokerBackend,
		NATSURL:           natsConfig.URL,
		NATSStreamName:    natsConfig.StreamName,
		NATSSubjectPrefix: natsConfig.SubjectPrefix,
		WorkerCount:       defaultWorkerCount,
		ShutdownTimeout:   defaultShutdownTimeout,
		JWTIssuer:         defaultJWTIssuer,
		JWTAudience:       defaultJWTAudience,
		JWTLeeway:         defaultJWTLeeway,
		RateLimit:         defaultRateLimit,
		RateLimitWindow:   defaultRateWindow,
		LogLevel:          defaultLogLevel,
		TraceExporter:     defaultTraceExporter,
		ServiceName:       defaultServiceName,
	}
}

// ConfigFromEnv reads optional FORGEFLOW_* settings from the process environment.
func ConfigFromEnv() (Config, error) {
	config := DefaultConfig()
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_ADDR")); value != "" {
		config.Address = value
	}
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_DATA_PATH")); value != "" {
		config.DataPath = value
	}
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_STORE")); value != "" {
		config.StoreBackend = strings.ToLower(value)
	}
	config.PostgresDSN = strings.TrimSpace(os.Getenv("FORGEFLOW_POSTGRES_DSN"))
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_BROKER")); value != "" {
		config.BrokerBackend = strings.ToLower(value)
	}
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_NATS_URL")); value != "" {
		config.NATSURL = value
	}
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_NATS_STREAM")); value != "" {
		config.NATSStreamName = value
	}
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_NATS_SUBJECT_PREFIX")); value != "" {
		config.NATSSubjectPrefix = value
	}
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_WORKERS")); value != "" {
		workerCount, err := strconv.Atoi(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse FORGEFLOW_WORKERS: %w", err)
		}
		config.WorkerCount = workerCount
	}
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_SHUTDOWN_TIMEOUT")); value != "" {
		shutdownTimeout, err := time.ParseDuration(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse FORGEFLOW_SHUTDOWN_TIMEOUT: %w", err)
		}
		config.ShutdownTimeout = shutdownTimeout
	}
	config.JWTPublicKeyPEM = strings.TrimSpace(os.Getenv("FORGEFLOW_JWT_PUBLIC_KEY"))
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_JWT_ISSUER")); value != "" {
		config.JWTIssuer = value
	}
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_JWT_AUDIENCE")); value != "" {
		config.JWTAudience = value
	}
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_JWT_LEEWAY")); value != "" {
		leeway, err := time.ParseDuration(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse FORGEFLOW_JWT_LEEWAY: %w", err)
		}
		config.JWTLeeway = leeway
	}
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_RATE_LIMIT")); value != "" {
		limit, err := strconv.Atoi(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse FORGEFLOW_RATE_LIMIT: %w", err)
		}
		config.RateLimit = limit
	}
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_RATE_LIMIT_WINDOW")); value != "" {
		window, err := time.ParseDuration(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse FORGEFLOW_RATE_LIMIT_WINDOW: %w", err)
		}
		config.RateLimitWindow = window
	}
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_LOG_LEVEL")); value != "" {
		config.LogLevel = strings.ToLower(value)
	}
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_TRACE_EXPORTER")); value != "" {
		config.TraceExporter = strings.ToLower(value)
	}
	if value := strings.TrimSpace(os.Getenv("FORGEFLOW_SERVICE_NAME")); value != "" {
		config.ServiceName = value
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// Validate checks process configuration before any resources are opened.
func (config Config) Validate() error {
	if strings.TrimSpace(config.Address) == "" {
		return errors.New("ForgeFlow address must not be empty")
	}
	switch config.StoreBackend {
	case "file":
		if strings.TrimSpace(config.DataPath) == "" {
			return errors.New("ForgeFlow data path must not be empty for the file store")
		}
	case "postgres":
		if strings.TrimSpace(config.PostgresDSN) == "" {
			return errors.New("ForgeFlow PostgreSQL data source name must not be empty for the postgres store")
		}
	default:
		return fmt.Errorf("ForgeFlow store %q is unsupported: use file or postgres", config.StoreBackend)
	}
	switch config.BrokerBackend {
	case "memory":
	case "nats":
		natsConfig := broker.DefaultNATSConfig()
		natsConfig.URL = config.NATSURL
		natsConfig.StreamName = config.NATSStreamName
		natsConfig.SubjectPrefix = config.NATSSubjectPrefix
		if err := natsConfig.Validate(); err != nil {
			return fmt.Errorf("ForgeFlow NATS configuration: %w", err)
		}
	default:
		return fmt.Errorf("ForgeFlow broker %q is unsupported: use memory or nats", config.BrokerBackend)
	}
	if config.WorkerCount < 1 {
		return errors.New("ForgeFlow worker count must be at least one")
	}
	if config.ShutdownTimeout <= 0 {
		return errors.New("ForgeFlow shutdown timeout must be positive")
	}
	if strings.TrimSpace(config.JWTPublicKeyPEM) == "" || strings.TrimSpace(config.JWTIssuer) == "" ||
		strings.TrimSpace(config.JWTAudience) == "" {
		return errors.New("ForgeFlow JWT public key, issuer, and audience must not be empty")
	}
	if config.JWTLeeway < 0 {
		return errors.New("ForgeFlow JWT leeway must not be negative")
	}
	if config.RateLimit < 1 || config.RateLimitWindow <= 0 {
		return errors.New("ForgeFlow rate limit and window must be positive")
	}
	var logLevel slog.Level
	if err := logLevel.UnmarshalText([]byte(config.LogLevel)); err != nil {
		return fmt.Errorf("ForgeFlow log level %q is invalid: %w", config.LogLevel, err)
	}
	if err := (observability.TraceConfig{
		Exporter:    config.TraceExporter,
		ServiceName: config.ServiceName,
	}).Validate(); err != nil {
		return fmt.Errorf("ForgeFlow tracing configuration: %w", err)
	}
	return nil
}

// Run starts the API server and gracefully stops it when ctx is canceled.
func Run(ctx context.Context, config Config, output io.Writer) (resultErr error) {
	if ctx == nil {
		return errors.New("run ForgeFlow: context must not be nil")
	}
	if output == nil {
		return errors.New("run ForgeFlow: output must not be nil")
	}
	if err := config.Validate(); err != nil {
		return fmt.Errorf("run ForgeFlow: %w", err)
	}
	defer func() {
		resultErr = redactSensitiveError(
			resultErr,
			config.PostgresDSN,
			config.NATSURL,
			os.Getenv("OTEL_EXPORTER_OTLP_HEADERS"),
		)
	}()
	var logLevel slog.Level
	if err := logLevel.UnmarshalText([]byte(config.LogLevel)); err != nil {
		return fmt.Errorf("run ForgeFlow: parse log level: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{Level: logLevel}))
	tracerProvider, shutdownTracing, err := observability.OpenTracerProvider(
		ctx,
		observability.TraceConfig{
			Exporter:    config.TraceExporter,
			ServiceName: config.ServiceName,
		},
		output,
	)
	if err != nil {
		return fmt.Errorf("run ForgeFlow: configure tracing: %w", err)
	}
	defer func() {
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancelShutdown()
		resultErr = errors.Join(resultErr, shutdownTracing(shutdownCtx))
	}()
	instrumentation := observability.NewInstrumentation(
		logger,
		observability.NewMetrics(),
		tracerProvider,
		nil,
	)

	store, closeStore, err := openStore(ctx, config)
	if err != nil {
		return fmt.Errorf("run ForgeFlow: open persistence: %w", err)
	}
	defer closeStore()
	taskBroker, closeBroker, err := openBroker(ctx, config)
	if err != nil {
		return fmt.Errorf("run ForgeFlow: open task broker: %w", err)
	}
	defer closeBroker()
	registry, err := execution.NewDemoHandlerRegistry()
	if err != nil {
		return fmt.Errorf("run ForgeFlow: create handler registry: %w", err)
	}
	auth, err := security.NewJWTAuthenticator(security.JWTConfig{
		PublicKeyPEM: config.JWTPublicKeyPEM,
		Issuer:       config.JWTIssuer,
		Audience:     config.JWTAudience,
		Leeway:       config.JWTLeeway,
	})
	if err != nil {
		return fmt.Errorf("run ForgeFlow: configure JWT authentication: %w", err)
	}
	limiter, err := security.NewFixedWindowRateLimiter(config.RateLimit, config.RateLimitWindow, nil)
	if err != nil {
		return fmt.Errorf("run ForgeFlow: configure API rate limit: %w", err)
	}
	apiServer, err := api.NewInstrumentedServer(
		store,
		registry,
		config.WorkerCount,
		auth,
		limiter,
		instrumentation,
		dependencyReadiness(store, taskBroker),
		execution.WithBroker(taskBroker),
	)
	if err != nil {
		return fmt.Errorf("run ForgeFlow: create API server: %w", err)
	}

	listener, err := net.Listen("tcp", config.Address)
	if err != nil {
		return fmt.Errorf("run ForgeFlow: listen on %q: %w", config.Address, err)
	}
	httpServer := &http.Server{
		Handler:           apiServer,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	if _, err := fmt.Fprintf(output, "ForgeFlow listening on http://%s\n", listener.Addr()); err != nil {
		if closeErr := listener.Close(); closeErr != nil {
			return errors.Join(fmt.Errorf("run ForgeFlow: report listener: %w", err), closeErr)
		}
		return fmt.Errorf("run ForgeFlow: report listener: %w", err)
	}
	instrumentation.Log(
		ctx,
		slog.LevelInfo,
		"ForgeFlow HTTP server started",
		"address", listener.Addr().String(),
		"store_backend", config.StoreBackend,
		"broker_backend", config.BrokerBackend,
		"worker_count", config.WorkerCount,
	)

	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- httpServer.Serve(listener)
	}()

	select {
	case serveErr := <-serveErrors:
		apiServer.SetReady(false)
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancelShutdown()
		serviceErr := apiServer.Shutdown(shutdownCtx)
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		return errors.Join(serveErr, serviceErr)
	case <-ctx.Done():
		apiServer.SetReady(false)
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancelShutdown()

		httpShutdown := make(chan error, 1)
		go func() {
			httpShutdown <- httpServer.Shutdown(shutdownCtx)
		}()
		serviceErr := apiServer.Shutdown(shutdownCtx)
		httpErr := <-httpShutdown
		serveErr := <-serveErrors
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		return errors.Join(serviceErr, httpErr, serveErr)
	}
}

type readinessPinger interface {
	Ping(context.Context) error
}

func dependencyReadiness(store security.Store, taskBroker broker.Broker) api.DependencyCheck {
	return func(ctx context.Context) error {
		if pinger, ok := store.(readinessPinger); ok {
			if err := pinger.Ping(ctx); err != nil {
				return fmt.Errorf("persistence dependency is unavailable: %w", err)
			}
		}
		if pinger, ok := taskBroker.(broker.ReadinessChecker); ok {
			if err := pinger.Ping(ctx); err != nil {
				return fmt.Errorf("task broker dependency is unavailable: %w", err)
			}
		}
		return nil
	}
}

func redactSensitiveError(err error, sensitiveValues ...string) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	for _, value := range sensitiveValues {
		value = strings.TrimSpace(value)
		if value != "" {
			message = strings.ReplaceAll(message, value, "[REDACTED]")
		}
	}
	return errors.New(message)
}

func openBroker(ctx context.Context, config Config) (broker.Broker, func(), error) {
	switch config.BrokerBackend {
	case "memory":
		taskBroker := broker.NewInMemoryBroker()
		return taskBroker, func() { _ = taskBroker.Close() }, nil
	case "nats":
		natsConfig := broker.DefaultNATSConfig()
		natsConfig.URL = config.NATSURL
		natsConfig.StreamName = config.NATSStreamName
		natsConfig.SubjectPrefix = config.NATSSubjectPrefix
		taskBroker, err := broker.OpenNATSBroker(ctx, natsConfig)
		if err != nil {
			return nil, func() {}, err
		}
		return taskBroker, func() { _ = taskBroker.Close() }, nil
	default:
		return nil, func() {}, fmt.Errorf("unsupported task broker %q", config.BrokerBackend)
	}
}

func openStore(ctx context.Context, config Config) (security.Store, func(), error) {
	switch config.StoreBackend {
	case "file":
		store, err := persistence.OpenFileStore(config.DataPath)
		return store, func() {}, err
	case "postgres":
		store, err := persistence.OpenPostgresStore(ctx, config.PostgresDSN)
		if err != nil {
			return nil, func() {}, err
		}
		if err := store.Migrate(ctx); err != nil {
			store.Close()
			return nil, func() {}, err
		}
		return store, store.Close, nil
	default:
		return nil, func() {}, fmt.Errorf("unsupported persistence store %q", config.StoreBackend)
	}
}
