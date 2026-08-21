// Package app composes the ForgeFlow HTTP process from lightweight components.
package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/api"
	"github.com/vijaypratap3364/forgeflow/internal/execution"
	"github.com/vijaypratap3364/forgeflow/internal/persistence"
)

const (
	defaultAddress         = "127.0.0.1:8080"
	defaultDataPath        = "data/forgeflow.ffdb"
	defaultStoreBackend    = "file"
	defaultWorkerCount     = 4
	defaultShutdownTimeout = 10 * time.Second
)

// Config contains process settings for the ForgeFlow API server. PostgresDSN
// may contain credentials and must not be logged.
type Config struct {
	Address         string
	StoreBackend    string
	DataPath        string
	PostgresDSN     string
	WorkerCount     int
	ShutdownTimeout time.Duration
}

// DefaultConfig returns laptop-friendly local process defaults.
func DefaultConfig() Config {
	return Config{
		Address:         defaultAddress,
		StoreBackend:    defaultStoreBackend,
		DataPath:        defaultDataPath,
		WorkerCount:     defaultWorkerCount,
		ShutdownTimeout: defaultShutdownTimeout,
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
	if config.WorkerCount < 1 {
		return errors.New("ForgeFlow worker count must be at least one")
	}
	if config.ShutdownTimeout <= 0 {
		return errors.New("ForgeFlow shutdown timeout must be positive")
	}
	return nil
}

// Run starts the API server and gracefully stops it when ctx is canceled.
func Run(ctx context.Context, config Config, output io.Writer) error {
	if ctx == nil {
		return errors.New("run ForgeFlow: context must not be nil")
	}
	if output == nil {
		return errors.New("run ForgeFlow: output must not be nil")
	}
	if err := config.Validate(); err != nil {
		return fmt.Errorf("run ForgeFlow: %w", err)
	}

	store, closeStore, err := openStore(ctx, config)
	if err != nil {
		return fmt.Errorf("run ForgeFlow: open persistence: %w", err)
	}
	defer closeStore()
	registry, err := execution.NewDemoHandlerRegistry()
	if err != nil {
		return fmt.Errorf("run ForgeFlow: create handler registry: %w", err)
	}
	apiServer, err := api.NewServer(store, registry, config.WorkerCount)
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

func openStore(ctx context.Context, config Config) (execution.Store, func(), error) {
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
