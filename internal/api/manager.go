package api

import (
	"context"
	"errors"
	"sync"

	"github.com/vijaypratap3364/forgeflow/internal/execution"
)

var (
	errRunAlreadyActive = errors.New("workflow run is already active")
	errManagerClosed    = errors.New("workflow run manager is closed")
)

type runManager struct {
	engine *execution.Engine

	mu       sync.Mutex
	active   map[execution.RunID]context.CancelFunc
	failures map[execution.RunID]error
	closed   bool
	workers  sync.WaitGroup
}

func newRunManager(engine *execution.Engine) *runManager {
	return &runManager{
		engine:   engine,
		active:   make(map[execution.RunID]context.CancelFunc),
		failures: make(map[execution.RunID]error),
	}
}

func (manager *runManager) start(parent context.Context, runID execution.RunID) error {
	if parent == nil {
		return errors.New("start workflow run: context must not be nil")
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()

	if manager.closed {
		return errManagerClosed
	}
	if _, exists := manager.active[runID]; exists {
		return errRunAlreadyActive
	}
	// Submission acceptance detaches client cancellation while preserving
	// request IDs and trace context. The manager retains its own cancel handle.
	runCtx, cancelRun := context.WithCancel(context.WithoutCancel(parent))
	manager.active[runID] = cancelRun
	manager.workers.Add(1)
	go manager.recover(runCtx, runID)
	return nil
}

func (manager *runManager) recover(ctx context.Context, runID execution.RunID) {
	defer manager.workers.Done()
	defer func() {
		manager.mu.Lock()
		delete(manager.active, runID)
		manager.mu.Unlock()
	}()

	_, err := manager.engine.Recover(ctx, runID)
	manager.mu.Lock()
	if err == nil {
		delete(manager.failures, runID)
	} else {
		manager.failures[runID] = err
	}
	manager.mu.Unlock()
}

func (manager *runManager) cancel(runID execution.RunID) bool {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	cancelRun, exists := manager.active[runID]
	if exists {
		cancelRun()
	}
	return exists
}

func (manager *runManager) shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("shut down workflow run manager: context must not be nil")
	}
	manager.mu.Lock()
	if !manager.closed {
		manager.closed = true
		for _, cancelRun := range manager.active {
			cancelRun()
		}
	}
	manager.mu.Unlock()

	done := make(chan struct{})
	go func() {
		manager.workers.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
