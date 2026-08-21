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

	mu         sync.Mutex
	rootCtx    context.Context
	cancelRoot context.CancelFunc
	active     map[execution.RunID]context.CancelFunc
	failures   map[execution.RunID]error
	closed     bool
	workers    sync.WaitGroup
}

func newRunManager(engine *execution.Engine) *runManager {
	rootCtx, cancelRoot := context.WithCancel(context.Background())
	return &runManager{
		engine:     engine,
		rootCtx:    rootCtx,
		cancelRoot: cancelRoot,
		active:     make(map[execution.RunID]context.CancelFunc),
		failures:   make(map[execution.RunID]error),
	}
}

func (manager *runManager) start(runID execution.RunID) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	if manager.closed {
		return errManagerClosed
	}
	if _, exists := manager.active[runID]; exists {
		return errRunAlreadyActive
	}
	runCtx, cancelRun := context.WithCancel(manager.rootCtx)
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
		manager.cancelRoot()
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
