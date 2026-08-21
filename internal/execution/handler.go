package execution

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

const (
	// NoopHandlerName identifies the built-in no-operation handler.
	NoopHandlerName workflow.HandlerName = "noop"
	// DelayHandlerName identifies the built-in context-aware delay handler.
	DelayHandlerName workflow.HandlerName = "delay"
	// UppercaseHandlerName identifies the built-in deterministic text handler.
	UppercaseHandlerName workflow.HandlerName = "uppercase"
)

// TaskRequest contains the immutable inputs supplied to a task handler.
type TaskRequest struct {
	RunID     RunID
	TaskRunID TaskRunID
	AttemptID AttemptID
	Task      workflow.TaskDefinition
}

// TaskHandler executes one safe, registered task operation. Implementations may
// be called concurrently and must return promptly when the context is canceled.
type TaskHandler interface {
	Execute(context.Context, TaskRequest) (string, error)
}

// TaskHandlerFunc adapts a function to TaskHandler.
type TaskHandlerFunc func(context.Context, TaskRequest) (string, error)

// Execute invokes the adapted function.
func (handler TaskHandlerFunc) Execute(ctx context.Context, request TaskRequest) (string, error) {
	return handler(ctx, request)
}

// HandlerRegistry stores safe task handlers by name.
type HandlerRegistry struct {
	mu       sync.RWMutex
	handlers map[workflow.HandlerName]TaskHandler
}

// NewHandlerRegistry creates an empty handler registry.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[workflow.HandlerName]TaskHandler),
	}
}

// Register adds a handler without replacing an existing registration.
func (registry *HandlerRegistry) Register(name workflow.HandlerName, handler TaskHandler) error {
	if !validIdentifier(string(name)) {
		return &HandlerRegistrationError{
			HandlerName: name,
			Reason:      "handler names must be non-empty and contain no whitespace or control characters",
		}
	}
	if isNilHandler(handler) {
		return &HandlerRegistrationError{
			HandlerName: name,
			Reason:      "handler must not be nil",
		}
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()

	if _, exists := registry.handlers[name]; exists {
		return &HandlerRegistrationError{
			HandlerName: name,
			Reason:      "handler is already registered",
		}
	}
	registry.handlers[name] = handler
	return nil
}

// Handler returns a registered handler by name.
func (registry *HandlerRegistry) Handler(name workflow.HandlerName) (TaskHandler, bool) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	handler, exists := registry.handlers[name]
	return handler, exists
}

// NewDemoHandlerRegistry creates a registry containing ForgeFlow's safe demo handlers.
func NewDemoHandlerRegistry() (*HandlerRegistry, error) {
	registry := NewHandlerRegistry()
	registrations := []struct {
		name    workflow.HandlerName
		handler TaskHandler
	}{
		{name: NoopHandlerName, handler: NoopHandler{}},
		{name: DelayHandlerName, handler: DelayHandler{}},
		{name: UppercaseHandlerName, handler: UppercaseHandler{}},
	}
	for _, registration := range registrations {
		if err := registry.Register(registration.name, registration.handler); err != nil {
			return nil, fmt.Errorf("register demo handler: %w", err)
		}
	}
	return registry, nil
}

// NoopHandler completes without performing work.
type NoopHandler struct{}

// Execute returns immediately unless the context is already canceled.
func (NoopHandler) Execute(ctx context.Context, _ TaskRequest) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return "", nil
}

// DelayHandler waits for the Go duration supplied in TaskDefinition.Input.
type DelayHandler struct{}

// Execute performs a context-aware delay and returns the duration string.
func (DelayHandler) Execute(ctx context.Context, request TaskRequest) (string, error) {
	duration, err := time.ParseDuration(strings.TrimSpace(request.Task.Input))
	if err != nil {
		return "", fmt.Errorf("parse delay for task %q: %w", request.Task.ID, err)
	}
	if duration < 0 {
		return "", fmt.Errorf("delay for task %q must not be negative", request.Task.ID)
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-timer.C:
		return request.Task.Input, nil
	}
}

// UppercaseHandler transforms TaskDefinition.Input to uppercase.
type UppercaseHandler struct{}

// Execute returns a deterministic uppercase transformation.
func (UppercaseHandler) Execute(ctx context.Context, request TaskRequest) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return strings.ToUpper(request.Task.Input), nil
}

func isNilHandler(handler TaskHandler) bool {
	if handler == nil {
		return true
	}

	value := reflect.ValueOf(handler)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
