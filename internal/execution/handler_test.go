package execution

import (
	"context"
	"errors"
	"testing"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

func TestHandlerRegistry(t *testing.T) {
	t.Parallel()

	registry := NewHandlerRegistry()
	handler := TaskHandlerFunc(func(context.Context, TaskRequest) (string, error) {
		return "handled", nil
	})

	if err := registry.Register("custom", handler); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	got, exists := registry.Handler("custom")
	if !exists {
		t.Fatal("Handler() did not find registered handler")
	}
	output, err := got.Execute(context.Background(), TaskRequest{})
	if err != nil || output != "handled" {
		t.Fatalf("registered handler returned output=%q error=%v", output, err)
	}

	tests := []struct {
		name    workflow.HandlerName
		handler TaskHandler
	}{
		{name: "custom", handler: NoopHandler{}},
		{name: "", handler: NoopHandler{}},
		{name: "bad name", handler: NoopHandler{}},
		{name: "nil", handler: nil},
		{name: "typed-nil", handler: TaskHandlerFunc(nil)},
	}
	for _, test := range tests {
		err := registry.Register(test.name, test.handler)
		var registrationError *HandlerRegistrationError
		if !errors.As(err, &registrationError) {
			t.Fatalf("Register(%q) error = %v, want *HandlerRegistrationError", test.name, err)
		}
	}
}

func TestDemoHandlers(t *testing.T) {
	t.Parallel()

	registry, err := NewDemoHandlerRegistry()
	if err != nil {
		t.Fatalf("NewDemoHandlerRegistry() error = %v", err)
	}

	tests := []struct {
		name       string
		handler    workflow.HandlerName
		input      string
		cancel     bool
		wantOutput string
		wantError  bool
	}{
		{name: "noop", handler: NoopHandlerName},
		{name: "uppercase", handler: UppercaseHandlerName, input: "ForgeFlow", wantOutput: "FORGEFLOW"},
		{name: "zero delay", handler: DelayHandlerName, input: "0s", wantOutput: "0s"},
		{name: "invalid delay", handler: DelayHandlerName, input: "not-a-duration", wantError: true},
		{name: "canceled delay", handler: DelayHandlerName, input: "1h", cancel: true, wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			handler, exists := registry.Handler(test.handler)
			if !exists {
				t.Fatalf("Handler(%q) was not registered", test.handler)
			}

			ctx, cancel := context.WithCancel(context.Background())
			if test.cancel {
				cancel()
			} else {
				defer cancel()
			}

			output, err := handler.Execute(ctx, TaskRequest{
				RunID: "run",
				Task: workflow.TaskDefinition{
					ID:      "task",
					Name:    "Task",
					Handler: test.handler,
					Input:   test.input,
				},
			})
			if test.wantError && err == nil {
				t.Fatal("Execute() error = nil, want error")
			}
			if !test.wantError && err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if output != test.wantOutput {
				t.Fatalf("Execute() output = %q, want %q", output, test.wantOutput)
			}
			if test.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("Execute() error = %v, want context.Canceled", err)
			}
		})
	}
}
