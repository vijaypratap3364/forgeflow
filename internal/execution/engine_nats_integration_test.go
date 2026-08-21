//go:build integration

package execution

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/broker"
	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

func TestEngineExecutesWorkflowThroughNATSJetStream(t *testing.T) {
	natsURL := os.Getenv("FORGEFLOW_NATS_TEST_URL")
	if natsURL == "" {
		t.Skip("set FORGEFLOW_NATS_TEST_URL to run NATS JetStream integration tests")
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	config := broker.DefaultNATSConfig()
	config.URL = natsURL
	config.StreamName = "FF_ENGINE_TEST_" + suffix
	config.SubjectPrefix = "forgeflow.engine.test." + suffix
	config.MaxAge = 10 * time.Minute
	taskBroker, err := broker.OpenNATSBroker(context.Background(), config)
	if err != nil {
		t.Fatalf("OpenNATSBroker() error = %v", err)
	}
	t.Cleanup(func() {
		if err := taskBroker.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	registry, err := NewDemoHandlerRegistry()
	if err != nil {
		t.Fatalf("NewDemoHandlerRegistry() error = %v", err)
	}
	engine, err := NewEngine(2, registry, newMemoryTestStore(), WithBroker(taskBroker))
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	definition := workflow.WorkflowDefinition{
		ID: "nats-engine-workflow",
		Tasks: []workflow.TaskDefinition{
			{ID: "root", Name: "root", Handler: UppercaseHandlerName, Input: "forge"},
			{ID: "leaf", Name: "leaf", Handler: UppercaseHandlerName, Input: "flow", Dependencies: []workflow.TaskID{"root"}},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run, err := engine.Execute(ctx, "nats-engine-run", definition)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if run.Status() != WorkflowRunSucceeded {
		t.Fatalf("run status = %q, want %q", run.Status(), WorkflowRunSucceeded)
	}
	if task := requireTaskRun(t, run, "root"); task.Output != "FORGE" {
		t.Fatalf("root output = %q, want FORGE", task.Output)
	}
	if task := requireTaskRun(t, run, "leaf"); task.Output != "FLOW" {
		t.Fatalf("leaf output = %q, want FLOW", task.Output)
	}
}
