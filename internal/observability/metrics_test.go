package observability

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestMetricsExposeCountersGaugesAndHistograms(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	metrics.WorkflowSubmitted()
	metrics.WorkflowCompleted("succeeded", 2*time.Second)
	metrics.WorkflowCompleted("failed", 3*time.Second)
	metrics.WorkflowCompleted("canceled", time.Second)
	metrics.WorkerStarted()
	metrics.WorkerStopped()
	metrics.QueuePublished()
	metrics.QueueClaimed()
	metrics.TaskStarted()
	metrics.TaskCompleted("succeeded", 25*time.Millisecond)
	metrics.TaskStarted()
	metrics.RetryScheduled(50 * time.Millisecond)
	metrics.TaskStarted()
	metrics.TaskCompleted("failed", 75*time.Millisecond)
	metrics.HTTPRequest("GET", "/healthz", 200, time.Millisecond)

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatalf("WritePrometheus() error = %v", err)
	}
	for _, sample := range []string{
		"forgeflow_workflows_submitted_total 1",
		"forgeflow_workflows_succeeded_total 1",
		"forgeflow_workflows_failed_total 1",
		"forgeflow_workflows_canceled_total 1",
		`forgeflow_tasks_executed_total{status="failed"} 2`,
		`forgeflow_tasks_executed_total{status="succeeded"} 1`,
		"forgeflow_task_failures_total 2",
		"forgeflow_task_retries_total 1",
		"forgeflow_active_workers 0",
		"forgeflow_queue_depth 0",
		"forgeflow_running_tasks 0",
		"forgeflow_task_duration_seconds_count 3",
		"forgeflow_workflow_duration_seconds_count 3",
		`forgeflow_http_requests_total{method="GET",route="/healthz",status="200"} 1`,
	} {
		if !strings.Contains(output.String(), sample) {
			t.Errorf("metrics output does not contain %q\n%s", sample, output.String())
		}
	}
	if !strings.HasSuffix(output.String(), "\n") {
		t.Fatal("Prometheus exposition does not end with a line feed")
	}
}

func TestMetricsScrapeDoesNotBlockRecordingOnSlowClient(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics()
	writer := &blockingWriter{started: make(chan struct{}), release: make(chan struct{})}
	scrapeDone := make(chan error, 1)
	go func() {
		scrapeDone <- metrics.WritePrometheus(writer)
	}()
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("metrics scrape did not reach the slow writer")
	}

	recorded := make(chan struct{})
	go func() {
		metrics.TaskStarted()
		close(recorded)
	}()
	select {
	case <-recorded:
	case <-time.After(time.Second):
		close(writer.release)
		t.Fatal("metric recording blocked behind slow scrape output")
	}
	close(writer.release)
	if err := <-scrapeDone; err != nil {
		t.Fatalf("WritePrometheus() error = %v", err)
	}
}

type blockingWriter struct {
	started chan struct{}
	release chan struct{}
}

func (writer *blockingWriter) Write(data []byte) (int, error) {
	select {
	case <-writer.started:
	default:
		close(writer.started)
	}
	<-writer.release
	return len(data), nil
}
