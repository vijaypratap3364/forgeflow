package observability

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"time"
)

var (
	requestDurationBuckets  = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	taskDurationBuckets     = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}
	workflowDurationBuckets = []float64{0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}
)

type httpMetricKey struct {
	method string
	route  string
	status int
}

type histogram struct {
	bounds []float64
	bucket []uint64
	count  uint64
	sum    float64
}

func newHistogram(bounds []float64) histogram {
	return histogram{
		bounds: append([]float64(nil), bounds...),
		bucket: make([]uint64, len(bounds)),
	}
}

func (histogram *histogram) observe(value float64) {
	if value < 0 {
		value = 0
	}
	histogram.count++
	histogram.sum += value
	for index, bound := range histogram.bounds {
		if value <= bound {
			histogram.bucket[index]++
		}
	}
}

// Metrics is a concurrency-safe, process-local Prometheus text registry.
// Resource identifiers are deliberately excluded from labels.
type Metrics struct {
	mu sync.Mutex

	workflowsSubmitted uint64
	workflowsSucceeded uint64
	workflowsFailed    uint64
	workflowsCanceled  uint64
	tasksExecuted      map[string]uint64
	taskFailures       uint64
	taskRetries        uint64
	activeWorkers      int64
	queueDepth         int64
	runningTasks       int64
	httpRequests       map[httpMetricKey]uint64
	httpDuration       histogram
	taskDuration       histogram
	workflowDuration   histogram
}

type metricsSnapshot struct {
	workflowsSubmitted uint64
	workflowsSucceeded uint64
	workflowsFailed    uint64
	workflowsCanceled  uint64
	tasksExecuted      map[string]uint64
	taskFailures       uint64
	taskRetries        uint64
	activeWorkers      int64
	queueDepth         int64
	runningTasks       int64
	httpRequests       map[httpMetricKey]uint64
	httpDuration       histogram
	taskDuration       histogram
	workflowDuration   histogram
}

// NewMetrics returns an empty registry with fixed, deterministic histogram
// buckets suitable for workflow and API latency.
func NewMetrics() *Metrics {
	return &Metrics{
		tasksExecuted:    make(map[string]uint64),
		httpRequests:     make(map[httpMetricKey]uint64),
		httpDuration:     newHistogram(requestDurationBuckets),
		taskDuration:     newHistogram(taskDurationBuckets),
		workflowDuration: newHistogram(workflowDurationBuckets),
	}
}

// WorkflowSubmitted records a durably created workflow run.
func (metrics *Metrics) WorkflowSubmitted() {
	metrics.mu.Lock()
	metrics.workflowsSubmitted++
	metrics.mu.Unlock()
}

// WorkflowCompleted records one terminal run and its wall-clock duration.
func (metrics *Metrics) WorkflowCompleted(status string, duration time.Duration) {
	metrics.mu.Lock()
	switch status {
	case "succeeded":
		metrics.workflowsSucceeded++
	case "failed":
		metrics.workflowsFailed++
	case "canceled":
		metrics.workflowsCanceled++
	}
	metrics.workflowDuration.observe(duration.Seconds())
	metrics.mu.Unlock()
}

// TaskStarted records an attempt entering handler execution.
func (metrics *Metrics) TaskStarted() {
	metrics.mu.Lock()
	metrics.runningTasks++
	metrics.mu.Unlock()
}

// TaskCompleted records one finished handler attempt.
func (metrics *Metrics) TaskCompleted(status string, duration time.Duration) {
	metrics.mu.Lock()
	metrics.tasksExecuted[status]++
	if status == "failed" {
		metrics.taskFailures++
	}
	if metrics.runningTasks > 0 {
		metrics.runningTasks--
	}
	metrics.taskDuration.observe(duration.Seconds())
	metrics.mu.Unlock()
}

// RetryScheduled records a failed attempt that remains eligible to retry.
func (metrics *Metrics) RetryScheduled(duration time.Duration) {
	metrics.mu.Lock()
	metrics.tasksExecuted["failed"]++
	metrics.taskFailures++
	metrics.taskRetries++
	if metrics.runningTasks > 0 {
		metrics.runningTasks--
	}
	metrics.taskDuration.observe(duration.Seconds())
	metrics.mu.Unlock()
}

// WorkerStarted increments the number of active worker goroutines.
func (metrics *Metrics) WorkerStarted() {
	metrics.mu.Lock()
	metrics.activeWorkers++
	metrics.mu.Unlock()
}

// WorkerStopped decrements the number of active worker goroutines.
func (metrics *Metrics) WorkerStopped() {
	metrics.mu.Lock()
	if metrics.activeWorkers > 0 {
		metrics.activeWorkers--
	}
	metrics.mu.Unlock()
}

// QueuePublished records one task message published by this process.
func (metrics *Metrics) QueuePublished() {
	metrics.mu.Lock()
	metrics.queueDepth++
	metrics.mu.Unlock()
}

// QueueClaimed records one locally observed dispatch leaving the ready queue.
func (metrics *Metrics) QueueClaimed() {
	metrics.mu.Lock()
	if metrics.queueDepth > 0 {
		metrics.queueDepth--
	}
	metrics.mu.Unlock()
}

// HTTPRequest records one completed HTTP request using bounded labels.
func (metrics *Metrics) HTTPRequest(method, route string, status int, duration time.Duration) {
	if route == "" {
		route = "unmatched"
	}
	key := httpMetricKey{method: method, route: route, status: status}
	metrics.mu.Lock()
	metrics.httpRequests[key]++
	metrics.httpDuration.observe(duration.Seconds())
	metrics.mu.Unlock()
}

// WritePrometheus writes the Prometheus 0.0.4 text exposition format.
func (metrics *Metrics) WritePrometheus(writer io.Writer) error {
	snapshot := metrics.snapshot()

	buffered := bufio.NewWriter(writer)
	writeCounter := func(name, help string, value uint64) error {
		if _, err := fmt.Fprintf(buffered, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, value); err != nil {
			return err
		}
		return nil
	}
	if err := writeCounter("forgeflow_workflows_submitted_total", "Total workflow runs durably submitted.", snapshot.workflowsSubmitted); err != nil {
		return err
	}
	if err := writeCounter("forgeflow_workflows_succeeded_total", "Total workflow runs that succeeded.", snapshot.workflowsSucceeded); err != nil {
		return err
	}
	if err := writeCounter("forgeflow_workflows_failed_total", "Total workflow runs that failed.", snapshot.workflowsFailed); err != nil {
		return err
	}
	if err := writeCounter("forgeflow_workflows_canceled_total", "Total workflow runs that were canceled.", snapshot.workflowsCanceled); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(buffered, "# HELP forgeflow_tasks_executed_total Total finished task-handler attempts."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(buffered, "# TYPE forgeflow_tasks_executed_total counter"); err != nil {
		return err
	}
	taskStatuses := make([]string, 0, len(snapshot.tasksExecuted))
	for status := range snapshot.tasksExecuted {
		taskStatuses = append(taskStatuses, status)
	}
	sort.Strings(taskStatuses)
	for _, status := range taskStatuses {
		if _, err := fmt.Fprintf(buffered, "forgeflow_tasks_executed_total{status=%q} %d\n", status, snapshot.tasksExecuted[status]); err != nil {
			return err
		}
	}
	if err := writeCounter("forgeflow_task_failures_total", "Total failed task-handler attempts, including retryable failures.", snapshot.taskFailures); err != nil {
		return err
	}
	if err := writeCounter("forgeflow_task_retries_total", "Total task retries scheduled after failed attempts.", snapshot.taskRetries); err != nil {
		return err
	}
	if err := writeGauge(buffered, "forgeflow_active_workers", "Current in-process worker goroutines.", snapshot.activeWorkers); err != nil {
		return err
	}
	if err := writeGauge(buffered, "forgeflow_queue_depth", "Task messages published by this process and not yet claimed locally.", snapshot.queueDepth); err != nil {
		return err
	}
	if err := writeGauge(buffered, "forgeflow_running_tasks", "Current task attempts executing in handlers.", snapshot.runningTasks); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(buffered, "# HELP forgeflow_http_requests_total Total HTTP requests by method, route, and status."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(buffered, "# TYPE forgeflow_http_requests_total counter"); err != nil {
		return err
	}
	httpKeys := make([]httpMetricKey, 0, len(snapshot.httpRequests))
	for key := range snapshot.httpRequests {
		httpKeys = append(httpKeys, key)
	}
	sort.Slice(httpKeys, func(left, right int) bool {
		if httpKeys[left].method != httpKeys[right].method {
			return httpKeys[left].method < httpKeys[right].method
		}
		if httpKeys[left].route != httpKeys[right].route {
			return httpKeys[left].route < httpKeys[right].route
		}
		return httpKeys[left].status < httpKeys[right].status
	})
	for _, key := range httpKeys {
		if _, err := fmt.Fprintf(
			buffered,
			"forgeflow_http_requests_total{method=%q,route=%q,status=%q} %d\n",
			key.method,
			key.route,
			strconv.Itoa(key.status),
			snapshot.httpRequests[key],
		); err != nil {
			return err
		}
	}
	if err := writeHistogram(buffered, "forgeflow_http_request_duration_seconds", "HTTP request duration in seconds.", snapshot.httpDuration); err != nil {
		return err
	}
	if err := writeHistogram(buffered, "forgeflow_task_duration_seconds", "Task-handler attempt duration in seconds.", snapshot.taskDuration); err != nil {
		return err
	}
	if err := writeHistogram(buffered, "forgeflow_workflow_duration_seconds", "Workflow run duration in seconds.", snapshot.workflowDuration); err != nil {
		return err
	}
	return buffered.Flush()
}

func (metrics *Metrics) snapshot() metricsSnapshot {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	snapshot := metricsSnapshot{
		workflowsSubmitted: metrics.workflowsSubmitted,
		workflowsSucceeded: metrics.workflowsSucceeded,
		workflowsFailed:    metrics.workflowsFailed,
		workflowsCanceled:  metrics.workflowsCanceled,
		taskFailures:       metrics.taskFailures,
		taskRetries:        metrics.taskRetries,
		activeWorkers:      metrics.activeWorkers,
		queueDepth:         metrics.queueDepth,
		runningTasks:       metrics.runningTasks,
		tasksExecuted:      make(map[string]uint64, len(metrics.tasksExecuted)),
		httpRequests:       make(map[httpMetricKey]uint64, len(metrics.httpRequests)),
		httpDuration:       cloneHistogram(metrics.httpDuration),
		taskDuration:       cloneHistogram(metrics.taskDuration),
		workflowDuration:   cloneHistogram(metrics.workflowDuration),
	}
	for status, count := range metrics.tasksExecuted {
		snapshot.tasksExecuted[status] = count
	}
	for key, count := range metrics.httpRequests {
		snapshot.httpRequests[key] = count
	}
	return snapshot
}

func cloneHistogram(source histogram) histogram {
	return histogram{
		bounds: append([]float64(nil), source.bounds...),
		bucket: append([]uint64(nil), source.bucket...),
		count:  source.count,
		sum:    source.sum,
	}
}

func writeGauge(writer io.Writer, name, help string, value int64) error {
	_, err := fmt.Fprintf(writer, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", name, help, name, name, value)
	return err
}

func writeHistogram(writer io.Writer, name, help string, histogram histogram) error {
	if _, err := fmt.Fprintf(writer, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name); err != nil {
		return err
	}
	for index, bound := range histogram.bounds {
		if _, err := fmt.Fprintf(
			writer,
			"%s_bucket{le=%q} %d\n",
			name,
			strconv.FormatFloat(bound, 'g', -1, 64),
			histogram.bucket[index],
		); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(writer, "%s_bucket{le=\"+Inf\"} %d\n", name, histogram.count); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "%s_sum %s\n", name, strconv.FormatFloat(histogram.sum, 'g', -1, 64)); err != nil {
		return err
	}
	_, err := fmt.Fprintf(writer, "%s_count %d\n", name, histogram.count)
	return err
}
