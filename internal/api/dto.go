package api

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/execution"
	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

// WorkflowRequest is the transport representation used to create a workflow.
type WorkflowRequest struct {
	ID    string        `json:"id"`
	Tasks []TaskRequest `json:"tasks"`
}

// TaskRequest is the transport representation of one workflow task.
type TaskRequest struct {
	ID           string             `json:"id"`
	Name         string             `json:"name"`
	Dependencies []string           `json:"dependencies,omitempty"`
	Handler      string             `json:"handler"`
	Input        string             `json:"input,omitempty"`
	Retry        RetryPolicyRequest `json:"retry,omitempty"`
}

// RetryPolicyRequest uses Go duration strings at the HTTP boundary.
type RetryPolicyRequest struct {
	MaxAttempts    int    `json:"max_attempts,omitempty"`
	InitialBackoff string `json:"initial_backoff,omitempty"`
	MaxBackoff     string `json:"max_backoff,omitempty"`
}

// WorkflowResponse is the normalized API representation of a workflow.
type WorkflowResponse struct {
	ID    workflow.WorkflowID `json:"id"`
	Tasks []TaskResponse      `json:"tasks"`
}

// TaskResponse is the normalized API representation of a task definition.
type TaskResponse struct {
	ID           workflow.TaskID      `json:"id"`
	Name         string               `json:"name"`
	Dependencies []workflow.TaskID    `json:"dependencies,omitempty"`
	Handler      workflow.HandlerName `json:"handler"`
	Input        string               `json:"input,omitempty"`
	Retry        RetryPolicyResponse  `json:"retry"`
}

// RetryPolicyResponse describes total attempts and retry delay configuration.
type RetryPolicyResponse struct {
	MaxAttempts    int    `json:"max_attempts"`
	InitialBackoff string `json:"initial_backoff"`
	MaxBackoff     string `json:"max_backoff,omitempty"`
}

// CreateRunRequest optionally supplies a caller-selected stable run ID.
type CreateRunRequest struct {
	RunID string `json:"run_id,omitempty"`
}

// RunResponse describes current aggregate workflow execution state.
type RunResponse struct {
	ID         execution.RunID             `json:"id"`
	WorkflowID workflow.WorkflowID         `json:"workflow_id"`
	Status     execution.WorkflowRunStatus `json:"status"`
	CreatedAt  time.Time                   `json:"created_at"`
	UpdatedAt  time.Time                   `json:"updated_at"`
	Tasks      TaskCounts                  `json:"task_counts"`
}

// TaskCounts summarizes task state without embedding every task in a run response.
type TaskCounts struct {
	Pending      int `json:"pending"`
	Ready        int `json:"ready"`
	Running      int `json:"running"`
	RetryWaiting int `json:"retry_wait"`
	Succeeded    int `json:"succeeded"`
	Failed       int `json:"failed"`
	Canceled     int `json:"canceled"`
}

// TaskRunsResponse lists all task executions for one workflow run.
type TaskRunsResponse struct {
	RunID execution.RunID   `json:"run_id"`
	Tasks []TaskRunResponse `json:"tasks"`
}

// TaskRunResponse describes the latest durable state of one logical task run.
type TaskRunResponse struct {
	TaskID           workflow.TaskID         `json:"task_id"`
	TaskRunID        execution.TaskRunID     `json:"task_run_id"`
	Status           execution.TaskRunStatus `json:"status"`
	Output           string                  `json:"output,omitempty"`
	Error            string                  `json:"error,omitempty"`
	AttemptCount     int                     `json:"attempt_count"`
	CurrentAttemptID execution.AttemptID     `json:"current_attempt_id,omitempty"`
	NextAttemptAt    *time.Time              `json:"next_attempt_at,omitempty"`
	Lease            *LeaseResponse          `json:"lease,omitempty"`
	UpdatedAt        time.Time               `json:"updated_at"`
	StartedAt        *time.Time              `json:"started_at,omitempty"`
	FinishedAt       *time.Time              `json:"finished_at,omitempty"`
}

// LeaseResponse exposes current ownership without changing the lease model.
type LeaseResponse struct {
	WorkerID  execution.WorkerID  `json:"worker_id"`
	AttemptID execution.AttemptID `json:"attempt_id"`
	ExpiresAt time.Time           `json:"expires_at"`
}

// CancellationResponse confirms that cancellation was accepted asynchronously.
type CancellationResponse struct {
	RunID  execution.RunID `json:"run_id"`
	Status string          `json:"status"`
}

// StatusResponse is returned by health and readiness probes.
type StatusResponse struct {
	Status string `json:"status"`
}

// ErrorResponse is the stable envelope for all API failures.
type ErrorResponse struct {
	Error APIError `json:"error"`
}

// APIError is a machine-readable error with an optional field reference.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

type requestValidationError struct {
	field   string
	message string
}

func (err *requestValidationError) Error() string {
	return err.message
}

func (request WorkflowRequest) definition() (workflow.WorkflowDefinition, error) {
	definition := workflow.WorkflowDefinition{
		ID:    workflow.WorkflowID(request.ID),
		Tasks: make([]workflow.TaskDefinition, 0, len(request.Tasks)),
	}
	for index, task := range request.Tasks {
		initialBackoff, err := parseDurationField(
			fmt.Sprintf("tasks[%d].retry.initial_backoff", index),
			task.Retry.InitialBackoff,
		)
		if err != nil {
			return workflow.WorkflowDefinition{}, err
		}
		maxBackoff, err := parseDurationField(
			fmt.Sprintf("tasks[%d].retry.max_backoff", index),
			task.Retry.MaxBackoff,
		)
		if err != nil {
			return workflow.WorkflowDefinition{}, err
		}
		dependencies := make([]workflow.TaskID, 0, len(task.Dependencies))
		for _, dependency := range task.Dependencies {
			dependencies = append(dependencies, workflow.TaskID(dependency))
		}
		definition.Tasks = append(definition.Tasks, workflow.TaskDefinition{
			ID:           workflow.TaskID(task.ID),
			Name:         task.Name,
			Dependencies: dependencies,
			Handler:      workflow.HandlerName(task.Handler),
			Input:        task.Input,
			Retry: workflow.RetryPolicy{
				MaxAttempts:    task.Retry.MaxAttempts,
				InitialBackoff: initialBackoff,
				MaxBackoff:     maxBackoff,
			},
		})
	}
	return definition, nil
}

func parseDurationField(field, value string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, &requestValidationError{
			field:   field,
			message: fmt.Sprintf("%s must be a valid Go duration", field),
		}
	}
	return duration, nil
}

func workflowDTO(definition workflow.WorkflowDefinition) WorkflowResponse {
	tasks := append([]workflow.TaskDefinition(nil), definition.Tasks...)
	sort.Slice(tasks, func(left, right int) bool { return tasks[left].ID < tasks[right].ID })
	response := WorkflowResponse{
		ID:    definition.ID,
		Tasks: make([]TaskResponse, 0, len(tasks)),
	}
	for _, task := range tasks {
		dependencies := append([]workflow.TaskID(nil), task.Dependencies...)
		sort.Slice(dependencies, func(left, right int) bool { return dependencies[left] < dependencies[right] })
		response.Tasks = append(response.Tasks, TaskResponse{
			ID:           task.ID,
			Name:         task.Name,
			Dependencies: dependencies,
			Handler:      task.Handler,
			Input:        task.Input,
			Retry: RetryPolicyResponse{
				MaxAttempts:    task.Retry.EffectiveMaxAttempts(),
				InitialBackoff: task.Retry.InitialBackoff.String(),
				MaxBackoff:     optionalDurationString(task.Retry.MaxBackoff),
			},
		})
	}
	return response
}

func optionalDurationString(duration time.Duration) string {
	if duration == 0 {
		return ""
	}
	return duration.String()
}

func runDTO(snapshot execution.WorkflowRunSnapshot) RunResponse {
	response := RunResponse{
		ID:         snapshot.ID,
		WorkflowID: snapshot.WorkflowID,
		Status:     snapshot.Status,
		CreatedAt:  snapshot.CreatedAt,
		UpdatedAt:  snapshot.UpdatedAt,
	}
	for _, task := range snapshot.Tasks {
		switch task.Status {
		case execution.TaskRunPending:
			response.Tasks.Pending++
		case execution.TaskRunReady:
			response.Tasks.Ready++
		case execution.TaskRunRunning:
			response.Tasks.Running++
		case execution.TaskRunRetryWaiting:
			response.Tasks.RetryWaiting++
		case execution.TaskRunSucceeded:
			response.Tasks.Succeeded++
		case execution.TaskRunFailed:
			response.Tasks.Failed++
		case execution.TaskRunCanceled:
			response.Tasks.Canceled++
		}
	}
	return response
}

func taskRunsDTO(snapshot execution.WorkflowRunSnapshot) TaskRunsResponse {
	tasks := append([]execution.TaskRun(nil), snapshot.Tasks...)
	sort.Slice(tasks, func(left, right int) bool { return tasks[left].TaskID < tasks[right].TaskID })
	response := TaskRunsResponse{
		RunID: snapshot.ID,
		Tasks: make([]TaskRunResponse, 0, len(tasks)),
	}
	for _, task := range tasks {
		taskResponse := TaskRunResponse{
			TaskID:           task.TaskID,
			TaskRunID:        task.TaskRunID,
			Status:           task.Status,
			Output:           task.Output,
			Error:            task.Error,
			AttemptCount:     task.AttemptCount,
			CurrentAttemptID: task.CurrentAttemptID,
			NextAttemptAt:    optionalTime(task.NextAttemptAt),
			UpdatedAt:        task.UpdatedAt,
			StartedAt:        optionalTime(task.StartedAt),
			FinishedAt:       optionalTime(task.FinishedAt),
		}
		if task.Lease != nil {
			taskResponse.Lease = &LeaseResponse{
				WorkerID:  task.Lease.WorkerID,
				AttemptID: task.Lease.AttemptID,
				ExpiresAt: task.Lease.ExpiresAt,
			}
		}
		response.Tasks = append(response.Tasks, taskResponse)
	}
	return response
}

func optionalTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	copy := value
	return &copy
}
