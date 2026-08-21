package persistence

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vijaypratap3364/forgeflow/internal/execution"
	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

// PostgresStore is the production execution.Store implementation. Every run
// mutation is one PostgreSQL transaction and uses workflow_runs.version as an
// optimistic concurrency token.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// OpenPostgresStore creates and verifies a PostgreSQL connection pool. Schema
// migrations are explicit: callers must invoke Migrate before using the store.
func OpenPostgresStore(ctx context.Context, dataSourceName string) (*PostgresStore, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(dataSourceName) == "" {
		return nil, errors.New("open ForgeFlow PostgreSQL store: data source name is empty")
	}
	config, err := pgxpool.ParseConfig(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("parse ForgeFlow PostgreSQL data source name: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("open ForgeFlow PostgreSQL connection pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping ForgeFlow PostgreSQL store: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

// NewPostgresStore wraps an existing connection pool. It is useful when the
// host application owns pool configuration or test isolation.
func NewPostgresStore(pool *pgxpool.Pool) (*PostgresStore, error) {
	if pool == nil {
		return nil, errors.New("create ForgeFlow PostgreSQL store: pool is nil")
	}
	return &PostgresStore{pool: pool}, nil
}

// Close releases all pooled PostgreSQL connections.
func (store *PostgresStore) Close() {
	if store != nil && store.pool != nil {
		store.pool.Close()
	}
}

// Ping verifies that PostgreSQL can accept a query.
func (store *PostgresStore) Ping(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := store.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping ForgeFlow PostgreSQL store: %w", err)
	}
	return nil
}

// SaveWorkflow persists one immutable workflow definition transactionally.
// Re-saving byte-for-byte equivalent domain content is idempotent.
func (store *PostgresStore) SaveWorkflow(ctx context.Context, definition workflow.WorkflowDefinition) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := definition.Validate(); err != nil {
		return fmt.Errorf("save workflow definition: %w", err)
	}

	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin save workflow definition %q: %w", definition.ID, err)
	}
	defer rollbackPostgresTx(ctx, tx)

	tag, err := tx.Exec(ctx, `
		INSERT INTO workflow_definitions (workflow_id)
		VALUES ($1)
		ON CONFLICT (workflow_id) DO NOTHING
	`, definition.ID)
	if err != nil {
		return fmt.Errorf("insert workflow definition %q: %w", definition.ID, err)
	}
	if tag.RowsAffected() == 0 {
		existing, found, err := loadPostgresWorkflowTx(ctx, tx, definition.ID)
		if err != nil {
			return err
		}
		if !found || !reflect.DeepEqual(existing, definition) {
			return &execution.WorkflowConflictError{WorkflowID: definition.ID}
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit idempotent workflow definition %q: %w", definition.ID, err)
		}
		return nil
	}

	for taskPosition, task := range definition.Tasks {
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_definitions (
				workflow_id, task_id, position, task_name, handler_name, task_input,
				max_attempts, initial_backoff_ns, max_backoff_ns
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		`,
			definition.ID,
			task.ID,
			taskPosition,
			task.Name,
			task.Handler,
			task.Input,
			task.Retry.MaxAttempts,
			int64(task.Retry.InitialBackoff),
			int64(task.Retry.MaxBackoff),
		); err != nil {
			return fmt.Errorf("insert task definition %q for workflow %q: %w", task.ID, definition.ID, err)
		}
	}
	for _, task := range definition.Tasks {
		for dependencyPosition, dependencyID := range task.Dependencies {
			if _, err := tx.Exec(ctx, `
				INSERT INTO task_dependencies (
					workflow_id, task_id, dependency_task_id, position
				) VALUES ($1, $2, $3, $4)
			`, definition.ID, task.ID, dependencyID, dependencyPosition); err != nil {
				return fmt.Errorf(
					"insert dependency %q for task %q in workflow %q: %w",
					dependencyID,
					task.ID,
					definition.ID,
					err,
				)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit workflow definition %q: %w", definition.ID, err)
	}
	return nil
}

// LoadWorkflow returns one immutable workflow definition when it exists.
func (store *PostgresStore) LoadWorkflow(
	ctx context.Context,
	workflowID workflow.WorkflowID,
) (workflow.WorkflowDefinition, bool, error) {
	if err := contextError(ctx); err != nil {
		return workflow.WorkflowDefinition{}, false, err
	}
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return workflow.WorkflowDefinition{}, false, fmt.Errorf("begin load workflow definition %q: %w", workflowID, err)
	}
	defer rollbackPostgresTx(ctx, tx)

	definition, found, err := loadPostgresWorkflowTx(ctx, tx, workflowID)
	if err != nil {
		return workflow.WorkflowDefinition{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return workflow.WorkflowDefinition{}, false, fmt.Errorf("commit load workflow definition %q: %w", workflowID, err)
	}
	return definition, found, nil
}

// CreateRun atomically creates every row in a workflow-run aggregate and
// returns the new persisted version one.
func (store *PostgresStore) CreateRun(
	ctx context.Context,
	snapshot execution.WorkflowRunSnapshot,
) (execution.WorkflowRunSnapshot, error) {
	if err := contextError(ctx); err != nil {
		return execution.WorkflowRunSnapshot{}, err
	}
	if err := snapshot.Validate(); err != nil {
		return execution.WorkflowRunSnapshot{}, fmt.Errorf("create workflow run: %w", err)
	}
	if snapshot.Version != 0 {
		return execution.WorkflowRunSnapshot{}, fmt.Errorf(
			"create workflow run %q: initial version must be zero",
			snapshot.ID,
		)
	}

	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return execution.WorkflowRunSnapshot{}, fmt.Errorf("begin create workflow run %q: %w", snapshot.ID, err)
	}
	defer rollbackPostgresTx(ctx, tx)

	definition, found, err := loadPostgresWorkflowTx(ctx, tx, snapshot.WorkflowID)
	if err != nil {
		return execution.WorkflowRunSnapshot{}, err
	}
	if !found {
		return execution.WorkflowRunSnapshot{}, &execution.WorkflowNotFoundError{WorkflowID: snapshot.WorkflowID}
	}
	run, err := execution.RestoreWorkflowRun(snapshot, definition)
	if err != nil {
		return execution.WorkflowRunSnapshot{}, fmt.Errorf("validate workflow run against definition: %w", err)
	}

	created := run.Snapshot()
	created.Version = 1
	_, err = tx.Exec(ctx, `
		INSERT INTO workflow_runs (
			run_id, workflow_id, version, status, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6)
	`, created.ID, created.WorkflowID, int64(created.Version), created.Status, created.CreatedAt, created.UpdatedAt)
	if err != nil {
		if postgresErrorCode(err, "23505") {
			return execution.WorkflowRunSnapshot{}, &execution.RunAlreadyExistsError{RunID: created.ID}
		}
		return execution.WorkflowRunSnapshot{}, fmt.Errorf("insert workflow run %q: %w", created.ID, err)
	}
	if err := writePostgresRunChildren(ctx, tx, created); err != nil {
		return execution.WorkflowRunSnapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return execution.WorkflowRunSnapshot{}, fmt.Errorf("commit workflow run %q: %w", created.ID, err)
	}
	return created, nil
}

// SaveRun replaces one workflow-run aggregate if Version still matches the
// database. The conditional parent UPDATE serializes contenders; children and
// the incremented version commit in the same transaction.
func (store *PostgresStore) SaveRun(
	ctx context.Context,
	snapshot execution.WorkflowRunSnapshot,
) (execution.WorkflowRunSnapshot, error) {
	if err := contextError(ctx); err != nil {
		return execution.WorkflowRunSnapshot{}, err
	}
	if err := snapshot.Validate(); err != nil {
		return execution.WorkflowRunSnapshot{}, fmt.Errorf("save workflow run: %w", err)
	}
	if snapshot.Version == 0 || snapshot.Version >= math.MaxInt64 {
		return execution.WorkflowRunSnapshot{}, fmt.Errorf(
			"save workflow run %q: version %d is outside PostgreSQL bigint range",
			snapshot.ID,
			snapshot.Version,
		)
	}

	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return execution.WorkflowRunSnapshot{}, fmt.Errorf("begin save workflow run %q: %w", snapshot.ID, err)
	}
	defer rollbackPostgresTx(ctx, tx)
	definition, found, err := loadPostgresWorkflowTx(ctx, tx, snapshot.WorkflowID)
	if err != nil {
		return execution.WorkflowRunSnapshot{}, err
	}
	if !found {
		return execution.WorkflowRunSnapshot{}, &execution.WorkflowNotFoundError{WorkflowID: snapshot.WorkflowID}
	}
	run, err := execution.RestoreWorkflowRun(snapshot, definition)
	if err != nil {
		return execution.WorkflowRunSnapshot{}, fmt.Errorf("validate workflow run against definition: %w", err)
	}
	snapshot = run.Snapshot()

	var nextVersion int64
	err = tx.QueryRow(ctx, `
		UPDATE workflow_runs
		SET status = $1, updated_at = $2, version = version + 1
		WHERE run_id = $3 AND workflow_id = $4 AND version = $5
		RETURNING version
	`, snapshot.Status, snapshot.UpdatedAt, snapshot.ID, snapshot.WorkflowID, int64(snapshot.Version)).Scan(&nextVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		var actualVersion int64
		lookupErr := tx.QueryRow(
			ctx,
			`SELECT version FROM workflow_runs WHERE run_id = $1`,
			snapshot.ID,
		).Scan(&actualVersion)
		switch {
		case errors.Is(lookupErr, pgx.ErrNoRows):
			return execution.WorkflowRunSnapshot{}, &execution.RunNotFoundError{RunID: snapshot.ID}
		case lookupErr != nil:
			return execution.WorkflowRunSnapshot{}, fmt.Errorf("read current version for workflow run %q: %w", snapshot.ID, lookupErr)
		default:
			return execution.WorkflowRunSnapshot{}, &execution.RunVersionConflictError{
				RunID:           snapshot.ID,
				ExpectedVersion: snapshot.Version,
				ActualVersion:   uint64(actualVersion),
			}
		}
	}
	if err != nil {
		return execution.WorkflowRunSnapshot{}, fmt.Errorf("conditionally update workflow run %q: %w", snapshot.ID, err)
	}

	for _, task := range snapshot.Tasks {
		tag, err := tx.Exec(ctx, `
			UPDATE task_runs SET
				status = $1,
				output = $2,
				error_message = $3,
				attempt_count = $4,
				current_attempt_id = $5,
				next_attempt_at = $6,
				updated_at = $7,
				started_at = $8,
				finished_at = $9
			WHERE run_id = $10 AND task_id = $11 AND task_run_id = $12
		`,
			task.Status,
			task.Output,
			task.Error,
			task.AttemptCount,
			task.CurrentAttemptID,
			nullablePostgresTime(task.NextAttemptAt),
			task.UpdatedAt,
			nullablePostgresTime(task.StartedAt),
			nullablePostgresTime(task.FinishedAt),
			snapshot.ID,
			task.TaskID,
			task.TaskRunID,
		)
		if err != nil {
			return execution.WorkflowRunSnapshot{}, fmt.Errorf("update task run %q in workflow run %q: %w", task.TaskID, snapshot.ID, err)
		}
		if tag.RowsAffected() != 1 {
			return execution.WorkflowRunSnapshot{}, fmt.Errorf(
				"update task run %q in workflow run %q: persisted task identity is missing",
				task.TaskID,
				snapshot.ID,
			)
		}
	}
	if err := replacePostgresWorkersAndLeases(ctx, tx, snapshot); err != nil {
		return execution.WorkflowRunSnapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return execution.WorkflowRunSnapshot{}, fmt.Errorf("commit workflow run %q at version %d: %w", snapshot.ID, nextVersion, err)
	}

	stored := cloneSnapshot(snapshot)
	stored.Version = uint64(nextVersion)
	return stored, nil
}

// LoadRun reads an aggregate from one repeatable-read transaction so the
// parent version, task states, workers, and leases come from one database view.
func (store *PostgresStore) LoadRun(
	ctx context.Context,
	runID execution.RunID,
) (execution.WorkflowRunSnapshot, bool, error) {
	if err := contextError(ctx); err != nil {
		return execution.WorkflowRunSnapshot{}, false, err
	}
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return execution.WorkflowRunSnapshot{}, false, fmt.Errorf("begin load workflow run %q: %w", runID, err)
	}
	defer rollbackPostgresTx(ctx, tx)

	snapshot, found, err := loadPostgresRunTx(ctx, tx, runID)
	if err != nil {
		return execution.WorkflowRunSnapshot{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return execution.WorkflowRunSnapshot{}, false, fmt.Errorf("commit load workflow run %q: %w", runID, err)
	}
	return snapshot, found, nil
}

func loadPostgresWorkflowTx(
	ctx context.Context,
	tx pgx.Tx,
	workflowID workflow.WorkflowID,
) (workflow.WorkflowDefinition, bool, error) {
	var storedWorkflowID string
	err := tx.QueryRow(
		ctx,
		`SELECT workflow_id FROM workflow_definitions WHERE workflow_id = $1`,
		workflowID,
	).Scan(&storedWorkflowID)
	if errors.Is(err, pgx.ErrNoRows) {
		return workflow.WorkflowDefinition{}, false, nil
	}
	if err != nil {
		return workflow.WorkflowDefinition{}, false, fmt.Errorf("load workflow definition %q: %w", workflowID, err)
	}

	rows, err := tx.Query(ctx, `
		SELECT task_id, task_name, handler_name, task_input,
		       max_attempts, initial_backoff_ns, max_backoff_ns
		FROM task_definitions
		WHERE workflow_id = $1
		ORDER BY position
	`, workflowID)
	if err != nil {
		return workflow.WorkflowDefinition{}, false, fmt.Errorf("load tasks for workflow definition %q: %w", workflowID, err)
	}
	defer rows.Close()

	definition := workflow.WorkflowDefinition{ID: workflow.WorkflowID(storedWorkflowID)}
	for rows.Next() {
		var taskID, taskName, handlerName, input string
		var maxAttempts int
		var initialBackoff, maxBackoff int64
		if err := rows.Scan(
			&taskID,
			&taskName,
			&handlerName,
			&input,
			&maxAttempts,
			&initialBackoff,
			&maxBackoff,
		); err != nil {
			return workflow.WorkflowDefinition{}, false, fmt.Errorf("scan task in workflow definition %q: %w", workflowID, err)
		}
		definition.Tasks = append(definition.Tasks, workflow.TaskDefinition{
			ID:      workflow.TaskID(taskID),
			Name:    taskName,
			Handler: workflow.HandlerName(handlerName),
			Input:   input,
			Retry: workflow.RetryPolicy{
				MaxAttempts:    maxAttempts,
				InitialBackoff: time.Duration(initialBackoff),
				MaxBackoff:     time.Duration(maxBackoff),
			},
		})
	}
	if err := rows.Err(); err != nil {
		return workflow.WorkflowDefinition{}, false, fmt.Errorf("iterate tasks for workflow definition %q: %w", workflowID, err)
	}
	rows.Close()

	dependencies, err := tx.Query(ctx, `
		SELECT task_id, dependency_task_id
		FROM task_dependencies
		WHERE workflow_id = $1
		ORDER BY task_id, position
	`, workflowID)
	if err != nil {
		return workflow.WorkflowDefinition{}, false, fmt.Errorf("load dependencies for workflow definition %q: %w", workflowID, err)
	}
	defer dependencies.Close()
	taskIndexes := make(map[workflow.TaskID]int, len(definition.Tasks))
	for index := range definition.Tasks {
		taskIndexes[definition.Tasks[index].ID] = index
	}
	for dependencies.Next() {
		var taskID, dependencyID string
		if err := dependencies.Scan(&taskID, &dependencyID); err != nil {
			return workflow.WorkflowDefinition{}, false, fmt.Errorf("scan dependency in workflow definition %q: %w", workflowID, err)
		}
		index, exists := taskIndexes[workflow.TaskID(taskID)]
		if !exists {
			return workflow.WorkflowDefinition{}, false, fmt.Errorf(
				"load workflow definition %q: dependency references missing task %q",
				workflowID,
				taskID,
			)
		}
		definition.Tasks[index].Dependencies = append(
			definition.Tasks[index].Dependencies,
			workflow.TaskID(dependencyID),
		)
	}
	if err := dependencies.Err(); err != nil {
		return workflow.WorkflowDefinition{}, false, fmt.Errorf("iterate dependencies for workflow definition %q: %w", workflowID, err)
	}
	if err := definition.Validate(); err != nil {
		return workflow.WorkflowDefinition{}, false, fmt.Errorf("validate loaded workflow definition %q: %w", workflowID, err)
	}
	return definition, true, nil
}

func writePostgresRunChildren(ctx context.Context, tx pgx.Tx, snapshot execution.WorkflowRunSnapshot) error {
	for _, task := range snapshot.Tasks {
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_runs (
				run_id, workflow_id, task_id, task_run_id, status, output,
				error_message, attempt_count, current_attempt_id, next_attempt_at,
				updated_at, started_at, finished_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		`,
			snapshot.ID,
			snapshot.WorkflowID,
			task.TaskID,
			task.TaskRunID,
			task.Status,
			task.Output,
			task.Error,
			task.AttemptCount,
			task.CurrentAttemptID,
			nullablePostgresTime(task.NextAttemptAt),
			task.UpdatedAt,
			nullablePostgresTime(task.StartedAt),
			nullablePostgresTime(task.FinishedAt),
		); err != nil {
			return fmt.Errorf("insert task run %q in workflow run %q: %w", task.TaskID, snapshot.ID, err)
		}
	}
	return insertPostgresWorkersAndLeases(ctx, tx, snapshot)
}

func replacePostgresWorkersAndLeases(ctx context.Context, tx pgx.Tx, snapshot execution.WorkflowRunSnapshot) error {
	if _, err := tx.Exec(ctx, `DELETE FROM task_leases WHERE run_id = $1`, snapshot.ID); err != nil {
		return fmt.Errorf("delete leases for workflow run %q: %w", snapshot.ID, err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM workers WHERE run_id = $1`, snapshot.ID); err != nil {
		return fmt.Errorf("delete workers for workflow run %q: %w", snapshot.ID, err)
	}
	return insertPostgresWorkersAndLeases(ctx, tx, snapshot)
}

func insertPostgresWorkersAndLeases(ctx context.Context, tx pgx.Tx, snapshot execution.WorkflowRunSnapshot) error {
	for _, worker := range snapshot.Workers {
		if _, err := tx.Exec(ctx, `
			INSERT INTO workers (run_id, worker_id, last_heartbeat_at)
			VALUES ($1, $2, $3)
		`, snapshot.ID, worker.WorkerID, worker.LastHeartbeatAt); err != nil {
			return fmt.Errorf("insert worker %q for workflow run %q: %w", worker.WorkerID, snapshot.ID, err)
		}
	}
	for _, task := range snapshot.Tasks {
		if task.Lease == nil {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_leases (
				run_id, task_id, task_run_id, worker_id, attempt_id, expires_at
			) VALUES ($1, $2, $3, $4, $5, $6)
		`,
			snapshot.ID,
			task.TaskID,
			task.Lease.TaskRunID,
			task.Lease.WorkerID,
			task.Lease.AttemptID,
			task.Lease.ExpiresAt,
		); err != nil {
			return fmt.Errorf("insert lease for task %q in workflow run %q: %w", task.TaskID, snapshot.ID, err)
		}
	}
	return nil
}

func loadPostgresRunTx(
	ctx context.Context,
	tx pgx.Tx,
	runID execution.RunID,
) (execution.WorkflowRunSnapshot, bool, error) {
	var storedRunID, workflowID, status string
	var version int64
	var createdAt, updatedAt time.Time
	err := tx.QueryRow(ctx, `
		SELECT run_id, workflow_id, version, status, created_at, updated_at
		FROM workflow_runs
		WHERE run_id = $1
	`, runID).Scan(&storedRunID, &workflowID, &version, &status, &createdAt, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return execution.WorkflowRunSnapshot{}, false, nil
	}
	if err != nil {
		return execution.WorkflowRunSnapshot{}, false, fmt.Errorf("load workflow run %q: %w", runID, err)
	}
	if version < 1 {
		return execution.WorkflowRunSnapshot{}, false, fmt.Errorf("load workflow run %q: invalid version %d", runID, version)
	}
	snapshot := execution.WorkflowRunSnapshot{
		ID:         execution.RunID(storedRunID),
		WorkflowID: workflow.WorkflowID(workflowID),
		Version:    uint64(version),
		Status:     execution.WorkflowRunStatus(status),
		CreatedAt:  createdAt,
		UpdatedAt:  updatedAt,
	}

	rows, err := tx.Query(ctx, `
		SELECT task_id, task_run_id, status, output, error_message, attempt_count,
		       current_attempt_id, next_attempt_at, updated_at, started_at, finished_at
		FROM task_runs
		WHERE run_id = $1
		ORDER BY task_id
	`, runID)
	if err != nil {
		return execution.WorkflowRunSnapshot{}, false, fmt.Errorf("load tasks for workflow run %q: %w", runID, err)
	}
	defer rows.Close()
	taskIndexes := make(map[workflow.TaskID]int)
	for rows.Next() {
		var taskID, taskRunID, taskStatus, output, taskError, attemptID string
		var attemptCount int
		var nextAttemptAt, startedAt, finishedAt pgtype.Timestamptz
		var taskUpdatedAt time.Time
		if err := rows.Scan(
			&taskID,
			&taskRunID,
			&taskStatus,
			&output,
			&taskError,
			&attemptCount,
			&attemptID,
			&nextAttemptAt,
			&taskUpdatedAt,
			&startedAt,
			&finishedAt,
		); err != nil {
			return execution.WorkflowRunSnapshot{}, false, fmt.Errorf("scan task in workflow run %q: %w", runID, err)
		}
		task := execution.TaskRun{
			TaskID:           workflow.TaskID(taskID),
			TaskRunID:        execution.TaskRunID(taskRunID),
			Status:           execution.TaskRunStatus(taskStatus),
			Output:           output,
			Error:            taskError,
			AttemptCount:     attemptCount,
			CurrentAttemptID: execution.AttemptID(attemptID),
			UpdatedAt:        taskUpdatedAt,
		}
		if nextAttemptAt.Valid {
			task.NextAttemptAt = nextAttemptAt.Time
		}
		if startedAt.Valid {
			task.StartedAt = startedAt.Time
		}
		if finishedAt.Valid {
			task.FinishedAt = finishedAt.Time
		}
		taskIndexes[task.TaskID] = len(snapshot.Tasks)
		snapshot.Tasks = append(snapshot.Tasks, task)
	}
	if err := rows.Err(); err != nil {
		return execution.WorkflowRunSnapshot{}, false, fmt.Errorf("iterate tasks for workflow run %q: %w", runID, err)
	}
	rows.Close()

	workers, err := tx.Query(ctx, `
		SELECT worker_id, last_heartbeat_at
		FROM workers
		WHERE run_id = $1
		ORDER BY worker_id
	`, runID)
	if err != nil {
		return execution.WorkflowRunSnapshot{}, false, fmt.Errorf("load workers for workflow run %q: %w", runID, err)
	}
	defer workers.Close()
	for workers.Next() {
		var workerID string
		var lastHeartbeatAt time.Time
		if err := workers.Scan(&workerID, &lastHeartbeatAt); err != nil {
			return execution.WorkflowRunSnapshot{}, false, fmt.Errorf("scan worker in workflow run %q: %w", runID, err)
		}
		snapshot.Workers = append(snapshot.Workers, execution.WorkerHeartbeat{
			WorkerID:        execution.WorkerID(workerID),
			LastHeartbeatAt: lastHeartbeatAt,
		})
	}
	if err := workers.Err(); err != nil {
		return execution.WorkflowRunSnapshot{}, false, fmt.Errorf("iterate workers for workflow run %q: %w", runID, err)
	}
	workers.Close()

	leases, err := tx.Query(ctx, `
		SELECT task_id, task_run_id, worker_id, attempt_id, expires_at
		FROM task_leases
		WHERE run_id = $1
		ORDER BY task_id
	`, runID)
	if err != nil {
		return execution.WorkflowRunSnapshot{}, false, fmt.Errorf("load leases for workflow run %q: %w", runID, err)
	}
	defer leases.Close()
	for leases.Next() {
		var taskID, taskRunID, workerID, attemptID string
		var expiresAt time.Time
		if err := leases.Scan(&taskID, &taskRunID, &workerID, &attemptID, &expiresAt); err != nil {
			return execution.WorkflowRunSnapshot{}, false, fmt.Errorf("scan lease in workflow run %q: %w", runID, err)
		}
		index, exists := taskIndexes[workflow.TaskID(taskID)]
		if !exists {
			return execution.WorkflowRunSnapshot{}, false, fmt.Errorf(
				"load workflow run %q: lease references missing task %q",
				runID,
				taskID,
			)
		}
		snapshot.Tasks[index].Lease = &execution.TaskLease{
			WorkerID:  execution.WorkerID(workerID),
			TaskRunID: execution.TaskRunID(taskRunID),
			AttemptID: execution.AttemptID(attemptID),
			ExpiresAt: expiresAt,
		}
	}
	if err := leases.Err(); err != nil {
		return execution.WorkflowRunSnapshot{}, false, fmt.Errorf("iterate leases for workflow run %q: %w", runID, err)
	}
	if err := snapshot.Validate(); err != nil {
		return execution.WorkflowRunSnapshot{}, false, fmt.Errorf("validate loaded workflow run %q: %w", runID, err)
	}
	return snapshot, true, nil
}

func nullablePostgresTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func postgresErrorCode(err error, code string) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) && postgresError.Code == code
}

func rollbackPostgresTx(ctx context.Context, tx pgx.Tx) {
	rollbackErr := tx.Rollback(context.WithoutCancel(ctx))
	if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
		// The operation already has a more actionable error. PostgreSQL will
		// release an uncommitted transaction when its connection is closed.
		return
	}
}

var _ execution.Store = (*PostgresStore)(nil)
