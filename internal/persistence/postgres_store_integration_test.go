//go:build integration

package persistence

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vijaypratap3364/forgeflow/internal/execution"
	"github.com/vijaypratap3364/forgeflow/internal/security"
	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

const postgresTestDSNEnvironment = "FORGEFLOW_POSTGRES_TEST_DSN"

func TestPostgresStoreProjectAuthorizationContract(t *testing.T) {
	store := openIntegrationPostgresStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now().UTC().Truncate(time.Microsecond)
	admin := security.User{ID: "pg-admin", DisplayName: "Postgres Admin", CreatedAt: now}
	project := security.Project{ID: "pg-project", Name: "Postgres Project", CreatedBy: admin.ID, CreatedAt: now, UpdatedAt: now}
	adminRole := security.Membership{ProjectID: project.ID, UserID: admin.ID, Role: security.RoleAdmin, CreatedAt: now, UpdatedAt: now}
	createdEvent := security.AuditEvent{ID: "pg-audit-create", ProjectID: project.ID, ActorUserID: admin.ID,
		Action: "project.created", ResourceType: "project", ResourceID: string(project.ID), OccurredAt: now}
	if err := store.CreateProject(ctx, admin, project, adminRole, createdEvent); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}

	member := security.User{ID: "pg-member", DisplayName: "Postgres Member", CreatedAt: now.Add(time.Second)}
	membership := security.Membership{ProjectID: project.ID, UserID: member.ID, Role: security.RoleMember,
		CreatedAt: member.CreatedAt, UpdatedAt: member.CreatedAt}
	membershipEvent := security.AuditEvent{ID: "pg-audit-member", ProjectID: project.ID, ActorUserID: admin.ID,
		Action: "membership.updated", ResourceType: "user", ResourceID: string(member.ID), OccurredAt: member.CreatedAt,
		Metadata: map[string]string{"new_role": "member"}}
	if err := store.PutMembership(ctx, member, membership, membershipEvent); err != nil {
		t.Fatalf("PutMembership() error = %v", err)
	}
	updatedProject := project
	updatedProject.Name = "Renamed PostgreSQL Project"
	updatedProject.UpdatedAt = now.Add(2 * time.Second)
	projectUpdateEvent := security.AuditEvent{ID: "pg-audit-project-update", ProjectID: project.ID, ActorUserID: admin.ID,
		Action: "project.updated", ResourceType: "project", ResourceID: string(project.ID), OccurredAt: updatedProject.UpdatedAt,
		Metadata: map[string]string{"old_name": project.Name, "new_name": updatedProject.Name}}
	if err := store.SaveProject(ctx, updatedProject, projectUpdateEvent); err != nil {
		t.Fatalf("SaveProject() error = %v", err)
	}
	gotProject, found, err := store.LoadProject(ctx, project.ID)
	if err != nil || !found || gotProject.ID != updatedProject.ID || gotProject.Name != updatedProject.Name ||
		gotProject.CreatedBy != updatedProject.CreatedBy || !gotProject.CreatedAt.Equal(updatedProject.CreatedAt) ||
		!gotProject.UpdatedAt.Equal(updatedProject.UpdatedAt) {
		t.Fatalf("LoadProject() = %#v, %v, %v; want %#v, true, nil", gotProject, found, err, updatedProject)
	}

	definition := workflow.WorkflowDefinition{ID: "pg-owned-workflow", Tasks: []workflow.TaskDefinition{
		{ID: "task", Name: "Task", Handler: "noop"},
	}}
	ownership := security.WorkflowOwnership{WorkflowID: definition.ID, ProjectID: project.ID,
		OwnerUserID: member.ID, CreatedAt: now.Add(3 * time.Second)}
	if err := store.SaveWorkflowForProject(ctx, definition, ownership); err != nil {
		t.Fatalf("SaveWorkflowForProject() error = %v", err)
	}
	gotOwnership, found, err := store.LoadWorkflowOwnership(ctx, definition.ID)
	if err != nil || !found || gotOwnership.WorkflowID != ownership.WorkflowID ||
		gotOwnership.ProjectID != ownership.ProjectID || gotOwnership.OwnerUserID != ownership.OwnerUserID ||
		!gotOwnership.CreatedAt.Equal(ownership.CreatedAt) {
		t.Fatalf("LoadWorkflowOwnership() = %#v, %v, %v; want %#v, true, nil", gotOwnership, found, err, ownership)
	}
	memberships, err := store.ListMemberships(ctx, project.ID)
	if err != nil || len(memberships) != 2 {
		t.Fatalf("ListMemberships() = %#v, error %v", memberships, err)
	}
	events, err := store.ListAuditEvents(ctx, project.ID, 10)
	if err != nil || len(events) != 3 || events[0].ID != projectUpdateEvent.ID ||
		!reflect.DeepEqual(events[0].Metadata, projectUpdateEvent.Metadata) {
		t.Fatalf("ListAuditEvents() = %#v, error %v", events, err)
	}

	rejectedUpdate := updatedProject
	rejectedUpdate.Name = "Must Roll Back"
	rejectedUpdate.UpdatedAt = now.Add(4 * time.Second)
	duplicateAudit := security.AuditEvent{ID: createdEvent.ID, ProjectID: project.ID, ActorUserID: admin.ID,
		Action: "project.updated", ResourceType: "project", ResourceID: string(project.ID), OccurredAt: rejectedUpdate.UpdatedAt}
	var auditConflict *security.AuditEventAlreadyExistsError
	if err := store.SaveProject(ctx, rejectedUpdate, duplicateAudit); !errors.As(err, &auditConflict) {
		t.Fatalf("SaveProject() duplicate audit error = %T %v, want *AuditEventAlreadyExistsError", err, err)
	}
	rolledBackProject, found, err := store.LoadProject(ctx, project.ID)
	if err != nil || !found || rolledBackProject.Name != updatedProject.Name ||
		!rolledBackProject.UpdatedAt.Equal(updatedProject.UpdatedAt) {
		t.Fatalf("LoadProject() after rolled-back audit = %#v, %v, %v; want %#v, true, nil", rolledBackProject, found, err, updatedProject)
	}
	rolledBackEvents, err := store.ListAuditEvents(ctx, project.ID, 10)
	if err != nil || len(rolledBackEvents) != 3 {
		t.Fatalf("ListAuditEvents() after rolled-back audit = %#v, error %v", rolledBackEvents, err)
	}

	outsider := security.User{ID: "pg-outsider", DisplayName: "PostgreSQL Outsider", CreatedAt: now.Add(5 * time.Second)}
	otherProject := security.Project{ID: "pg-other-project", Name: "Other Project", CreatedBy: outsider.ID,
		CreatedAt: outsider.CreatedAt, UpdatedAt: outsider.CreatedAt}
	otherMembership := security.Membership{ProjectID: otherProject.ID, UserID: outsider.ID, Role: security.RoleAdmin,
		CreatedAt: outsider.CreatedAt, UpdatedAt: outsider.CreatedAt}
	otherEvent := security.AuditEvent{ID: "pg-other-project-created", ProjectID: otherProject.ID, ActorUserID: outsider.ID,
		Action: "project.created", ResourceType: "project", ResourceID: string(otherProject.ID), OccurredAt: outsider.CreatedAt}
	if err := store.CreateProject(ctx, outsider, otherProject, otherMembership, otherEvent); err != nil {
		t.Fatalf("CreateProject(other) error = %v", err)
	}
	rejectedDefinition := workflow.WorkflowDefinition{ID: "pg-rejected-owner", Tasks: []workflow.TaskDefinition{
		{ID: "task", Name: "Task", Handler: "noop"},
	}}
	err = store.SaveWorkflowForProject(ctx, rejectedDefinition, security.WorkflowOwnership{
		WorkflowID: rejectedDefinition.ID, ProjectID: project.ID, OwnerUserID: outsider.ID, CreatedAt: now.Add(6 * time.Second),
	})
	if err == nil {
		t.Fatal("SaveWorkflowForProject() cross-project owner error = nil")
	}
	if _, found, loadErr := store.LoadWorkflow(ctx, rejectedDefinition.ID); loadErr != nil || found {
		t.Fatalf("LoadWorkflow() after rejected ownership = found %v, error %v", found, loadErr)
	}
	if _, found, loadErr := store.LoadWorkflowOwnership(ctx, rejectedDefinition.ID); loadErr != nil || found {
		t.Fatalf("LoadWorkflowOwnership() after rejected ownership = found %v, error %v", found, loadErr)
	}
}

func TestPostgresStoreWorkflowAndRunContract(t *testing.T) {
	store := openIntegrationPostgresStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	definition := workflow.WorkflowDefinition{
		ID: "postgres-contract",
		Tasks: []workflow.TaskDefinition{
			{
				ID:           "second",
				Name:         "Second",
				Dependencies: []workflow.TaskID{"first"},
				Handler:      "uppercase",
				Input:        "forgeflow",
				Retry: workflow.RetryPolicy{
					MaxAttempts:    3,
					InitialBackoff: 5 * time.Millisecond,
					MaxBackoff:     20 * time.Millisecond,
				},
			},
			{ID: "first", Name: "First"},
		},
	}
	if err := store.SaveWorkflow(ctx, definition); err != nil {
		t.Fatalf("SaveWorkflow() error = %v", err)
	}
	if err := store.SaveWorkflow(ctx, definition); err != nil {
		t.Fatalf("idempotent SaveWorkflow() error = %v", err)
	}
	loadedDefinition, found, err := store.LoadWorkflow(ctx, definition.ID)
	if err != nil {
		t.Fatalf("LoadWorkflow() error = %v", err)
	}
	if !found || !reflect.DeepEqual(loadedDefinition, definition) {
		t.Fatalf("LoadWorkflow() = %#v, %t; want %#v, true", loadedDefinition, found, definition)
	}

	conflictingDefinition := definition
	conflictingDefinition.Tasks = append([]workflow.TaskDefinition(nil), definition.Tasks...)
	conflictingDefinition.Tasks[0].Name = "Changed"
	var workflowConflict *execution.WorkflowConflictError
	if err := store.SaveWorkflow(ctx, conflictingDefinition); !errors.As(err, &workflowConflict) {
		t.Fatalf("conflicting SaveWorkflow() error = %T %v, want *WorkflowConflictError", err, err)
	}

	run, err := execution.NewWorkflowRun("postgres-contract-run", definition)
	if err != nil {
		t.Fatalf("NewWorkflowRun() error = %v", err)
	}
	initial := truncateSnapshotTimes(run.Snapshot())
	created, err := store.CreateRun(ctx, initial)
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	if created.Version != 1 {
		t.Fatalf("CreateRun() version = %d, want 1", created.Version)
	}
	var duplicateRun *execution.RunAlreadyExistsError
	if _, err := store.CreateRun(ctx, initial); !errors.As(err, &duplicateRun) {
		t.Fatalf("duplicate CreateRun() error = %T %v, want *RunAlreadyExistsError", err, err)
	}
	invalidAggregate := created
	invalidAggregate.Tasks = invalidAggregate.Tasks[:1]
	var invalidSnapshot *execution.SnapshotValidationError
	if _, err := store.SaveRun(ctx, invalidAggregate); !errors.As(err, &invalidSnapshot) {
		t.Fatalf("invalid SaveRun() error = %T %v, want *SnapshotValidationError", err, err)
	}
	unchanged, found, err := store.LoadRun(ctx, created.ID)
	if err != nil || !found || unchanged.Version != 1 {
		t.Fatalf("LoadRun() after rejected aggregate = version %d, found %t, error %v", unchanged.Version, found, err)
	}

	start := make(chan struct{})
	results := make(chan saveRunResult, 2)
	for index := 0; index < 2; index++ {
		go func() {
			<-start
			stored, err := store.SaveRun(ctx, created)
			results <- saveRunResult{snapshot: stored, err: err}
		}()
	}
	close(start)
	winners := 0
	conflicts := 0
	for index := 0; index < 2; index++ {
		result := <-results
		if result.err == nil {
			winners++
			if result.snapshot.Version != 2 {
				t.Errorf("winning SaveRun() version = %d, want 2", result.snapshot.Version)
			}
			continue
		}
		var versionConflict *execution.RunVersionConflictError
		if !errors.As(result.err, &versionConflict) {
			t.Errorf("losing SaveRun() error = %T %v, want *RunVersionConflictError", result.err, result.err)
			continue
		}
		conflicts++
		if versionConflict.ExpectedVersion != 1 || versionConflict.ActualVersion != 2 {
			t.Errorf("version conflict = %#v, want expected 1 actual 2", versionConflict)
		}
	}
	if winners != 1 || conflicts != 1 {
		t.Fatalf("concurrent SaveRun() results = %d winners, %d conflicts", winners, conflicts)
	}

	loadedRun, found, err := store.LoadRun(ctx, created.ID)
	if err != nil {
		t.Fatalf("LoadRun() error = %v", err)
	}
	if !found || loadedRun.Version != 2 {
		t.Fatalf("LoadRun() = version %d, found %t; want version 2, found", loadedRun.Version, found)
	}
	if _, found, err := store.LoadRun(ctx, "missing-run"); err != nil || found {
		t.Fatalf("LoadRun(missing) = found %t, error %v", found, err)
	}
}

func TestPostgresStorePersistsEngineLeasesAndCompletion(t *testing.T) {
	store := openIntegrationPostgresStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	started := make(chan struct{})
	release := make(chan struct{})
	registry := execution.NewHandlerRegistry()
	if err := registry.Register("blocking", execution.TaskHandlerFunc(func(ctx context.Context, _ execution.TaskRequest) (string, error) {
		close(started)
		select {
		case <-release:
			return "persisted", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	engine, err := execution.NewEngine(
		1,
		registry,
		store,
		execution.WithWorkerNamespace("postgres-integration"),
	)
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	definition := workflow.WorkflowDefinition{
		ID: "postgres-engine",
		Tasks: []workflow.TaskDefinition{
			{ID: "task", Name: "Task", Handler: "blocking"},
		},
	}

	type executionResult struct {
		run *execution.WorkflowRun
		err error
	}
	result := make(chan executionResult, 1)
	go func() {
		run, err := engine.Execute(ctx, "postgres-engine-run", definition)
		result <- executionResult{run: run, err: err}
	}()
	waitForIntegrationSignal(t, started, "blocking handler to start")

	running, found, err := store.LoadRun(ctx, "postgres-engine-run")
	if err != nil {
		t.Fatalf("LoadRun(running) error = %v", err)
	}
	if !found || len(running.Tasks) != 1 || running.Tasks[0].Status != execution.TaskRunRunning {
		t.Fatalf("running snapshot = %#v, found %t", running, found)
	}
	if running.Tasks[0].Lease == nil || len(running.Workers) != 1 {
		t.Fatalf("running snapshot did not persist worker and lease: %#v", running)
	}
	if running.Tasks[0].Lease.WorkerID != running.Workers[0].WorkerID {
		t.Fatalf("lease worker = %q, heartbeat worker = %q", running.Tasks[0].Lease.WorkerID, running.Workers[0].WorkerID)
	}

	close(release)
	select {
	case completed := <-result:
		if completed.err != nil {
			t.Fatalf("Engine.Execute() error = %v", completed.err)
		}
		if completed.run.Status() != execution.WorkflowRunSucceeded {
			t.Fatalf("Engine.Execute() status = %q, want succeeded", completed.run.Status())
		}
	case <-ctx.Done():
		t.Fatalf("Engine.Execute() did not finish: %v", ctx.Err())
	}

	completed, found, err := store.LoadRun(ctx, "postgres-engine-run")
	if err != nil {
		t.Fatalf("LoadRun(completed) error = %v", err)
	}
	if !found || completed.Status != execution.WorkflowRunSucceeded {
		t.Fatalf("completed snapshot status = %q, found %t", completed.Status, found)
	}
	if completed.Tasks[0].Status != execution.TaskRunSucceeded || completed.Tasks[0].Output != "persisted" {
		t.Fatalf("completed task = %#v", completed.Tasks[0])
	}
	if completed.Tasks[0].Lease != nil || completed.Tasks[0].AttemptCount != 1 {
		t.Fatalf("completed task lease/attempt state = %#v", completed.Tasks[0])
	}
}

func TestPostgresMigrationCreatesClaimConstraintsAndIndexes(t *testing.T) {
	store := openIntegrationPostgresStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	constraintRows, err := store.pool.Query(ctx, `
		SELECT conname
		FROM pg_constraint
		WHERE connamespace = current_schema()::regnamespace
	`)
	if err != nil {
		t.Fatalf("query constraints: %v", err)
	}
	constraints, err := pgx.CollectRows(constraintRows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("collect constraints: %v", err)
	}
	sort.Strings(constraints)
	for _, constraint := range []string{
		"project_memberships_role_valid",
		"task_definitions_pkey",
		"task_dependencies_dependency_fk",
		"task_leases_attempt_unique",
		"task_leases_task_fk",
		"task_leases_worker_unique",
		"workflow_runs_status_valid",
		"workflow_runs_workflow_fk",
		"workflow_ownership_member_fk",
	} {
		position := sort.SearchStrings(constraints, constraint)
		if position == len(constraints) || constraints[position] != constraint {
			t.Errorf("constraint %q is missing; got %v", constraint, constraints)
		}
	}

	indexRows, err := store.pool.Query(ctx, `
		SELECT indexname
		FROM pg_indexes
		WHERE schemaname = current_schema()
	`)
	if err != nil {
		t.Fatalf("query indexes: %v", err)
	}
	indexes, err := pgx.CollectRows(indexRows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("collect indexes: %v", err)
	}
	sort.Strings(indexes)
	for _, index := range []string{
		"audit_events_project_occurred_idx",
		"audit_events_pkey",
		"project_memberships_pkey",
		"project_memberships_user_project_idx",
		"task_definitions_position_unique",
		"task_dependencies_position_unique",
		"task_leases_attempt_unique",
		"task_leases_pkey",
		"task_runs_pkey",
		"workers_pkey",
		"workflow_runs_pkey",
		"workflow_ownership_pkey",
		"workflow_ownership_project_workflow_idx",
	} {
		position := sort.SearchStrings(indexes, index)
		if position == len(indexes) || indexes[position] != index {
			t.Errorf("index %q is missing; got %v", index, indexes)
		}
	}

	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate() error = %v", err)
	}
}

type saveRunResult struct {
	snapshot execution.WorkflowRunSnapshot
	err      error
}

func openIntegrationPostgresStore(t *testing.T) *PostgresStore {
	t.Helper()
	dataSourceName := os.Getenv(postgresTestDSNEnvironment)
	if dataSourceName == "" {
		t.Skipf("%s is not set; PostgreSQL integration tests run in CI", postgresTestDSNEnvironment)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, dataSourceName)
	if err != nil {
		t.Fatalf("connect to PostgreSQL test service: %v", err)
	}
	randomSuffix := make([]byte, 8)
	if _, err := rand.Read(randomSuffix); err != nil {
		admin.Close(ctx)
		t.Fatalf("generate test schema name: %v", err)
	}
	schema := "forgeflow_test_" + hex.EncodeToString(randomSuffix)
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		admin.Close(ctx)
		t.Fatalf("create PostgreSQL test schema: %v", err)
	}

	config, err := pgxpool.ParseConfig(dataSourceName)
	if err != nil {
		admin.Close(ctx)
		t.Fatalf("parse PostgreSQL test DSN: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		admin.Close(ctx)
		t.Fatalf("open PostgreSQL test pool: %v", err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		pool.Close()
		admin.Close(ctx)
		t.Fatalf("NewPostgresStore() error = %v", err)
	}
	t.Cleanup(func() {
		store.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Errorf("drop PostgreSQL test schema: %v", err)
		}
		if err := admin.Close(cleanupCtx); err != nil {
			t.Errorf("close PostgreSQL admin connection: %v", err)
		}
	})
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	return store
}

func truncateSnapshotTimes(snapshot execution.WorkflowRunSnapshot) execution.WorkflowRunSnapshot {
	snapshot.CreatedAt = snapshot.CreatedAt.Truncate(time.Microsecond)
	snapshot.UpdatedAt = snapshot.UpdatedAt.Truncate(time.Microsecond)
	for index := range snapshot.Tasks {
		snapshot.Tasks[index].UpdatedAt = snapshot.Tasks[index].UpdatedAt.Truncate(time.Microsecond)
	}
	return snapshot
}

func waitForIntegrationSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}
