package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/vijaypratap3364/forgeflow/internal/security"
	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

// CreateProject transactionally creates the project boundary, creator, first
// admin membership, and audit event.
func (store *PostgresStore) CreateProject(
	ctx context.Context,
	user security.User,
	project security.Project,
	membership security.Membership,
	event security.AuditEvent,
) error {
	if err := validateProjectCreation(ctx, user, project, membership, event); err != nil {
		return err
	}
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin create project %q: %w", project.ID, err)
	}
	defer rollbackPostgresTx(ctx, tx)
	if err := insertPostgresUser(ctx, tx, user); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO projects (project_id, project_name, created_by, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5)
	`, project.ID, project.Name, project.CreatedBy, project.CreatedAt, project.UpdatedAt); err != nil {
		if postgresErrorCode(err, "23505") {
			return &security.ProjectAlreadyExistsError{ProjectID: project.ID}
		}
		return fmt.Errorf("insert project %q: %w", project.ID, err)
	}
	if err := upsertPostgresMembership(ctx, tx, membership); err != nil {
		return err
	}
	if err := insertPostgresAuditEvent(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit project %q: %w", project.ID, err)
	}
	return nil
}

// LoadUser returns one persisted user.
func (store *PostgresStore) LoadUser(ctx context.Context, userID security.UserID) (security.User, bool, error) {
	if err := contextError(ctx); err != nil {
		return security.User{}, false, err
	}
	var user security.User
	err := store.pool.QueryRow(ctx, `
		SELECT user_id, display_name, disabled, created_at FROM users WHERE user_id = $1
	`, userID).Scan(&user.ID, &user.DisplayName, &user.Disabled, &user.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return security.User{}, false, nil
	}
	if err != nil {
		return security.User{}, false, fmt.Errorf("load user %q: %w", userID, err)
	}
	return user, true, nil
}

// LoadProject returns one persisted project.
func (store *PostgresStore) LoadProject(ctx context.Context, projectID security.ProjectID) (security.Project, bool, error) {
	if err := contextError(ctx); err != nil {
		return security.Project{}, false, err
	}
	var project security.Project
	err := store.pool.QueryRow(ctx, `
		SELECT project_id, project_name, created_by, created_at, updated_at
		FROM projects WHERE project_id = $1
	`, projectID).Scan(&project.ID, &project.Name, &project.CreatedBy, &project.CreatedAt, &project.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return security.Project{}, false, nil
	}
	if err != nil {
		return security.Project{}, false, fmt.Errorf("load project %q: %w", projectID, err)
	}
	return project, true, nil
}

// SaveProject updates project metadata and its audit event in one transaction.
func (store *PostgresStore) SaveProject(ctx context.Context, project security.Project, event security.AuditEvent) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := project.Validate(); err != nil {
		return fmt.Errorf("save project: %w", err)
	}
	if err := event.Validate(); err != nil {
		return fmt.Errorf("save project audit event: %w", err)
	}
	if event.ProjectID != project.ID {
		return errors.New("save project: audit event belongs to another project")
	}
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin save project %q: %w", project.ID, err)
	}
	defer rollbackPostgresTx(ctx, tx)
	tag, err := tx.Exec(ctx, `
		UPDATE projects SET project_name = $1, updated_at = $2
		WHERE project_id = $3 AND created_by = $4 AND created_at = $5
	`, project.Name, project.UpdatedAt, project.ID, project.CreatedBy, project.CreatedAt)
	if err != nil {
		return fmt.Errorf("update project %q: %w", project.ID, err)
	}
	if tag.RowsAffected() != 1 {
		return &security.ProjectNotFoundError{ProjectID: project.ID}
	}
	if err := insertPostgresAuditEvent(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit project %q update: %w", project.ID, err)
	}
	return nil
}

// PutMembership upserts a project role and records its audit event atomically.
func (store *PostgresStore) PutMembership(
	ctx context.Context,
	user security.User,
	membership security.Membership,
	event security.AuditEvent,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := user.Validate(); err != nil {
		return fmt.Errorf("put membership user: %w", err)
	}
	if err := membership.Validate(); err != nil {
		return fmt.Errorf("put membership: %w", err)
	}
	if err := event.Validate(); err != nil {
		return fmt.Errorf("put membership audit event: %w", err)
	}
	if membership.UserID != user.ID || event.ProjectID != membership.ProjectID {
		return errors.New("put membership: user or audit project does not match membership")
	}
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin put membership: %w", err)
	}
	defer rollbackPostgresTx(ctx, tx)
	if err := insertPostgresUser(ctx, tx, user); err != nil {
		return err
	}
	if err := upsertPostgresMembership(ctx, tx, membership); err != nil {
		if postgresErrorCode(err, "23503") {
			return &security.ProjectNotFoundError{ProjectID: membership.ProjectID}
		}
		return err
	}
	if err := insertPostgresAuditEvent(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit membership for user %q: %w", user.ID, err)
	}
	return nil
}

// LoadMembership returns one project-scoped role.
func (store *PostgresStore) LoadMembership(
	ctx context.Context,
	projectID security.ProjectID,
	userID security.UserID,
) (security.Membership, bool, error) {
	if err := contextError(ctx); err != nil {
		return security.Membership{}, false, err
	}
	var membership security.Membership
	err := store.pool.QueryRow(ctx, `
		SELECT project_id, user_id, role, created_at, updated_at
		FROM project_memberships WHERE project_id = $1 AND user_id = $2
	`, projectID, userID).Scan(&membership.ProjectID, &membership.UserID, &membership.Role,
		&membership.CreatedAt, &membership.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return security.Membership{}, false, nil
	}
	if err != nil {
		return security.Membership{}, false, fmt.Errorf("load membership: %w", err)
	}
	return membership, true, nil
}

// ListMemberships returns project roles in stable user order.
func (store *PostgresStore) ListMemberships(ctx context.Context, projectID security.ProjectID) ([]security.Membership, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	rows, err := store.pool.Query(ctx, `
		SELECT project_id, user_id, role, created_at, updated_at
		FROM project_memberships WHERE project_id = $1 ORDER BY user_id
	`, projectID)
	if err != nil {
		return nil, fmt.Errorf("list memberships for project %q: %w", projectID, err)
	}
	defer rows.Close()
	var memberships []security.Membership
	for rows.Next() {
		var membership security.Membership
		if err := rows.Scan(&membership.ProjectID, &membership.UserID, &membership.Role,
			&membership.CreatedAt, &membership.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan membership for project %q: %w", projectID, err)
		}
		memberships = append(memberships, membership)
	}
	return memberships, rows.Err()
}

// SaveWorkflowForProject commits the immutable definition and ownership row in
// one transaction.
func (store *PostgresStore) SaveWorkflowForProject(
	ctx context.Context,
	definition workflow.WorkflowDefinition,
	ownership security.WorkflowOwnership,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := definition.Validate(); err != nil {
		return fmt.Errorf("save owned workflow: %w", err)
	}
	if err := ownership.Validate(); err != nil {
		return fmt.Errorf("save workflow ownership: %w", err)
	}
	if ownership.WorkflowID != definition.ID {
		return errors.New("save workflow ownership: definition identity does not match")
	}
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin save owned workflow %q: %w", definition.ID, err)
	}
	defer rollbackPostgresTx(ctx, tx)
	if err := savePostgresWorkflowTx(ctx, tx, definition); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO workflow_ownership (workflow_id, project_id, owner_user_id, created_at)
		VALUES ($1, $2, $3, $4) ON CONFLICT (workflow_id) DO NOTHING
	`, ownership.WorkflowID, ownership.ProjectID, ownership.OwnerUserID, ownership.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert ownership for workflow %q: %w", definition.ID, err)
	}
	if tag.RowsAffected() == 0 {
		var existing security.WorkflowOwnership
		err := tx.QueryRow(ctx, `
			SELECT workflow_id, project_id, owner_user_id, created_at
			FROM workflow_ownership WHERE workflow_id = $1
		`, definition.ID).Scan(&existing.WorkflowID, &existing.ProjectID, &existing.OwnerUserID, &existing.CreatedAt)
		if err != nil || existing != ownership {
			return &security.WorkflowOwnershipConflictError{WorkflowID: definition.ID}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit owned workflow %q: %w", definition.ID, err)
	}
	return nil
}

// LoadWorkflowOwnership resolves a definition to its project and owner.
func (store *PostgresStore) LoadWorkflowOwnership(
	ctx context.Context,
	workflowID workflow.WorkflowID,
) (security.WorkflowOwnership, bool, error) {
	if err := contextError(ctx); err != nil {
		return security.WorkflowOwnership{}, false, err
	}
	var ownership security.WorkflowOwnership
	err := store.pool.QueryRow(ctx, `
		SELECT workflow_id, project_id, owner_user_id, created_at
		FROM workflow_ownership WHERE workflow_id = $1
	`, workflowID).Scan(&ownership.WorkflowID, &ownership.ProjectID, &ownership.OwnerUserID, &ownership.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return security.WorkflowOwnership{}, false, nil
	}
	if err != nil {
		return security.WorkflowOwnership{}, false, fmt.Errorf("load workflow %q ownership: %w", workflowID, err)
	}
	return ownership, true, nil
}

// ListAuditEvents returns newest events first.
func (store *PostgresStore) ListAuditEvents(
	ctx context.Context,
	projectID security.ProjectID,
	limit int,
) ([]security.AuditEvent, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		return nil, errors.New("list audit events: limit must be between 1 and 1000")
	}
	rows, err := store.pool.Query(ctx, `
		SELECT event_id, project_id, actor_user_id, action, resource_type,
		       resource_id, occurred_at, metadata
		FROM audit_events WHERE project_id = $1
		ORDER BY occurred_at DESC, event_id DESC LIMIT $2
	`, projectID, limit)
	if err != nil {
		return nil, fmt.Errorf("list audit events for project %q: %w", projectID, err)
	}
	defer rows.Close()
	var events []security.AuditEvent
	for rows.Next() {
		var event security.AuditEvent
		var metadata []byte
		if err := rows.Scan(&event.ID, &event.ProjectID, &event.ActorUserID, &event.Action,
			&event.ResourceType, &event.ResourceID, &event.OccurredAt, &metadata); err != nil {
			return nil, fmt.Errorf("scan audit event: %w", err)
		}
		if err := json.Unmarshal(metadata, &event.Metadata); err != nil {
			return nil, fmt.Errorf("decode audit event %q metadata: %w", event.ID, err)
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func insertPostgresUser(ctx context.Context, tx pgx.Tx, user security.User) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO users (user_id, display_name, disabled, created_at)
		VALUES ($1, $2, $3, $4) ON CONFLICT (user_id) DO NOTHING
	`, user.ID, user.DisplayName, user.Disabled, user.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert user %q: %w", user.ID, err)
	}
	var disabled bool
	if err := tx.QueryRow(ctx, `SELECT disabled FROM users WHERE user_id = $1`, user.ID).Scan(&disabled); err != nil {
		return fmt.Errorf("verify user %q state: %w", user.ID, err)
	}
	if disabled {
		return &security.UserDisabledError{UserID: user.ID}
	}
	return nil
}

func upsertPostgresMembership(ctx context.Context, tx pgx.Tx, membership security.Membership) error {
	var existingCreatedAt, existingUpdatedAt time.Time
	err := tx.QueryRow(ctx, `
		SELECT created_at, updated_at FROM project_memberships
		WHERE project_id = $1 AND user_id = $2 FOR UPDATE
	`, membership.ProjectID, membership.UserID).Scan(&existingCreatedAt, &existingUpdatedAt)
	switch {
	case err == nil:
		membership.CreatedAt = existingCreatedAt
		if membership.UpdatedAt.Before(existingUpdatedAt) {
			return errors.New("put membership: update timestamp moved backwards")
		}
	case !errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("lock membership for user %q: %w", membership.UserID, err)
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO project_memberships (project_id, user_id, role, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (project_id, user_id) DO UPDATE
		SET role = EXCLUDED.role, updated_at = EXCLUDED.updated_at
		WHERE EXCLUDED.updated_at >= project_memberships.updated_at
	`, membership.ProjectID, membership.UserID, membership.Role, membership.CreatedAt, membership.UpdatedAt)
	if err != nil {
		return fmt.Errorf("upsert membership for user %q: %w", membership.UserID, err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("put membership: update timestamp moved backwards")
	}
	return nil
}

func insertPostgresAuditEvent(ctx context.Context, tx pgx.Tx, event security.AuditEvent) error {
	metadata, err := json.Marshal(event.Metadata)
	if err != nil {
		return fmt.Errorf("encode audit event %q metadata: %w", event.ID, err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO audit_events (
			event_id, project_id, actor_user_id, action, resource_type,
			resource_id, occurred_at, metadata
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, event.ID, event.ProjectID, event.ActorUserID, event.Action, event.ResourceType,
		event.ResourceID, event.OccurredAt, metadata)
	if err != nil {
		if postgresErrorCode(err, "23505") {
			return &security.AuditEventAlreadyExistsError{EventID: event.ID}
		}
		return fmt.Errorf("insert audit event %q: %w", event.ID, err)
	}
	return nil
}

var _ security.Store = (*PostgresStore)(nil)
