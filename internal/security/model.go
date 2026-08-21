// Package security defines ForgeFlow's authenticated identity, tenancy, and
// authorization model.
package security

import (
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

const (
	maxIdentifierLength = 256
	maxNameLength       = 200
)

// UserID is the stable subject from ForgeFlow's configured JWT issuer.
type UserID string

// ProjectID identifies one authorization and workflow-ownership boundary.
type ProjectID string

// AuditEventID identifies one immutable administrative audit record.
type AuditEventID string

// Role is a project-scoped collection of permissions.
type Role string

const (
	// RoleMember may create workflows, start runs, and inspect aggregate status.
	RoleMember Role = "member"
	// RoleOperator adds task inspection and run cancellation/retry operations.
	RoleOperator Role = "operator"
	// RoleAdmin adds project, membership, and audit administration.
	RoleAdmin Role = "admin"
)

// Permission is one resource-bound action evaluated from persisted membership.
type Permission string

const (
	PermissionProjectRead   Permission = "project:read"
	PermissionWorkflowWrite Permission = "workflow:write"
	PermissionRunStart      Permission = "run:start"
	PermissionRunRead       Permission = "run:read"
	PermissionTaskRead      Permission = "task:read"
	PermissionRunOperate    Permission = "run:operate"
	PermissionProjectManage Permission = "project:manage"
	PermissionAuditRead     Permission = "audit:read"
)

// Allows reports whether a role grants permission.
func (role Role) Allows(permission Permission) bool {
	switch role {
	case RoleMember:
		return permission == PermissionProjectRead ||
			permission == PermissionWorkflowWrite ||
			permission == PermissionRunStart ||
			permission == PermissionRunRead
	case RoleOperator:
		return RoleMember.Allows(permission) ||
			permission == PermissionTaskRead ||
			permission == PermissionRunOperate
	case RoleAdmin:
		return RoleOperator.Allows(permission) ||
			permission == PermissionProjectManage ||
			permission == PermissionAuditRead
	default:
		return false
	}
}

// Validate reports whether role is one of ForgeFlow's supported project roles.
func (role Role) Validate() error {
	if role != RoleMember && role != RoleOperator && role != RoleAdmin {
		return fmt.Errorf("invalid project role %q", role)
	}
	return nil
}

// User records a JWT subject known to ForgeFlow. Passwords and signing keys are
// deliberately outside this model.
type User struct {
	ID          UserID
	DisplayName string
	Disabled    bool
	CreatedAt   time.Time
}

// Validate checks durable user invariants.
func (user User) Validate() error {
	if !validIdentifier(string(user.ID)) {
		return fmt.Errorf("invalid user ID %q", user.ID)
	}
	if !validName(user.DisplayName) {
		return fmt.Errorf("invalid display name for user %q", user.ID)
	}
	if user.CreatedAt.IsZero() {
		return fmt.Errorf("user %q has no creation timestamp", user.ID)
	}
	return nil
}

// Project is a workspace that owns workflows, memberships, and audit events.
type Project struct {
	ID        ProjectID
	Name      string
	CreatedBy UserID
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Validate checks durable project invariants.
func (project Project) Validate() error {
	if !validIdentifier(string(project.ID)) {
		return fmt.Errorf("invalid project ID %q", project.ID)
	}
	if !validName(project.Name) {
		return fmt.Errorf("invalid name for project %q", project.ID)
	}
	if !validIdentifier(string(project.CreatedBy)) {
		return fmt.Errorf("project %q has invalid creator %q", project.ID, project.CreatedBy)
	}
	if project.CreatedAt.IsZero() || project.UpdatedAt.Before(project.CreatedAt) {
		return fmt.Errorf("project %q has invalid timestamps", project.ID)
	}
	return nil
}

// Membership assigns one user exactly one role within a project.
type Membership struct {
	ProjectID ProjectID
	UserID    UserID
	Role      Role
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Validate checks durable membership invariants.
func (membership Membership) Validate() error {
	if !validIdentifier(string(membership.ProjectID)) || !validIdentifier(string(membership.UserID)) {
		return fmt.Errorf("membership has invalid project or user identity")
	}
	if err := membership.Role.Validate(); err != nil {
		return err
	}
	if membership.CreatedAt.IsZero() || membership.UpdatedAt.Before(membership.CreatedAt) {
		return fmt.Errorf("membership for user %q in project %q has invalid timestamps", membership.UserID, membership.ProjectID)
	}
	return nil
}

// WorkflowOwnership binds an immutable definition to its project and creator.
type WorkflowOwnership struct {
	WorkflowID  workflow.WorkflowID
	ProjectID   ProjectID
	OwnerUserID UserID
	CreatedAt   time.Time
}

// Validate checks durable ownership invariants.
func (ownership WorkflowOwnership) Validate() error {
	if !validIdentifier(string(ownership.WorkflowID)) ||
		!validIdentifier(string(ownership.ProjectID)) ||
		!validIdentifier(string(ownership.OwnerUserID)) {
		return fmt.Errorf("workflow ownership has an invalid identity")
	}
	if ownership.CreatedAt.IsZero() {
		return fmt.Errorf("workflow %q ownership has no creation timestamp", ownership.WorkflowID)
	}
	return nil
}

// AuditEvent is an immutable record of a security-relevant administrative action.
type AuditEvent struct {
	ID           AuditEventID
	ProjectID    ProjectID
	ActorUserID  UserID
	Action       string
	ResourceType string
	ResourceID   string
	OccurredAt   time.Time
	Metadata     map[string]string
}

// Validate checks durable audit-event invariants.
func (event AuditEvent) Validate() error {
	if !validIdentifier(string(event.ID)) || !validIdentifier(string(event.ProjectID)) ||
		!validIdentifier(string(event.ActorUserID)) {
		return fmt.Errorf("audit event has an invalid identity")
	}
	if !validIdentifier(event.Action) || !validIdentifier(event.ResourceType) || !validIdentifier(event.ResourceID) {
		return fmt.Errorf("audit event %q has invalid action or resource", event.ID)
	}
	if event.OccurredAt.IsZero() {
		return fmt.Errorf("audit event %q has no timestamp", event.ID)
	}
	for key, value := range event.Metadata {
		if !validIdentifier(key) || len(value) > 500 {
			return fmt.Errorf("audit event %q has invalid metadata", event.ID)
		}
	}
	return nil
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > maxIdentifierLength {
		return false
	}
	for _, character := range value {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validName(value string) bool {
	trimmed := strings.TrimSpace(value)
	return trimmed != "" && len(trimmed) <= maxNameLength
}
