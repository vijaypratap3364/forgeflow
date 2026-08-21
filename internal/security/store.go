package security

import (
	"context"

	"github.com/vijaypratap3364/forgeflow/internal/execution"
	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

// Store extends workflow execution persistence with project-scoped security
// state. Compound mutations must commit their supplied audit event atomically.
type Store interface {
	execution.Store
	CreateProject(context.Context, User, Project, Membership, AuditEvent) error
	LoadUser(context.Context, UserID) (User, bool, error)
	LoadProject(context.Context, ProjectID) (Project, bool, error)
	SaveProject(context.Context, Project, AuditEvent) error
	PutMembership(context.Context, User, Membership, AuditEvent) error
	LoadMembership(context.Context, ProjectID, UserID) (Membership, bool, error)
	ListMemberships(context.Context, ProjectID) ([]Membership, error)
	SaveWorkflowForProject(context.Context, workflow.WorkflowDefinition, WorkflowOwnership) error
	LoadWorkflowOwnership(context.Context, workflow.WorkflowID) (WorkflowOwnership, bool, error)
	ListAuditEvents(context.Context, ProjectID, int) ([]AuditEvent, error)
}
