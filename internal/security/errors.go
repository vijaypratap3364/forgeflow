package security

import (
	"errors"
	"fmt"

	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

// ErrUnauthenticated is the public classification for missing or invalid credentials.
var ErrUnauthenticated = errors.New("request is unauthenticated")

// ProjectAlreadyExistsError reports a duplicate project identity.
type ProjectAlreadyExistsError struct{ ProjectID ProjectID }

func (e *ProjectAlreadyExistsError) Error() string {
	return fmt.Sprintf("project %q already exists", e.ProjectID)
}

// ProjectNotFoundError reports an administrative mutation for an unknown project.
type ProjectNotFoundError struct{ ProjectID ProjectID }

func (e *ProjectNotFoundError) Error() string {
	return fmt.Sprintf("project %q was not found", e.ProjectID)
}

// UserDisabledError reports a valid identity disabled in ForgeFlow state.
type UserDisabledError struct{ UserID UserID }

func (e *UserDisabledError) Error() string {
	return fmt.Sprintf("user %q is disabled", e.UserID)
}

// WorkflowOwnershipConflictError reports reuse of a workflow ID across tenants.
type WorkflowOwnershipConflictError struct {
	WorkflowID workflow.WorkflowID
}

func (e *WorkflowOwnershipConflictError) Error() string {
	return fmt.Sprintf("workflow %q already has different ownership", e.WorkflowID)
}

// AuditEventAlreadyExistsError reports reuse of an immutable event ID.
type AuditEventAlreadyExistsError struct{ EventID AuditEventID }

func (e *AuditEventAlreadyExistsError) Error() string {
	return fmt.Sprintf("audit event %q already exists", e.EventID)
}
