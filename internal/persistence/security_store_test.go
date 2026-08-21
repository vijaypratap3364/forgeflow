package persistence

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/security"
)

func TestFileStorePersistsProjectAuthorizationState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "forgeflow.ffdb")
	store, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("OpenFileStore() error = %v", err)
	}
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	admin := security.User{ID: "admin-1", DisplayName: "Admin", CreatedAt: now}
	project := security.Project{ID: "project-1", Name: "Project", CreatedBy: admin.ID, CreatedAt: now, UpdatedAt: now}
	membership := security.Membership{ProjectID: project.ID, UserID: admin.ID, Role: security.RoleAdmin, CreatedAt: now, UpdatedAt: now}
	createdEvent := security.AuditEvent{ID: "audit-1", ProjectID: project.ID, ActorUserID: admin.ID,
		Action: "project.created", ResourceType: "project", ResourceID: string(project.ID), OccurredAt: now}
	if err := store.CreateProject(ctx, admin, project, membership, createdEvent); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}

	member := security.User{ID: "member-1", DisplayName: "Member", CreatedAt: now.Add(time.Second)}
	memberRole := security.Membership{ProjectID: project.ID, UserID: member.ID, Role: security.RoleMember,
		CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second)}
	membershipEvent := security.AuditEvent{ID: "audit-2", ProjectID: project.ID, ActorUserID: admin.ID,
		Action: "membership.updated", ResourceType: "user", ResourceID: string(member.ID), OccurredAt: now.Add(time.Second),
		Metadata: map[string]string{"role": "member"}}
	if err := store.PutMembership(ctx, member, memberRole, membershipEvent); err != nil {
		t.Fatalf("PutMembership() error = %v", err)
	}

	definition := repositoryWorkflow()
	ownership := security.WorkflowOwnership{WorkflowID: definition.ID, ProjectID: project.ID,
		OwnerUserID: member.ID, CreatedAt: now.Add(2 * time.Second)}
	if err := store.SaveWorkflowForProject(ctx, definition, ownership); err != nil {
		t.Fatalf("SaveWorkflowForProject() error = %v", err)
	}

	reopened, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("reopen OpenFileStore() error = %v", err)
	}
	gotProject, found, err := reopened.LoadProject(ctx, project.ID)
	if err != nil || !found || gotProject != project {
		t.Fatalf("LoadProject() = %#v, %v, %v; want %#v, true, nil", gotProject, found, err, project)
	}
	gotMembership, found, err := reopened.LoadMembership(ctx, project.ID, member.ID)
	if err != nil || !found || gotMembership != memberRole {
		t.Fatalf("LoadMembership() = %#v, %v, %v; want %#v, true, nil", gotMembership, found, err, memberRole)
	}
	gotOwnership, found, err := reopened.LoadWorkflowOwnership(ctx, definition.ID)
	if err != nil || !found || gotOwnership != ownership {
		t.Fatalf("LoadWorkflowOwnership() = %#v, %v, %v; want %#v, true, nil", gotOwnership, found, err, ownership)
	}
	events, err := reopened.ListAuditEvents(ctx, project.ID, 10)
	if err != nil {
		t.Fatalf("ListAuditEvents() error = %v", err)
	}
	wantEvents := []security.AuditEvent{membershipEvent, createdEvent}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("ListAuditEvents() = %#v, want %#v", events, wantEvents)
	}
}

func TestFileStoreOwnedWorkflowRejectsCrossProjectOwner(t *testing.T) {
	t.Parallel()
	store, err := OpenFileStore(filepath.Join(t.TempDir(), "forgeflow.ffdb"))
	if err != nil {
		t.Fatalf("OpenFileStore() error = %v", err)
	}
	now := time.Now().UTC()
	admin := security.User{ID: "admin", DisplayName: "Admin", CreatedAt: now}
	project := security.Project{ID: "project", Name: "Project", CreatedBy: admin.ID, CreatedAt: now, UpdatedAt: now}
	membership := security.Membership{ProjectID: project.ID, UserID: admin.ID, Role: security.RoleAdmin, CreatedAt: now, UpdatedAt: now}
	event := security.AuditEvent{ID: "audit", ProjectID: project.ID, ActorUserID: admin.ID,
		Action: "project.created", ResourceType: "project", ResourceID: "project", OccurredAt: now}
	if err := store.CreateProject(context.Background(), admin, project, membership, event); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	definition := repositoryWorkflow()
	err = store.SaveWorkflowForProject(context.Background(), definition, security.WorkflowOwnership{
		WorkflowID: definition.ID, ProjectID: project.ID, OwnerUserID: "outsider", CreatedAt: now,
	})
	if err == nil {
		t.Fatal("SaveWorkflowForProject() error = nil, want non-member error")
	}
	if _, found, loadErr := store.LoadWorkflow(context.Background(), definition.ID); loadErr != nil || found {
		t.Fatalf("LoadWorkflow() after rejected ownership = found %v, error %v", found, loadErr)
	}
}
