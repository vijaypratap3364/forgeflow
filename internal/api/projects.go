package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/vijaypratap3364/forgeflow/internal/security"
)

type createProjectRequest struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type updateProjectRequest struct {
	Name string `json:"name"`
}

type putMembershipRequest struct {
	DisplayName string        `json:"display_name"`
	Role        security.Role `json:"role"`
}

type projectResponse struct {
	ID        security.ProjectID `json:"id"`
	Name      string             `json:"name"`
	CreatedBy security.UserID    `json:"created_by"`
	CreatedAt time.Time          `json:"created_at"`
	UpdatedAt time.Time          `json:"updated_at"`
}

type membershipResponse struct {
	ProjectID security.ProjectID `json:"project_id"`
	UserID    security.UserID    `json:"user_id"`
	Role      security.Role      `json:"role"`
	CreatedAt time.Time          `json:"created_at"`
	UpdatedAt time.Time          `json:"updated_at"`
}

type membershipsResponse struct {
	Memberships []membershipResponse `json:"memberships"`
}

type auditEventResponse struct {
	ID           security.AuditEventID `json:"id"`
	ProjectID    security.ProjectID    `json:"project_id"`
	ActorUserID  security.UserID       `json:"actor_user_id"`
	Action       string                `json:"action"`
	ResourceType string                `json:"resource_type"`
	ResourceID   string                `json:"resource_id"`
	OccurredAt   time.Time             `json:"occurred_at"`
	Metadata     map[string]string     `json:"metadata,omitempty"`
}

type auditEventsResponse struct {
	Events []auditEventResponse `json:"events"`
}

func (server *Server) handleProjects(writer http.ResponseWriter, request *http.Request) {
	if !requireMethod(writer, request, http.MethodPost) {
		return
	}
	if !requireJSONContentType(writer, request, false) {
		return
	}
	var payload createProjectRequest
	if !decodeJSONBody(writer, request, &payload, false) {
		return
	}
	if !validResourceID(payload.ID) {
		writeAPIError(writer, http.StatusUnprocessableEntity, "invalid_project_id", "project ID is invalid", "id")
		return
	}
	principal := mustPrincipal(request.Context())
	now := time.Now().UTC()
	user := security.User{ID: principal.UserID, DisplayName: principal.DisplayName, CreatedAt: now}
	existingUser, found, err := server.store.LoadUser(request.Context(), principal.UserID)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	if found {
		if existingUser.Disabled {
			writeAPIError(writer, http.StatusForbidden, "user_disabled", "authenticated user is disabled", "")
			return
		}
		user = existingUser
	}
	project := security.Project{ID: security.ProjectID(payload.ID), Name: payload.Name,
		CreatedBy: principal.UserID, CreatedAt: now, UpdatedAt: now}
	membership := security.Membership{ProjectID: project.ID, UserID: principal.UserID,
		Role: security.RoleAdmin, CreatedAt: now, UpdatedAt: now}
	event, err := newAuditEvent(project.ID, principal.UserID, "project.created", "project", payload.ID, nil, now)
	if err != nil {
		writeAPIError(writer, http.StatusInternalServerError, "internal_error", "could not create audit identity", "")
		return
	}
	if err := user.Validate(); err != nil {
		writeAPIError(writer, http.StatusUnprocessableEntity, "invalid_user", err.Error(), "")
		return
	}
	if err := project.Validate(); err != nil {
		writeAPIError(writer, http.StatusUnprocessableEntity, "invalid_project", err.Error(), "name")
		return
	}
	if err := server.store.CreateProject(request.Context(), user, project, membership, event); err != nil {
		var exists *security.ProjectAlreadyExistsError
		if errors.As(err, &exists) {
			writeAPIError(writer, http.StatusConflict, "project_exists", exists.Error(), "id")
			return
		}
		writeRequestOrStoreError(writer, err)
		return
	}
	writer.Header().Set("Location", "/api/v1/projects/"+payload.ID)
	writeJSON(writer, http.StatusCreated, projectDTO(project))
}

func (server *Server) handleProject(writer http.ResponseWriter, request *http.Request) {
	projectID, ok := requestProjectID(writer, request)
	if !ok {
		return
	}
	switch request.Method {
	case http.MethodGet:
		if !server.authorizeProject(writer, request, projectID, security.PermissionProjectRead) {
			return
		}
		project, found, err := server.store.LoadProject(request.Context(), projectID)
		if err != nil {
			writeStoreError(writer, err)
			return
		}
		if !found {
			writeAPIError(writer, http.StatusNotFound, "project_not_found", "project was not found", "project_id")
			return
		}
		writeJSON(writer, http.StatusOK, projectDTO(project))
	case http.MethodPatch:
		server.updateProject(writer, request, projectID)
	default:
		writer.Header().Set("Allow", http.MethodGet+", "+http.MethodPatch)
		writeAPIError(writer, http.StatusMethodNotAllowed, "method_not_allowed", "HTTP method is not allowed for this route", "")
	}
}

func (server *Server) updateProject(writer http.ResponseWriter, request *http.Request, projectID security.ProjectID) {
	if !server.authorizeProject(writer, request, projectID, security.PermissionProjectManage) {
		return
	}
	if !requireJSONContentType(writer, request, false) {
		return
	}
	var payload updateProjectRequest
	if !decodeJSONBody(writer, request, &payload, false) {
		return
	}
	project, found, err := server.store.LoadProject(request.Context(), projectID)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	if !found {
		writeAPIError(writer, http.StatusNotFound, "project_not_found", "project was not found", "project_id")
		return
	}
	oldName := project.Name
	project.Name = payload.Name
	project.UpdatedAt = time.Now().UTC()
	principal := mustPrincipal(request.Context())
	event, err := newAuditEvent(projectID, principal.UserID, "project.updated", "project", string(projectID),
		map[string]string{"old_name": oldName, "new_name": project.Name}, project.UpdatedAt)
	if err != nil {
		writeAPIError(writer, http.StatusInternalServerError, "internal_error", "could not create audit identity", "")
		return
	}
	if err := project.Validate(); err != nil {
		writeAPIError(writer, http.StatusUnprocessableEntity, "invalid_project", err.Error(), "name")
		return
	}
	if err := server.store.SaveProject(request.Context(), project, event); err != nil {
		writeRequestOrStoreError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, projectDTO(project))
}

func (server *Server) handleProjectMembers(writer http.ResponseWriter, request *http.Request) {
	if !requireMethod(writer, request, http.MethodGet) {
		return
	}
	projectID, ok := requestProjectID(writer, request)
	if !ok || !server.authorizeProject(writer, request, projectID, security.PermissionProjectManage) {
		return
	}
	memberships, err := server.store.ListMemberships(request.Context(), projectID)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	response := membershipsResponse{Memberships: make([]membershipResponse, 0, len(memberships))}
	for _, membership := range memberships {
		response.Memberships = append(response.Memberships, membershipDTO(membership))
	}
	writeJSON(writer, http.StatusOK, response)
}

func (server *Server) handleProjectMember(writer http.ResponseWriter, request *http.Request) {
	if !requireMethod(writer, request, http.MethodPut) {
		return
	}
	projectID, ok := requestProjectID(writer, request)
	if !ok || !server.authorizeProject(writer, request, projectID, security.PermissionProjectManage) {
		return
	}
	userID := security.UserID(request.PathValue("userID"))
	if !validIdentifier(string(userID)) {
		writeAPIError(writer, http.StatusBadRequest, "invalid_user_id", "user ID is invalid", "userID")
		return
	}
	if !requireJSONContentType(writer, request, false) {
		return
	}
	var payload putMembershipRequest
	if !decodeJSONBody(writer, request, &payload, false) {
		return
	}
	if err := payload.Role.Validate(); err != nil {
		writeAPIError(writer, http.StatusUnprocessableEntity, "invalid_role", err.Error(), "role")
		return
	}
	now := time.Now().UTC()
	existing, existed, err := server.store.LoadMembership(request.Context(), projectID, userID)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	if existed && existing.Role == security.RoleAdmin && payload.Role != security.RoleAdmin {
		memberships, err := server.store.ListMemberships(request.Context(), projectID)
		if err != nil {
			writeStoreError(writer, err)
			return
		}
		admins := 0
		for _, membership := range memberships {
			if membership.Role == security.RoleAdmin {
				admins++
			}
		}
		if admins == 1 {
			writeAPIError(writer, http.StatusConflict, "last_admin", "a project must retain at least one admin", "role")
			return
		}
	}
	createdAt := now
	oldRole := ""
	if existed {
		createdAt = existing.CreatedAt
		oldRole = string(existing.Role)
	}
	user := security.User{ID: userID, DisplayName: payload.DisplayName, CreatedAt: now}
	membership := security.Membership{ProjectID: projectID, UserID: userID, Role: payload.Role,
		CreatedAt: createdAt, UpdatedAt: now}
	principal := mustPrincipal(request.Context())
	event, err := newAuditEvent(projectID, principal.UserID, "membership.updated", "user", string(userID),
		map[string]string{"old_role": oldRole, "new_role": string(payload.Role)}, now)
	if err != nil {
		writeAPIError(writer, http.StatusInternalServerError, "internal_error", "could not create audit identity", "")
		return
	}
	if err := user.Validate(); err != nil {
		writeAPIError(writer, http.StatusUnprocessableEntity, "invalid_user", err.Error(), "display_name")
		return
	}
	if err := server.store.PutMembership(request.Context(), user, membership, event); err != nil {
		writeRequestOrStoreError(writer, err)
		return
	}
	status := http.StatusCreated
	if existed {
		status = http.StatusOK
	}
	writeJSON(writer, status, membershipDTO(membership))
}

func (server *Server) handleProjectAuditEvents(writer http.ResponseWriter, request *http.Request) {
	if !requireMethod(writer, request, http.MethodGet) {
		return
	}
	projectID, ok := requestProjectID(writer, request)
	if !ok || !server.authorizeProject(writer, request, projectID, security.PermissionAuditRead) {
		return
	}
	events, err := server.store.ListAuditEvents(request.Context(), projectID, 100)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	response := auditEventsResponse{Events: make([]auditEventResponse, 0, len(events))}
	for _, event := range events {
		response.Events = append(response.Events, auditEventResponse(event))
	}
	writeJSON(writer, http.StatusOK, response)
}

func requestProjectID(writer http.ResponseWriter, request *http.Request) (security.ProjectID, bool) {
	projectID := security.ProjectID(request.PathValue("projectID"))
	if !validResourceID(string(projectID)) {
		writeAPIError(writer, http.StatusBadRequest, "invalid_project_id", "project ID is invalid", "projectID")
		return "", false
	}
	return projectID, true
}

func newAuditEvent(
	projectID security.ProjectID,
	actor security.UserID,
	action, resourceType, resourceID string,
	metadata map[string]string,
	now time.Time,
) (security.AuditEvent, error) {
	id, err := generateRandomID("audit-")
	if err != nil {
		return security.AuditEvent{}, err
	}
	return security.AuditEvent{ID: security.AuditEventID(id), ProjectID: projectID, ActorUserID: actor,
		Action: action, ResourceType: resourceType, ResourceID: resourceID, OccurredAt: now, Metadata: metadata}, nil
}

func projectDTO(project security.Project) projectResponse {
	return projectResponse{ID: project.ID, Name: project.Name, CreatedBy: project.CreatedBy,
		CreatedAt: project.CreatedAt, UpdatedAt: project.UpdatedAt}
}

func membershipDTO(membership security.Membership) membershipResponse {
	return membershipResponse{ProjectID: membership.ProjectID, UserID: membership.UserID, Role: membership.Role,
		CreatedAt: membership.CreatedAt, UpdatedAt: membership.UpdatedAt}
}
