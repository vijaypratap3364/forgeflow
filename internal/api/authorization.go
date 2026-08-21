package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/vijaypratap3364/forgeflow/internal/security"
)

type principalContextKey struct{}

func withPrincipal(ctx context.Context, principal security.Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

func mustPrincipal(ctx context.Context) security.Principal {
	principal, ok := ctx.Value(principalContextKey{}).(security.Principal)
	if !ok {
		panic("ForgeFlow API principal missing after authentication middleware")
	}
	return principal
}

func bearerToken(header string) string {
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return parts[1]
}

func (server *Server) authorizeProject(
	writer http.ResponseWriter,
	request *http.Request,
	projectID security.ProjectID,
	permission security.Permission,
) bool {
	principal := mustPrincipal(request.Context())
	user, found, err := server.store.LoadUser(request.Context(), principal.UserID)
	if err != nil {
		writeStoreError(writer, err)
		return false
	}
	if !found {
		writeAPIError(writer, http.StatusForbidden, "forbidden", "authenticated user is not provisioned", "")
		return false
	}
	if user.Disabled {
		writeAPIError(writer, http.StatusForbidden, "user_disabled", "authenticated user is disabled", "")
		return false
	}
	membership, found, err := server.store.LoadMembership(request.Context(), projectID, principal.UserID)
	if err != nil {
		writeStoreError(writer, err)
		return false
	}
	if !found {
		// Deliberately conceal whether another tenant's project exists.
		writeAPIError(writer, http.StatusNotFound, "project_not_found", "project was not found", "project_id")
		return false
	}
	if !membership.Role.Allows(permission) {
		writeAPIError(writer, http.StatusForbidden, "forbidden", "project role does not allow this operation", "")
		return false
	}
	return true
}
