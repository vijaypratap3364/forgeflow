package api

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/vijaypratap3364/forgeflow/internal/execution"
	"github.com/vijaypratap3364/forgeflow/internal/persistence"
	"github.com/vijaypratap3364/forgeflow/internal/security"
)

const (
	testIssuer   = "https://identity.example.test"
	testAudience = "forgeflow-api"
)

func TestAuthenticationRejectsMissingInvalidAndExpiredTokens(t *testing.T) {
	server, _, issue := newSecurityTestServer(t, 100)

	missingRequest := httptest.NewRequest(http.MethodGet, "/api/v1/projects/project-main", nil)
	missingRecorder := httptest.NewRecorder()
	server.ServeHTTP(missingRecorder, missingRequest)
	assertStatus(t, missingRecorder, http.StatusUnauthorized)
	assertErrorCode(t, missingRecorder, "unauthenticated")
	if missingRecorder.Header().Get("WWW-Authenticate") == "" {
		t.Fatal("missing WWW-Authenticate challenge")
	}

	invalid := securityRequest(t, server, http.MethodGet, "/api/v1/projects/project-main", "", "", "not-a-jwt")
	assertStatus(t, invalid, http.StatusUnauthorized)
	assertErrorCode(t, invalid, "unauthenticated")

	expiredToken := issue(t, "admin", "Admin", time.Now().Add(-time.Minute))
	expired := securityRequest(t, server, http.MethodGet, "/api/v1/projects/project-main", "", "", expiredToken)
	assertStatus(t, expired, http.StatusUnauthorized)
	assertErrorCode(t, expired, "unauthenticated")
}

func TestProjectRBACOwnershipAndAdministrativeAudit(t *testing.T) {
	server, store, issue := newSecurityTestServer(t, 1000)
	adminToken := issue(t, "admin", "Admin", time.Now().Add(time.Hour))
	memberToken := issue(t, "member", "Member", time.Now().Add(time.Hour))
	operatorToken := issue(t, "operator", "Operator", time.Now().Add(time.Hour))
	outsiderToken := issue(t, "outsider", "Outsider", time.Now().Add(time.Hour))
	provisionProjectSecurity(t, store)

	memberWorkflow := securityRequest(t, server, http.MethodPost, "/api/v1/workflows", `{
		"id":"secure-workflow","project_id":"project-main",
		"tasks":[{"id":"task","name":"Task","handler":"noop"}]
	}`, "application/json", memberToken)
	assertStatus(t, memberWorkflow, http.StatusCreated)

	memberRun := securityRequest(t, server, http.MethodPost, "/api/v1/workflows/secure-workflow/runs",
		`{"run_id":"secure-run"}`, "application/json", memberToken)
	assertStatus(t, memberRun, http.StatusAccepted)
	memberStatus := securityRequest(t, server, http.MethodGet, "/api/v1/runs/secure-run", "", "", memberToken)
	assertStatus(t, memberStatus, http.StatusOK)

	memberTasks := securityRequest(t, server, http.MethodGet, "/api/v1/runs/secure-run/tasks", "", "", memberToken)
	assertStatus(t, memberTasks, http.StatusForbidden)
	assertErrorCode(t, memberTasks, "forbidden")
	memberCancel := securityRequest(t, server, http.MethodPost, "/api/v1/runs/secure-run/cancel", "", "", memberToken)
	assertStatus(t, memberCancel, http.StatusForbidden)

	operatorTasks := securityRequest(t, server, http.MethodGet, "/api/v1/runs/secure-run/tasks", "", "", operatorToken)
	assertStatus(t, operatorTasks, http.StatusOK)

	blockingWorkflow := securityRequest(t, server, http.MethodPost, "/api/v1/workflows", `{
		"id":"blocking-workflow","project_id":"project-main",
		"tasks":[{"id":"wait","name":"Wait","handler":"delay","input":"5s"}]
	}`, "application/json", memberToken)
	assertStatus(t, blockingWorkflow, http.StatusCreated)
	blockingRun := securityRequest(t, server, http.MethodPost, "/api/v1/workflows/blocking-workflow/runs",
		`{"run_id":"blocking-run"}`, "application/json", memberToken)
	assertStatus(t, blockingRun, http.StatusAccepted)
	operatorCancel := securityRequest(t, server, http.MethodPost, "/api/v1/runs/blocking-run/cancel", "", "", operatorToken)
	assertStatus(t, operatorCancel, http.StatusAccepted)

	failingWorkflow := securityRequest(t, server, http.MethodPost, "/api/v1/workflows", `{
		"id":"failing-workflow","project_id":"project-main",
		"tasks":[{"id":"fail","name":"Fail","handler":"test-fail"}]
	}`, "application/json", memberToken)
	assertStatus(t, failingWorkflow, http.StatusCreated)
	failingRun := securityRequest(t, server, http.MethodPost, "/api/v1/workflows/failing-workflow/runs",
		`{"run_id":"failing-run"}`, "application/json", memberToken)
	assertStatus(t, failingRun, http.StatusAccepted)
	terminalEvents := securityRequest(t, server, http.MethodGet, "/api/v1/runs/failing-run/events", "", "", operatorToken)
	assertStatus(t, terminalEvents, http.StatusOK)
	retry := securityRequest(t, server, http.MethodPost, "/api/v1/runs/failing-run/retry",
		`{"run_id":"retry-run"}`, "application/json", operatorToken)
	assertStatus(t, retry, http.StatusAccepted)

	wrongProject := securityRequest(t, server, http.MethodGet, "/api/v1/workflows/secure-workflow", "", "", outsiderToken)
	assertStatus(t, wrongProject, http.StatusNotFound)
	assertErrorCode(t, wrongProject, "project_not_found")
	wrongProjectRun := securityRequest(t, server, http.MethodGet, "/api/v1/runs/secure-run", "", "", outsiderToken)
	assertStatus(t, wrongProjectRun, http.StatusNotFound)
	assertErrorCode(t, wrongProjectRun, "project_not_found")
	wrongProjectStart := securityRequest(t, server, http.MethodPost, "/api/v1/workflows/secure-workflow/runs",
		`{"run_id":"cross-project-run"}`, "application/json", outsiderToken)
	assertStatus(t, wrongProjectStart, http.StatusNotFound)
	assertErrorCode(t, wrongProjectStart, "project_not_found")
	if _, found, err := store.LoadRun(context.Background(), "cross-project-run"); err != nil || found {
		t.Fatalf("cross-project run persisted = found %v, error %v", found, err)
	}

	memberAdmin := securityRequest(t, server, http.MethodPut, "/api/v1/projects/project-main/members/new-user",
		`{"display_name":"New User","role":"member"}`, "application/json", memberToken)
	assertStatus(t, memberAdmin, http.StatusForbidden)
	memberEscalation := securityRequest(t, server, http.MethodPut, "/api/v1/projects/project-main/members/member",
		`{"display_name":"Member","role":"admin"}`, "application/json", memberToken)
	assertStatus(t, memberEscalation, http.StatusForbidden)
	assertErrorCode(t, memberEscalation, "forbidden")
	operatorEscalation := securityRequest(t, server, http.MethodPut, "/api/v1/projects/project-main/members/operator",
		`{"display_name":"Operator","role":"admin"}`, "application/json", operatorToken)
	assertStatus(t, operatorEscalation, http.StatusForbidden)
	assertErrorCode(t, operatorEscalation, "forbidden")
	memberProjectUpdate := securityRequest(t, server, http.MethodPatch, "/api/v1/projects/project-main",
		`{"name":"Denied Rename"}`, "application/json", memberToken)
	assertStatus(t, memberProjectUpdate, http.StatusForbidden)
	adminProjectUpdate := securityRequest(t, server, http.MethodPatch, "/api/v1/projects/project-main",
		`{"name":"Renamed Project"}`, "application/json", adminToken)
	assertStatus(t, adminProjectUpdate, http.StatusOK)

	adminUpdate := securityRequest(t, server, http.MethodPut, "/api/v1/projects/project-main/members/new-user",
		`{"display_name":"New User","role":"operator"}`, "application/json", adminToken)
	assertStatus(t, adminUpdate, http.StatusCreated)
	audit := securityRequest(t, server, http.MethodGet, "/api/v1/projects/project-main/audit-events", "", "", adminToken)
	assertStatus(t, audit, http.StatusOK)
	var auditResponse auditEventsResponse
	decodeResponse(t, audit, &auditResponse)
	if len(auditResponse.Events) < 2 || auditResponse.Events[0].Action != "membership.updated" {
		t.Fatalf("audit events = %#v, want membership update followed by project creation", auditResponse.Events)
	}
	actions := make(map[string]int)
	for _, event := range auditResponse.Events {
		actions[event.Action]++
	}
	if actions["project.created"] == 0 || actions["project.updated"] == 0 || actions["membership.updated"] == 0 {
		t.Fatalf("audit action counts = %#v, want project creation/update and membership update", actions)
	}
	latest := auditResponse.Events[0]
	if latest.ActorUserID != "admin" || latest.ResourceType != "user" || latest.ResourceID != "new-user" ||
		latest.Metadata["new_role"] != "operator" {
		t.Fatalf("latest administrative audit event = %#v", latest)
	}
}

func TestAuthenticatedUserCanCreateProjectAndBecomesAdmin(t *testing.T) {
	server, store, issue := newSecurityTestServer(t, 100)
	token := issue(t, "founder", "Founder", time.Now().Add(time.Hour))
	created := securityRequest(t, server, http.MethodPost, "/api/v1/projects",
		`{"id":"new-project","name":"New Project"}`, "application/json", token)
	assertStatus(t, created, http.StatusCreated)
	membership, found, err := store.LoadMembership(context.Background(), "new-project", "founder")
	if err != nil || !found || membership.Role != security.RoleAdmin {
		t.Fatalf("founder membership = %#v, found %v, error %v", membership, found, err)
	}
	events, err := store.ListAuditEvents(context.Background(), "new-project", 10)
	if err != nil || len(events) != 1 || events[0].Action != "project.created" {
		t.Fatalf("project audit events = %#v, error %v", events, err)
	}
}

func TestAuthenticatedAPIRateLimit(t *testing.T) {
	server, store, issue := newSecurityTestServer(t, 2)
	provisionProjectSecurity(t, store)
	token := issue(t, "member", "Member", time.Now().Add(time.Hour))
	var allowed []*httptest.ResponseRecorder
	for index := 0; index < 2; index++ {
		response := securityRequest(t, server, http.MethodGet, "/api/v1/projects/project-main", "", "", token)
		assertStatus(t, response, http.StatusOK)
		allowed = append(allowed, response)
	}
	if allowed[0].Header().Get("RateLimit-Limit") != "2" || allowed[0].Header().Get("RateLimit-Remaining") != "1" {
		t.Fatalf("first rate-limit headers = %#v", allowed[0].Header())
	}
	if allowed[1].Header().Get("RateLimit-Remaining") != "0" {
		t.Fatalf("second rate-limit headers = %#v", allowed[1].Header())
	}
	resetAt := allowed[0].Header().Get("RateLimit-Reset")
	if resetAt == "" || allowed[1].Header().Get("RateLimit-Reset") != resetAt {
		t.Fatalf("allowed rate-limit reset headers = first %q, second %q", resetAt, allowed[1].Header().Get("RateLimit-Reset"))
	}
	limited := securityRequest(t, server, http.MethodGet, "/api/v1/projects/project-main", "", "", token)
	assertStatus(t, limited, http.StatusTooManyRequests)
	assertErrorCode(t, limited, "rate_limited")
	if limited.Header().Get("Retry-After") == "" || limited.Header().Get("RateLimit-Limit") != "2" ||
		limited.Header().Get("RateLimit-Remaining") != "0" || limited.Header().Get("RateLimit-Reset") != resetAt {
		t.Fatalf("rate-limit headers = %#v", limited.Header())
	}
	probe := securityRequest(t, server, http.MethodGet, "/healthz", "", "", "")
	assertStatus(t, probe, http.StatusOK)
	if probe.Header().Get("RateLimit-Limit") != "" {
		t.Fatalf("public health probe was rate limited: headers = %#v", probe.Header())
	}
}

func newSecurityTestServer(
	t *testing.T,
	rateLimit int,
) (*Server, *persistence.FileStore, func(*testing.T, string, string, time.Time) string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	encodedPublicKey, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey() error = %v", err)
	}
	auth, err := security.NewJWTAuthenticator(security.JWTConfig{
		PublicKeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encodedPublicKey})),
		Issuer:       testIssuer,
		Audience:     testAudience,
	})
	if err != nil {
		t.Fatalf("NewJWTAuthenticator() error = %v", err)
	}
	limiter, err := security.NewFixedWindowRateLimiter(rateLimit, time.Minute, nil)
	if err != nil {
		t.Fatalf("NewFixedWindowRateLimiter() error = %v", err)
	}
	store, err := persistence.OpenFileStore(filepath.Join(t.TempDir(), "forgeflow.ffdb"))
	if err != nil {
		t.Fatalf("OpenFileStore() error = %v", err)
	}
	registry, err := execution.NewDemoHandlerRegistry()
	if err != nil {
		t.Fatalf("NewDemoHandlerRegistry() error = %v", err)
	}
	if err := registry.Register("test-fail", execution.TaskHandlerFunc(func(context.Context, execution.TaskRequest) (string, error) {
		return "", errors.New("deterministic terminal failure")
	})); err != nil {
		t.Fatalf("Register(test-fail) error = %v", err)
	}
	server, err := NewServer(store, registry, 2, auth, limiter)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})
	issue := func(t *testing.T, subject, name string, expiresAt time.Time) string {
		t.Helper()
		claims := jwt.MapClaims{
			"sub": subject, "name": name, "iss": testIssuer, "aud": testAudience,
			"iat": time.Now().Unix(), "exp": expiresAt.Unix(),
		}
		token, err := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims).SignedString(privateKey)
		if err != nil {
			t.Fatalf("SignedString() error = %v", err)
		}
		return token
	}
	return server, store, issue
}

func provisionProjectSecurity(t *testing.T, store *persistence.FileStore) {
	t.Helper()
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	admin := security.User{ID: "admin", DisplayName: "Admin", CreatedAt: now}
	project := security.Project{ID: "project-main", Name: "Project", CreatedBy: admin.ID, CreatedAt: now, UpdatedAt: now}
	adminMembership := security.Membership{ProjectID: project.ID, UserID: admin.ID, Role: security.RoleAdmin, CreatedAt: now, UpdatedAt: now}
	event := security.AuditEvent{ID: "project-created", ProjectID: project.ID, ActorUserID: admin.ID,
		Action: "project.created", ResourceType: "project", ResourceID: string(project.ID), OccurredAt: now}
	if err := store.CreateProject(context.Background(), admin, project, adminMembership, event); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	for index, fixture := range []struct {
		id   security.UserID
		role security.Role
	}{{"member", security.RoleMember}, {"operator", security.RoleOperator}, {"outsider", security.RoleMember}} {
		user := security.User{ID: fixture.id, DisplayName: string(fixture.id), CreatedAt: now.Add(time.Duration(index+1) * time.Second)}
		membershipProject := project.ID
		if fixture.id == "outsider" {
			membershipProject = "other-project"
			other := security.Project{ID: membershipProject, Name: "Other", CreatedBy: user.ID, CreatedAt: user.CreatedAt, UpdatedAt: user.CreatedAt}
			otherMembership := security.Membership{ProjectID: other.ID, UserID: user.ID, Role: security.RoleAdmin,
				CreatedAt: user.CreatedAt, UpdatedAt: user.CreatedAt}
			otherEvent := security.AuditEvent{ID: "other-created", ProjectID: other.ID, ActorUserID: user.ID,
				Action: "project.created", ResourceType: "project", ResourceID: string(other.ID), OccurredAt: user.CreatedAt}
			if err := store.CreateProject(context.Background(), user, other, otherMembership, otherEvent); err != nil {
				t.Fatalf("CreateProject(other) error = %v", err)
			}
			continue
		}
		membership := security.Membership{ProjectID: membershipProject, UserID: user.ID, Role: fixture.role,
			CreatedAt: user.CreatedAt, UpdatedAt: user.CreatedAt}
		membershipEvent := security.AuditEvent{ID: security.AuditEventID("member-" + string(fixture.id)), ProjectID: project.ID,
			ActorUserID: admin.ID, Action: "membership.updated", ResourceType: "user", ResourceID: string(user.ID), OccurredAt: user.CreatedAt}
		if err := store.PutMembership(context.Background(), user, membership, membershipEvent); err != nil {
			t.Fatalf("PutMembership(%s) error = %v", fixture.id, err)
		}
	}
}

func securityRequest(
	t *testing.T,
	handler http.Handler,
	method, path, body, contentType, token string,
) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, nil)
	if body != "" {
		request = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
