// Package persistence contains durable implementations of execution.Store.
package persistence

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/vijaypratap3364/forgeflow/internal/execution"
	"github.com/vijaypratap3364/forgeflow/internal/security"
	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

const journalVersion = 1

const (
	operationSaveWorkflow      = "save_workflow"
	operationCreateRun         = "create_run"
	operationSaveRun           = "save_run"
	operationCreateProject     = "create_project"
	operationSaveProject       = "save_project"
	operationPutMembership     = "put_membership"
	operationSaveOwnedWorkflow = "save_owned_workflow"
)

// FileStore is a single-process embedded store backed by an append-only,
// checksummed JSON journal. Every mutation appends and syncs one complete
// aggregate snapshot before it becomes visible in memory.
type FileStore struct {
	mu          sync.RWMutex
	path        string
	workflows   map[workflow.WorkflowID]workflow.WorkflowDefinition
	runs        map[execution.RunID]execution.WorkflowRunSnapshot
	users       map[security.UserID]security.User
	projects    map[security.ProjectID]security.Project
	memberships map[security.ProjectID]map[security.UserID]security.Membership
	ownerships  map[workflow.WorkflowID]security.WorkflowOwnership
	auditEvents map[security.ProjectID][]security.AuditEvent
	auditIDs    map[security.AuditEventID]struct{}
}

type journalRecord struct {
	Version    int
	Operation  string
	Workflow   *workflow.WorkflowDefinition
	Run        *execution.WorkflowRunSnapshot
	User       *security.User
	Project    *security.Project
	Membership *security.Membership
	Ownership  *security.WorkflowOwnership
	AuditEvent *security.AuditEvent
}

type journalEnvelope struct {
	Checksum string
	Payload  json.RawMessage
}

// OpenFileStore opens or creates a journal at path and reconstructs its latest
// state. An incomplete final record is truncated because it was never committed.
func OpenFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, errors.New("open ForgeFlow file store: path is empty")
	}

	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve ForgeFlow file store path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolutePath), 0o750); err != nil {
		return nil, fmt.Errorf("create ForgeFlow file store directory: %w", err)
	}

	store := &FileStore{
		path:        absolutePath,
		workflows:   make(map[workflow.WorkflowID]workflow.WorkflowDefinition),
		runs:        make(map[execution.RunID]execution.WorkflowRunSnapshot),
		users:       make(map[security.UserID]security.User),
		projects:    make(map[security.ProjectID]security.Project),
		memberships: make(map[security.ProjectID]map[security.UserID]security.Membership),
		ownerships:  make(map[workflow.WorkflowID]security.WorkflowOwnership),
		auditEvents: make(map[security.ProjectID][]security.AuditEvent),
		auditIDs:    make(map[security.AuditEventID]struct{}),
	}
	if err := store.replay(); err != nil {
		return nil, err
	}
	return store, nil
}

// SaveWorkflow persists a validated immutable definition. Re-saving identical
// content is idempotent; different content under the same ID is rejected.
func (store *FileStore) SaveWorkflow(ctx context.Context, definition workflow.WorkflowDefinition) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := definition.Validate(); err != nil {
		return fmt.Errorf("save workflow definition: %w", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if existing, exists := store.workflows[definition.ID]; exists {
		if reflect.DeepEqual(existing, definition) {
			return nil
		}
		return &execution.WorkflowConflictError{WorkflowID: definition.ID}
	}
	if err := contextError(ctx); err != nil {
		return err
	}

	definition = cloneDefinition(definition)
	record := journalRecord{
		Version:   journalVersion,
		Operation: operationSaveWorkflow,
		Workflow:  &definition,
	}
	if err := store.appendRecord(record); err != nil {
		return err
	}
	store.workflows[definition.ID] = definition
	return nil
}

// LoadWorkflow returns a defensive copy of a definition when it exists.
func (store *FileStore) LoadWorkflow(
	ctx context.Context,
	workflowID workflow.WorkflowID,
) (workflow.WorkflowDefinition, bool, error) {
	if err := contextError(ctx); err != nil {
		return workflow.WorkflowDefinition{}, false, err
	}

	store.mu.RLock()
	defer store.mu.RUnlock()
	definition, exists := store.workflows[workflowID]
	if !exists {
		return workflow.WorkflowDefinition{}, false, nil
	}
	return cloneDefinition(definition), true, nil
}

// CreateRun atomically creates a run snapshot at version one and rejects
// duplicate run IDs.
func (store *FileStore) CreateRun(
	ctx context.Context,
	snapshot execution.WorkflowRunSnapshot,
) (execution.WorkflowRunSnapshot, error) {
	if err := contextError(ctx); err != nil {
		return execution.WorkflowRunSnapshot{}, err
	}
	if err := snapshot.Validate(); err != nil {
		return execution.WorkflowRunSnapshot{}, fmt.Errorf("create workflow run: %w", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.runs[snapshot.ID]; exists {
		return execution.WorkflowRunSnapshot{}, &execution.RunAlreadyExistsError{RunID: snapshot.ID}
	}
	if snapshot.Version != 0 {
		return execution.WorkflowRunSnapshot{}, fmt.Errorf(
			"create workflow run %q: initial version must be zero",
			snapshot.ID,
		)
	}
	if err := store.validateRunDefinition(snapshot); err != nil {
		return execution.WorkflowRunSnapshot{}, err
	}
	if err := contextError(ctx); err != nil {
		return execution.WorkflowRunSnapshot{}, err
	}

	snapshot = cloneSnapshot(snapshot)
	snapshot.Version = 1
	record := journalRecord{
		Version:   journalVersion,
		Operation: operationCreateRun,
		Run:       &snapshot,
	}
	if err := store.appendRecord(record); err != nil {
		return execution.WorkflowRunSnapshot{}, err
	}
	store.runs[snapshot.ID] = snapshot
	return cloneSnapshot(snapshot), nil
}

// SaveRun atomically replaces the latest snapshot when its expected Version is
// current, then returns the stored snapshot with the next version.
func (store *FileStore) SaveRun(
	ctx context.Context,
	snapshot execution.WorkflowRunSnapshot,
) (execution.WorkflowRunSnapshot, error) {
	if err := contextError(ctx); err != nil {
		return execution.WorkflowRunSnapshot{}, err
	}
	if err := snapshot.Validate(); err != nil {
		return execution.WorkflowRunSnapshot{}, fmt.Errorf("save workflow run: %w", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	current, exists := store.runs[snapshot.ID]
	if !exists {
		return execution.WorkflowRunSnapshot{}, &execution.RunNotFoundError{RunID: snapshot.ID}
	}
	if snapshot.Version != current.Version {
		return execution.WorkflowRunSnapshot{}, &execution.RunVersionConflictError{
			RunID:           snapshot.ID,
			ExpectedVersion: snapshot.Version,
			ActualVersion:   current.Version,
		}
	}
	if err := store.validateRunDefinition(snapshot); err != nil {
		return execution.WorkflowRunSnapshot{}, err
	}
	if err := contextError(ctx); err != nil {
		return execution.WorkflowRunSnapshot{}, err
	}

	snapshot = cloneSnapshot(snapshot)
	snapshot.Version++
	record := journalRecord{
		Version:   journalVersion,
		Operation: operationSaveRun,
		Run:       &snapshot,
	}
	if err := store.appendRecord(record); err != nil {
		return execution.WorkflowRunSnapshot{}, err
	}
	store.runs[snapshot.ID] = snapshot
	return cloneSnapshot(snapshot), nil
}

// LoadRun returns a defensive copy of a run snapshot when it exists.
func (store *FileStore) LoadRun(
	ctx context.Context,
	runID execution.RunID,
) (execution.WorkflowRunSnapshot, bool, error) {
	if err := contextError(ctx); err != nil {
		return execution.WorkflowRunSnapshot{}, false, err
	}

	store.mu.RLock()
	defer store.mu.RUnlock()
	snapshot, exists := store.runs[runID]
	if !exists {
		return execution.WorkflowRunSnapshot{}, false, nil
	}
	return cloneSnapshot(snapshot), true, nil
}

// CreateProject atomically records a project, its creator, initial admin
// membership, and the corresponding immutable audit event.
func (store *FileStore) CreateProject(
	ctx context.Context,
	user security.User,
	project security.Project,
	membership security.Membership,
	event security.AuditEvent,
) error {
	if err := validateProjectCreation(ctx, user, project, membership, event); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.projects[project.ID]; exists {
		return &security.ProjectAlreadyExistsError{ProjectID: project.ID}
	}
	if existing, exists := store.users[user.ID]; exists {
		if existing.Disabled {
			return &security.UserDisabledError{UserID: user.ID}
		}
		user = existing
	}
	if _, exists := store.auditIDs[event.ID]; exists {
		return &security.AuditEventAlreadyExistsError{EventID: event.ID}
	}
	record := journalRecord{Version: journalVersion, Operation: operationCreateProject,
		User: &user, Project: &project, Membership: &membership, AuditEvent: &event}
	if err := store.appendRecord(record); err != nil {
		return err
	}
	store.applySecurityRecord(record)
	return nil
}

// LoadUser returns a user known through project creation or membership.
func (store *FileStore) LoadUser(ctx context.Context, userID security.UserID) (security.User, bool, error) {
	if err := contextError(ctx); err != nil {
		return security.User{}, false, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	user, exists := store.users[userID]
	return user, exists, nil
}

// LoadProject returns one project when it exists.
func (store *FileStore) LoadProject(ctx context.Context, projectID security.ProjectID) (security.Project, bool, error) {
	if err := contextError(ctx); err != nil {
		return security.Project{}, false, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	project, exists := store.projects[projectID]
	return project, exists, nil
}

// SaveProject atomically updates project metadata and appends its audit event.
func (store *FileStore) SaveProject(ctx context.Context, project security.Project, event security.AuditEvent) error {
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
	store.mu.Lock()
	defer store.mu.Unlock()
	existing, exists := store.projects[project.ID]
	if !exists {
		return &security.ProjectNotFoundError{ProjectID: project.ID}
	}
	if existing.CreatedAt != project.CreatedAt || existing.CreatedBy != project.CreatedBy {
		return errors.New("save project: immutable creator fields changed")
	}
	if _, exists := store.auditIDs[event.ID]; exists {
		return &security.AuditEventAlreadyExistsError{EventID: event.ID}
	}
	record := journalRecord{Version: journalVersion, Operation: operationSaveProject,
		Project: &project, AuditEvent: &event}
	if err := store.appendRecord(record); err != nil {
		return err
	}
	store.applySecurityRecord(record)
	return nil
}

// PutMembership atomically creates or replaces a project membership and
// records who performed the administrative change.
func (store *FileStore) PutMembership(
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
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.projects[membership.ProjectID]; !exists {
		return &security.ProjectNotFoundError{ProjectID: membership.ProjectID}
	}
	if existing, exists := store.users[user.ID]; exists {
		if existing.Disabled {
			return &security.UserDisabledError{UserID: user.ID}
		}
		user = existing
	}
	if existing, exists := store.memberships[membership.ProjectID][membership.UserID]; exists {
		membership.CreatedAt = existing.CreatedAt
		if membership.UpdatedAt.Before(existing.UpdatedAt) {
			return errors.New("put membership: update timestamp moved backwards")
		}
	}
	if _, exists := store.auditIDs[event.ID]; exists {
		return &security.AuditEventAlreadyExistsError{EventID: event.ID}
	}
	record := journalRecord{Version: journalVersion, Operation: operationPutMembership,
		User: &user, Membership: &membership, AuditEvent: &event}
	if err := store.appendRecord(record); err != nil {
		return err
	}
	store.applySecurityRecord(record)
	return nil
}

// LoadMembership returns one user's project role when assigned.
func (store *FileStore) LoadMembership(
	ctx context.Context,
	projectID security.ProjectID,
	userID security.UserID,
) (security.Membership, bool, error) {
	if err := contextError(ctx); err != nil {
		return security.Membership{}, false, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	membership, exists := store.memberships[projectID][userID]
	return membership, exists, nil
}

// ListMemberships returns stable user-ID ordering for deterministic APIs.
func (store *FileStore) ListMemberships(ctx context.Context, projectID security.ProjectID) ([]security.Membership, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	memberships := make([]security.Membership, 0, len(store.memberships[projectID]))
	for _, membership := range store.memberships[projectID] {
		memberships = append(memberships, membership)
	}
	slices.SortFunc(memberships, func(left, right security.Membership) int {
		return strings.Compare(string(left.UserID), string(right.UserID))
	})
	return memberships, nil
}

// SaveWorkflowForProject atomically persists an immutable definition and its
// project ownership boundary.
func (store *FileStore) SaveWorkflowForProject(
	ctx context.Context,
	definition workflow.WorkflowDefinition,
	ownership security.WorkflowOwnership,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := definition.Validate(); err != nil {
		return fmt.Errorf("save owned workflow definition: %w", err)
	}
	if err := ownership.Validate(); err != nil {
		return fmt.Errorf("save workflow ownership: %w", err)
	}
	if ownership.WorkflowID != definition.ID {
		return errors.New("save workflow ownership: definition identity does not match")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.projects[ownership.ProjectID]; !exists {
		return &security.ProjectNotFoundError{ProjectID: ownership.ProjectID}
	}
	if _, exists := store.memberships[ownership.ProjectID][ownership.OwnerUserID]; !exists {
		return errors.New("save workflow ownership: owner is not a project member")
	}
	if owner := store.users[ownership.OwnerUserID]; owner.Disabled {
		return &security.UserDisabledError{UserID: ownership.OwnerUserID}
	}
	if existing, exists := store.workflows[definition.ID]; exists && !reflect.DeepEqual(existing, definition) {
		return &execution.WorkflowConflictError{WorkflowID: definition.ID}
	}
	if existing, exists := store.ownerships[definition.ID]; exists {
		if reflect.DeepEqual(existing, ownership) {
			return nil
		}
		return &security.WorkflowOwnershipConflictError{WorkflowID: definition.ID}
	}
	definition = cloneDefinition(definition)
	record := journalRecord{Version: journalVersion, Operation: operationSaveOwnedWorkflow,
		Workflow: &definition, Ownership: &ownership}
	if err := store.appendRecord(record); err != nil {
		return err
	}
	store.applySecurityRecord(record)
	return nil
}

// LoadWorkflowOwnership resolves a workflow to its authorization boundary.
func (store *FileStore) LoadWorkflowOwnership(
	ctx context.Context,
	workflowID workflow.WorkflowID,
) (security.WorkflowOwnership, bool, error) {
	if err := contextError(ctx); err != nil {
		return security.WorkflowOwnership{}, false, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	ownership, exists := store.ownerships[workflowID]
	return ownership, exists, nil
}

// ListAuditEvents returns newest administrative events first.
func (store *FileStore) ListAuditEvents(
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
	store.mu.RLock()
	defer store.mu.RUnlock()
	source := store.auditEvents[projectID]
	if limit > len(source) {
		limit = len(source)
	}
	events := make([]security.AuditEvent, 0, limit)
	for index := len(source) - 1; index >= len(source)-limit; index-- {
		events = append(events, cloneSecurityAuditEvent(source[index]))
	}
	return events, nil
}

func (store *FileStore) validateRunDefinition(snapshot execution.WorkflowRunSnapshot) error {
	definition, exists := store.workflows[snapshot.WorkflowID]
	if !exists {
		return &execution.WorkflowNotFoundError{WorkflowID: snapshot.WorkflowID}
	}
	if _, err := execution.RestoreWorkflowRun(snapshot, definition); err != nil {
		return fmt.Errorf("validate workflow run against definition: %w", err)
	}
	return nil
}

func (store *FileStore) appendRecord(record journalRecord) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode ForgeFlow journal record: %w", err)
	}
	checksum := sha256.Sum256(payload)
	envelope := journalEnvelope{
		Checksum: hex.EncodeToString(checksum[:]),
		Payload:  payload,
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode ForgeFlow journal envelope: %w", err)
	}
	encoded = append(encoded, '\n')

	file, err := os.OpenFile(store.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open ForgeFlow journal for append: %w", err)
	}
	writeErr := writeAll(file, encoded)
	syncErr := error(nil)
	if writeErr == nil {
		syncErr = file.Sync()
	}
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return fmt.Errorf("commit ForgeFlow journal record: %w", err)
	}
	return nil
}

func (store *FileStore) replay() error {
	file, err := os.OpenFile(store.path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open ForgeFlow journal: %w", err)
	}

	reader := bufio.NewReader(file)
	committedOffset := int64(0)
	for {
		line, readErr := reader.ReadBytes('\n')
		if readErr == nil {
			if err := store.replayLine(line); err != nil {
				closeErr := file.Close()
				return errors.Join(err, closeErr)
			}
			committedOffset += int64(len(line))
			continue
		}
		if !errors.Is(readErr, io.EOF) {
			closeErr := file.Close()
			return errors.Join(fmt.Errorf("read ForgeFlow journal: %w", readErr), closeErr)
		}

		if len(line) > 0 {
			if err := file.Truncate(committedOffset); err != nil {
				closeErr := file.Close()
				return errors.Join(fmt.Errorf("truncate incomplete ForgeFlow journal record: %w", err), closeErr)
			}
			if err := file.Sync(); err != nil {
				closeErr := file.Close()
				return errors.Join(fmt.Errorf("sync truncated ForgeFlow journal: %w", err), closeErr)
			}
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("close ForgeFlow journal: %w", err)
		}
		return nil
	}
}

func (store *FileStore) replayLine(line []byte) error {
	var envelope journalEnvelope
	if err := json.Unmarshal(line, &envelope); err != nil {
		return fmt.Errorf("decode ForgeFlow journal envelope: %w", err)
	}
	actualChecksum := sha256.Sum256(envelope.Payload)
	if envelope.Checksum != hex.EncodeToString(actualChecksum[:]) {
		return errors.New("verify ForgeFlow journal record: checksum mismatch")
	}

	var record journalRecord
	if err := json.Unmarshal(envelope.Payload, &record); err != nil {
		return fmt.Errorf("decode ForgeFlow journal record: %w", err)
	}
	if record.Version != journalVersion {
		return fmt.Errorf("decode ForgeFlow journal record: unsupported version %d", record.Version)
	}
	return store.applyRecord(record)
}

func (store *FileStore) applyRecord(record journalRecord) error {
	switch record.Operation {
	case operationSaveWorkflow:
		if record.Workflow == nil || record.Run != nil {
			return errors.New("replay ForgeFlow journal: malformed workflow record")
		}
		if err := record.Workflow.Validate(); err != nil {
			return fmt.Errorf("replay ForgeFlow workflow definition: %w", err)
		}
		if existing, exists := store.workflows[record.Workflow.ID]; exists &&
			!reflect.DeepEqual(existing, *record.Workflow) {
			return &execution.WorkflowConflictError{WorkflowID: record.Workflow.ID}
		}
		store.workflows[record.Workflow.ID] = cloneDefinition(*record.Workflow)
		return nil

	case operationCreateRun, operationSaveRun:
		if record.Run == nil || record.Workflow != nil {
			return errors.New("replay ForgeFlow journal: malformed workflow run record")
		}
		run := cloneSnapshot(*record.Run)
		current, exists := store.runs[run.ID]
		if run.Version == 0 {
			// Journals created before optimistic versioning did not encode a run
			// version. Replay assigns their revisions in record order.
			run.Version = 1
			if exists {
				run.Version = current.Version + 1
			}
		}
		if err := run.Validate(); err != nil {
			return fmt.Errorf("replay ForgeFlow workflow run: %w", err)
		}
		if err := store.validateRunDefinition(run); err != nil {
			return fmt.Errorf("replay ForgeFlow workflow run: %w", err)
		}
		if record.Operation == operationCreateRun && exists {
			return &execution.RunAlreadyExistsError{RunID: run.ID}
		}
		if record.Operation == operationSaveRun && !exists {
			return &execution.RunNotFoundError{RunID: run.ID}
		}
		if record.Operation == operationCreateRun && run.Version != 1 {
			return fmt.Errorf("replay ForgeFlow workflow run %q: create version is %d, want 1", run.ID, run.Version)
		}
		if record.Operation == operationSaveRun && run.Version != current.Version+1 {
			return fmt.Errorf(
				"replay ForgeFlow workflow run %q: version is %d, want %d",
				run.ID,
				run.Version,
				current.Version+1,
			)
		}
		store.runs[run.ID] = run
		return nil

	case operationCreateProject:
		if record.User == nil || record.Project == nil || record.Membership == nil || record.AuditEvent == nil {
			return errors.New("replay ForgeFlow journal: malformed create project record")
		}
		if err := validateProjectCreation(context.Background(), *record.User, *record.Project, *record.Membership, *record.AuditEvent); err != nil {
			return fmt.Errorf("replay ForgeFlow project: %w", err)
		}
		if _, exists := store.projects[record.Project.ID]; exists {
			return &security.ProjectAlreadyExistsError{ProjectID: record.Project.ID}
		}
		return store.applySecurityRecord(record)

	case operationSaveProject:
		if record.Project == nil || record.AuditEvent == nil {
			return errors.New("replay ForgeFlow journal: malformed save project record")
		}
		if err := record.Project.Validate(); err != nil {
			return fmt.Errorf("replay ForgeFlow project: %w", err)
		}
		if _, exists := store.projects[record.Project.ID]; !exists {
			return &security.ProjectNotFoundError{ProjectID: record.Project.ID}
		}
		if record.AuditEvent.ProjectID != record.Project.ID {
			return errors.New("replay ForgeFlow project: audit project does not match")
		}
		return store.applySecurityRecord(record)

	case operationPutMembership:
		if record.User == nil || record.Membership == nil || record.AuditEvent == nil {
			return errors.New("replay ForgeFlow journal: malformed membership record")
		}
		if err := record.User.Validate(); err != nil {
			return fmt.Errorf("replay ForgeFlow membership user: %w", err)
		}
		if err := record.Membership.Validate(); err != nil {
			return fmt.Errorf("replay ForgeFlow membership: %w", err)
		}
		if record.Membership.UserID != record.User.ID || record.AuditEvent.ProjectID != record.Membership.ProjectID {
			return errors.New("replay ForgeFlow membership: identities do not match")
		}
		if _, exists := store.projects[record.Membership.ProjectID]; !exists {
			return &security.ProjectNotFoundError{ProjectID: record.Membership.ProjectID}
		}
		return store.applySecurityRecord(record)

	case operationSaveOwnedWorkflow:
		if record.Workflow == nil || record.Ownership == nil {
			return errors.New("replay ForgeFlow journal: malformed owned workflow record")
		}
		if err := record.Workflow.Validate(); err != nil {
			return fmt.Errorf("replay ForgeFlow owned workflow: %w", err)
		}
		if err := record.Ownership.Validate(); err != nil {
			return fmt.Errorf("replay ForgeFlow workflow ownership: %w", err)
		}
		if record.Ownership.WorkflowID != record.Workflow.ID {
			return errors.New("replay ForgeFlow owned workflow: ownership identity does not match")
		}
		if _, exists := store.projects[record.Ownership.ProjectID]; !exists {
			return &security.ProjectNotFoundError{ProjectID: record.Ownership.ProjectID}
		}
		if _, exists := store.memberships[record.Ownership.ProjectID][record.Ownership.OwnerUserID]; !exists {
			return errors.New("replay ForgeFlow owned workflow: owner is not a project member")
		}
		if existing, exists := store.workflows[record.Workflow.ID]; exists && !reflect.DeepEqual(existing, *record.Workflow) {
			return &execution.WorkflowConflictError{WorkflowID: record.Workflow.ID}
		}
		if existing, exists := store.ownerships[record.Workflow.ID]; exists && existing != *record.Ownership {
			return &security.WorkflowOwnershipConflictError{WorkflowID: record.Workflow.ID}
		}
		return store.applySecurityRecord(record)

	default:
		return fmt.Errorf("replay ForgeFlow journal: unknown operation %q", record.Operation)
	}
}

func validateProjectCreation(
	ctx context.Context,
	user security.User,
	project security.Project,
	membership security.Membership,
	event security.AuditEvent,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := user.Validate(); err != nil {
		return fmt.Errorf("create project user: %w", err)
	}
	if err := project.Validate(); err != nil {
		return fmt.Errorf("create project: %w", err)
	}
	if err := membership.Validate(); err != nil {
		return fmt.Errorf("create project membership: %w", err)
	}
	if err := event.Validate(); err != nil {
		return fmt.Errorf("create project audit event: %w", err)
	}
	if project.CreatedBy != user.ID || membership.ProjectID != project.ID || membership.UserID != user.ID ||
		membership.Role != security.RoleAdmin || event.ProjectID != project.ID || event.ActorUserID != user.ID {
		return errors.New("create project: creator, initial admin, or audit identity does not match")
	}
	return nil
}

func (store *FileStore) applySecurityRecord(record journalRecord) error {
	if record.AuditEvent != nil {
		if err := record.AuditEvent.Validate(); err != nil {
			return err
		}
		if _, exists := store.auditIDs[record.AuditEvent.ID]; exists {
			return &security.AuditEventAlreadyExistsError{EventID: record.AuditEvent.ID}
		}
	}
	if record.User != nil {
		if existing, exists := store.users[record.User.ID]; exists {
			if existing.Disabled != record.User.Disabled {
				return fmt.Errorf("user %q immutable disabled state differs", record.User.ID)
			}
		} else {
			store.users[record.User.ID] = *record.User
		}
	}
	if record.Project != nil {
		store.projects[record.Project.ID] = *record.Project
	}
	if record.Membership != nil {
		if store.memberships[record.Membership.ProjectID] == nil {
			store.memberships[record.Membership.ProjectID] = make(map[security.UserID]security.Membership)
		}
		store.memberships[record.Membership.ProjectID][record.Membership.UserID] = *record.Membership
	}
	if record.Workflow != nil {
		store.workflows[record.Workflow.ID] = cloneDefinition(*record.Workflow)
	}
	if record.Ownership != nil {
		store.ownerships[record.Ownership.WorkflowID] = *record.Ownership
	}
	if record.AuditEvent != nil {
		event := cloneSecurityAuditEvent(*record.AuditEvent)
		store.auditEvents[event.ProjectID] = append(store.auditEvents[event.ProjectID], event)
		store.auditIDs[event.ID] = struct{}{}
	}
	return nil
}

func cloneSecurityAuditEvent(event security.AuditEvent) security.AuditEvent {
	clone := event
	if event.Metadata != nil {
		clone.Metadata = make(map[string]string, len(event.Metadata))
		for key, value := range event.Metadata {
			clone.Metadata[key] = value
		}
	}
	return clone
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("ForgeFlow store context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("ForgeFlow store operation: %w", err)
	}
	return nil
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func cloneDefinition(definition workflow.WorkflowDefinition) workflow.WorkflowDefinition {
	clone := definition
	clone.Tasks = make([]workflow.TaskDefinition, len(definition.Tasks))
	for index, task := range definition.Tasks {
		clone.Tasks[index] = task
		clone.Tasks[index].Dependencies = append([]workflow.TaskID(nil), task.Dependencies...)
	}
	return clone
}

func cloneSnapshot(snapshot execution.WorkflowRunSnapshot) execution.WorkflowRunSnapshot {
	clone := snapshot
	clone.Tasks = make([]execution.TaskRun, len(snapshot.Tasks))
	for index, task := range snapshot.Tasks {
		clone.Tasks[index] = task
		if task.Lease != nil {
			lease := *task.Lease
			clone.Tasks[index].Lease = &lease
		}
	}
	clone.Workers = append([]execution.WorkerHeartbeat(nil), snapshot.Workers...)
	return clone
}

var _ execution.Store = (*FileStore)(nil)
var _ security.Store = (*FileStore)(nil)
