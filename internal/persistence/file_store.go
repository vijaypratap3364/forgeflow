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
	"sync"

	"github.com/vijaypratap3364/forgeflow/internal/execution"
	"github.com/vijaypratap3364/forgeflow/internal/workflow"
)

const journalVersion = 1

const (
	operationSaveWorkflow = "save_workflow"
	operationCreateRun    = "create_run"
	operationSaveRun      = "save_run"
)

// FileStore is a single-process embedded store backed by an append-only,
// checksummed JSON journal. Every mutation appends and syncs one complete
// aggregate snapshot before it becomes visible in memory.
type FileStore struct {
	mu        sync.RWMutex
	path      string
	workflows map[workflow.WorkflowID]workflow.WorkflowDefinition
	runs      map[execution.RunID]execution.WorkflowRunSnapshot
}

type journalRecord struct {
	Version   int
	Operation string
	Workflow  *workflow.WorkflowDefinition
	Run       *execution.WorkflowRunSnapshot
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
		path:      absolutePath,
		workflows: make(map[workflow.WorkflowID]workflow.WorkflowDefinition),
		runs:      make(map[execution.RunID]execution.WorkflowRunSnapshot),
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

	default:
		return fmt.Errorf("replay ForgeFlow journal: unknown operation %q", record.Operation)
	}
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
