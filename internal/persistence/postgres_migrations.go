package persistence

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

const postgresMigrationLockID int64 = 0x466F726765466C6F

//go:embed migrations/postgres/*.up.sql
var postgresMigrationFiles embed.FS

type postgresMigration struct {
	version  int64
	name     string
	checksum string
	sql      string
}

// Migrate applies every pending embedded PostgreSQL migration in version order.
// An advisory transaction lock serializes concurrent application startups, and
// checksums detect edits to migrations that were already applied.
func (store *PostgresStore) Migrate(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if _, err := store.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS forgeflow_schema_migrations (
			version bigint PRIMARY KEY,
			name text NOT NULL,
			checksum text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
		)
	`); err != nil {
		return fmt.Errorf("create ForgeFlow migration ledger: %w", err)
	}

	migrations, err := loadPostgresMigrations()
	if err != nil {
		return err
	}
	for _, migration := range migrations {
		if err := store.applyMigration(ctx, migration); err != nil {
			return err
		}
	}
	return nil
}

func (store *PostgresStore) applyMigration(ctx context.Context, migration postgresMigration) error {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin PostgreSQL migration %d: %w", migration.version, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, postgresMigrationLockID); err != nil {
		return fmt.Errorf("lock PostgreSQL migrations: %w", err)
	}
	var appliedChecksum string
	err = tx.QueryRow(
		ctx,
		`SELECT checksum FROM forgeflow_schema_migrations WHERE version = $1`,
		migration.version,
	).Scan(&appliedChecksum)
	switch {
	case err == nil:
		if appliedChecksum != migration.checksum {
			return fmt.Errorf(
				"PostgreSQL migration %d checksum changed: database has %s, binary has %s",
				migration.version,
				appliedChecksum,
				migration.checksum,
			)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit verified PostgreSQL migration %d: %w", migration.version, err)
		}
		return nil
	case !errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("read PostgreSQL migration %d: %w", migration.version, err)
	}

	if _, err := tx.Exec(ctx, migration.sql); err != nil {
		return fmt.Errorf("apply PostgreSQL migration %d (%s): %w", migration.version, migration.name, err)
	}
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO forgeflow_schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`,
		migration.version,
		migration.name,
		migration.checksum,
	); err != nil {
		return fmt.Errorf("record PostgreSQL migration %d: %w", migration.version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit PostgreSQL migration %d: %w", migration.version, err)
	}
	return nil
}

func loadPostgresMigrations() ([]postgresMigration, error) {
	paths, err := fs.Glob(postgresMigrationFiles, "migrations/postgres/*.up.sql")
	if err != nil {
		return nil, fmt.Errorf("list embedded PostgreSQL migrations: %w", err)
	}
	migrations := make([]postgresMigration, 0, len(paths))
	seen := make(map[int64]string, len(paths))
	for _, path := range paths {
		name := filepath.Base(path)
		parts := strings.SplitN(name, "_", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid PostgreSQL migration filename %q", name)
		}
		version, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil || version < 1 {
			return nil, fmt.Errorf("invalid PostgreSQL migration version in %q", name)
		}
		if previous, exists := seen[version]; exists {
			return nil, fmt.Errorf("PostgreSQL migrations %q and %q share version %d", previous, name, version)
		}
		contents, err := postgresMigrationFiles.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read embedded PostgreSQL migration %q: %w", name, err)
		}
		checksum := sha256.Sum256(contents)
		seen[version] = name
		migrations = append(migrations, postgresMigration{
			version:  version,
			name:     name,
			checksum: hex.EncodeToString(checksum[:]),
			sql:      string(contents),
		})
	}
	sort.Slice(migrations, func(left, right int) bool {
		return migrations[left].version < migrations[right].version
	})
	return migrations, nil
}
