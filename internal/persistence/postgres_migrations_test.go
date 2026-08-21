package persistence

import (
	"strings"
	"testing"
)

func TestPostgresMigrationsAreEmbeddedAndVersioned(t *testing.T) {
	t.Parallel()

	migrations, err := loadPostgresMigrations()
	if err != nil {
		t.Fatalf("loadPostgresMigrations() error = %v", err)
	}
	if len(migrations) == 0 {
		t.Fatal("migration count = 0, want at least the initial migration")
	}
	migration := migrations[0]
	if migration.version != 1 || migration.name != "000001_initial.up.sql" {
		t.Fatalf("migration identity = %d %q", migration.version, migration.name)
	}
	if len(migration.checksum) != 64 {
		t.Fatalf("migration checksum length = %d, want 64", len(migration.checksum))
	}
	for _, table := range []string{
		"workflow_definitions",
		"task_definitions",
		"task_dependencies",
		"workflow_runs",
		"task_runs",
		"workers",
		"task_leases",
	} {
		if !strings.Contains(migration.sql, "CREATE TABLE "+table) {
			t.Errorf("initial migration does not create %s", table)
		}
	}
}
