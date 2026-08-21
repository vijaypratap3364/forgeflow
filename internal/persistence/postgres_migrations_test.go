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
	if len(migrations) < 2 {
		t.Fatalf("migration count = %d, want security migration", len(migrations))
	}
	securityMigration := migrations[1]
	if securityMigration.version != 2 || securityMigration.name != "000002_security.up.sql" {
		t.Fatalf("security migration identity = %d %q", securityMigration.version, securityMigration.name)
	}
	for _, table := range []string{"users", "projects", "project_memberships", "workflow_ownership", "audit_events"} {
		if !strings.Contains(securityMigration.sql, "CREATE TABLE "+table) {
			t.Errorf("security migration does not create %s", table)
		}
	}
}
