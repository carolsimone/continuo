// Package testmigrations applies Flyway-style migration files against a
// *sql.DB at test setup time, keeping integration-test schemas in lock-step
// with production migrations and eliminating the recurring drift-from-
// hardcoded-DDL bug.
//
// It is intended for test code only; callers pass an absolute directory path
// containing `V<n>__*.sql` files and the package executes them in ascending
// version order.
package testmigrations

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// migrationVersionRE matches Flyway versioned migrations: V<n>__<description>.sql
var migrationVersionRE = regexp.MustCompile(`^V(\d+)__`)

// Apply runs every V<n>__*.sql file in migrationDir against db, in ascending
// version order. Non-`.sql` files and non-versioned filenames are ignored.
//
// Each file's contents are executed as a single Exec call; multi-statement
// SQL is supported via lib/pq's batched execution. Errors short-circuit and
// return wrapped with the failing file's name.
func Apply(db *sql.DB, migrationDir string) error {
	entries, err := os.ReadDir(migrationDir)
	if err != nil {
		return fmt.Errorf("read migration dir %s: %w", migrationDir, err)
	}

	type migration struct {
		version int
		name    string
		path    string
	}
	var migrations []migration
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		match := migrationVersionRE.FindStringSubmatch(e.Name())
		if match == nil {
			continue
		}
		v, err := strconv.Atoi(match[1])
		if err != nil {
			return fmt.Errorf("parse version from %s: %w", e.Name(), err)
		}
		migrations = append(migrations, migration{v, e.Name(), filepath.Join(migrationDir, e.Name())})
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].version < migrations[j].version })

	for _, m := range migrations {
		contents, err := os.ReadFile(m.path)
		if err != nil {
			return fmt.Errorf("read %s: %w", m.name, err)
		}
		if _, err := db.Exec(string(contents)); err != nil {
			return fmt.Errorf("apply %s: %w", m.name, err)
		}
	}
	return nil
}
