package deploycheck

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every outbox table a database has once its migrations ran must carry the
// indexes pkg/outbox's queries rely on (the claim, the per-aggregate order
// check, the dead-letter count) under the table's current name, and the
// trigger that wakes its relay on commit. The migrations are read in Flyway's
// order and their outbox DDL applied to a model of the catalog, so a table,
// an index or a trigger a later migration renames or drops is checked under
// the name it ends with.
func TestEveryOutboxTableHasClaimIndexesAndNotifyTrigger(t *testing.T) {
	dirs, err := filepath.Glob(filepath.Join(repoRoot(t), "db", "migration", "*"))
	require.NoError(t, err)
	tables := 0
	for _, dir := range dirs {
		info, err := os.Stat(dir)
		require.NoError(t, err)
		if !info.IsDir() {
			continue
		}
		cat := catalogOf(readMigrations(t, dir))
		tables += len(cat.tables)
		for _, problem := range cat.problems() {
			t.Errorf("%s: %s", filepath.Base(dir), problem)
		}
	}
	assert.Equal(t, 6, tables, "outbox tables found under db/migration")
}

// A rename the catalog model does not follow would pass a migration that leaves
// an outbox table without its claim indexes or notify trigger. Deleting, from
// run-controller's migrations, the statement that gives the table, one of its
// indexes or its trigger the name the service's outbox has must therefore fail
// the check, one object at a time.
func TestOutboxCheckFailsWhenARenameIsDeleted(t *testing.T) {
	dir := filepath.Join(repoRoot(t), "db", "migration", "run_controller")
	assert.Empty(t, catalogOf(readMigrations(t, dir)).problems(), "the migrations as committed")

	const table = "run_controller_outbox"
	for _, tc := range []struct {
		name    string
		rename  *regexp.Regexp // the statement to delete
		problem string         // what the check must then report
	}{
		{
			"the claim index rename",
			regexp.MustCompile(`^ALTER INDEX \S+ RENAME TO idx_` + table + `_claimable$`),
			"lacks index idx_" + table + "_claimable",
		},
		{
			"the open-by-aggregate index rename",
			regexp.MustCompile(`^ALTER INDEX \S+ RENAME TO idx_` + table + `_open_by_aggregate$`),
			"lacks index idx_" + table + "_open_by_aggregate",
		},
		{
			"the failed index rename",
			regexp.MustCompile(`^ALTER INDEX \S+ RENAME TO idx_` + table + `_failed$`),
			"lacks index idx_" + table + "_failed",
		},
		{
			"the trigger rename",
			regexp.MustCompile(`^ALTER TRIGGER \S+ ON ` + table + ` RENAME TO ` + table + `_notify$`),
			"lacks trigger " + table + "_notify",
		},
		{
			"the table rename",
			regexp.MustCompile(`^ALTER TABLE \S+ RENAME TO ` + table + `$`),
			"lacks index",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var edited []migration
			removed := 0
			for _, m := range readMigrations(t, dir) {
				without, n := dropStatements(m.sql, tc.rename)
				removed += n
				edited = append(edited, migration{m.version, m.name, without})
			}
			require.Equal(t, 1, removed, "exactly one migration statement matches %s: update this test with the migration", tc.rename)

			problems := catalogOf(edited).problems()
			require.NotEmpty(t, problems, "the check accepted a database without %s", tc.name)
			assert.Contains(t, strings.Join(problems, "\n"), tc.problem)
		})
	}
}

// The model follows a table, an index and a trigger through their renames and
// drops, and keeps the definitions the checks compare.
func TestOutboxCatalogFollowsRenamesAndDrops(t *testing.T) {
	cat := catalogOf([]migration{
		{1, "V1__init.sql", `
			CREATE TABLE a_outbox (id UUID PRIMARY KEY);
			CREATE TABLE b_outbox (id UUID PRIMARY KEY);
			CREATE TABLE unrelated (id UUID PRIMARY KEY);`},
		{2, "V2__indexes.sql", `
			-- a comment with a semicolon; and an index
			CREATE INDEX IF NOT EXISTS idx_a_outbox_claimable ON a_outbox (created_at, id) WHERE status IN ('pending', 'scheduled');
			CREATE INDEX idx_a_outbox_gone ON a_outbox (created_at);
			CREATE OR REPLACE FUNCTION continuo_outbox_notify() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN PERFORM pg_notify(TG_TABLE_NAME, ''); RETURN NULL; END;
			$$;
			CREATE TRIGGER a_outbox_notify AFTER INSERT ON a_outbox FOR EACH STATEMENT EXECUTE FUNCTION continuo_outbox_notify();
			DROP TABLE IF EXISTS b_outbox CASCADE;`},
		{10, "V10__rename.sql", `
			ALTER TABLE a_outbox RENAME TO c_outbox;
			ALTER INDEX idx_a_outbox_claimable RENAME TO idx_c_outbox_claimable;
			DROP INDEX IF EXISTS idx_a_outbox_gone;
			ALTER TRIGGER a_outbox_notify ON c_outbox RENAME TO c_outbox_notify;`},
	})

	assert.Equal(t, map[string]bool{"c_outbox": true}, cat.tables)
	assert.Equal(t, map[string]catalogEntry{
		"idx_c_outbox_claimable": {table: "c_outbox", def: "(created_at, id) WHERE status IN ('pending', 'scheduled')"},
	}, cat.indexes)
	assert.Equal(t, map[string]catalogEntry{
		"c_outbox_notify": {table: "c_outbox", def: "AFTER INSERT FOR EACH STATEMENT EXECUTE FUNCTION continuo_outbox_notify()"},
	}, cat.triggers)
}

// Flyway applies V2 before V10, though a lexical sort puts V10 first.
func TestReadMigrationsOrdersByVersionNumber(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"V10__b.sql", "V2__a.sql", "V1__z.sql"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("SELECT 1;"), 0o600))
	}
	var got []string
	for _, m := range readMigrations(t, dir) {
		got = append(got, m.name)
	}
	assert.Equal(t, []string{"V1__z.sql", "V2__a.sql", "V10__b.sql"}, got)
}

// migration is one versioned SQL file of a database.
type migration struct {
	version int
	name    string
	sql     string
}

// readMigrations returns the versioned migrations of dir in the order Flyway
// applies them.
func readMigrations(t *testing.T, dir string) []migration {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "V*.sql"))
	require.NoError(t, err)
	versioned := regexp.MustCompile(`^V(\d+)__`)
	var out []migration
	for _, f := range files {
		m := versioned.FindStringSubmatch(filepath.Base(f))
		require.NotNil(t, m, "%s is not named V<version>__<description>.sql", f)
		version, err := strconv.Atoi(m[1])
		require.NoError(t, err)
		b, err := os.ReadFile(f) //nolint:gosec // G304: f comes from globbing dir under db/migration, not external input
		require.NoError(t, err)
		out = append(out, migration{version, filepath.Base(f), string(b)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out
}

// dropStatements returns sql without the statements that match drop, and how
// many it removed.
func dropStatements(sql string, drop *regexp.Regexp) (string, int) {
	var kept []string
	removed := 0
	for _, s := range statements(sql) {
		if drop.MatchString(s) {
			removed++
			continue
		}
		kept = append(kept, s)
	}
	return strings.Join(kept, ";\n") + ";\n", removed
}

var lineComment = regexp.MustCompile(`--[^\n]*`)

// statements splits sql into its statements, each with its whitespace
// collapsed and no trailing semicolon: line comments are removed and a
// semicolon inside a dollar-quoted body does not end a statement.
func statements(sql string) []string {
	sql = lineComment.ReplaceAllString(sql, "")
	var out []string
	var cur strings.Builder
	flush := func() {
		if s := strings.Join(strings.Fields(cur.String()), " "); s != "" {
			out = append(out, s)
		}
		cur.Reset()
	}
	inBody := false
	for i := 0; i < len(sql); i++ {
		switch {
		case strings.HasPrefix(sql[i:], "$$"):
			inBody = !inBody
			cur.WriteString("$$")
			i++
		case sql[i] == ';' && !inBody:
			flush()
		default:
			cur.WriteByte(sql[i])
		}
	}
	flush()
	return out
}

// catalogEntry is an index or a trigger: the outbox table it is on and its
// definition without the table's name, so a rename of the table keeps it.
type catalogEntry struct{ table, def string }

// outboxCatalog models the outbox tables, their indexes and their triggers
// that a database's migrations leave behind. Only objects on a table named
// *_outbox are tracked.
type outboxCatalog struct {
	tables   map[string]bool
	indexes  map[string]catalogEntry
	triggers map[string]catalogEntry
}

const outboxTableName = `[a-z0-9_]+_outbox`

var (
	createTableRe   = regexp.MustCompile(`(?i)^CREATE TABLE (?:IF NOT EXISTS )?(` + outboxTableName + `)\b`)
	dropTableRe     = regexp.MustCompile(`(?i)^DROP TABLE (?:IF EXISTS )?(` + outboxTableName + `)\b`)
	renameTableRe   = regexp.MustCompile(`(?i)^ALTER TABLE (?:IF EXISTS )?(` + outboxTableName + `) RENAME TO (` + outboxTableName + `)$`)
	createIndexRe   = regexp.MustCompile(`(?i)^CREATE (?:UNIQUE )?INDEX (?:IF NOT EXISTS )?([a-z0-9_]+) ON (` + outboxTableName + `) (.+)$`)
	dropIndexRe     = regexp.MustCompile(`(?i)^DROP INDEX (?:IF EXISTS )?([a-z0-9_, ]+?)(?: CASCADE| RESTRICT)?$`)
	renameIndexRe   = regexp.MustCompile(`(?i)^ALTER INDEX (?:IF EXISTS )?([a-z0-9_]+) RENAME TO ([a-z0-9_]+)$`)
	createTriggerRe = regexp.MustCompile(`(?i)^CREATE (?:OR REPLACE )?TRIGGER ([a-z0-9_]+) (.+?) ON (` + outboxTableName + `) (.+)$`)
	dropTriggerRe   = regexp.MustCompile(`(?i)^DROP TRIGGER (?:IF EXISTS )?([a-z0-9_]+) ON (` + outboxTableName + `)\b`)
	renameTriggerRe = regexp.MustCompile(`(?i)^ALTER TRIGGER ([a-z0-9_]+) ON (` + outboxTableName + `) RENAME TO ([a-z0-9_]+)$`)
)

// catalogOf applies the outbox DDL of migrations, in the order given, to a
// fresh catalog.
func catalogOf(migrations []migration) *outboxCatalog {
	c := &outboxCatalog{
		tables:   map[string]bool{},
		indexes:  map[string]catalogEntry{},
		triggers: map[string]catalogEntry{},
	}
	for _, m := range migrations {
		for _, stmt := range statements(m.sql) {
			c.apply(stmt)
		}
	}
	return c
}

func (c *outboxCatalog) apply(stmt string) {
	if m := createTableRe.FindStringSubmatch(stmt); m != nil {
		c.tables[m[1]] = true
	} else if m := dropTableRe.FindStringSubmatch(stmt); m != nil {
		delete(c.tables, m[1])
		c.forget(m[1])
	} else if m := renameTableRe.FindStringSubmatch(stmt); m != nil {
		delete(c.tables, m[1])
		c.tables[m[2]] = true
		for _, entries := range []map[string]catalogEntry{c.indexes, c.triggers} {
			for name, e := range entries {
				if e.table == m[1] {
					entries[name] = catalogEntry{table: m[2], def: e.def}
				}
			}
		}
	} else if m := createIndexRe.FindStringSubmatch(stmt); m != nil {
		c.indexes[m[1]] = catalogEntry{table: m[2], def: m[3]}
	} else if m := dropIndexRe.FindStringSubmatch(stmt); m != nil {
		for _, name := range strings.Split(m[1], ",") {
			delete(c.indexes, strings.TrimSpace(name))
		}
	} else if m := renameIndexRe.FindStringSubmatch(stmt); m != nil {
		if e, ok := c.indexes[m[1]]; ok {
			delete(c.indexes, m[1])
			c.indexes[m[2]] = e
		}
	} else if m := createTriggerRe.FindStringSubmatch(stmt); m != nil {
		c.triggers[m[1]] = catalogEntry{table: m[3], def: m[2] + " " + m[4]}
	} else if m := dropTriggerRe.FindStringSubmatch(stmt); m != nil {
		delete(c.triggers, m[1])
	} else if m := renameTriggerRe.FindStringSubmatch(stmt); m != nil {
		if e, ok := c.triggers[m[1]]; ok && e.table == m[2] {
			delete(c.triggers, m[1])
			c.triggers[m[3]] = e
		}
	}
}

// forget drops the indexes and triggers of a table that no longer exists.
func (c *outboxCatalog) forget(table string) {
	for _, entries := range []map[string]catalogEntry{c.indexes, c.triggers} {
		for name, e := range entries {
			if e.table == table {
				delete(entries, name)
			}
		}
	}
}

// problems lists what each live outbox table lacks of the indexes pkg/outbox
// queries and the trigger that notifies its relay, each under the name the
// table's own name gives it.
func (c *outboxCatalog) problems() []string {
	var out []string
	for table := range c.tables {
		for _, want := range []struct{ name, def string }{
			{"idx_" + table + "_claimable", "(created_at, id) WHERE status IN ('pending', 'scheduled')"},
			{"idx_" + table + "_open_by_aggregate", "(aggregate_type, aggregate_id, created_at) WHERE status IN ('pending', 'scheduled')"},
			{"idx_" + table + "_failed", "(created_at) WHERE status = 'failed'"},
		} {
			if got, ok := c.indexes[want.name]; !ok || got.table != table || got.def != want.def {
				out = append(out, fmt.Sprintf("%s lacks index %s %s", table, want.name, want.def))
			}
		}
		notify := table + "_notify"
		if got, ok := c.triggers[notify]; !ok || got.table != table ||
			got.def != "AFTER INSERT FOR EACH STATEMENT EXECUTE FUNCTION continuo_outbox_notify()" {
			out = append(out, fmt.Sprintf("%s lacks trigger %s (AFTER INSERT, FOR EACH STATEMENT, continuo_outbox_notify())", table, notify))
		}
	}
	sort.Strings(out)
	return out
}
