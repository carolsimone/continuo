//go:build integration

package postgres_test

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tenantKeyExemptTables are the tables of the run-controller database whose keys do not
// lead with tenant_id: the stream dedup and the outbox, whose rows are keyed by
// their own ids, and Flyway's history.
var tenantKeyExemptTables = []string{"message_processing", "run_controller_outbox", "flyway_schema_history"}

// tableKey is a primary key, unique index or foreign key of a run-controller table, with
// its columns in key order and, for a foreign key, the referenced columns.
type tableKey struct {
	Table      string
	Name       string
	Columns    []string
	RefColumns []string
}

// tableKeys reads every primary key, unique index and foreign key of the
// public tables outside tenantKeyExemptTables, with their columns in key order.
func tableKeys(t *testing.T, db *sqlx.DB) []tableKey {
	t.Helper()
	var rows []struct {
		Table      string `db:"table_name"`
		Name       string `db:"key_name"`
		Columns    string `db:"columns"`
		RefColumns string `db:"ref_columns"`
	}
	require.NoError(t, db.Select(&rows, `
		WITH keys AS (
		    -- primary keys and every unique index, constraint-backed or not
		    SELECT i.indrelid AS rel, ic.relname AS key_name, i.indkey::int2[] AS cols,
		           NULL::oid AS ref_rel, NULL::int2[] AS ref_cols
		    FROM pg_index i
		    JOIN pg_class ic ON ic.oid = i.indexrelid
		    WHERE i.indisunique
		  UNION ALL
		    SELECT c.conrelid, c.conname, c.conkey, c.confrelid, c.confkey
		    FROM pg_constraint c
		    WHERE c.contype = 'f'
		)
		SELECT r.relname AS table_name, k.key_name,
		       (SELECT string_agg(a.attname, ',' ORDER BY o.n)
		          FROM unnest(k.cols) WITH ORDINALITY AS o(attnum, n)
		          JOIN pg_attribute a ON a.attrelid = k.rel AND a.attnum = o.attnum) AS columns,
		       COALESCE((SELECT string_agg(a.attname, ',' ORDER BY o.n)
		          FROM unnest(k.ref_cols) WITH ORDINALITY AS o(attnum, n)
		          JOIN pg_attribute a ON a.attrelid = k.ref_rel AND a.attnum = o.attnum), '') AS ref_columns
		FROM keys k
		JOIN pg_class r ON r.oid = k.rel
		JOIN pg_namespace ns ON ns.oid = r.relnamespace
		WHERE ns.nspname = 'public' AND r.relkind = 'r' AND NOT (r.relname = ANY($1))
		ORDER BY r.relname, k.key_name`, pq.Array(tenantKeyExemptTables)))
	out := make([]tableKey, 0, len(rows))
	for _, row := range rows {
		k := tableKey{Table: row.Table, Name: row.Name, Columns: strings.Split(row.Columns, ",")}
		if row.RefColumns != "" {
			k.RefColumns = strings.Split(row.RefColumns, ",")
		}
		out = append(out, k)
	}
	return out
}

// Every primary key, unique index and foreign key of the run-controller tables, except
// the dedup's and the outbox's, starts with tenant_id, and a foreign key
// references a key that starts with tenant_id too. The keys the run tables and
// the schedules hold are exactly the tenant-led ones listed.
func TestTenantKeysSchema_EveryKeyStartsWithTenantID(t *testing.T) {
	db := newTestDB(t)
	keys := tableKeys(t, db)
	require.NotEmpty(t, keys)

	byName := map[string]tableKey{}
	for _, k := range keys {
		byName[k.Name] = k
		require.NotEmpty(t, k.Columns, "%s.%s has columns", k.Table, k.Name)
		assert.Equal(t, "tenant_id", k.Columns[0], "%s.%s %v starts with tenant_id", k.Table, k.Name, k.Columns)
		if k.RefColumns != nil {
			assert.Equal(t, "tenant_id", k.RefColumns[0], "%s.%s references %v, which starts with tenant_id",
				k.Table, k.Name, k.RefColumns)
		}
	}

	for name, want := range map[string]tableKey{
		"runs_pkey":                   {Table: "runs", Columns: []string{"tenant_id", "run_id"}},
		"uq_runs_active_per_schedule": {Table: "runs", Columns: []string{"tenant_id", "schedule_name"}},
		"tasks_pkey":                  {Table: "tasks", Columns: []string{"tenant_id", "task_id"}},
		"uq_tasks_run_node":           {Table: "tasks", Columns: []string{"tenant_id", "run_id", "node_key"}},
		"tasks_run_id_fkey": {Table: "tasks", Columns: []string{"tenant_id", "run_id"},
			RefColumns: []string{"tenant_id", "run_id"}},
		"task_attempts_pkey": {Table: "task_attempts", Columns: []string{"tenant_id", "task_id", "attempt"}},
		"task_attempts_task_id_fkey": {Table: "task_attempts", Columns: []string{"tenant_id", "task_id"},
			RefColumns: []string{"tenant_id", "task_id"}},
		"node_leases_pkey":        {Table: "node_leases", Columns: []string{"tenant_id", "node_key"}},
		"node_leases_task_id_key": {Table: "node_leases", Columns: []string{"tenant_id", "task_id"}},
		"node_leases_task_id_fkey": {Table: "node_leases", Columns: []string{"tenant_id", "task_id"},
			RefColumns: []string{"tenant_id", "task_id"}},
		"uq_runs_scheduled_for": {Table: "runs", Columns: []string{"tenant_id", "schedule_name", "scheduled_for"}},
		"schedules_pkey":        {Table: "schedules", Columns: []string{"tenant_id", "name"}},
	} {
		got, ok := byName[name]
		if !assert.True(t, ok, "%s exists", name) {
			continue
		}
		want.Name = name
		assert.Equal(t, want, got, name)
	}

	var waiterIndex string
	require.NoError(t, db.Get(&waiterIndex, `SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_tasks_lease_waiters'`))
	assert.Contains(t, waiterIndex, "(tenant_id, node_key, lease_wait_since) WHERE ((wait_reason)::text = 'node_lease'::text)",
		"the waiters of a node are read per tenant")
}

// A task cannot belong to another tenant's run: the foreign key to its run
// includes the tenant.
func TestTenantKeysSchema_ATaskOfAnotherTenantsRunIsRefused(t *testing.T) {
	db := newTestDB(t)
	owner, other := "keys-"+uuid.NewString(), "keys-"+uuid.NewString()
	runID := uuid.New()
	_, err := db.Exec(`
		INSERT INTO runs (tenant_id, run_id, schedule_name, status)
		VALUES ($1, $2, $3, 'running')`, owner, runID, "keys-"+runID.String()[:8])
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM runs WHERE tenant_id = $1 AND run_id = $2`, owner, runID) })

	insertTask := func(tenant string) error {
		_, err := db.Exec(`
			INSERT INTO tasks (tenant_id, task_id, run_id, service_name, schema_name, table_name, job_name, status, max_retries)
			VALUES ($1, $2, $3, 'svc', 'sch', 'tbl', 'job-0', 'pending', 2)`, tenant, uuid.New(), runID)
		return err
	}

	err = insertTask(other)
	require.Error(t, err)
	assert.Equal(t, "23503", pqCode(t, err), "a task of another tenant's run violates its foreign key")
	assert.NoError(t, insertTask(owner), "a task of its own tenant's run is stored")
}

// tenant_id is required on every table that carries it, and has no default:
// an insert that leaves it out fails.
func TestTenantKeysSchema_TenantIDHasNoDefault(t *testing.T) {
	db := newTestDB(t)
	for _, table := range []string{"runs", "tasks", "task_attempts", "node_leases", "schedules"} {
		var col struct {
			Nullable string  `db:"is_nullable"`
			Default  *string `db:"column_default"`
		}
		require.NoError(t, db.Get(&col, `
			SELECT is_nullable, column_default FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = $1 AND column_name = 'tenant_id'`, table), table)
		assert.Equal(t, "NO", col.Nullable, "%s.tenant_id is NOT NULL", table)
		assert.Nil(t, col.Default, "%s.tenant_id has no default", table)
	}

	runID := uuid.New()
	_, err := db.Exec(`INSERT INTO runs (run_id, schedule_name, status) VALUES ($1, $2, 'running')`,
		runID, "keys-"+runID.String()[:8])
	if err == nil {
		_, _ = db.Exec(`DELETE FROM runs WHERE run_id = $1`, runID)
	}
	require.Error(t, err)
	assert.Equal(t, "23502", pqCode(t, err), "a run without a tenant is refused")
}
