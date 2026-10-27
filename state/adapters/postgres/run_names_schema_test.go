package postgres_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// oldRunNames matches every name the run tables carried before they were named
// runs and tasks: the two table names, the run key column and the progress
// clock column.
const oldRunNames = `scheduler_tracker|task_tracker|schedule_id|last_heartbeat_at`

// No constraint, index, trigger, function or column of the state schema is
// named after the old run tables or their old columns, and none of their
// definitions refers to them.
func TestRunNamesSchema_NoObjectNamesTheOldTables(t *testing.T) {
	db := newTestDB(t)

	var objects []string
	require.NoError(t, db.Select(&objects, `
		SELECT 'constraint ' || conname || ': ' || pg_get_constraintdef(c.oid)
		  FROM pg_constraint c
		  JOIN pg_namespace n ON n.oid = c.connamespace
		 WHERE n.nspname = 'public'
		   AND (conname ~ $1 OR pg_get_constraintdef(c.oid) ~ $1)
		UNION ALL
		SELECT 'index ' || indexname || ': ' || indexdef
		  FROM pg_indexes
		 WHERE schemaname = 'public' AND (indexname ~ $1 OR indexdef ~ $1)
		UNION ALL
		SELECT 'trigger ' || tgname || ': ' || pg_get_triggerdef(t.oid)
		  FROM pg_trigger t
		 WHERE NOT tgisinternal AND (tgname ~ $1 OR pg_get_triggerdef(t.oid) ~ $1)
		UNION ALL
		SELECT 'function ' || proname
		  FROM pg_proc p
		  JOIN pg_namespace n ON n.oid = p.pronamespace
		 WHERE n.nspname = 'public' AND (proname ~ $1 OR prosrc ~ $1)
		UNION ALL
		SELECT 'column ' || table_name || '.' || column_name
		  FROM information_schema.columns
		 WHERE table_schema = 'public' AND (table_name ~ $1 OR column_name ~ $1)
		UNION ALL
		SELECT 'table ' || tablename
		  FROM pg_tables
		 WHERE schemaname = 'public' AND tablename ~ $1
		ORDER BY 1`, oldRunNames))

	assert.Empty(t, objects, "the state schema still names the old run tables or columns")
}

// The run tables are runs and tasks, keyed by run_id, and a run carries the
// clock of its last progress.
func TestRunNamesSchema_RunsAndTasksExist(t *testing.T) {
	db := newTestDB(t)

	for _, want := range []struct{ table, column string }{
		{"runs", "run_id"},
		{"runs", "last_progress_at"},
		{"tasks", "run_id"},
	} {
		var n int
		require.NoError(t, db.Get(&n, `
			SELECT count(*) FROM information_schema.columns
			 WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2`,
			want.table, want.column))
		assert.Equal(t, 1, n, "%s.%s exists", want.table, want.column)
	}
}

// The constraints and indexes of the run tables are named after runs and
// tasks.
func TestRunNamesSchema_ObjectsTakeTheNewNames(t *testing.T) {
	db := newTestDB(t)

	var constraints []string
	require.NoError(t, db.Select(&constraints, `
		SELECT conname FROM pg_constraint c
		  JOIN pg_namespace n ON n.oid = c.connamespace
		 WHERE n.nspname = 'public' AND conrelid IN ('public.runs'::regclass, 'public.tasks'::regclass)`))
	assert.Subset(t, constraints, []string{"runs_pkey", "tasks_pkey", "tasks_run_id_fkey"})

	var indexes []string
	require.NoError(t, db.Select(&indexes, `
		SELECT indexname FROM pg_indexes
		 WHERE schemaname = 'public' AND tablename IN ('runs', 'tasks')`))
	assert.Subset(t, indexes, []string{
		"uq_runs_active_per_schedule", "idx_runs_kind", "idx_tasks_lease_waiters", "uq_tasks_run_node",
	})
}
