package test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The five migration files must produce exactly the effective schema the
// service is written against. Column order does not matter; names, types,
// nullability and defaults do.
func TestExecutionSchemaMatchesContract(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	type col struct{ Name, Type, Nullable string; Default *string }
	columns := func(table string) map[string]col {
		rows, err := db.Query(`SELECT column_name, data_type, is_nullable, column_default
			FROM information_schema.columns WHERE table_schema='public' AND table_name=$1`, table)
		require.NoError(t, err)
		defer rows.Close()
		out := map[string]col{}
		for rows.Next() {
			var c col
			require.NoError(t, rows.Scan(&c.Name, &c.Type, &c.Nullable, &c.Default))
			out[c.Name] = c
		}
		return out
	}
	names := func(m map[string]col) []string {
		var n []string
		for k := range m {
			n = append(n, k)
		}
		return n
	}

	require.ElementsMatch(t, []string{"id", "message_id", "stream_name", "state", "payload", "error", "created_at", "updated_at", "outbox_entry_id"}, names(columns("message_processing")))
	require.ElementsMatch(t, []string{"id", "message_processing_id", "aggregate_type", "aggregate_id", "event_type", "payload", "stream_name", "status", "retry_count", "max_retries", "created_at", "processed_at", "error_message", "next_attempt_at"}, names(columns("execution_outbox")))
	require.ElementsMatch(t, []string{"schedule_id", "cancelled_at"}, names(columns("cancelled_schedules")))
	require.ElementsMatch(t, []string{"id", "message_processing_id", "task_id", "schedule_id", "job_params", "status", "retry_count", "max_retries", "next_attempt_at", "created_at", "deployed_at", "error_message", "mode", "release_id", "node_id", "outcome", "dbt_log_uri", "outcome_at", "run_results_uri", "failed_container"}, names(columns("deployments")))
	require.ElementsMatch(t, []string{"release_id", "aggregate_emitted_at", "mode"}, names(columns("validation_aggregates")))

	require.Equal(t, "13", *columns("execution_outbox")["max_retries"].Default)
	require.Equal(t, "NO", columns("deployments")["mode"].Nullable)

	var indexes []string
	rows, err := db.Query(`SELECT indexname FROM pg_indexes WHERE schemaname='public' ORDER BY 1`)
	require.NoError(t, err)
	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		indexes = append(indexes, n)
	}
	rows.Close()
	require.Subset(t, indexes, []string{
		"idx_execution_outbox_pending", "idx_execution_outbox_aggregate", "idx_execution_outbox_due",
		"idx_message_processing_outbox_entry_id_stream", "message_processing_message_id_stream_name_key",
		"idx_deployments_due", "idx_deployments_candidate_release", "uq_deployments_candidate_release_node_mode",
	})

	var checks []string
	rows, err = db.Query(`SELECT conname FROM pg_constraint WHERE contype='c' AND conrelid IN ('deployments'::regclass,'execution_outbox'::regclass,'validation_aggregates'::regclass,'message_processing'::regclass) ORDER BY 1`)
	require.NoError(t, err)
	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		checks = append(checks, n)
	}
	rows.Close()
	require.ElementsMatch(t, []string{
		"deployments_status_check", "deployments_mode_check", "deployments_outcome_check", "deployments_candidate_identity_check",
		"execution_outbox_status_check", "validation_aggregates_mode_check", "message_processing_state_check",
	}, checks)
}
