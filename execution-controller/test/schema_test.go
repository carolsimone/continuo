package test

import (
	"regexp"
	"testing"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/stretchr/testify/require"
)

// The migration files must produce exactly the effective schema the
// service is written against. Column order does not matter; names, types,
// nullability and defaults do.
func TestExecutionSchemaMatchesContract(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	type col struct {
		Name, Type, Nullable string
		Default              *string
	}
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
	require.ElementsMatch(t, []string{"id", "message_processing_id", "task_id", "schedule_id", "job_params", "status", "retry_count", "max_retries", "next_attempt_at", "created_at", "deployed_at", "error_message", "mode", "release_id", "node_id", "outcome", "dbt_log_uri", "outcome_at", "run_results_uri", "failed_container", "state_changed_at", "job_name"}, names(columns("deployments")))
	require.ElementsMatch(t, []string{"scope", "created_at"}, names(columns("admission_capacity")))
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
		"idx_execution_outbox_claimable", "idx_execution_outbox_open_by_aggregate", "idx_execution_outbox_failed", "idx_execution_outbox_aggregate",
		"idx_message_processing_outbox_entry_id_stream", "message_processing_message_id_stream_name_key",
		"idx_deployments_due", "idx_deployments_candidate_release", "uq_deployments_candidate_release_node_mode",
		"idx_deployments_message_processing_id", "idx_execution_outbox_message_processing_id",
		"idx_deployments_job_name", "idx_deployments_in_flight",
	})

	require.NotContains(t, indexes, "idx_execution_outbox_pending")
	require.NotContains(t, indexes, "idx_execution_outbox_due")

	var notifyTriggers int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM pg_trigger WHERE tgname='execution_outbox_notify' AND tgrelid='execution_outbox'::regclass AND NOT tgisinternal`).Scan(&notifyTriggers))
	require.Equal(t, 1, notifyTriggers)

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

	// deployments.message_processing_id is provenance only: pruning an aged
	// dedup row clears it rather than being refused.
	var onDelete string
	require.NoError(t, db.QueryRow(`SELECT confdeltype FROM pg_constraint
		WHERE conname = 'deployments_message_processing_id_fkey' AND conrelid = 'deployments'::regclass`).Scan(&onDelete))
	require.Equal(t, "n", onDelete, "deployments.message_processing_id is ON DELETE SET NULL")

	var scopes []string
	require.NoError(t, db.Select(&scopes, `SELECT scope FROM admission_capacity ORDER BY scope`))
	require.Equal(t, []string{"global"}, scopes, "the global capacity record is seeded and never deleted")

	var admissionTriggers int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM pg_trigger WHERE tgrelid='deployments'::regclass
		AND tgname IN ('deployments_accepted_notify','deployments_admission_notify') AND NOT tgisinternal`).Scan(&admissionTriggers))
	require.Equal(t, 2, admissionTriggers)

	// The status sets the SQL repeats must equal the Go state machine's.
	quoted := regexp.MustCompile(`'([a-z_]+)'`)
	valuesIn := func(def string) []string {
		var out []string
		for _, m := range quoted.FindAllStringSubmatch(def, -1) {
			out = append(out, m[1])
		}
		return out
	}
	var statusCheck string
	require.NoError(t, db.QueryRow(`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname='deployments_status_check'`).Scan(&statusCheck))
	require.ElementsMatch(t, model.StatusStrings(model.AllStatuses()), valuesIn(statusCheck),
		"deployments_status_check lists exactly model.AllStatuses()")

	var inFlightIndex string
	require.NoError(t, db.QueryRow(`SELECT pg_get_indexdef('idx_deployments_in_flight'::regclass)`).Scan(&inFlightIndex))
	require.ElementsMatch(t, model.StatusStrings(model.InFlightStatuses()), valuesIn(inFlightIndex),
		"idx_deployments_in_flight covers exactly model.InFlightStatuses()")

	// The admission trigger's WHEN names 'pending' once, then the in-flight set
	// twice (for OLD and NEW).
	var triggerDef string
	require.NoError(t, db.QueryRow(`SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname='deployments_admission_notify'`).Scan(&triggerDef))
	var want []string
	want = append(want, string(model.StatusPending), string(model.StatusPending))
	for i := 0; i < 2; i++ {
		want = append(want, model.StatusStrings(model.InFlightStatuses())...)
	}
	require.ElementsMatch(t, want, valuesIn(triggerDef),
		"deployments_admission_notify wakes on 'pending' and on leaving exactly model.InFlightStatuses(): %s", triggerDef)
}
