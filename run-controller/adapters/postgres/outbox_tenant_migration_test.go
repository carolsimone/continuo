package postgres_test

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tenantKeysOutboxQuery extracts the expression and predicate from the
// tenant-keys migration's UPDATE of the outbox rows, and evaluates them over a
// fixture in a read-only SELECT. The migration is read as written, so the
// statement is matched by its shape and not by the outbox table's name.
func tenantKeysOutboxQuery(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(thisFile))))
	b, err := os.ReadFile(filepath.Join(repoRoot, "db", "migration", "run_controller", "V53__tenant_keys.sql")) //nolint:gosec // G304: a fixed migration path
	require.NoError(t, err)
	body := regexp.MustCompile(`(?m)^\s*--[^\n]*`).ReplaceAllString(string(b), "")
	statement := regexp.MustCompile(`(?s)UPDATE\s+[a-z_]+_outbox\s+SET\s+payload\s*=\s*([^;]+?)\s+WHERE\s+([^;]+)\s*;`)
	parts := statement.FindStringSubmatch(body)
	require.Len(t, parts, 3, "V53 must contain the outbox payload UPDATE")
	return `SELECT (CASE WHEN ` + parts[2] + ` THEN ` + parts[1] + ` ELSE payload END)::text
		FROM (SELECT $1::jsonb AS payload, $2::text AS event_type) AS fixture`
}

// V53 stores a dispatch or cancel row written before the migration beside the
// default tenant, leaves a row that already stores a tenant as it is, and
// leaves every other event type alone.
func TestV53_WrapsTheDispatchAndCancelRowsThatStoreNoTenant(t *testing.T) {
	db := newTestDB(t)
	query := tenantKeysOutboxQuery(t)
	dispatch := `{"run_id":"r","task_id":"t","attempt":0}`
	cancel := `{"run_id":"r","cancelled_by":"system"}`
	wrappedDispatch := `{"tenant_id":"acme","payload":` + dispatch + `}`
	wrappedCancel := `{"tenant_id":"acme","payload":` + cancel + `}`
	for name, tc := range map[string]struct{ eventType, input, want string }{
		"dispatch without a tenant": {streams.TaskDispatchV1, dispatch, `{"tenant_id":"default","payload":` + dispatch + `}`},
		"cancel without a tenant":   {streams.RunCancelledV1, cancel, `{"tenant_id":"default","payload":` + cancel + `}`},
		"dispatch with a tenant":    {streams.TaskDispatchV1, wrappedDispatch, wrappedDispatch},
		"cancel with a tenant":      {streams.RunCancelledV1, wrappedCancel, wrappedCancel},
		"another event type":        {"other.event:v1", dispatch, dispatch},
	} {
		t.Run(name, func(t *testing.T) {
			var got string
			require.NoError(t, db.Get(&got, query, tc.input, tc.eventType))
			assert.JSONEq(t, tc.want, got)
		})
	}
}
