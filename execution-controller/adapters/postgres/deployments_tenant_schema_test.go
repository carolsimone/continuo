//go:build integration

package postgres_test

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A deployment names the tenant of the dispatch it runs. The column is NOT NULL
// and has no default, so an insert that leaves the tenant out fails.
func TestExecutionSchema_DeploymentsHaveATenant(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	var column struct {
		DataType   string  `db:"data_type"`
		IsNullable string  `db:"is_nullable"`
		Default    *string `db:"column_default"`
	}
	require.NoError(t, db.Get(&column,
		`SELECT data_type, is_nullable, column_default FROM information_schema.columns
		  WHERE table_schema = 'public' AND table_name = 'deployments' AND column_name = 'tenant_id'`))
	assert.Equal(t, "text", column.DataType)
	assert.Equal(t, "NO", column.IsNullable)
	assert.Nil(t, column.Default, "an insert must name its tenant")
}

// A production deployment is stored with the tenant of its command and read
// back with it; a release leg is stored under the release-leg tenant.
func TestRepo_ADeploymentKeepsItsTenant(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	cmd := validCmd()
	cmd.TenantID = "acme"
	added, err := repo.AddAttempt(ctx, model.NewDeployment(cmd, nil, time.Now()))
	require.NoError(t, err)
	require.True(t, added)
	leg := model.NewValidationDeployment(validValidationCmd("rel-"+uuid.NewString(), "node-1"), nil, time.Now(), false)
	require.NoError(t, repo.Add(ctx, leg))

	got, found, err := repo.LoadByTaskAttempt(ctx, cmd.TaskID, cmd.Attempt)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "acme", got.TenantID())
	assert.Equal(t, "acme", got.Command().TenantID)
	var column string
	require.NoError(t, db.Get(&column, `SELECT tenant_id FROM deployments WHERE id = $1`, leg.ID()))
	assert.Equal(t, model.ReleaseLegTenantID, column)
}

// v15WrapQuery extracts the expression and predicate from V15's UPDATE of the
// report rows of execution_outbox, and evaluates them over a fixture in a
// read-only SELECT.
func v15WrapQuery(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(thisFile))))
	b, err := os.ReadFile(filepath.Join(repoRoot, "db", "migration", "execution", "V15__deployments_tenant.sql"))
	require.NoError(t, err)
	body := regexp.MustCompile(`(?m)^\s*--[^\n]*`).ReplaceAllString(string(b), "")
	statement := regexp.MustCompile(`(?s)UPDATE\s+execution_outbox\s+SET\s+payload\s*=\s*([^;]+?)\s+WHERE\s+([^;]+)\s*;`)
	parts := statement.FindStringSubmatch(body)
	require.Len(t, parts, 3, "V15 must contain the execution_outbox payload UPDATE")
	return `SELECT (CASE WHEN ` + parts[2] + ` THEN ` + parts[1] + ` ELSE payload END)::text
		FROM (SELECT $1::jsonb AS payload, $2::text AS event_type) AS fixture`
}

// V15 stores a task report row written before the migration beside the default
// tenant, leaves a row that already stores a tenant as it is, and leaves every
// other event type alone.
func TestV15_WrapsTheReportRowsThatStoreNoTenant(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	query := v15WrapQuery(t)
	report := `{"run_id":"r","task_id":"t","status":"queued"}`
	wrapped := `{"tenant_id":"acme","payload":` + report + `}`
	for name, tc := range map[string]struct{ eventType, input, want string }{
		"report without a tenant": {"task_status_report", report, `{"tenant_id":"default","payload":` + report + `}`},
		"report with a tenant":    {"task_status_report", wrapped, wrapped},
		"release leg row":         {"validation_completed", `{"release_id":"rel"}`, `{"release_id":"rel"}`},
	} {
		t.Run(name, func(t *testing.T) {
			var got string
			require.NoError(t, db.Get(&got, query, tc.input, tc.eventType))
			assert.JSONEq(t, tc.want, got)
		})
	}
}
