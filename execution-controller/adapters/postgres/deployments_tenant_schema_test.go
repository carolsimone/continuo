//go:build integration

package postgres_test

import (
	"context"
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
