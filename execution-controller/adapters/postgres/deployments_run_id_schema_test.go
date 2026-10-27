//go:build integration

package postgres_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A deployment keys the run it belongs to as run_id, and the index that serves
// the stop of a cancelled run is built on that column.
func TestExecutionSchema_DeploymentsKeyTheRunAsRunID(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	var columns []string
	require.NoError(t, db.Select(&columns,
		`SELECT column_name FROM information_schema.columns
		  WHERE table_schema = 'public' AND table_name = 'deployments'`))
	assert.Contains(t, columns, "run_id")

	var definition string
	require.NoError(t, db.Get(&definition,
		`SELECT indexdef FROM pg_indexes
		  WHERE schemaname = 'public' AND indexname = 'idx_deployments_open_by_run'`))
	assert.Contains(t, definition, "(run_id)")
	assert.Contains(t, definition, "mode = 'production'")
}
