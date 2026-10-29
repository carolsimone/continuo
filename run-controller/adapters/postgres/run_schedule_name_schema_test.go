//go:build integration

package postgres_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
)

// runs.schedule_name stores exactly the longest schedule name a run carries,
// run.MaxScheduleNameChars characters: a narrower column would refuse a name
// the run admits, and a wider one would leave the limit to the domain alone.
func TestRunsSchema_ScheduleNameHoldsTheLongestNameARunCarries(t *testing.T) {
	db := newTestDB(t)

	var width sql.NullInt64
	require.NoError(t, db.Get(&width, `
		SELECT character_maximum_length FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = 'runs' AND column_name = 'schedule_name'`),
		"runs.schedule_name exists; apply the run-controller migrations")
	require.True(t, width.Valid, "runs.schedule_name has a maximum length")
	assert.Equal(t, int64(run.MaxScheduleNameChars), width.Int64)
}
