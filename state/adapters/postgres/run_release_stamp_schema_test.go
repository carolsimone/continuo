package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stampRun inserts a run with the given promotion seq and deletes it (and,
// through the foreign key, its tasks) when the test ends.
func stampRun(t *testing.T, db *sqlx.DB, seq any) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(`
		INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, promotion_seq)
		VALUES ($1, $2, 'running', $3)`, id, "stamp-"+id.String()[:8], seq)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, id)
	})
	return id
}

// stampTask inserts one task of runID at nodeKey (nil for none).
func stampTask(db *sqlx.DB, runID uuid.UUID, nodeKey any) error {
	_, err := db.Exec(`
		INSERT INTO task_tracker (task_id, schedule_id, service_name, schema_name, table_name, job_name, status, max_retries, node_key)
		VALUES ($1, $2, 'svc', 'raw', 'orders', 'job', 'pending', 2, $3)`, uuid.New(), runID, nodeKey)
	return err
}

func TestRunReleaseStamp_RunStoresItsPromotionSeq(t *testing.T) {
	db := newTestDB(t)
	id := stampRun(t, db, int64(12))

	var seq *int64
	require.NoError(t, db.Get(&seq, `SELECT promotion_seq FROM scheduler_tracker WHERE schedule_id = $1`, id))
	require.NotNil(t, seq)
	assert.Equal(t, int64(12), *seq)
}

func TestRunReleaseStamp_PromotionSeqIsNullable(t *testing.T) {
	db := newTestDB(t)
	id := stampRun(t, db, nil)

	var seq *int64
	require.NoError(t, db.Get(&seq, `SELECT promotion_seq FROM scheduler_tracker WHERE schedule_id = $1`, id))
	assert.Nil(t, seq)
}

func TestRunReleaseStamp_ARunHoldsOneTaskPerNode(t *testing.T) {
	db := newTestDB(t)
	id := stampRun(t, db, int64(1))

	require.NoError(t, stampTask(db, id, "raw.orders"))
	err := stampTask(db, id, "raw.orders")
	require.Error(t, err)
	assert.Equal(t, "23505", pqCode(t, err))
}

func TestRunReleaseStamp_TwoRunsMayHoldTheSameNode(t *testing.T) {
	db := newTestDB(t)
	first := stampRun(t, db, int64(1))
	second := stampRun(t, db, int64(1))

	require.NoError(t, stampTask(db, first, "raw.orders"))
	require.NoError(t, stampTask(db, second, "raw.orders"))
}

func TestRunReleaseStamp_TasksWithoutANodeKeyAreNotUnique(t *testing.T) {
	db := newTestDB(t)
	id := stampRun(t, db, nil)

	require.NoError(t, stampTask(db, id, nil))
	require.NoError(t, stampTask(db, id, nil))
}
