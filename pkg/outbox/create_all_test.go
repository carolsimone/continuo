package outbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CreateAll writes more entries than one statement holds, in one
// transaction: every row pending under the outbox budget, created_at strictly
// increasing in slice order, and each entry's id and created_at set to what
// was stored.
func TestCreateAll_WritesEveryEntryInSliceOrder(t *testing.T) {
	ctx := context.Background()
	db := dbForTest(t)
	aggregate := uuid.New()
	entries := make([]*outbox.Entry, 2500)
	for i := range entries {
		entries[i] = &outbox.Entry{
			AggregateType: "scheduler", AggregateID: aggregate, EventType: streams.TaskDispatchV1,
			Payload: []byte(`{"n":"` + uuid.NewString() + `"}`), StreamName: streams.TaskDispatchV1,
		}
	}

	tx, err := db.Beginx()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, outbox.CreateAll(ctx, tx, testOutboxTable, entries))
	require.NoError(t, tx.Commit())

	var rows []struct {
		ID         uuid.UUID `db:"id"`
		Status     string    `db:"status"`
		MaxRetries int       `db:"max_retries"`
		CreatedAt  time.Time `db:"created_at"`
	}
	require.NoError(t, db.Select(&rows, `
		SELECT id, status, max_retries, created_at FROM `+testOutboxTable+`
		WHERE aggregate_id = $1 ORDER BY created_at, id`, aggregate))
	require.Len(t, rows, len(entries))
	for i, row := range rows {
		assert.Equal(t, entries[i].ID, row.ID, "row %d is entry %d", i, i)
		assert.True(t, entries[i].CreatedAt.Equal(row.CreatedAt))
		assert.Equal(t, "pending", row.Status)
		assert.Equal(t, outbox.MaxAttempts, row.MaxRetries)
		if i > 0 {
			assert.True(t, row.CreatedAt.After(rows[i-1].CreatedAt), "created_at increases at row %d", i)
		}
	}
}

func TestCreateAll_NoEntriesWritesNothing(t *testing.T) {
	db := dbForTest(t)
	require.NoError(t, outbox.CreateAll(context.Background(), db, testOutboxTable, nil))
	var n int
	require.NoError(t, db.Get(&n, `SELECT count(*) FROM `+testOutboxTable))
	assert.Equal(t, 0, n)
}
