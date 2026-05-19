// pkg/outbox/processor_test.go
package outbox_test

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakePublisher struct {
	failTimes int // first N calls error; subsequent succeed
	calls     int
	lastEntry *outbox.Entry
}

func (f *fakePublisher) Publish(_ context.Context, e *outbox.Entry) error {
	f.calls++
	f.lastEntry = e
	if f.calls <= f.failTimes {
		return errors.New("synthetic publisher error")
	}
	return nil
}

func seedRow(t *testing.T, db *sqlx.DB, maxRetries int) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(
		`INSERT INTO test_outbox (id, aggregate_type, aggregate_id, event_type, payload, stream_name, max_retries)
		 VALUES ($1, 'task', $2, 'x', '{}'::jsonb, 'x:v1', $3)`,
		id, uuid.New(), maxRetries,
	)
	require.NoError(t, err)
	return id
}

func TestProcessor_SuccessMarksProcessed(t *testing.T) {
	db := dbForTest(t)
	id := seedRow(t, db, 3)

	pub := &fakePublisher{}
	p := outbox.NewProcessor(db, "test_outbox", pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM test_outbox WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "processed", status)
	assert.Equal(t, 1, pub.calls)
}

func TestProcessor_TransientErrorIncrementsRetry(t *testing.T) {
	db := dbForTest(t)
	id := seedRow(t, db, 3)

	pub := &fakePublisher{failTimes: 1}
	p := outbox.NewProcessor(db, "test_outbox", pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	var status string
	var rc int
	require.NoError(t, db.QueryRow(`SELECT status, retry_count FROM test_outbox WHERE id=$1`, id).Scan(&status, &rc))
	assert.Equal(t, "pending", status)
	assert.Equal(t, 1, rc)
}

func TestProcessor_RetryBudgetExhaustedMarksFailedAndCallsHook(t *testing.T) {
	db := dbForTest(t)
	// MaxRetries = 1 so even attempt 1 exhausts immediately on failure.
	id := seedRow(t, db, 1)

	hookCalled := 0
	hook := outbox.TerminalFailureHook(func(_ context.Context, e *outbox.Entry, cause error) error {
		hookCalled++
		assert.Equal(t, id, e.ID)
		assert.Error(t, cause)
		return nil
	})
	pub := &fakePublisher{failTimes: 10}
	p := outbox.NewProcessor(db, "test_outbox", pub, hook, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	var status string
	var errMsg string
	require.NoError(t, db.QueryRow(`SELECT status, error_message FROM test_outbox WHERE id=$1`, id).Scan(&status, &errMsg))
	assert.Equal(t, "failed", status)
	assert.Equal(t, "synthetic publisher error", errMsg)
	assert.Equal(t, 1, hookCalled)
}

func TestProcessor_NoHookConfiguredStillMarksFailed(t *testing.T) {
	db := dbForTest(t)
	id := seedRow(t, db, 1)

	pub := &fakePublisher{failTimes: 10}
	p := outbox.NewProcessor(db, "test_outbox", pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	var status string
	require.NoError(t, db.QueryRow(`SELECT status FROM test_outbox WHERE id=$1`, id).Scan(&status))
	assert.Equal(t, "failed", status)
}
