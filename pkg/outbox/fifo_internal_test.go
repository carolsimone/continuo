package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNextCreatedAt_StrictlyIncreasingAtMicroseconds(t *testing.T) {
	now := time.Now()
	prev := nextCreatedAt(now)
	for i := 0; i < 10000; i++ {
		got := nextCreatedAt(now)
		require.True(t, got.After(prev), "iteration %d", i)
		require.Equal(t, got, got.Truncate(time.Microsecond))
		prev = got
	}
}

func TestWithholdBlocked_DropsBlockedRowsAndTheirYoungerSiblings(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	rows := []*Entry{
		{ID: uuid.New(), AggregateType: "run", AggregateID: a},
		{ID: uuid.New(), AggregateType: "run", AggregateID: b},
		{ID: uuid.New(), AggregateType: "run", AggregateID: a},
		{ID: uuid.New(), AggregateType: "run", AggregateID: b},
	}
	withheld := map[aggregateKey]bool{}
	got := withholdBlocked(rows, map[uuid.UUID]bool{rows[1].ID: true}, withheld)
	assert.Equal(t, []*Entry{rows[0], rows[2]}, got, "b's first row is blocked, so b's later row waits too")
	assert.Equal(t, map[aggregateKey]bool{{"run", b}: true}, withheld)
}

// A row of an aggregate an earlier claim withheld is dropped too.
func TestWithholdBlocked_DropsRowsOfAggregatesWithheldEarlier(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	rows := []*Entry{
		{ID: uuid.New(), AggregateType: "run", AggregateID: a},
		{ID: uuid.New(), AggregateType: "run", AggregateID: b},
	}
	withheld := map[aggregateKey]bool{{"run", a}: true}
	got := withholdBlocked(rows, map[uuid.UUID]bool{}, withheld)
	assert.Equal(t, []*Entry{rows[1]}, got)
}

// On the one-row publish path a row that fails stops the younger rows of its
// aggregate in the batch; rows of other aggregates are still sent. An aggregate
// is the (aggregate_type, aggregate_id) pair.
func TestPublish_OneRowPathHoldsTheYoungerRowsOfAFailedRow(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	entries := []*Entry{
		{ID: uuid.New(), AggregateType: "run", AggregateID: a},
		{ID: uuid.New(), AggregateType: "run", AggregateID: b},
		{ID: uuid.New(), AggregateType: "run", AggregateID: a},
		{ID: uuid.New(), AggregateType: "task", AggregateID: a},
	}
	boom := errors.New("boom")
	pub := &plainPublisher{failIdx: map[int]error{1: boom}}

	errs := newProc(pub).publish(context.Background(), entries)

	require.Len(t, errs, 4)
	assert.ErrorIs(t, errs[0], boom)
	assert.NoError(t, errs[1])
	assert.ErrorIs(t, errs[2], errHeldBehindFailedRow)
	assert.NoError(t, errs[3])
	assert.Equal(t, 3, pub.calls, "the held row is never sent")
}

// A pipeline goes out whole, so every slot carries the publisher's own result
// even when an older row of the same aggregate failed.
func TestPublish_PipelinedPathSendsTheWholeBatch(t *testing.T) {
	agg := uuid.New()
	entries := []*Entry{
		{ID: uuid.New(), AggregateType: "run", AggregateID: agg},
		{ID: uuid.New(), AggregateType: "run", AggregateID: agg},
	}
	boom := errors.New("xadd failed")
	pub := &batchPublisher{returnErrs: func(entries []*Entry) []error {
		errs := make([]error, len(entries))
		errs[0] = boom
		return errs
	}}

	errs := newProc(pub).publish(context.Background(), entries)

	require.Len(t, errs, 2)
	assert.ErrorIs(t, errs[0], boom)
	assert.NoError(t, errs[1])
	assert.Equal(t, 2, pub.batchEntries)
}
