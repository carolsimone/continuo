package outbox_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeadLetterBacklog_CountsFailedRows(t *testing.T) {
	db := dbForTest(t)
	// Two permanent failures => two 'failed' rows (plus their dead-letter rows,
	// which are 'pending', not counted).
	for i := 0; i < 2; i++ {
		seedRow(t, db, 10)
	}
	pub := &permanentFailingPublisher{}
	p := outbox.NewProcessor(db, testOutboxTable, pub, nil, newTestLogger(), outbox.ProcessorConfig{})
	require.NoError(t, p.ProcessBatch(context.Background()))

	n, err := p.DeadLetterBacklog(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, n, "backlog counts terminal 'failed' rows only")
}
