package outbox_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessor_Backlog(t *testing.T) {
	db := dbForTest(t)
	old := seedRow(t, db, 0)
	seedRow(t, db, 0)
	failed := seedRow(t, db, 0)
	_, err := db.Exec(fmt.Sprintf(`UPDATE %s SET created_at = clock_timestamp() - interval '90 seconds' WHERE id = $1`, testOutboxTable), old)
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`UPDATE %s SET status = 'failed' WHERE id = $1`, testOutboxTable), failed)
	require.NoError(t, err)

	p := outbox.NewProcessor(db, testOutboxTable, &fakePublisher{}, nil, newTestLogger(), outbox.ProcessorConfig{})
	b, err := p.Backlog(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, b.Open)
	assert.Equal(t, 1, b.DeadLettered)
	assert.GreaterOrEqual(t, b.OldestOpenAge, 90*time.Second)
	assert.Equal(t, testOutboxTable, p.Table())
}
