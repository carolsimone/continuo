//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The run-controller database keeps its outbox in run_controller_outbox: the
// table, its constraints, its indexes and the trigger that notifies its relay
// all carry the service's name, and the relay targets the same table through
// postgres.OutboxTable.
func TestOutboxTableIsRunControllerOutbox(t *testing.T) {
	db := newTestDB(t)

	assert.Equal(t, "run_controller_outbox", postgres.OutboxTable)

	var exists bool
	require.NoError(t, db.Get(&exists, `SELECT to_regclass('run_controller_outbox') IS NOT NULL`))
	assert.True(t, exists, "table run_controller_outbox exists")
	var outboxTables []string
	require.NoError(t, db.Select(&outboxTables, `
		SELECT relname FROM pg_class
		WHERE relkind = 'r' AND relnamespace = 'public'::regnamespace AND relname LIKE '%\_outbox'
		ORDER BY relname`))
	assert.Equal(t, []string{"run_controller_outbox"}, outboxTables, "the database holds no other outbox table")

	var triggers int
	require.NoError(t, db.Get(&triggers, `
		SELECT count(*) FROM pg_trigger
		WHERE tgname = 'run_controller_outbox_notify'
		  AND tgrelid = to_regclass('run_controller_outbox')
		  AND NOT tgisinternal`))
	assert.Equal(t, 1, triggers, "trigger run_controller_outbox_notify is on run_controller_outbox")

	var oldTriggers int
	require.NoError(t, db.Get(&oldTriggers, `
		SELECT count(*) FROM pg_trigger
		WHERE tgrelid = to_regclass('run_controller_outbox') AND NOT tgisinternal AND tgname <> 'run_controller_outbox_notify'`))
	assert.Zero(t, oldTriggers, "the outbox carries no other trigger than its notify trigger")

	var indexes []string
	require.NoError(t, db.Select(&indexes, `SELECT indexname FROM pg_indexes WHERE tablename = 'run_controller_outbox' ORDER BY indexname`))
	var constraints []string
	require.NoError(t, db.Select(&constraints, `
		SELECT conname FROM pg_constraint WHERE conrelid = to_regclass('run_controller_outbox') ORDER BY conname`))

	for _, name := range append(append([]string{}, indexes...), constraints...) {
		assert.True(t, strings.HasPrefix(name, "run_controller_outbox") || strings.HasPrefix(name, "idx_run_controller_outbox"),
			"%s does not carry the table's name", name)
	}
	// The indexes the relay's queries rely on.
	for _, want := range []string{
		"idx_run_controller_outbox_claimable",
		"idx_run_controller_outbox_open_by_aggregate",
		"idx_run_controller_outbox_failed",
	} {
		assert.Contains(t, indexes, want)
	}
	assert.Contains(t, indexes, "run_controller_outbox_pkey")
	assert.Contains(t, constraints, "run_controller_outbox_pkey")
	assert.Contains(t, constraints, "run_controller_outbox_status_check")
	assert.Contains(t, constraints, "run_controller_outbox_message_processing_id_fkey")
}

// The outbox relay's waker listens on the channel named after
// postgres.OutboxTable, and a row the publisher commits to the outbox wakes it.
func TestOutboxWakerWakesOnRunControllerOutbox(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	waker, err := pkgoutbox.NewPostgresWaker(ctx, pkgconfig.LoadPostgres(&pkgconfig.Validator{}).DSN(), postgres.OutboxTable, discardLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = waker.Close() })

	runID := uuid.New()
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, `DELETE FROM run_controller_outbox WHERE aggregate_id = $1`, runID)
	})

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, postgres.NewOutboxPublisher(tx, discardLogger()).Append(ctx, []run.DomainEvent{
		run.RunCancelled{ID: runID, TenantID: "acme", Name: "daily", By: "tester", CancellationReason: "drift"},
	}, uuid.Nil))
	require.NoError(t, tx.Commit())

	select {
	case <-waker.Wake():
	case <-time.After(5 * time.Second):
		t.Fatal("the outbox waker was not woken by a row committed to run_controller_outbox")
	}

	var rows int
	require.NoError(t, db.GetContext(ctx, &rows, `SELECT count(*) FROM run_controller_outbox WHERE aggregate_id = $1`, runID))
	assert.Equal(t, 1, rows, "the publisher wrote its row to run_controller_outbox")
}
