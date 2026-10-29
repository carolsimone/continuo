package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A run's terminal status is its row: RunFinalized and RunDispatchTerminal
// write no outbox row, and a transaction that records only them writes none.
func TestOutboxPublisher_RunFinalizedWritesNoRow(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	scheduleID := uuid.New()

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, postgres.NewOutboxPublisher(tx, discardLogger()).Append(ctx, []run.DomainEvent{
		run.RunFinalized{ID: scheduleID, Name: "daily", Outcome: run.RunStatusSucceeded},
		run.RunDispatchTerminal{ID: scheduleID, Name: "daily", Reason: "runplan: the selection is empty"},
	}, uuid.Nil))
	require.NoError(t, tx.Commit())

	var count int
	require.NoError(t, db.GetContext(ctx, &count, `SELECT count(*) FROM state_outbox WHERE aggregate_id = $1`, scheduleID))
	assert.Zero(t, count)
}

func TestOutboxPublisher_RunCancelledPair(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	pub := postgres.NewOutboxPublisher(tx, discardLogger())

	id := uuid.New()
	err = pub.Append(ctx, []run.DomainEvent{
		run.RunCancelled{ID: id, TenantID: "acme", Name: "e2e-schedule", By: "tester", CancellationReason: "drift"},
		run.RunFinalized{ID: id, Name: "e2e-schedule", Outcome: run.RunStatusCancelled},
	}, uuid.Nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	defer db.ExecContext(ctx, "DELETE FROM state_outbox WHERE aggregate_id = $1", id)

	type outboxRow struct {
		StreamName string `db:"stream_name"`
		Payload    []byte `db:"payload"`
	}
	var rows []outboxRow
	err = db.SelectContext(ctx, &rows,
		`SELECT stream_name, payload FROM state_outbox WHERE aggregate_id = $1 ORDER BY id`,
		id,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1, "a cancel writes its run.cancelled:v1 row only; its RunFinalized writes no row")
	assert.Equal(t, streams.RunCancelledV1, rows[0].StreamName)

	// The row carries the cancelling user (provenance).
	tenantID, wire, err := pkgoutbox.UnwrapTenant(rows[0].Payload)
	require.NoError(t, err)
	assert.Equal(t, "acme", tenantID, "the row stores the run's tenant")
	var cancelled pkgevents.RunCancelled
	require.NoError(t, json.Unmarshal(wire, &cancelled))
	assert.Equal(t, "tester", cancelled.CancelledBy, "the cancel must carry cancelled_by")
}

// A RunCancelled is stored as one run.cancelled:v1 row of the run, the
// stream name as its event type and the run as its aggregate, like the run's
// dispatches, so the relay publishes it after every dispatch written before
// it. Its payload is the cancel: the run, who cancelled it, why, and each
// attempt it names with the attempt's Job; a cancel naming none carries an
// empty list.
func TestTranslateRunEvent_RunCancelledIsARunCancelledV1Row(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	taskA, taskB := uuid.New(), uuid.New()
	for name, tc := range map[string]struct {
		attempts []run.StopTarget
		want     []pkgevents.CancelledAttempt
	}{
		"the first send names no attempt": {want: []pkgevents.CancelledAttempt{}},
		"a re-send names the unanswered attempts": {
			attempts: []run.StopTarget{
				{TaskID: taskA, Attempt: 0, JobName: "service-1-e2e-schema-table-a-7f3e1a2b", ChecksSent: 2},
				{TaskID: taskB, Attempt: 1, JobName: "service-1-e2e-schema-table-b-7f3e1a2b-r1", ChecksSent: 2},
			},
			want: []pkgevents.CancelledAttempt{
				{TaskID: taskA.String(), Attempt: 0, JobName: "service-1-e2e-schema-table-a-7f3e1a2b"},
				{TaskID: taskB.String(), Attempt: 1, JobName: "service-1-e2e-schema-table-b-7f3e1a2b-r1"},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			id := uuid.New()
			t.Cleanup(func() { _, _ = db.ExecContext(ctx, "DELETE FROM state_outbox WHERE aggregate_id = $1", id) })
			tx, err := db.BeginTxx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, postgres.NewOutboxPublisher(tx, discardLogger()).Append(ctx, []run.DomainEvent{
				run.RunCancelled{ID: id, TenantID: "acme", Name: "daily", By: "alice@example.com", CancellationReason: "bad upstream data", Attempts: tc.attempts},
			}, uuid.Nil))
			require.NoError(t, tx.Commit())

			var stored struct {
				StreamName    string `db:"stream_name"`
				EventType     string `db:"event_type"`
				AggregateType string `db:"aggregate_type"`
				Payload       []byte `db:"payload"`
			}
			require.NoError(t, db.GetContext(ctx, &stored,
				`SELECT stream_name, event_type, aggregate_type, payload FROM state_outbox WHERE aggregate_id = $1`, id))
			assert.Equal(t, streams.RunCancelledV1, stored.StreamName)
			assert.Equal(t, streams.RunCancelledV1, stored.EventType, "the row's event type is the stream name")
			assert.Equal(t, "scheduler", stored.AggregateType, "the run's dispatches share its aggregate")
			tenantID, wire, err := pkgoutbox.UnwrapTenant(stored.Payload)
			require.NoError(t, err)
			assert.Equal(t, "acme", tenantID, "the row stores the run's tenant")
			var payload pkgevents.RunCancelled
			require.NoError(t, json.Unmarshal(wire, &payload))
			assert.Equal(t, pkgevents.RunCancelled{
				RunID: id.String(), CancelledBy: "alice@example.com", Reason: "bad upstream data", Attempts: tc.want,
			}, payload)
		})
	}
}

// TestOutboxPublisher_CancelledBy_EmptyBecomesSystem verifies a cancellation
// with no recorded actor (a platform-initiated cancel) carries the "system"
// sentinel on the wire rather than a blank string.
func TestOutboxPublisher_CancelledBy_EmptyBecomesSystem(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	pub := postgres.NewOutboxPublisher(tx, discardLogger())
	id := uuid.New()
	require.NoError(t, pub.Append(ctx, []run.DomainEvent{
		run.RunCancelled{ID: id, TenantID: "acme", Name: "sched", By: "", CancellationReason: "drift"},
	}, uuid.Nil))
	require.NoError(t, tx.Commit())
	defer db.ExecContext(ctx, "DELETE FROM state_outbox WHERE aggregate_id = $1", id)

	var raw []byte
	require.NoError(t, db.GetContext(ctx, &raw,
		`SELECT payload FROM state_outbox WHERE aggregate_id = $1 AND stream_name = $2 LIMIT 1`,
		id, streams.RunCancelledV1,
	))
	_, wire, err := pkgoutbox.UnwrapTenant(raw)
	require.NoError(t, err)
	var payload pkgevents.RunCancelled
	require.NoError(t, json.Unmarshal(wire, &payload))
	assert.Equal(t, "system", payload.CancelledBy)
}

// A run event whose run names no tenant writes nothing: its row could not be
// rendered, so Append refuses it before the insert.
func TestOutboxPublisher_AnEventWithoutATenantWritesNoRow(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	for name, event := range map[string]func(uuid.UUID) run.DomainEvent{
		"a cancel": func(id uuid.UUID) run.DomainEvent { return run.RunCancelled{ID: id, Name: "daily"} },
		"a dispatch": func(id uuid.UUID) run.DomainEvent {
			return run.TaskDispatched{ID: id, ScheduleName: "daily", TaskID: uuid.New()}
		},
	} {
		t.Run(name, func(t *testing.T) {
			id := uuid.New()
			tx, err := db.BeginTxx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()

			err = postgres.NewOutboxPublisher(tx, discardLogger()).Append(ctx, []run.DomainEvent{event(id)}, uuid.Nil)

			require.ErrorIs(t, err, pkgoutbox.ErrNoTenant)
			var count int
			require.NoError(t, tx.GetContext(ctx, &count, `SELECT count(*) FROM state_outbox WHERE aggregate_id = $1`, id))
			assert.Zero(t, count)
		})
	}
}

func TestOutboxPublisher_AllEventTypes(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	scheduleID := uuid.New()

	tests := []struct {
		name          string
		event         run.DomainEvent
		wantStream    string
		wantEventType string
		wantAggType   string
		wantRows      int // expected outbox rows (0 for RunFinalized, RunDispatchTerminal)
	}{
		{
			name:          "RunCancelled",
			event:         run.RunCancelled{ID: scheduleID, TenantID: "acme", Name: "sched"},
			wantStream:    streams.RunCancelledV1,
			wantEventType: streams.RunCancelledV1,
			wantAggType:   "scheduler",
			wantRows:      1,
		},
		{
			name:     "RunFinalized produces no row",
			event:    run.RunFinalized{ID: scheduleID, Name: "sched", Outcome: run.RunStatusFailed},
			wantRows: 0,
		},
		{
			name:     "RunDispatchTerminal produces no row",
			event:    run.RunDispatchTerminal{ID: scheduleID, Name: "sched", Reason: "target_not_found"},
			wantRows: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Use a fresh aggregate ID per sub-test to avoid cross-test pollution.
			aggID := uuid.New()

			// Patch the event's ID field via a new event with the unique aggID.
			// Since event structs are value types, build a new one where needed.
			event := patchEventID(tc.event, aggID)

			tx, err := db.BeginTxx(ctx, nil)
			require.NoError(t, err)

			pub := postgres.NewOutboxPublisher(tx, discardLogger())
			err = pub.Append(ctx, []run.DomainEvent{event}, uuid.Nil)
			require.NoError(t, err)
			require.NoError(t, tx.Commit())

			defer db.ExecContext(ctx, "DELETE FROM state_outbox WHERE aggregate_id = $1", aggID)

			if tc.wantRows == 0 {
				// Verify no row was inserted.
				var count int
				_ = db.GetContext(ctx, &count, `SELECT COUNT(*) FROM state_outbox WHERE aggregate_id = $1`, aggID)
				assert.Equal(t, 0, count, "%s should produce no outbox row", tc.name)
				return
			}

			type outboxRow struct {
				StreamName    string    `db:"stream_name"`
				EventType     string    `db:"event_type"`
				AggregateType string    `db:"aggregate_type"`
				MaxRetries    int       `db:"max_retries"`
				CreatedAt     time.Time `db:"created_at"`
			}
			var found outboxRow
			err = db.GetContext(ctx, &found,
				`SELECT stream_name, event_type, aggregate_type, max_retries, created_at
				 FROM state_outbox WHERE aggregate_id = $1 AND stream_name = $2 LIMIT 1`,
				aggID, tc.wantStream,
			)
			require.NoErrorf(t, err, "expected outbox row with stream %q for %s", tc.wantStream, tc.name)
			assert.Equal(t, tc.wantEventType, found.EventType, "event_type")
			assert.Equal(t, tc.wantAggType, found.AggregateType, "aggregate_type")
			assert.Equal(t, pkgoutbox.MaxAttempts, found.MaxRetries, "max_retries is written with the one outbox budget")
		})
	}
}

// patchEventID returns a copy of evt with its ID field replaced by newID.
// This lets each sub-test use a unique aggregate_id so they don't pollute each other.
func patchEventID(evt run.DomainEvent, newID uuid.UUID) run.DomainEvent {
	switch e := evt.(type) {
	case run.RunFinalized:
		e.ID = newID
		return e
	case run.RunCancelled:
		e.ID = newID
		return e
	case run.RunDispatchTerminal:
		e.ID = newID
		return e
	default:
		return evt
	}
}
