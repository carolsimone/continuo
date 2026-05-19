package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutboxPublisher_RunFinalized(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	pub := postgres.NewOutboxPublisher(discardLogger())

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	scheduleID := uuid.New()
	// Use uuid.Nil so MessageProcessingID is set to nil in the entry.
	// Passing a random UUID would trigger a FK constraint violation against
	// message_processing unless a matching row is seeded first.
	err = pub.Append(ctx, tx, []run.DomainEvent{
		run.RunFinalized{ID: scheduleID, Name: "daily", Outcome: run.SchedulerStatusSucceeded},
	}, uuid.Nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	defer db.ExecContext(ctx, "DELETE FROM state_outbox WHERE aggregate_id = $1", scheduleID)

	// Read back the row directly from the table.
	type outboxRow struct {
		StreamName    string    `db:"stream_name"`
		AggregateType string    `db:"aggregate_type"`
		EventType     string    `db:"event_type"`
		MaxRetries    int       `db:"max_retries"`
		MsgProcID     *uuid.UUID `db:"message_processing_id"`
		Payload       []byte    `db:"payload"`
	}
	var found outboxRow
	err = db.GetContext(ctx, &found,
		`SELECT stream_name, aggregate_type, event_type, max_retries, message_processing_id, payload
		 FROM state_outbox WHERE aggregate_id = $1 LIMIT 1`,
		scheduleID,
	)
	require.NoError(t, err)

	assert.Equal(t, "run.finalized:v1", found.StreamName, "stream_name")
	assert.Equal(t, "scheduler_tracker", found.AggregateType, "aggregate_type")
	assert.Equal(t, "run.finalized:v1", found.EventType, "event_type")
	assert.Equal(t, 5, found.MaxRetries, "max_retries")
	assert.Nil(t, found.MsgProcID, "message_processing_id should be nil for uuid.Nil input")

	var payload map[string]string
	require.NoError(t, json.Unmarshal(found.Payload, &payload))
	assert.Equal(t, scheduleID.String(), payload["schedule_id"])
	assert.Equal(t, "daily", payload["schedule_name"])
	assert.Equal(t, "succeeded", payload["status"])
}

func TestOutboxPublisher_AllEventTypes(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	pub := postgres.NewOutboxPublisher(discardLogger())

	scheduleID := uuid.New()
	sourceID := uuid.New()

	tests := []struct {
		name          string
		event         run.DomainEvent
		wantStream    string
		wantEventType string
		wantAggType   string
		wantRetries   int
		wantRows      int // expected outbox rows (0 for RunDispatchFailed)
	}{
		{
			name: "RunStarted",
			event: run.RunStarted{
				ID:              scheduleID,
				Name:            "sched",
				K:               run.KindCron,
				ServiceMetadata: map[string]run.ServiceMetadata{},
			},
			wantStream:    "scheduler.started:v1",
			wantEventType: "scheduler_started",
			wantAggType:   "scheduler",
			wantRetries:   3,
			wantRows:      1,
		},
		{
			name:          "RunFinalized",
			event:         run.RunFinalized{ID: scheduleID, Name: "sched", Outcome: run.SchedulerStatusFailed},
			wantStream:    "run.finalized:v1",
			wantEventType: "run.finalized:v1",
			wantAggType:   "scheduler_tracker",
			wantRetries:   5,
			wantRows:      1,
		},
		{
			name:          "RunCancelled",
			event:         run.RunCancelled{ID: scheduleID, Name: "sched"},
			wantStream:    "schedule.cancelled:v1",
			wantEventType: "schedule_cancelled",
			wantAggType:   "scheduler",
			wantRetries:   3,
			wantRows:      1,
		},
		{
			name:          "RerunRequested",
			event:         run.RerunRequested{ID: scheduleID, Name: "sched", SourceID: sourceID},
			wantStream:    "trigger.rerun:v1",
			wantEventType: "rerun",
			wantAggType:   "scheduler",
			wantRetries:   3,
			wantRows:      1,
		},
		{
			name:          "RebaseRequested",
			event:         run.RebaseRequested{ID: scheduleID, Name: "sched", SourceID: sourceID},
			wantStream:    "trigger.rebase:v1",
			wantEventType: "rebase",
			wantAggType:   "scheduler",
			wantRetries:   3,
			wantRows:      1,
		},
		{
			name: "SingleNodeRunRequested",
			event: run.SingleNodeRunRequested{
				ID:   scheduleID,
				Name: "sched",
				Target: run.NodeID{
					ServiceName: "svc",
					SchemaName:  "sch",
					TableName:   "tbl",
				},
				MetadataSource: run.MetadataSourceLatest,
			},
			wantStream:    "trigger.single_node_run:v1",
			wantEventType: "single_node_run",
			wantAggType:   "scheduler",
			wantRetries:   3,
			wantRows:      1,
		},
		{
			name:     "RunDispatchFailed produces no row",
			event:    run.RunDispatchFailed{ID: scheduleID, Name: "sched", Reason: "target_not_found"},
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

			err = pub.Append(ctx, tx, []run.DomainEvent{event}, uuid.Nil)
			require.NoError(t, err)
			require.NoError(t, tx.Commit())

			defer db.ExecContext(ctx, "DELETE FROM state_outbox WHERE aggregate_id = $1", aggID)

			if tc.wantRows == 0 {
				// Verify no row was inserted.
				var count int
				_ = db.GetContext(ctx, &count, `SELECT COUNT(*) FROM state_outbox WHERE aggregate_id = $1`, aggID)
				assert.Equal(t, 0, count, "RunDispatchFailed should produce no outbox row")
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
			assert.Equal(t, tc.wantRetries, found.MaxRetries, "max_retries")
		})
	}
}

// patchEventID returns a copy of evt with its ID field replaced by newID.
// This lets each sub-test use a unique aggregate_id so they don't pollute each other.
func patchEventID(evt run.DomainEvent, newID uuid.UUID) run.DomainEvent {
	switch e := evt.(type) {
	case run.RunStarted:
		e.ID = newID
		return e
	case run.RunFinalized:
		e.ID = newID
		return e
	case run.RunCancelled:
		e.ID = newID
		return e
	case run.RerunRequested:
		e.ID = newID
		return e
	case run.RebaseRequested:
		e.ID = newID
		return e
	case run.SingleNodeRunRequested:
		e.ID = newID
		return e
	case run.RunDispatchFailed:
		e.ID = newID
		return e
	default:
		return evt
	}
}
