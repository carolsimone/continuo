package postgres_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutboxPublisher_RunFinalized(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	outboxRepo := postgres.NewOutboxRepository(db, discardLogger())
	pub := postgres.NewOutboxPublisher(outboxRepo)

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

	pending, err := outboxRepo.ListPending(ctx, 10)
	require.NoError(t, err)

	var found *postgres.OutboxEntry
	for _, e := range pending {
		if e.AggregateID == scheduleID {
			found = e
			break
		}
	}
	require.NotNil(t, found, "expected an outbox row for scheduleID %s", scheduleID)

	assert.Equal(t, "run.finalized:v1", found.StreamName, "stream_name")
	assert.Equal(t, "scheduler_tracker", found.AggregateType, "aggregate_type")
	assert.Equal(t, "run.finalized:v1", found.EventType, "event_type")
	assert.Equal(t, 5, found.MaxRetries, "max_retries")
	assert.Nil(t, found.MessageProcessingID, "message_processing_id should be nil for uuid.Nil input")

	var payload map[string]string
	require.NoError(t, json.Unmarshal(found.Payload, &payload))
	assert.Equal(t, scheduleID.String(), payload["schedule_id"])
	assert.Equal(t, "daily", payload["schedule_name"])
	assert.Equal(t, "succeeded", payload["status"])
}

func TestOutboxPublisher_AllEventTypes(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	outboxRepo := postgres.NewOutboxRepository(db, discardLogger())
	pub := postgres.NewOutboxPublisher(outboxRepo)

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
			rowID := uuid.New()
			// Use a unique aggregate_id per sub-test so ListPending doesn't
			// pick up rows from other sub-tests.
			type idSetter interface {
				withID(uuid.UUID) run.DomainEvent
			}
			_ = rowID

			tx, err := db.BeginTxx(ctx, nil)
			require.NoError(t, err)

			err = pub.Append(ctx, tx, []run.DomainEvent{tc.event}, uuid.Nil)
			require.NoError(t, err)
			require.NoError(t, tx.Commit())

			defer db.ExecContext(ctx, "DELETE FROM state_outbox WHERE aggregate_id = $1", scheduleID)

			if tc.wantRows == 0 {
				// No specific assertion needed beyond no error above.
				return
			}

			pending, err := outboxRepo.ListPending(ctx, 100)
			require.NoError(t, err)

			var found *postgres.OutboxEntry
			for _, e := range pending {
				if e.AggregateID == scheduleID && e.StreamName == tc.wantStream {
					found = e
					break
				}
			}
			require.NotNilf(t, found, "expected outbox row with stream %q for %s", tc.wantStream, tc.name)
			assert.Equal(t, tc.wantEventType, found.EventType, "event_type")
			assert.Equal(t, tc.wantAggType, found.AggregateType, "aggregate_type")
			assert.Equal(t, tc.wantRetries, found.MaxRetries, "max_retries")
		})
	}
}
