package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutboxPublisher_RunFinalized(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	pub := postgres.NewOutboxPublisher(tx, discardLogger())

	scheduleID := uuid.New()
	// Use uuid.Nil so MessageProcessingID is set to nil in the entry.
	// Passing a random UUID would trigger a FK constraint violation against
	// message_processing unless a matching row is seeded first.
	err = pub.Append(ctx, []run.DomainEvent{
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

// TestOutboxPublisher_RunStarted_ServiceMetadataKeysAreSnakeCase pins the
// scheduler.started:v1 wire contract: the per-service metadata block is keyed
// manifest_version / image_tag, not the Go field names. The domain value object
// carries no JSON tags, so the publisher must route it through the adapter DTO.
func TestOutboxPublisher_RunStarted_ServiceMetadataKeysAreSnakeCase(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	pub := postgres.NewOutboxPublisher(tx, discardLogger())

	scheduleID := uuid.New()
	err = pub.Append(ctx, []run.DomainEvent{
		run.RunStarted{
			ID:   scheduleID,
			Name: "daily",
			K:    run.KindCron,
			ServiceMetadata: map[string]run.ServiceMetadata{
				"service-1": {ManifestVersion: "v42", ImageTag: "sha-abc"},
			},
		},
	}, uuid.Nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	defer db.ExecContext(ctx, "DELETE FROM state_outbox WHERE aggregate_id = $1", scheduleID)

	var payload []byte
	require.NoError(t, db.GetContext(ctx, &payload,
		`SELECT payload FROM state_outbox WHERE aggregate_id = $1 AND stream_name = $2 LIMIT 1`,
		scheduleID, streams.SchedulerStartedV1,
	))

	var decoded struct {
		ServiceMetadata map[string]map[string]string `json:"service_metadata"`
	}
	require.NoError(t, json.Unmarshal(payload, &decoded))

	svc, ok := decoded.ServiceMetadata["service-1"]
	require.True(t, ok, "service-1 metadata must be present")
	assert.Equal(t, "v42", svc["manifest_version"], "snake_case manifest_version key")
	assert.Equal(t, "sha-abc", svc["image_tag"], "snake_case image_tag key")
	_, hasPascal := svc["ManifestVersion"]
	assert.False(t, hasPascal, "must not leak Go field name ManifestVersion onto the wire")
}

// TestOutboxPublisher_RunCancelledPair verifies that appending the
// [RunCancelled, RunFinalized] pair (as produced by Run.Cancel) writes exactly
// two outbox rows on their respective streams, and that the run.finalized:v1
// row carries status "cancelled".
func TestOutboxPublisher_RunCancelledPair(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	pub := postgres.NewOutboxPublisher(tx, discardLogger())

	id := uuid.New()
	err = pub.Append(ctx, []run.DomainEvent{
		run.RunCancelled{ID: id, Name: "e2e-schedule", By: "tester", CancellationReason: "drift"},
		run.RunFinalized{ID: id, Name: "e2e-schedule", Outcome: run.SchedulerStatusCancelled},
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
	require.Len(t, rows, 2, "expected exactly 2 outbox rows for the cancel pair")

	// Collect stream names for assertion regardless of order.
	streamNames := make(map[string][]byte, 2)
	for _, r := range rows {
		streamNames[r.StreamName] = r.Payload
	}

	assert.Contains(t, streamNames, streams.ScheduleCancelledV1, "missing schedule.cancelled:v1 row")
	assert.Contains(t, streamNames, streams.RunFinalizedV1, "missing run.finalized:v1 row")

	// Verify the finalized row carries status "cancelled".
	finalizedPayload, ok := streamNames[streams.RunFinalizedV1]
	require.True(t, ok, "run.finalized:v1 row must be present")
	var payload map[string]string
	require.NoError(t, json.Unmarshal(finalizedPayload, &payload))
	assert.Equal(t, "cancelled", payload["status"], "run.finalized:v1 status must be 'cancelled'")
}

func TestOutboxPublisher_AllEventTypes(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

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

			pub := postgres.NewOutboxPublisher(tx, discardLogger())
			err = pub.Append(ctx, []run.DomainEvent{event}, uuid.Nil)
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

// TestOutboxPublisher_InitiatedByProvenance pins the provenance contract: every
// run-creation event carries initiated_by on the wire, and an empty initiator
// (a platform-initiated run) is recorded as the "system" sentinel rather than a
// blank string.
func TestOutboxPublisher_InitiatedByProvenance(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	sourceID := uuid.New()

	cases := []struct {
		name       string
		event      func(id uuid.UUID) run.DomainEvent
		wantStream string
		want       string
	}{
		{
			name: "RunStarted carries the user",
			event: func(id uuid.UUID) run.DomainEvent {
				return run.RunStarted{ID: id, Name: "sched", K: run.KindTrigger, InitiatedBy: "okta|alice", ServiceMetadata: map[string]run.ServiceMetadata{}}
			},
			wantStream: streams.SchedulerStartedV1,
			want:       "okta|alice",
		},
		{
			name: "RunStarted empty initiator becomes system",
			event: func(id uuid.UUID) run.DomainEvent {
				return run.RunStarted{ID: id, Name: "sched", K: run.KindCron, InitiatedBy: "", ServiceMetadata: map[string]run.ServiceMetadata{}}
			},
			wantStream: streams.SchedulerStartedV1,
			want:       "system",
		},
		{
			name: "RerunRequested carries the user",
			event: func(id uuid.UUID) run.DomainEvent {
				return run.RerunRequested{ID: id, Name: "sched", SourceID: sourceID, InitiatedBy: "okta|bob"}
			},
			wantStream: streams.TriggerRerunV1,
			want:       "okta|bob",
		},
		{
			name: "RebaseRequested carries the user",
			event: func(id uuid.UUID) run.DomainEvent {
				return run.RebaseRequested{ID: id, Name: "sched", SourceID: sourceID, InitiatedBy: "okta|carol"}
			},
			wantStream: streams.TriggerRebaseV1,
			want:       "okta|carol",
		},
		{
			name: "SingleNodeRunRequested carries the user",
			event: func(id uuid.UUID) run.DomainEvent {
				return run.SingleNodeRunRequested{
					ID: id, Name: "sched",
					Target:         run.NodeID{ServiceName: "svc", SchemaName: "sch", TableName: "tbl"},
					MetadataSource: run.MetadataSourceLatest,
					InitiatedBy:    "okta|dave",
				}
			},
			wantStream: streams.TriggerSingleNodeRunV1,
			want:       "okta|dave",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			aggID := uuid.New()
			tx, err := db.BeginTxx(ctx, nil)
			require.NoError(t, err)

			pub := postgres.NewOutboxPublisher(tx, discardLogger())
			require.NoError(t, pub.Append(ctx, []run.DomainEvent{tc.event(aggID)}, uuid.Nil))
			require.NoError(t, tx.Commit())
			defer db.ExecContext(ctx, "DELETE FROM state_outbox WHERE aggregate_id = $1", aggID)

			var raw []byte
			require.NoError(t, db.GetContext(ctx, &raw,
				`SELECT payload FROM state_outbox WHERE aggregate_id = $1 AND stream_name = $2 LIMIT 1`,
				aggID, tc.wantStream,
			))
			var payload map[string]interface{}
			require.NoError(t, json.Unmarshal(raw, &payload))
			assert.Equal(t, tc.want, payload["initiated_by"], "initiated_by on %s", tc.wantStream)
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
