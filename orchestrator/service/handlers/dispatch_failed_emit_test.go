package handlers_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmitDispatchFailed_WritesOutboxEntry(t *testing.T) {
	ctx := context.Background()
	u := newFakeUnitOfWork()
	runID := uuid.New()
	msgProcID := uuid.New()

	err := handlers.EmitDispatchFailed(ctx, u, newTestLogger(), handlers.DispatchFailedParams{
		RunID:               runID.String(),
		ScheduleName:        "daily",
		MessageProcessingID: msgProcID,
		Reason:              pkgEvents.DispatchFailedReasonEmptyProjection,
	})
	require.NoError(t, err)
	require.Len(t, u.outboxRepo.CreatedEntries, 1)

	e := u.outboxRepo.CreatedEntries[0]
	assert.Equal(t, "run.entries.dispatch_failed:v1", e.StreamName)
	assert.Equal(t, "run_entries_dispatch_failed", e.EventType)
	assert.Equal(t, "orchestrator", e.AggregateType)
	assert.Equal(t, runID, e.AggregateID)
	assert.NotEqual(t, uuid.Nil, e.ID, "outbox entry must have a fresh UUID")
	require.NotNil(t, e.MessageProcessingID)
	assert.Equal(t, msgProcID, *e.MessageProcessingID)
	assert.Equal(t, "pending", e.Status)
	assert.Equal(t, 3, e.MaxRetries)

	var payload pkgEvents.RunEntriesDispatchFailed
	require.NoError(t, json.Unmarshal(e.Payload, &payload))
	assert.Equal(t, runID.String(), payload.ScheduleID)
	assert.Equal(t, "daily", payload.ScheduleName)
	assert.Equal(t, pkgEvents.DispatchFailedReasonEmptyProjection, payload.Reason)
}

func TestEmitDispatchFailed_InvalidRunID_ReturnsError(t *testing.T) {
	ctx := context.Background()
	u := newFakeUnitOfWork()

	err := handlers.EmitDispatchFailed(ctx, u, newTestLogger(), handlers.DispatchFailedParams{
		RunID:  "not-a-uuid",
		Reason: pkgEvents.DispatchFailedReasonEmptyProjection,
	})
	require.Error(t, err)
	require.Empty(t, u.outboxRepo.CreatedEntries, "no outbox entry on parse failure")
}
