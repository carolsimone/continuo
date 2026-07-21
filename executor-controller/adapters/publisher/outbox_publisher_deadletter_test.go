package publisher_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/adapters/publisher"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPublisher_PublishesDeadLetterRow asserts the typed-switch publisher
// routes a DeadLetterEventType row through outbox.DeadLetterValues to
// entry.StreamName generically, with expanded scalar fields (e.g.
// failure_kind), instead of hitting the switch's "unknown event_type" default.
func TestPublisher_PublishesDeadLetterRow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	entry := &outbox.Entry{
		ID:            uuid.New(),
		AggregateType: outbox.DeadLetterAggregateType,
		AggregateID:   uuid.New(),
		EventType:     outbox.DeadLetterEventType,
		StreamName:    streams.OutboxDeadLetterV1,
		Payload:       []byte(`{"failure_kind":"permanent","original_event_type":"node_deployed"}`),
	}
	require.NoError(t, pub.Publish(context.Background(), entry))

	v := lastEntryFields(t, r, streams.OutboxDeadLetterV1)
	assert.Equal(t, "permanent", v["failure_kind"])
	assert.Equal(t, entry.ID.String(), v["outbox_entry_id"])
}
