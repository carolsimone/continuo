package publisher

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// newTestRedis starts an in-memory miniredis instance and returns a client
// wired to it, mirroring the harness executor-controller's publisher tests
// already use.
func newTestRedis(t *testing.T) (*goredis.Client, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	return client, func() {
		client.Close()
		mr.Close()
	}
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// TestOutboxPublisher_PublishesDeadLetterRow asserts a DeadLetterEventType row
// publishes generically to entry.StreamName via outbox.DeadLetterValues, with
// its scalar fields (e.g. failure_kind) expanded onto the stream entry.
func TestOutboxPublisher_PublishesDeadLetterRow(t *testing.T) {
	rdb, cleanup := newTestRedis(t)
	defer cleanup()
	p := NewOutboxPublisher(rdb, newTestLogger())

	entry := &outbox.Entry{
		ID:            uuid.New(),
		AggregateType: outbox.DeadLetterAggregateType,
		AggregateID:   uuid.New(),
		EventType:     outbox.DeadLetterEventType,
		StreamName:    streams.OutboxDeadLetterV1,
		Payload:       []byte(`{"failure_kind":"permanent","original_event_type":"compile_requested"}`),
	}
	require.NoError(t, p.Publish(context.Background(), entry))

	res, err := rdb.XRange(context.Background(), streams.OutboxDeadLetterV1, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, "permanent", res[0].Values["failure_kind"])
	require.Equal(t, entry.ID.String(), res[0].Values["outbox_entry_id"])
}

// TestOutboxPublisher_PublishBatch_PublishesDeadLetterRow asserts a batch that
// mixes a dead-letter row among normal rows publishes the dead-letter row via
// the same generic passthrough as a single Publish call. PublishBatch funnels
// every entry through xaddArgs, so this guards against the dead-letter branch
// only being wired into the single-Publish path.
func TestOutboxPublisher_PublishBatch_PublishesDeadLetterRow(t *testing.T) {
	rdb, cleanup := newTestRedis(t)
	defer cleanup()
	p := NewOutboxPublisher(rdb, newTestLogger())

	normal := &outbox.Entry{
		ID:         uuid.New(),
		EventType:  "compile_requested",
		StreamName: streams.CompileRequestedV1,
		Payload:    []byte(`{"release_id":"rel_1"}`),
	}
	deadLetter := &outbox.Entry{
		ID:            uuid.New(),
		AggregateType: outbox.DeadLetterAggregateType,
		AggregateID:   uuid.New(),
		EventType:     outbox.DeadLetterEventType,
		StreamName:    streams.OutboxDeadLetterV1,
		Payload:       []byte(`{"failure_kind":"transient_exhausted","original_event_type":"node_updated"}`),
	}

	errs := p.PublishBatch(context.Background(), []*outbox.Entry{normal, deadLetter})
	require.Len(t, errs, 2)
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])

	res, err := rdb.XRange(context.Background(), streams.OutboxDeadLetterV1, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, "transient_exhausted", res[0].Values["failure_kind"])
	require.Equal(t, deadLetter.ID.String(), res[0].Values["outbox_entry_id"])
}
