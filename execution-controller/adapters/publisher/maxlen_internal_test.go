package publisher

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/serialization"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// TestXAddArgs_SetsMaxLenApprox asserts the merged publisher caps every stream
// with MaxLen/Approx (streams.StreamMaxLen), matching state's and the
// orchestrator's publishers so execution-controller streams cannot grow
// unbounded. It seeds a stream past the cap with a pipelined bulk insert (bulk
// XADDs without MAXLEN, so no trim runs on them), then publishes exactly one
// more row through the publisher; that single XADD carries MaxLen/Approx, so
// miniredis's own trim behavior brings the stream back down to the cap.
func TestXAddArgs_SetsMaxLenApprox(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	r := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})

	stream := streams.NodeUpdatedV1

	ctx := context.Background()
	pipe := r.Pipeline()
	for i := 0; i < streams.StreamMaxLen; i++ {
		pipe.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]interface{}{"i": i}})
	}
	_, err = pipe.Exec(ctx)
	require.NoError(t, err)

	xlenBefore, err := r.XLen(ctx, stream).Result()
	require.NoError(t, err)
	require.Equal(t, int64(streams.StreamMaxLen), xlenBefore, "setup: stream must be exactly at the cap before the capped publish")

	p := NewOutboxPublisher(r, nil)
	payload, err := json.Marshal(serialization.NodeUpdatedFromDomain(event.NodeUpdated{Status: "SUCCEEDED"}))
	require.NoError(t, err)

	require.NoError(t, p.Publish(ctx, &outbox.Entry{
		ID:         uuid.New(),
		StreamName: stream,
		EventType:  event.EventTypeNodeUpdated,
		Payload:    payload,
	}))

	xlenAfter, err := r.XLen(ctx, stream).Result()
	require.NoError(t, err)
	require.Equal(t, int64(streams.StreamMaxLen), xlenAfter,
		"MaxLen cap must keep the stream at the cap after publishing one more entry past it")
}
