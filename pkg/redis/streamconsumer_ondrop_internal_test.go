package redis

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOnDropped_FiresRegisteredCallback verifies the drop seam: when a message
// is abandoned, the registered DropHandler is called with that message and the
// cause, so the owning service can finalize any in-flight state it committed for
// the message before the consumer gave up on it.
func TestOnDropped_FiresRegisteredCallback(t *testing.T) {
	var gotMsg goredis.XMessage
	var gotErr error
	called := false
	c := NewStreamConsumer(nil, "s", "g", nil, discardLog(),
		WithOnDrop(func(_ context.Context, msg goredis.XMessage, cause error) {
			called = true
			gotMsg = msg
			gotErr = cause
		}))

	cause := errors.New("boom")
	c.onDropped(context.Background(), goredis.XMessage{ID: "1-0"}, cause)

	require.True(t, called, "the registered drop handler must be invoked")
	assert.Equal(t, "1-0", gotMsg.ID)
	assert.Equal(t, cause, gotErr)
}

// TestOnDropped_NoCallbackIsNoOp verifies the default: a consumer with no drop
// handler registered drops messages exactly as before, without panicking.
func TestOnDropped_NoCallbackIsNoOp(t *testing.T) {
	c := NewStreamConsumer(nil, "s", "g", nil, discardLog())

	require.NotPanics(t, func() {
		c.onDropped(context.Background(), goredis.XMessage{ID: "1-0"}, errors.New("boom"))
	})
}

// TestOnDropped_RecoversPanickingCallback verifies isolation: a drop handler
// that panics must not unwind into the consumer loop and kill the process — the
// drop path is best-effort housekeeping, never on the critical path.
func TestOnDropped_RecoversPanickingCallback(t *testing.T) {
	c := NewStreamConsumer(nil, "s", "g", nil, discardLog(),
		WithOnDrop(func(context.Context, goredis.XMessage, error) {
			panic("callback blew up")
		}))

	require.NotPanics(t, func() {
		c.onDropped(context.Background(), goredis.XMessage{ID: "1-0"}, errors.New("boom"))
	})
}

// TestStreamConsumer_ReclaimPath_PoisonDrop_InvokesOnDrop is the regression for
// the orphaned-in-flight-row bug: when a poison message is quarantined at the
// reclaim path, the registered DropHandler must fire with that message so the
// owning service can finalize the in-flight state the drop leaves behind. Redis-
// gated, mirroring the poison-quarantine test's setup.
func TestStreamConsumer_ReclaimPath_PoisonDrop_InvokesOnDrop(t *testing.T) {
	rc := internalRedisClient(t)
	ctx := context.Background()

	stream := fmt.Sprintf("test-stream-poison-ondrop-%d", time.Now().UnixNano())
	group := "test-group"
	t.Cleanup(func() { rc.Del(ctx, stream) })

	require.NoError(t, rc.XGroupCreateMkStream(ctx, stream, group, "0").Err())
	msgID, err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]interface{}{"payload": "p"}}).Result()
	require.NoError(t, err)

	// Seed the PEL so reclaimPending's XAUTOCLAIM keeps bumping the delivery count.
	_, err = rc.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: group, Consumer: "seed-consumer", Streams: []string{stream, ">"}, Count: 10,
	}).Result()
	require.NoError(t, err)

	poison := func(context.Context, goredis.XMessage) error {
		return errors.New("transient handler failure that never clears")
	}
	var drops atomic.Int32
	var droppedID atomic.Value
	c := NewStreamConsumer(rc, stream, group, poison, discardLog(),
		WithReclaimMinIdle(0),
		WithOnDrop(func(_ context.Context, msg goredis.XMessage, cause error) {
			drops.Add(1)
			droppedID.Store(msg.ID)
			require.Error(t, cause)
		}))

	pendingCount := func() int64 {
		res, perr := rc.XPending(ctx, stream, group).Result()
		require.NoError(t, perr)
		return res.Count
	}
	require.Eventually(t, func() bool {
		require.NoError(t, c.reclaimPending(ctx))
		return pendingCount() == 0
	}, 10*time.Second, 50*time.Millisecond, "poison message must be ACK-dropped once it exceeds maxDeliveries")

	require.Equal(t, int32(1), drops.Load(), "onDrop must fire exactly once when the poison message is quarantined")
	assert.Equal(t, msgID, droppedID.Load(), "onDrop must carry the dropped message so its in-flight state can be found")
}
