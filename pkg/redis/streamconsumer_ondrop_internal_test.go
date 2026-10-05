package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
)

// TestProcessOne_NotifiesDropOnlyAfterAckSucceeds pins the ordering: the read
// path writes the dead letter first, ACKs the message only after that, and
// notifies the drop only once the message is actually ACKed. A failed XACK
// leaves the message in the PEL to be reprocessed, so finalizing its in-flight
// state then would abandon a message that was never dropped — in
// agent-remediation the newly-terminal row lets the same trigger spend another
// attempt.
func TestProcessOne_NotifiesDropOnlyAfterAckSucceeds(t *testing.T) {
	permHandler := func(context.Context, goredis.XMessage) error {
		return fmt.Errorf("unprocessable: %w", events.ErrPermanent)
	}
	var calls []string
	c := NewStreamConsumer(nil, "s", "g", permHandler, discardLog(),
		WithOnDrop(func(context.Context, goredis.XMessage, error) { calls = append(calls, "drop") }))
	c.SetService("pkg-redis-test")
	c.deadLetterFn = func(context.Context, map[string]any) error { calls = append(calls, "dead-letter"); return nil }

	c.ackFn = func(context.Context, string) error {
		calls = append(calls, "ack-failed")
		return errors.New("XACK failed")
	}
	c.processOne(context.Background(), msg("1-0"))
	require.Equal(t, []string{"dead-letter", "ack-failed"}, calls,
		"the dead letter precedes the ACK; a failed ACK leaves the message pending — the drop must not be notified")

	calls = nil
	c.ackFn = func(context.Context, string) error { calls = append(calls, "ack"); return nil }
	c.processOne(context.Background(), msg("1-0"))
	require.Equal(t, []string{"dead-letter", "ack", "drop"}, calls,
		"a confirmed ACK after the dead letter notifies the drop exactly once")
}

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
// the orphaned-in-flight-row bug: when a message that keeps failing is
// dead-lettered on its fifth delivery at the reclaim path, the registered
// DropHandler must fire with that message — after its dead letter exists — so
// the owning service can finalize the in-flight state the message leaves
// behind. Redis-gated, mirroring the poison-dead-letter test's setup.
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
	// The drop handler runs off the test goroutine, so it only records what it
	// saw; the test goroutine asserts on it afterwards.
	var atDrop struct {
		sync.Mutex
		calls       int
		id          string
		cause       error
		deadLetters int
		readErr     error
	}
	c := NewStreamConsumer(rc, stream, group, poison, discardLog(),
		WithReclaimMinIdle(0),
		WithOnDrop(func(_ context.Context, msg goredis.XMessage, cause error) {
			dl, err := deadLettersIn(context.Background(), rc, stream)
			atDrop.Lock()
			defer atDrop.Unlock()
			atDrop.calls++
			atDrop.id, atDrop.cause, atDrop.deadLetters, atDrop.readErr = msg.ID, cause, len(dl), err
		}))
	c.SetService("pkg-redis-test")

	pendingCount := func() int64 {
		res, perr := rc.XPending(ctx, stream, group).Result()
		require.NoError(t, perr)
		return res.Count
	}
	require.Eventually(t, func() bool {
		require.NoError(t, c.reclaimPending(ctx))
		return pendingCount() == 0
	}, 10*time.Second, 50*time.Millisecond, "poison message must be dead-lettered and acknowledged on its fifth delivery")

	atDrop.Lock()
	defer atDrop.Unlock()
	require.Equal(t, 1, atDrop.calls, "onDrop must fire exactly once when the poison message is dead-lettered")
	assert.Equal(t, msgID, atDrop.id, "onDrop must carry the dropped message so its in-flight state can be found")
	assert.Error(t, atDrop.cause)
	require.NoError(t, atDrop.readErr)
	assert.Equal(t, 1, atDrop.deadLetters, "onDrop fires only after the dead letter exists")

	dl := deadLettersFor(t, rc, stream)
	require.Len(t, dl, 1)
	var p events.ConsumerDeadLetter
	require.NoError(t, json.Unmarshal([]byte(dl[0].Values["payload"].(string)), &p))
	assert.Equal(t, msgID, p.OriginalMessageID)
	assert.Equal(t, model.DeadLetterKindTransientExhausted, p.FailureKind)
	assert.Equal(t, int64(maxDeliveries), p.DeliveryCount, "dead-lettered on the fifth delivery")
}
