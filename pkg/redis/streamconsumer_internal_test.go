package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testConsumer(handler MessageHandler) *StreamConsumer {
	return &StreamConsumer{
		streamName:  "test-stream",
		logger:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		handler:     handler,
		workerCount: 1,
	}
}

func msgWithKey(id, keyField, keyVal string) goredis.XMessage {
	return goredis.XMessage{ID: id, Values: map[string]interface{}{keyField: keyVal}}
}

// safeInvoke must convert a handler panic into a non-permanent error so a single
// poison message cannot crash the consumer process; the message then stays in
// the PEL for the next sweep (transient path), it is not ACK-dropped.
func TestSafeInvoke_RecoversPanic(t *testing.T) {
	c := testConsumer(func(context.Context, goredis.XMessage) error {
		panic("boom")
	})
	err := c.safeInvoke(context.Background(), goredis.XMessage{ID: "1-0"})
	require.Error(t, err)
	assert.False(t, errors.Is(err, events.ErrPermanent), "recovered panic must be transient (stays in PEL)")
	assert.Contains(t, err.Error(), "panic")
}

func TestSafeInvoke_PassesThroughNilAndError(t *testing.T) {
	c := testConsumer(func(context.Context, goredis.XMessage) error { return nil })
	require.NoError(t, c.safeInvoke(context.Background(), goredis.XMessage{ID: "1-0"}))

	sentinel := errors.New("handler error")
	c = testConsumer(func(context.Context, goredis.XMessage) error { return sentinel })
	assert.ErrorIs(t, c.safeInvoke(context.Background(), goredis.XMessage{ID: "1-0"}), sentinel)
}

// invokeWithRetry must not let a handler panic escape: the panic is recovered as
// a transient error, retried, and the loop continues. Here the handler panics on
// its first attempt and succeeds on the second, proving the consumer survives.
func TestInvokeWithRetry_RecoversPanicThenRetries(t *testing.T) {
	var calls atomic.Int32
	c := testConsumer(func(context.Context, goredis.XMessage) error {
		if calls.Add(1) == 1 {
			panic("boom on first attempt")
		}
		return nil
	})
	err := c.invokeWithRetry(context.Background(), goredis.XMessage{ID: "1-0"})
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load())
}

// ── Worker-pool sharding (issue #140) ──────────────────────────────────────

// TestLaneFor_SameKeySameLane: messages carrying the same aggregate-key value
// must always route to the same worker lane — this is the invariant that gives
// per-aggregate ordering under N>1.
func TestLaneFor_SameKeySameLane(t *testing.T) {
	c := testConsumer(func(context.Context, goredis.XMessage) error { return nil })
	c.workerCount = 8
	c.aggregateKeyField = "schedule_id"

	first := c.laneFor(msgWithKey("1-0", "schedule_id", "run-abc"))
	for i := 0; i < 100; i++ {
		got := c.laneFor(msgWithKey(fmt.Sprintf("%d-0", i), "schedule_id", "run-abc"))
		require.Equal(t, first, got, "same key must always hash to the same lane")
	}
}

// TestLaneFor_MissingKeyIsStable: a message with no aggregate-key value must
// still route deterministically (to the empty-string lane) so order is never
// violated; only parallelism is forgone.
func TestLaneFor_MissingKeyIsStable(t *testing.T) {
	c := testConsumer(func(context.Context, goredis.XMessage) error { return nil })
	c.workerCount = 4
	c.aggregateKeyField = "schedule_id"

	emptyLane := c.laneFor(goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}})
	for i := 0; i < 20; i++ {
		got := c.laneFor(goredis.XMessage{ID: fmt.Sprintf("%d-0", i), Values: map[string]interface{}{}})
		require.Equal(t, emptyLane, got, "absent key must route to a single stable lane")
	}
	assert.GreaterOrEqual(t, emptyLane, 0)
	assert.Less(t, emptyLane, 4)
}

// TestProcessSharded_PerAggregateOrdering: under N>1, messages for the same
// aggregate must be handed to the handler in strict arrival order. We record
// the order each key was observed and assert it matches the submission order.
func TestProcessSharded_PerAggregateOrdering(t *testing.T) {
	var mu sync.Mutex
	seen := map[string][]string{} // key -> ordered list of message IDs

	c := testConsumer(func(_ context.Context, msg goredis.XMessage) error {
		key, _ := msg.Values["schedule_id"].(string)
		// Stagger to provoke reordering if lanes were not FIFO-per-key.
		time.Sleep(time.Millisecond)
		mu.Lock()
		seen[key] = append(seen[key], msg.ID)
		mu.Unlock()
		return nil
	})
	c.workerCount = 4
	c.aggregateKeyField = "schedule_id"

	var msgs []goredis.XMessage
	want := map[string][]string{}
	for _, key := range []string{"run-a", "run-b", "run-c"} {
		for i := 0; i < 10; i++ {
			id := fmt.Sprintf("%s-%d", key, i)
			msgs = append(msgs, goredis.XMessage{ID: id, Values: map[string]interface{}{"schedule_id": key}})
			want[key] = append(want[key], id)
		}
	}

	ackIDs := c.processSharded(context.Background(), msgs)
	require.Len(t, ackIDs, len(msgs), "every successfully-handled message must be acked")

	for key, order := range want {
		assert.Equal(t, order, seen[key], "messages for %s must be processed in arrival order", key)
	}
}

// TestProcessSharded_DistinctKeysRunInParallel: two different aggregates must
// be able to process concurrently. We block each lane on a barrier that only
// releases once BOTH handlers have started; if the lanes were serial this would
// deadlock and the test would time out.
func TestProcessSharded_DistinctKeysRunInParallel(t *testing.T) {
	const keys = 2
	var started sync.WaitGroup
	started.Add(keys)
	release := make(chan struct{})

	c := testConsumer(func(_ context.Context, msg goredis.XMessage) error {
		started.Done()
		// Block until every lane has entered the handler — only possible if the
		// lanes run in parallel.
		<-release
		return nil
	})
	c.workerCount = keys
	c.aggregateKeyField = "schedule_id"

	// Different keys are guaranteed distinct lanes here because we pick keys that
	// hash to different lanes; if they collided the barrier would deadlock, so we
	// assert separation first.
	keyA, keyB := "run-a", "run-b"
	require.NotEqual(t, c.laneFor(msgWithKey("a", "schedule_id", keyA)),
		c.laneFor(msgWithKey("b", "schedule_id", keyB)),
		"test precondition: chosen keys must land on different lanes")

	msgs := []goredis.XMessage{
		{ID: "a-0", Values: map[string]interface{}{"schedule_id": keyA}},
		{ID: "b-0", Values: map[string]interface{}{"schedule_id": keyB}},
	}

	done := make(chan []string, 1)
	go func() { done <- c.processSharded(context.Background(), msgs) }()

	// If processing were serial, started.Wait would block forever (the first
	// handler holds on <-release before the second starts).
	waitDone := make(chan struct{})
	go func() { started.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("distinct aggregates did not run in parallel (timed out waiting for both handlers to start)")
	}
	close(release)

	select {
	case ack := <-done:
		assert.ElementsMatch(t, []string{"a-0", "b-0"}, ack)
	case <-time.After(2 * time.Second):
		t.Fatal("processSharded did not complete")
	}
}

// TestProcessSerial_TransientFailureNotAcked: the serial (N=1) path must leave a
// transiently-failing message un-acked so it stays in the PEL, exactly as
// before. Permanent and success are acked.
func TestProcessSerial_AckSelectivity(t *testing.T) {
	c := testConsumer(func(_ context.Context, msg goredis.XMessage) error {
		switch msg.ID {
		case "ok-0":
			return nil
		case "perm-0":
			return fmt.Errorf("%w: bad", events.ErrPermanent)
		default:
			return errors.New("transient")
		}
	})
	msgs := []goredis.XMessage{{ID: "ok-0"}, {ID: "perm-0"}, {ID: "trans-0"}}
	ackIDs := c.processSerial(context.Background(), msgs)
	assert.ElementsMatch(t, []string{"ok-0", "perm-0"}, ackIDs,
		"success and permanent acked; transient left in PEL")
}

// TestConsumerName_StablePerHost: the derived consumer name must be stable
// across calls (same hostname) so restarts reuse the registry entry rather than
// leaking a new one each time.
func TestConsumerName_StablePerHost(t *testing.T) {
	a := consumerName("state-task-status-updated")
	b := consumerName("state-task-status-updated")
	assert.Equal(t, a, b, "consumer name must be stable across restarts (no registry leak)")
	assert.True(t, len(a) > len("state-task-status-updated"),
		"consumer name must include a per-pod suffix")
}
