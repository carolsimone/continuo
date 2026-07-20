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
		// No-op ack by default (no live Redis). Tests asserting ack behaviour
		// override this with a recorder.
		ackFn: func(context.Context, string) {},
	}
}

// ackRecorder returns an ackFn that records acknowledged message IDs and the
// recorded slice. Safe for concurrent lanes.
func ackRecorder() (func(context.Context, string), *[]string, *sync.Mutex) {
	var mu sync.Mutex
	var acked []string
	return func(_ context.Context, id string) {
		mu.Lock()
		acked = append(acked, id)
		mu.Unlock()
	}, &acked, &mu
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
	ack, acked, ackMu := ackRecorder()
	c.ackFn = ack

	var msgs []goredis.XMessage
	want := map[string][]string{}
	for _, key := range []string{"run-a", "run-b", "run-c"} {
		for i := 0; i < 10; i++ {
			id := fmt.Sprintf("%s-%d", key, i)
			msgs = append(msgs, goredis.XMessage{ID: id, Values: map[string]interface{}{"schedule_id": key}})
			want[key] = append(want[key], id)
		}
	}

	c.processSharded(context.Background(), msgs)

	ackMu.Lock()
	require.Len(t, *acked, len(msgs), "every successfully-handled message must be acked")
	ackMu.Unlock()

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
	ack, acked, ackMu := ackRecorder()
	c.ackFn = ack

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

	done := make(chan struct{}, 1)
	go func() { c.processSharded(context.Background(), msgs); done <- struct{}{} }()

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
	case <-done:
		ackMu.Lock()
		assert.ElementsMatch(t, []string{"a-0", "b-0"}, *acked)
		ackMu.Unlock()
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
	ack, acked, ackMu := ackRecorder()
	c.ackFn = ack
	msgs := []goredis.XMessage{{ID: "ok-0"}, {ID: "perm-0"}, {ID: "trans-0"}}
	c.processSerial(context.Background(), msgs)
	ackMu.Lock()
	assert.ElementsMatch(t, []string{"ok-0", "perm-0"}, *acked,
		"success and permanent acked; transient left in PEL")
	ackMu.Unlock()
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

// ── [P2/#4] permanent-vs-transient bootstrap error classification ───────────

// fakeRedisErr implements the goredis.Error interface (Error + RedisError) so
// tests can construct genuine Redis reply-errors — the only thing
// goredis.HasErrorPrefix will match — without a live server.
type fakeRedisErr string

func (e fakeRedisErr) Error() string { return string(e) }
func (e fakeRedisErr) RedisError()   {}

// TestIsPermanentBootstrapError pins the narrowed [#2] classification and the
// robust [#4] prefix matching. Pure unit test, no Redis.
func TestIsPermanentBootstrapError(t *testing.T) {
	// Structurally-permanent Redis reply errors (wrapping is transparent).
	permanent := []error{
		fmt.Errorf("failed to create consumer group: %w",
			fakeRedisErr("WRONGTYPE Operation against a key holding the wrong kind of value")),
		fakeRedisErr("ERR unknown command 'XGROUP', with args beginning with:"),
		fakeRedisErr("ERR unknown subcommand 'CREATE'"),
	}
	for _, e := range permanent {
		assert.Truef(t, isPermanentBootstrapError(e), "must be permanent: %v", e)
	}

	// Auth-class Redis errors are NOT permanent [#2]: a password rotation race
	// or ACL-propagation lag is transient, so these must retry (not crashloop
	// the fleet). Plus the ordinary transient server states.
	transientRedis := []error{
		fakeRedisErr("WRONGPASS invalid username-password pair or user is disabled."),
		fakeRedisErr("NOAUTH Authentication required."),
		fakeRedisErr("NOPERM this user has no permissions to run the 'xgroup|create' command"),
		fakeRedisErr("LOADING Redis is loading the dataset in memory"),
		fakeRedisErr("CLUSTERDOWN The cluster is down"),
	}
	for _, e := range transientRedis {
		assert.Falsef(t, isPermanentBootstrapError(e), "must be transient: %v", e)
	}

	// [#4] robustness: a NON-Redis error (e.g. a network error) whose text
	// merely CONTAINS — or even starts with — a permanent token must not be
	// misclassified, because it is not a Redis reply-error at all.
	assert.False(t, isPermanentBootstrapError(errors.New("unknown command received from a proxy: connection reset")),
		"a non-Redis error must never be classified permanent, even if its text starts with a token")
	assert.False(t, isPermanentBootstrapError(errors.New("dial tcp 10.0.0.1:6379: connect: connection refused")))

	// [#4] a genuine Redis error that merely CONTAINS a token mid-message (not
	// as the reply-error code prefix) must not match either.
	assert.False(t, isPermanentBootstrapError(fakeRedisErr("ERR background save failed; WRONGTYPE appeared in a log line")),
		"a token that is not the reply-error code prefix must not match")

	assert.False(t, isPermanentBootstrapError(nil), "nil is not a permanent error")
}

// ── [P1b] bounded handler deadline + per-attempt heartbeat ──────────────────

// TestSafeInvoke_AdvancesHeartbeatBeforeRunning proves the heartbeat advances
// at the START of each handler attempt, so a legitimately-slow handler keeps
// the liveness heartbeat fresh instead of freezing it at the read-loop
// iteration timestamp. We stall a stale heartbeat, block the handler mid-flight,
// and assert Healthy already reads fresh while the handler is still running.
func TestSafeInvoke_AdvancesHeartbeatBeforeRunning(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	c := testConsumer(func(context.Context, goredis.XMessage) error {
		close(started)
		<-release
		return nil
	})
	// Pretend the loop last made progress an hour ago.
	c.lastActivity.Store(time.Now().Add(-time.Hour).UnixNano())

	go func() { _ = c.safeInvoke(context.Background(), goredis.XMessage{ID: "1-0"}) }()

	<-started // handler is now blocked mid-flight
	assert.NoError(t, c.Healthy(time.Second),
		"the heartbeat must be advanced at handler start, so a slow in-flight handler never trips liveness")
	close(release)
}

// TestSafeInvoke_BoundsHandlerWithDeadline proves a configured handler timeout
// unblocks a handler that would otherwise hang: the handler waits on ctx.Done()
// and safeInvoke returns a DeadlineExceeded error promptly.
func TestSafeInvoke_BoundsHandlerWithDeadline(t *testing.T) {
	c := testConsumer(func(ctx context.Context, _ goredis.XMessage) error {
		<-ctx.Done() // a well-behaved handler observes cancellation and returns
		return ctx.Err()
	})
	c.SetHandlerTimeout(50 * time.Millisecond)

	start := time.Now()
	err := c.safeInvoke(context.Background(), goredis.XMessage{ID: "1-0"})
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 2*time.Second, "the deadline must unblock the handler well before this bound")
}

// TestSafeInvoke_NoDeadlineWhenTimeoutUnset confirms the default (0) leaves the
// handler context unbounded, preserving behaviour for callers that do not opt
// in (e.g. orchestrator/state).
func TestSafeInvoke_NoDeadlineWhenTimeoutUnset(t *testing.T) {
	var hadDeadline bool
	c := testConsumer(func(ctx context.Context, _ goredis.XMessage) error {
		_, hadDeadline = ctx.Deadline()
		return nil
	})
	require.NoError(t, c.safeInvoke(context.Background(), goredis.XMessage{ID: "1-0"}))
	assert.False(t, hadDeadline, "an unset handler timeout must not impose a deadline on the handler context")
}
