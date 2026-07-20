package redis

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func internalRedisClient(t *testing.T) *goredis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set — skipping Redis integration test")
	}
	c := goredis.NewClient(&goredis.Options{
		Addr:     addr,
		Password: os.Getenv("REDIS_PASSWORD"),
	})
	t.Cleanup(func() { c.Close() })
	return c
}

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func consumerNames(t *testing.T, rc *goredis.Client, ctx context.Context, stream, group string) []string {
	t.Helper()
	cons, err := rc.XInfoConsumers(ctx, stream, group).Result()
	require.NoError(t, err)
	names := make([]string, 0, len(cons))
	for _, c := range cons {
		names = append(names, c.Name)
	}
	return names
}

// TestStreamConsumer_CleanupStaleConsumers verifies the registry housekeeping
// that bounds growth under pod replacement: a drained (zero-pending) consumer
// left by a prior pod is deleted, while a consumer still holding pending entries
// and this consumer's own entry are preserved.
func TestStreamConsumer_CleanupStaleConsumers(t *testing.T) {
	rc := internalRedisClient(t)
	ctx := context.Background()
	stream := fmt.Sprintf("test-stream-cleanup-%d", time.Now().UnixNano())
	group := "test-group"
	t.Cleanup(func() { rc.Del(ctx, stream) })

	require.NoError(t, rc.XGroupCreateMkStream(ctx, stream, group, "0").Err())

	// A drained consumer left by a replaced pod: registered, zero pending.
	require.NoError(t, rc.XGroupCreateConsumer(ctx, stream, group, "ghost-old-pod").Err())

	// A consumer still holding an un-ACKed entry must be preserved.
	require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]interface{}{"k": "v"}}).Err())
	_, err := rc.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: group, Consumer: "busy-consumer", Streams: []string{stream, ">"}, Count: 1,
	}).Result()
	require.NoError(t, err)

	c := NewStreamConsumer(rc, stream, group, func(context.Context, goredis.XMessage) error { return nil }, discardLog())
	// Self must never be deleted even when drained.
	require.NoError(t, rc.XGroupCreateConsumer(ctx, stream, group, c.consumerName).Err())

	c.cleanupStaleConsumers(ctx, 0)

	names := consumerNames(t, rc, ctx, stream, group)
	assert.NotContains(t, names, "ghost-old-pod", "drained stale consumer must be deleted")
	assert.Contains(t, names, "busy-consumer", "consumer with pending entries must be kept")
	assert.Contains(t, names, c.consumerName, "must never delete itself")
}

// distinctLaneKeys finds two aggregate-key values that hash to different worker
// lanes, so a two-lane test can exercise genuine cross-lane parallelism.
func distinctLaneKeys(c *StreamConsumer) (laneA, laneB string) {
	for i := 0; ; i++ {
		k := fmt.Sprintf("key-%d", i)
		lane := c.laneFor(goredis.XMessage{Values: map[string]interface{}{c.aggregateKeyField: k}})
		if lane == 0 && laneA == "" {
			laneA = k
		}
		if lane == 1 && laneB == "" {
			laneB = k
		}
		if laneA != "" && laneB != "" {
			return laneA, laneB
		}
	}
}

// TestStreamConsumer_WorkerPool_AcksFinishedLaneWhileAnotherBlocks proves the
// ack-after-success guarantee under workerCount>1: a message whose handler has
// completed is ACKed immediately, even while a sibling lane is still blocked on
// a slow handler. Batch-level acking would hold the finished message in the PEL
// until the slow lane returned, exposing it to a peer's reclaim sweep.
func TestStreamConsumer_WorkerPool_AcksFinishedLaneWhileAnotherBlocks(t *testing.T) {
	rc := internalRedisClient(t)
	ctx := context.Background()
	stream := fmt.Sprintf("test-stream-pool-ack-%d", time.Now().UnixNano())
	group := "test-group"
	const keyField = "schedule_id"
	t.Cleanup(func() { rc.Del(ctx, stream) })
	require.NoError(t, rc.XGroupCreateMkStream(ctx, stream, group, "0").Err())

	probe := NewStreamConsumer(rc, stream, group, nil, discardLog(), WithWorkerPool(2, keyField))
	fastKey, slowKey := distinctLaneKeys(probe)

	block := make(chan struct{})
	handler := func(_ context.Context, m goredis.XMessage) error {
		if m.Values[keyField] == slowKey {
			<-block // hold this lane until the test releases it
		}
		return nil
	}

	// Add the slow message first: with batch-level acking the fast message would
	// be forced to wait behind it.
	slowID, err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]interface{}{keyField: slowKey}}).Result()
	require.NoError(t, err)
	fastID, err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]interface{}{keyField: fastKey}}).Result()
	require.NoError(t, err)

	c := NewStreamConsumer(rc, stream, group, handler, discardLog(), WithWorkerPool(2, keyField))
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go c.Start(runCtx) //nolint:errcheck

	pendingIDs := func() map[string]bool {
		pend, err := rc.XPendingExt(ctx, &goredis.XPendingExtArgs{
			Stream: stream, Group: group, Start: "-", End: "+", Count: 10,
		}).Result()
		require.NoError(t, err)
		ids := make(map[string]bool, len(pend))
		for _, p := range pend {
			ids[p.ID] = true
		}
		return ids
	}

	require.Eventually(t, func() bool {
		ids := pendingIDs()
		return !ids[fastID] && ids[slowID]
	}, 5*time.Second, 50*time.Millisecond, "fast lane must ACK while the slow lane is blocked")

	close(block) // release the slow lane

	require.Eventually(t, func() bool {
		return len(pendingIDs()) == 0
	}, 5*time.Second, 50*time.Millisecond, "slow lane ACKs once released")
}

// ── Read-loop / startup resilience (incident: consumer goroutines that die
// silently on a Redis blip and are never restarted) ────────────────────────

// TestStreamConsumer_Healthy_ReflectsHeartbeatFreshness is a pure unit test
// (no Redis needed) for the Healthy staleness check that backs the liveness
// probe: fresh activity reads healthy, activity older than maxStale reads
// unhealthy with a descriptive error, and a never-started consumer (no
// activity recorded at all) also reads unhealthy rather than nil-panicking.
func TestStreamConsumer_Healthy_ReflectsHeartbeatFreshness(t *testing.T) {
	c := NewStreamConsumer(goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"}),
		"stream", "group", func(context.Context, goredis.XMessage) error { return nil }, discardLog())

	assert.NoError(t, c.Healthy(time.Minute), "freshly constructed consumer must read healthy")

	c.lastActivity.Store(time.Now().Add(-time.Hour).UnixNano())
	err := c.Healthy(time.Minute)
	require.Error(t, err, "activity older than maxStale must read unhealthy")
	assert.Contains(t, err.Error(), "stalled")

	c.lastActivity.Store(time.Now().UnixNano())
	assert.NoError(t, c.Healthy(time.Minute), "a fresh heartbeat must clear the unhealthy state")

	var neverStarted StreamConsumer
	err = neverStarted.Healthy(time.Minute)
	require.Error(t, err, "a consumer with no recorded activity must read unhealthy, not panic")
	assert.Contains(t, err.Error(), "has not started")
}

// TestStreamConsumer_Start_SurvivesConnectionErrors_WithoutExiting is the
// regression test for the incident: a StreamConsumer pointed at an
// unreachable Redis address must keep retrying — logging and backing off —
// rather than returning from Start and leaving nothing to restart it. This
// covers both halves of the loop the incident exposed: the startup
// ensureConsumerGroup bootstrap, and (via the heartbeat) the read loop
// proper. No REDIS_ADDR is needed: the address is deliberately unreachable.
func TestStreamConsumer_Start_SurvivesConnectionErrors_WithoutExiting(t *testing.T) {
	unreachable := goredis.NewClient(&goredis.Options{
		Addr:        "127.0.0.1:1", // nothing listens here; every call fails fast
		DialTimeout: 200 * time.Millisecond,
	})
	t.Cleanup(func() { unreachable.Close() })

	c := NewStreamConsumer(unreachable, "stream", "group",
		func(context.Context, goredis.XMessage) error { return nil }, discardLog())

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Start(runCtx) }()

	// Let it fail and retry the startup bootstrap a few times.
	time.Sleep(1500 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Start exited on a transient connection error (err=%v); it must retry indefinitely instead", err)
	default:
		// still running — correct.
	}
	assert.NoError(t, c.Healthy(2*time.Second),
		"the retry loop must keep advancing its heartbeat even while every attempt fails")

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err, "Start must return cleanly on context cancellation")
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return promptly after ctx cancellation — the retry loop is not honoring ctx.Done()")
	}
}

// TestStreamConsumer_Start_PermanentBootstrapError_ReturnsSoHealthSurfaces is
// the [P2] regression: a PERMANENT consumer-group bootstrap failure — here a
// wrong-typed stream key, which makes XGroupCreateMkStream fail with WRONGTYPE
// on every attempt — must NOT be retried forever (which would keep the pod
// ready+live consuming nothing while the heartbeat stays green). Instead Start
// must return the error promptly, so the caller's WorkerExited(name, err)
// records it and readiness fails loudly. The handler must never run.
func TestStreamConsumer_Start_PermanentBootstrapError_ReturnsSoHealthSurfaces(t *testing.T) {
	rc := internalRedisClient(t)
	ctx := context.Background()

	stream := fmt.Sprintf("test-stream-permanent-boot-%d", time.Now().UnixNano())
	group := "test-group"
	t.Cleanup(func() { rc.Del(ctx, stream) })

	// Wrong-typed key: XGroupCreateMkStream on it fails deterministically with
	// WRONGTYPE — a misconfiguration no retry can ever clear.
	require.NoError(t, rc.Set(ctx, stream, "not-a-stream", 0).Err())

	var received atomic.Int32
	handler := func(_ context.Context, _ goredis.XMessage) error {
		received.Add(1)
		return nil
	}
	c := NewStreamConsumer(rc, stream, group, handler, discardLog())

	// Generous timeout: if the bug regressed (infinite retry), Start would keep
	// looping until this ctx cancels and we'd see a nil return well after the
	// short window below — the select's timeout branch catches that.
	runCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Start(runCtx) }()

	select {
	case err := <-done:
		require.Error(t, err, "a permanent bootstrap error must make Start return, not loop forever")
		assert.Contains(t, err.Error(), "WRONGTYPE",
			"the returned error must carry the underlying permanent cause")
		assert.Contains(t, err.Error(), "permanent consumer-group bootstrap failure")
		assert.Equal(t, int32(0), received.Load(), "the handler must never run on a permanent bootstrap failure")
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return on a permanent bootstrap error — it looped silently instead of surfacing it")
	}
}
