package redis

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
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
