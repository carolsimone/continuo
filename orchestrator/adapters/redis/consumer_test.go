package redis_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/carolsimone/continuo/orchestrator/adapters/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// redisClientForTest creates a Redis client for integration tests.
// Tests are skipped if REDIS_ADDR is not set.
func redisClientForTest(t *testing.T) *goredis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set — skipping Redis integration test")
	}
	return goredis.NewClient(&goredis.Options{Addr: addr})
}

// TestStreamConsumer_FailedHandlerMessageStaysInPEL verifies the pre-condition for
// the reclaimPending fix: when a handler returns an error the message is not ACKed
// and therefore remains in the Pending Entry List.
func TestStreamConsumer_FailedHandlerMessageStaysInPEL(t *testing.T) {
	rc := redisClientForTest(t)
	ctx := context.Background()
	defer rc.Close()

	stream := fmt.Sprintf("test-stream-%d", time.Now().UnixNano())
	group := "test-group"
	t.Cleanup(func() { rc.Del(ctx, stream) })

	// Publish a message before the consumer starts.
	msgID, err := rc.XAdd(ctx, &goredis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"k": "v"},
	}).Result()
	require.NoError(t, err)

	var called atomic.Int32
	failingHandler := func(_ context.Context, _ goredis.XMessage) error {
		called.Add(1)
		return errors.New("handler failure")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	consumer := redis.NewStreamConsumer(rc, stream, group, failingHandler, logger)

	// Ensure the consumer group exists so we can call readAndProcess-equivalent.
	require.NoError(t, rc.XGroupCreateMkStream(ctx, stream, group, "0").Err())

	// Read + process the message once via the exported Start path. We run it for
	// just long enough to process the single message then cancel.
	runCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	go consumer.Start(runCtx) //nolint:errcheck
	time.Sleep(2 * time.Second) // let the consumer read and fail once

	// The handler was called but returned an error, so the message must still be
	// in the PEL (not ACKed).
	assert.GreaterOrEqual(t, called.Load(), int32(1), "handler must have been called")

	pending, err := rc.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  "-",
		End:    "+",
		Count:  10,
	}).Result()
	require.NoError(t, err)

	found := false
	for _, p := range pending {
		if p.ID == msgID {
			found = true
			break
		}
	}
	assert.True(t, found, "failed message must remain in PEL (not ACKed)")
}

// TestStreamConsumer_ReclaimPendingRecoversStuckMessage verifies that after a
// handler failure leaves a message in the PEL, a new consumer instance (simulating
// a service restart or periodic reclaim) recovers and successfully processes it.
//
// This is the regression test for the root cause of CI TestE2E_FailurePath timeout:
// the scheduler.started:v1 handler failed due to a shared UnitOfWork returning
// "transaction already in progress" and the message was never redelivered.
func TestStreamConsumer_ReclaimPendingRecoversStuckMessage(t *testing.T) {
	rc := redisClientForTest(t)
	ctx := context.Background()
	defer rc.Close()

	stream := fmt.Sprintf("test-stream-reclaim-%d", time.Now().UnixNano())
	group := "test-group"
	t.Cleanup(func() { rc.Del(ctx, stream) })

	// Publish a message.
	_, err := rc.XAdd(ctx, &goredis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"k": "v"},
	}).Result()
	require.NoError(t, err)

	require.NoError(t, rc.XGroupCreateMkStream(ctx, stream, group, "0").Err())

	// First consumer: fails to process, leaves message in PEL.
	var firstCalled atomic.Int32
	failingHandler := func(_ context.Context, _ goredis.XMessage) error {
		firstCalled.Add(1)
		return errors.New("transient error")
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	failing := redis.NewStreamConsumer(rc, stream, group, failingHandler, logger)

	failCtx, failCancel := context.WithTimeout(ctx, 2*time.Second)
	defer failCancel()
	go failing.Start(failCtx) //nolint:errcheck
	time.Sleep(1500 * time.Millisecond)
	require.GreaterOrEqual(t, firstCalled.Load(), int32(1), "handler must have been called")

	// Verify message is stuck in PEL.
	pending, err := rc.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: stream, Group: group, Start: "-", End: "+", Count: 10,
	}).Result()
	require.NoError(t, err)
	require.NotEmpty(t, pending, "message must be stuck in PEL after handler failure")

	// Second consumer: new instance starts with reclaimPending, succeeds.
	var secondCalled atomic.Int32
	succeedingHandler := func(_ context.Context, _ goredis.XMessage) error {
		secondCalled.Add(1)
		return nil
	}
	succeeding := redis.NewStreamConsumer(rc, stream, group, succeedingHandler, logger)

	recoverCtx, recoverCancel := context.WithTimeout(ctx, 3*time.Second)
	defer recoverCancel()
	go succeeding.Start(recoverCtx) //nolint:errcheck
	time.Sleep(2 * time.Second)

	// reclaimPending on Start should have retrieved and ACKed the message.
	assert.GreaterOrEqual(t, secondCalled.Load(), int32(1), "second consumer must have reprocessed the stuck message")

	pending, err = rc.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: stream, Group: group, Start: "-", End: "+", Count: 10,
	}).Result()
	require.NoError(t, err)
	assert.Empty(t, pending, "PEL must be empty after reclaimPending reprocessed the message")
}
