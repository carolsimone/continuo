package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shortStartup is a startup policy that gives up on a missing group within a
// second, so a test of the wait does not take the default five minutes.
var shortStartup = StartupPolicy{Timeout: time.Second, InitialDelay: 100 * time.Millisecond, MaxDelay: 200 * time.Millisecond}

// keysTestStream returns a contract name unique to the test and deletes both of
// its keys when the test ends.
func keysTestStream(t *testing.T, rc *goredis.Client, label string) string {
	t.Helper()
	stream := fmt.Sprintf("test-%s-%d", label, time.Now().UnixNano())
	t.Cleanup(func() { rc.Del(context.Background(), Key(stream), LegacyKey(stream)) })
	return stream
}

// startConsumer runs c.Start in the background and returns the channel its
// result arrives on.
func startConsumer(ctx context.Context, c *StreamConsumer) <-chan error {
	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()
	return done
}

func TestConsumerNeverCreatesItsGroup(t *testing.T) {
	rc := internalRedisClient(t)
	ctx := context.Background()
	stream := keysTestStream(t, rc, "never-creates")
	group := "test-group"

	c := NewStreamConsumer(rc, stream, group, func(context.Context, goredis.XMessage) error { return nil }, discardLog())
	c.SetService("pkg-redis-test")
	c.startup = shortStartup

	select {
	case err := <-startConsumer(ctx, c):
		require.Error(t, err)
		assert.Contains(t, err.Error(),
			fmt.Sprintf(`consumer group %q on %q does not exist; the streams-init step creates it`, group, Key(stream)))
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not give up on a missing group within its startup policy")
	}
	assert.Equal(t, int64(0), rc.Exists(ctx, Key(stream)).Val(), "Start must not create the prefixed key")
	assert.Equal(t, int64(0), rc.Exists(ctx, LegacyKey(stream)).Val(), "Start must not create the legacy key")
}

func TestConsumerWaitsForItsGroup(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := keysTestStream(t, rc, "waits")
	group := "test-group"

	seen := make(chan string, 1)
	c := NewStreamConsumer(rc, stream, group, func(_ context.Context, m goredis.XMessage) error {
		seen <- m.Values["k"].(string)
		return nil
	}, discardLog())
	c.SetService("pkg-redis-test")
	c.startup = StartupPolicy{Timeout: 10 * time.Second, InitialDelay: 50 * time.Millisecond, MaxDelay: 100 * time.Millisecond}
	done := startConsumer(ctx, c)

	time.Sleep(300 * time.Millisecond)
	require.NoError(t, rc.XGroupCreateMkStream(ctx, Key(stream), group, "0").Err())
	require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: Key(stream), Values: map[string]any{"k": "late"}}).Err())

	select {
	case v := <-seen:
		assert.Equal(t, "late", v)
	case err := <-done:
		t.Fatalf("Start returned before the group appeared: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the consumer never read the entry once its group appeared")
	}
	cancel()
	assert.NoError(t, <-done)
}

func TestConsumerReadsTheLegacyKeyWhileItHoldsTheGroup(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := keysTestStream(t, rc, "legacy")
	group := "test-group"
	for _, key := range []string{Key(stream), LegacyKey(stream)} {
		require.NoError(t, rc.XGroupCreateMkStream(ctx, key, group, "0").Err())
		require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: key, Values: map[string]any{"from": key}}).Err())
	}

	var mu sync.Mutex
	var from []string
	c := NewStreamConsumer(rc, stream, group, func(_ context.Context, m goredis.XMessage) error {
		mu.Lock()
		defer mu.Unlock()
		from = append(from, m.Values["from"].(string))
		return nil
	}, discardLog())
	c.SetService("pkg-redis-test")
	c.startup = shortStartup
	done := startConsumer(ctx, c)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(from) == 2
	}, 5*time.Second, 20*time.Millisecond, "the handler sees the entry of each key")
	mu.Lock()
	assert.ElementsMatch(t, []string{Key(stream), LegacyKey(stream)}, from)
	mu.Unlock()
	for _, key := range []string{Key(stream), LegacyKey(stream)} {
		require.Eventually(t, func() bool {
			p, err := rc.XPending(ctx, key, group).Result()
			return err == nil && p.Count == 0
		}, 5*time.Second, 20*time.Millisecond, "each entry is acknowledged on the key it came from: %s", key)
	}
	assert.NoError(t, c.Healthy(time.Minute), "both loops are alive")
	cancel()
	assert.NoError(t, <-done)
}

func TestConsumerIgnoresAMissingLegacyKey(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := keysTestStream(t, rc, "no-legacy")
	group := "test-group"
	require.NoError(t, rc.XGroupCreateMkStream(ctx, Key(stream), group, "0").Err())
	require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: Key(stream), Values: map[string]any{"k": "v"}}).Err())

	seen := make(chan struct{}, 1)
	c := NewStreamConsumer(rc, stream, group, func(context.Context, goredis.XMessage) error {
		seen <- struct{}{}
		return nil
	}, discardLog())
	c.SetService("pkg-redis-test")
	c.startup = shortStartup
	done := startConsumer(ctx, c)

	select {
	case <-seen:
	case err := <-done:
		t.Fatalf("Start returned instead of reading the prefixed key: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the consumer never read the prefixed key")
	}
	assert.Equal(t, int64(0), rc.Exists(ctx, LegacyKey(stream)).Val(), "the legacy key is never created")
	cancel()
	assert.NoError(t, <-done)
}

func TestConsumerIgnoresALegacyKeyWithoutItsGroup(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := keysTestStream(t, rc, "legacy-other-group")
	group := "test-group"
	require.NoError(t, rc.XGroupCreateMkStream(ctx, Key(stream), group, "0").Err())
	require.NoError(t, rc.XGroupCreateMkStream(ctx, LegacyKey(stream), "another-group", "0").Err())
	require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: LegacyKey(stream), Values: map[string]any{"k": "legacy"}}).Err())
	require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: Key(stream), Values: map[string]any{"k": "prefixed"}}).Err())

	seen := make(chan string, 2)
	c := NewStreamConsumer(rc, stream, group, func(_ context.Context, m goredis.XMessage) error {
		seen <- m.Values["k"].(string)
		return nil
	}, discardLog())
	c.SetService("pkg-redis-test")
	c.startup = shortStartup
	done := startConsumer(ctx, c)

	select {
	case v := <-seen:
		assert.Equal(t, "prefixed", v)
	case err := <-done:
		t.Fatalf("Start returned instead of reading the prefixed key: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the consumer never read the prefixed key")
	}
	select {
	case v := <-seen:
		t.Fatalf("the consumer read %q from a legacy key that does not hold its group", v)
	case <-time.After(1500 * time.Millisecond):
	}
	groups, err := rc.XInfoGroups(ctx, LegacyKey(stream)).Result()
	require.NoError(t, err)
	require.Len(t, groups, 1, "no group is created on the legacy key")
	assert.Equal(t, "another-group", groups[0].Name)
	cancel()
	assert.NoError(t, <-done)
}

func TestConsumerStopsWhenItsGroupDisappears(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stream := keysTestStream(t, rc, "group-gone")
	group := "test-group"
	require.NoError(t, rc.XGroupCreateMkStream(ctx, Key(stream), group, "0").Err())

	c := NewStreamConsumer(rc, stream, group, func(context.Context, goredis.XMessage) error { return nil }, discardLog())
	c.SetService("pkg-redis-test")
	c.startup = shortStartup
	done := startConsumer(ctx, c)
	time.Sleep(300 * time.Millisecond)
	require.NoError(t, rc.XGroupDestroy(ctx, Key(stream), group).Err())

	select {
	case err := <-done:
		require.Error(t, err)
		assert.Contains(t, err.Error(), Key(stream))
		assert.Contains(t, err.Error(), group)
		assert.Contains(t, err.Error(), "NOGROUP")
	case <-time.After(10 * time.Second):
		t.Fatal("Start kept running after its group disappeared")
	}
	groups, err := rc.XInfoGroups(ctx, Key(stream)).Result()
	require.NoError(t, err)
	assert.Empty(t, groups, "the consumer does not re-create its group")
}

func TestConsumerDeadLettersToThePrefixedStream(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ensureDeadLetterStream(t, rc)
	stream := keysTestStream(t, rc, "dead-letter")
	group := "test-group"
	require.NoError(t, rc.XGroupCreateMkStream(ctx, Key(stream), group, "0").Err())
	msgID, err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: Key(stream), Values: map[string]any{"k": "v"}}).Result()
	require.NoError(t, err)

	c := NewStreamConsumer(rc, stream, group, func(context.Context, goredis.XMessage) error { return events.ErrPermanent }, discardLog())
	c.SetService("pkg-redis-test")
	c.startup = shortStartup
	done := startConsumer(ctx, c)

	require.Eventually(t, func() bool {
		dl, err := deadLettersIn(ctx, rc, stream)
		return err == nil && len(dl) == 1
	}, 5*time.Second, 20*time.Millisecond, "the dead letter lands on the prefixed dead-letter stream")
	dl := deadLettersFor(t, rc, stream)
	var p events.ConsumerDeadLetter
	require.NoError(t, json.Unmarshal([]byte(dl[0].Values["payload"].(string)), &p))
	assert.Equal(t, stream, p.OriginalStream, "the dead letter names the contract stream, not its key")
	assert.Equal(t, msgID, p.OriginalMessageID)

	legacy, err := rc.XRange(ctx, streams.ConsumerDeadLetterV1, "-", "+").Result()
	require.NoError(t, err)
	for _, e := range legacy {
		var lp events.ConsumerDeadLetter
		payload, _ := e.Values["payload"].(string)
		if json.Unmarshal([]byte(payload), &lp) == nil {
			assert.NotEqual(t, stream, lp.OriginalStream, "no dead letter lands on the unprefixed stream")
		}
	}
	cancel()
	assert.NoError(t, <-done)
}

func TestXaddDeadLetter_NeverCreatesTheDeadLetterStream(t *testing.T) {
	rc := internalRedisClient(t)
	ctx := context.Background()
	dlq := Key(streams.ConsumerDeadLetterV1)
	// The dead-letter stream is shared by the suite: move it aside for the
	// test and put it back afterwards.
	if rc.Exists(ctx, dlq).Val() != 0 {
		aside := fmt.Sprintf("test-dead-letter-aside-%d", time.Now().UnixNano())
		require.NoError(t, rc.Rename(ctx, dlq, aside).Err())
		t.Cleanup(func() { rc.Rename(context.Background(), aside, dlq) })
	}
	c := NewStreamConsumer(rc, "s", "g", nil, discardLog())
	err := c.xaddDeadLetter(ctx, map[string]any{"k": "v"})
	require.Error(t, err, "a dead letter to a missing stream fails rather than creating it")
	assert.Contains(t, err.Error(), dlq)
	assert.Equal(t, int64(0), rc.Exists(ctx, dlq).Val())
}

// peakHandler returns a handler that sleeps briefly and the peak number of
// its invocations that overlapped.
func peakHandler() (MessageHandler, *atomic.Int32) {
	var running, peak atomic.Int32
	return func(context.Context, goredis.XMessage) error {
		n := running.Add(1)
		defer running.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		return nil
	}, &peak
}

// invokeOnBoth runs safeInvoke five times on each loop, the loops in parallel.
func invokeOnBoth(a, b *StreamConsumer) {
	var wg sync.WaitGroup
	for _, l := range []*StreamConsumer{a, b} {
		wg.Go(func() {
			for range 5 {
				_, _ = l.safeInvoke(context.Background(), goredis.XMessage{ID: "1-0"})
			}
		})
	}
	wg.Wait()
}

func TestOnKey_CopiesTheConfigWithItsOwnKey(t *testing.T) {
	c := NewStreamConsumer(nil, "orders:v1", "svc-orders", nil, discardLog(), WithHandlerTimeout(time.Second), WithWorkerPool(3, "id"))
	c.SetService("pkg-redis-test")
	l := c.onKey(LegacyKey("orders:v1"))
	assert.Equal(t, Key("orders:v1"), c.key)
	assert.Equal(t, "orders:v1", l.key)
	assert.Equal(t, time.Second, l.handlerTimeout)
	assert.Equal(t, 3, l.workerCount)
	assert.Equal(t, "pkg-redis-test", l.service)
	assert.Equal(t, c.consumerName, l.consumerName)
	assert.NoError(t, l.Healthy(time.Minute), "a new loop starts with a fresh heartbeat")
}

func TestOnKey_SerializesTheHandlerOfASerialConsumer(t *testing.T) {
	handler, peak := peakHandler()
	c := NewStreamConsumer(nil, "orders:v1", "svc-orders", handler, discardLog())
	c.handlerMu = &sync.Mutex{}
	invokeOnBoth(c, c.onKey(LegacyKey("orders:v1")))
	assert.Equal(t, int32(1), peak.Load(), "the loops of a serial consumer never run its handler at once")
}

func TestOnKey_LeavesAWorkerPoolParallel(t *testing.T) {
	handler, peak := peakHandler()
	c := NewStreamConsumer(nil, "orders:v1", "svc-orders", handler, discardLog(), WithWorkerPool(2, "id"))
	invokeOnBoth(c, c.onKey(LegacyKey("orders:v1")))
	assert.Equal(t, int32(2), peak.Load(), "a worker-pool consumer's handler already runs concurrently")
}

func TestHealthy_ReportsAStalledKeyLoop(t *testing.T) {
	c := NewStreamConsumer(nil, "orders:v1", "svc-orders", nil, discardLog())
	l := c.onKey(LegacyKey("orders:v1"))
	c.keyLoops.Store(&[]*StreamConsumer{l})
	require.NoError(t, c.Healthy(time.Minute))
	l.lastActivity.Store(time.Now().Add(-time.Hour).UnixNano())
	err := c.Healthy(time.Minute)
	require.Error(t, err, "a stalled loop of the legacy key makes the consumer unhealthy")
	assert.Contains(t, err.Error(), `on "orders:v1"`)
}

func TestSafeInvoke_AdvancesTheHeartbeatWhileWaitingForTheHandlerLock(t *testing.T) {
	c := NewStreamConsumer(nil, "orders:v1", "svc-orders",
		func(context.Context, goredis.XMessage) error { return nil }, discardLog(), WithHandlerTimeout(time.Minute))
	c.handlerMu = &sync.Mutex{}
	// The other key's loop holds the lock for a whole handler invocation.
	c.handlerMu.Lock()
	stale := time.Now().Add(-time.Hour).UnixNano()
	c.lastActivity.Store(stale)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = c.safeInvoke(context.Background(), goredis.XMessage{ID: "1-0"})
	}()

	require.Eventually(t, func() bool { return c.lastActivity.Load() != stale }, 2*time.Second, 5*time.Millisecond,
		"a loop waiting for the handler lock advances its heartbeat before it blocks")
	select {
	case <-done:
		t.Fatal("safeInvoke ran the handler while the other loop held the lock")
	default:
	}
	assert.NoError(t, c.Healthy(c.handlerTimeout), "the waiting loop reads healthy within one handler timeout")

	c.handlerMu.Unlock()
	<-done
}
