package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recorder captures what a consumer did with Redis through its seams, in order.
type recorder struct {
	mu          sync.Mutex
	calls       []string
	deadLetters []map[string]any
	sleeps      []time.Duration
	failXadd    int
	deliveries  int64
}

func (r *recorder) record(s string) { r.mu.Lock(); r.calls = append(r.calls, s); r.mu.Unlock() }

func reliabilityConsumer(t *testing.T, handler MessageHandler, rec *recorder) (*StreamConsumer, *bytes.Buffer) {
	t.Helper()
	var logs bytes.Buffer
	c := NewStreamConsumer(nil, "orders:v1", "svc-orders", handler,
		slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	c.SetService("state")
	c.ackFn = func(_ context.Context, id string) error { rec.record("ack:" + id); return nil }
	c.deadLetterFn = func(_ context.Context, v map[string]any) error {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		if rec.failXadd > 0 {
			rec.failXadd--
			rec.calls = append(rec.calls, "xadd-failed")
			return &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}
		}
		rec.calls = append(rec.calls, "xadd")
		rec.deadLetters = append(rec.deadLetters, v)
		return nil
	}
	c.holdFn = func(_ context.Context, id string) { rec.record("hold:" + id) }
	c.deliveriesFn = func(context.Context, string) int64 { rec.record("deliveries"); return rec.deliveries }
	c.sleepFn = func(ctx context.Context, d time.Duration) bool {
		rec.mu.Lock()
		rec.sleeps = append(rec.sleeps, d)
		rec.mu.Unlock()
		return ctx.Err() == nil
	}
	c.nowFn = func() time.Time { return time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC) }
	return c, &logs
}

func payloadOf(t *testing.T, v map[string]any) events.ConsumerDeadLetter {
	t.Helper()
	var p events.ConsumerDeadLetter
	require.NoError(t, json.Unmarshal([]byte(v["payload"].(string)), &p))
	return p
}

func msg(id string) goredis.XMessage {
	return goredis.XMessage{ID: id, Values: map[string]any{"task_id": "t-1"}}
}

func TestNewStreamConsumer_DefaultsAndBudget(t *testing.T) {
	c := NewStreamConsumer(nil, "s", "g", func(context.Context, goredis.XMessage) error { return nil }, discardLog())
	assert.Equal(t, DefaultHandlerTimeout, c.handlerTimeout)
	assert.Equal(t, DefaultHandlerTimeout+2*time.Minute, c.HeartbeatBudget())
	WithHandlerTimeout(0)(c)
	assert.Equal(t, DefaultHandlerTimeout, c.handlerTimeout, "a non-positive timeout keeps the current one")
	c.SetHandlerTimeout(5 * time.Minute)
	assert.Equal(t, 7*time.Minute, c.HeartbeatBudget())
}

func TestStart_RefusesWithoutService(t *testing.T) {
	c := NewStreamConsumer(nil, "s", "g", func(context.Context, goredis.XMessage) error { return nil }, discardLog())
	err := c.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SetService")
}

func TestProcessOne_PermanentErrorDeadLettersThenAcks(t *testing.T) {
	rec := &recorder{}
	c, logs := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error {
		return fmt.Errorf("%w: bad field", events.ErrPermanent)
	}, rec)
	c.processOne(context.Background(), msg("1-0"))

	assert.Equal(t, []string{"xadd", "ack:1-0"}, rec.calls)
	p := payloadOf(t, rec.deadLetters[0])
	assert.Equal(t, model.DeadLetterKindPermanent, p.FailureKind)
	assert.Equal(t, int64(1), p.DeliveryCount)
	assert.Equal(t, "orders:v1", p.OriginalStream)
	assert.Equal(t, "svc-orders", p.OriginalGroup)
	assert.Equal(t, map[string]string{"task_id": "t-1"}, p.Fields)
	assert.Equal(t, "state", rec.deadLetters[0]["producer"])
	assert.Equal(t, events.DefaultTenantID, rec.deadLetters[0]["tenant_id"])
	assert.Equal(t, 1, strings.Count(logs.String(), logDeadLettered))
}

func TestProcessOne_DeadLetterWriteFailureKeepsMessagePending(t *testing.T) {
	rec := &recorder{failXadd: 2}
	c, logs := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error { return events.ErrPermanent }, rec)
	c.infraBackoffBase, c.pauseSlice = 10*time.Millisecond, 10*time.Millisecond
	c.processOne(context.Background(), msg("1-0"))

	assert.Equal(t, []string{"xadd-failed", "hold:1-0", "xadd-failed", "hold:1-0", "hold:1-0", "xadd", "ack:1-0"}, rec.calls,
		"the ACK waits for a successful dead-letter write; the handler is not re-run")
	assert.Equal(t, 1, strings.Count(logs.String(), logDeadLettered), "logged once, after the write succeeded")

	ctx, cancel := context.WithCancel(context.Background())
	rec2 := &recorder{failXadd: 1}
	c2, logs2 := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error { return events.ErrPermanent }, rec2)
	c2.sleepFn = func(context.Context, time.Duration) bool { cancel(); return false }
	c2.processOne(ctx, msg("2-0"))
	assert.NotContains(t, rec2.calls, "ack:2-0", "a shutdown during a failing write leaves the message pending")
	assert.Zero(t, strings.Count(logs2.String(), logDeadLettered))
}

func TestProcessOne_InfrastructureErrorPausesWithoutCounting(t *testing.T) {
	rec := &recorder{}
	calls := 0
	c, logs := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error {
		calls++
		if calls <= 3 {
			return fmt.Errorf("begin uow: %w", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED})
		}
		return nil
	}, rec)
	c.processOne(context.Background(), msg("1-0"))

	assert.Equal(t, 4, calls)
	assert.Equal(t, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}, rec.sleeps)
	assert.NotContains(t, rec.calls, "deliveries", "an outage never reads or spends the delivery count")
	assert.NotContains(t, rec.calls, "xadd")
	assert.Equal(t, "ack:1-0", rec.calls[len(rec.calls)-1])
	assert.Equal(t, 3, strings.Count(logs.String(), logInfraPause))
}

func TestPause_RefreshesHeartbeatAndIdleEverySlice(t *testing.T) {
	rec := &recorder{}
	c, _ := reliabilityConsumer(t, nil, rec)
	c.pauseSlice = 5 * time.Second
	require.True(t, c.pause(context.Background(), "9-0", 12*time.Second))
	assert.Equal(t, []time.Duration{5 * time.Second, 5 * time.Second, 2 * time.Second}, rec.sleeps)
	assert.Equal(t, []string{"hold:9-0", "hold:9-0", "hold:9-0"}, rec.calls)
}

func TestInfraBackoff_DoublesToTheCap(t *testing.T) {
	c := NewStreamConsumer(nil, "s", "g", nil, discardLog())
	var got []time.Duration
	for n := 1; n <= 8; n++ {
		got = append(got, c.infraBackoff(n))
	}
	assert.Equal(t, []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 32 * time.Second, 60 * time.Second, 60 * time.Second}, got)
}

func TestProcessOne_HandlerDeadlineCountsAsTransient(t *testing.T) {
	rec := &recorder{}
	c, _ := reliabilityConsumer(t, func(ctx context.Context, _ goredis.XMessage) error {
		<-ctx.Done()
		// A dial cut short by the deadline surfaces as a network timeout.
		return &net.OpError{Op: "dial", Net: "tcp", Err: ctx.Err()}
	}, rec)
	c.SetHandlerTimeout(20 * time.Millisecond)
	c.processOne(context.Background(), msg("1-0"))
	assert.NotContains(t, rec.calls, "hold:1-0", "a handler overrun never pauses the consumer")
	assert.NotContains(t, rec.calls, "xadd", "the read path leaves an overrun for the reclaim sweep")
	assert.NotContains(t, rec.calls, "ack:1-0")
}

func TestProcessOne_ShutdownLeavesPermanentFailurePending(t *testing.T) {
	rec := &recorder{}
	ctx, cancel := context.WithCancel(context.Background())
	c, _ := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error {
		cancel()
		return events.ErrPermanent
	}, rec)
	c.processOne(ctx, msg("1-0"))
	assert.Empty(t, rec.calls, "a message being handled when the service stops is neither dead-lettered nor acknowledged")
}

func TestSettleReclaimed_DeadLettersOnTheFifthDelivery(t *testing.T) {
	failing := func(context.Context, goredis.XMessage) error { return errors.New("still failing") }

	rec := &recorder{deliveries: 4}
	c, _ := reliabilityConsumer(t, failing, rec)
	assert.False(t, c.settleReclaimed(context.Background(), msg("1-0")))
	assert.NotContains(t, rec.calls, "xadd", "the 4th delivery stays pending")

	rec = &recorder{deliveries: 5}
	c, _ = reliabilityConsumer(t, failing, rec)
	assert.False(t, c.settleReclaimed(context.Background(), msg("1-0")), "dead-lettered messages are acked by deadLetterAndAck, not the batch")
	require.Len(t, rec.deadLetters, 1)
	p := payloadOf(t, rec.deadLetters[0])
	assert.Equal(t, model.DeadLetterKindTransientExhausted, p.FailureKind)
	assert.Equal(t, int64(5), p.DeliveryCount)

	rec = &recorder{}
	c, _ = reliabilityConsumer(t, func(context.Context, goredis.XMessage) error { return nil }, rec)
	assert.True(t, c.settleReclaimed(context.Background(), msg("1-0")), "a handled message joins the sweep's batch ACK")
}

func TestDeadLetter_CarriesTheMessageTenant(t *testing.T) {
	rec := &recorder{}
	c, _ := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error { return events.ErrPermanent }, rec)
	c.processOne(context.Background(), goredis.XMessage{ID: "1-0", Values: map[string]any{"tenant_id": "acme", "n": 7}})
	assert.Equal(t, "acme", rec.deadLetters[0]["tenant_id"])
	assert.Equal(t, map[string]string{"tenant_id": "acme", "n": "7"}, payloadOf(t, rec.deadLetters[0]).Fields)
}

// deadLettersFor returns the consumer.dead_letter:v1 entries for one test
// stream and deletes them when the test ends.
func deadLettersFor(t *testing.T, rc *goredis.Client, stream string) []goredis.XMessage {
	t.Helper()
	ctx := context.Background()
	all, err := rc.XRange(ctx, streams.ConsumerDeadLetterV1, "-", "+").Result()
	require.NoError(t, err)
	var mine []goredis.XMessage
	for _, e := range all {
		var p events.ConsumerDeadLetter
		if json.Unmarshal([]byte(e.Values["payload"].(string)), &p) == nil && p.OriginalStream == stream {
			mine = append(mine, e)
		}
	}
	t.Cleanup(func() {
		for _, e := range mine {
			rc.XDel(ctx, streams.ConsumerDeadLetterV1, e.ID)
		}
	})
	return mine
}

// DeadLettersFor exposes deadLettersFor to the package's external tests.
var DeadLettersFor = deadLettersFor

func TestStreamConsumer_InfraPause_PeerSweepDoesNotTakeMessage(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := fmt.Sprintf("test-infra-pause-%d", time.Now().UnixNano())
	group := "test-group"
	t.Cleanup(func() { rc.Del(context.Background(), stream) })
	require.NoError(t, rc.XGroupCreateMkStream(ctx, stream, group, "0").Err())
	require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"k": "v"}}).Err())

	var seenDeliveries []int64
	var a *StreamConsumer
	attempts := 0
	a = NewStreamConsumer(rc, stream, group, func(hctx context.Context, m goredis.XMessage) error {
		attempts++
		seenDeliveries = append(seenDeliveries, a.deliveryCount(hctx, m.ID))
		if attempts <= 3 {
			return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
		}
		return nil
	}, discardLog())
	a.SetService("pkg-redis-test")
	a.infraBackoffBase, a.pauseSlice = 400*time.Millisecond, 100*time.Millisecond

	peerCalls := 0
	b := NewStreamConsumer(rc, stream, group, func(context.Context, goredis.XMessage) error { peerCalls++; return nil },
		discardLog(), WithReclaimMinIdle(300*time.Millisecond))
	b.SetService("pkg-redis-test")
	b.consumerName = "peer"
	go func() {
		for ctx.Err() == nil {
			_ = b.reclaimPending(ctx)
			time.Sleep(150 * time.Millisecond)
		}
	}()

	require.NoError(t, a.readAndProcess(ctx))
	assert.Equal(t, 4, attempts)
	assert.Equal(t, []int64{1, 1, 1, 1}, seenDeliveries, "pauses never add a delivery")
	assert.Zero(t, peerCalls, "the peer never took the paused message")
	pending, err := rc.XPending(ctx, stream, group).Result()
	require.NoError(t, err)
	assert.Zero(t, pending.Count)
	assert.Empty(t, deadLettersFor(t, rc, stream))
}

func TestStreamConsumer_PermanentError_WritesDeadLetterBeforeAck(t *testing.T) {
	rc := internalRedisClient(t)
	ctx := context.Background()
	stream := fmt.Sprintf("test-dead-letter-%d", time.Now().UnixNano())
	group := "test-group"
	t.Cleanup(func() { rc.Del(ctx, stream) })
	require.NoError(t, rc.XGroupCreateMkStream(ctx, stream, group, "0").Err())
	id, err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"payload": "{"}}).Result()
	require.NoError(t, err)

	c := NewStreamConsumer(rc, stream, group, func(context.Context, goredis.XMessage) error { return events.ErrPermanent }, discardLog())
	c.SetService("pkg-redis-test")
	require.NoError(t, c.readAndProcess(ctx))

	dl := deadLettersFor(t, rc, stream)
	require.Len(t, dl, 1)
	assert.Equal(t, "pkg-redis-test", dl[0].Values["producer"])
	var p events.ConsumerDeadLetter
	require.NoError(t, json.Unmarshal([]byte(dl[0].Values["payload"].(string)), &p))
	assert.Equal(t, id, p.OriginalMessageID)
	assert.Equal(t, model.DeadLetterKindPermanent, p.FailureKind)
	pending, err := rc.XPending(ctx, stream, group).Result()
	require.NoError(t, err)
	assert.Zero(t, pending.Count)
}
