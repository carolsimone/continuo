package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	c.holdFn = func(_ context.Context, ids []string) { rec.record("hold:" + strings.Join(ids, ",")) }
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

func TestReclaimGate_FollowsTheHandlerTimeout(t *testing.T) {
	c := NewStreamConsumer(nil, "s", "g", func(context.Context, goredis.XMessage) error { return nil }, discardLog())
	assert.Equal(t, DefaultHandlerTimeout+reclaimMargin, c.reclaimGate())
	assert.Equal(t, 90*time.Second, c.reclaimGate())
	c.SetHandlerTimeout(5 * time.Minute)
	assert.Equal(t, 6*time.Minute, c.reclaimGate(), "a timeout set after construction moves the gate")

	c = NewStreamConsumer(nil, "s", "g", func(context.Context, goredis.XMessage) error { return nil }, discardLog(),
		WithHandlerTimeout(time.Minute))
	assert.Equal(t, 2*time.Minute, c.reclaimGate())
}

func TestReclaimGate_ExplicitValueWins(t *testing.T) {
	for _, gate := range []time.Duration{0, 2 * time.Minute} {
		c := NewStreamConsumer(nil, "s", "g", func(context.Context, goredis.XMessage) error { return nil }, discardLog(),
			WithReclaimMinIdle(gate), WithHandlerTimeout(time.Minute))
		c.SetHandlerTimeout(5 * time.Minute)
		assert.Equal(t, gate, c.reclaimGate(), "an explicit gate, 0 included, is kept whatever the handler timeout")
	}
}

func TestHoldWhileInHand_HoldsOnlyTheMessagesNotYetSettled(t *testing.T) {
	rec := &recorder{}
	c, _ := reliabilityConsumer(t, func(_ context.Context, m goredis.XMessage) error {
		switch m.ID {
		case "1-0":
			time.Sleep(100 * time.Millisecond)
		case "2-0":
			return errors.New("transient") // left pending for the reclaim sweep
		case "3-0":
			rec.record("slow-2-started")
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	}, rec)
	c.pauseSlice = 20 * time.Millisecond
	batch := []goredis.XMessage{msg("1-0"), msg("2-0"), msg("3-0"), msg("4-0")}
	c.holdWhileInHand(context.Background(), batch, func() { c.processSerial(context.Background(), batch) })

	rec.mu.Lock()
	calls := append([]string(nil), rec.calls...)
	rec.mu.Unlock()
	assert.Contains(t, calls, "hold:1-0,2-0,3-0,4-0",
		"while the first message runs, the siblings waiting behind it are held with it")
	marker := slices.Index(calls, "slow-2-started")
	require.Positive(t, marker)
	var later []string
	for _, call := range calls[marker:] {
		if strings.HasPrefix(call, "hold:") {
			later = append(later, call)
		}
	}
	require.GreaterOrEqual(t, len(later), 3)
	withLeft := 0
	for _, h := range later {
		if strings.Contains(h, "2-0") {
			withLeft++
		}
	}
	assert.LessOrEqual(t, withLeft, 1,
		"a message left pending for the sweep leaves the set; at most a re-claim already in flight touches it")
	assert.Equal(t, "hold:3-0,4-0", later[len(later)-1])
	assert.Empty(t, c.inHand.snapshot(), "every message leaves the set as it settles")

	time.Sleep(60 * time.Millisecond)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	assert.Len(t, rec.calls, len(calls), "the holder stops once the batch is settled")
}

func TestHoldWhileInHand_StopsWithTheConsumer(t *testing.T) {
	rec := &recorder{}
	ctx, cancel := context.WithCancel(context.Background())
	c, _ := reliabilityConsumer(t, func(hctx context.Context, _ goredis.XMessage) error {
		cancel()
		<-hctx.Done()
		return hctx.Err()
	}, rec)
	c.pauseSlice = 10 * time.Millisecond
	batch := []goredis.XMessage{msg("1-0")}
	c.holdWhileInHand(ctx, batch, func() { c.processSerial(ctx, batch) })
	assert.Empty(t, rec.calls, "a message in hand at shutdown is neither acknowledged nor held further")
	assert.Empty(t, c.inHand.snapshot())
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

	assert.Equal(t, []string{"xadd-failed", "xadd-failed", "xadd", "ack:1-0"}, rec.calls,
		"the ACK waits for a successful dead-letter write; the handler is not re-run")
	assert.Equal(t, []time.Duration{10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond}, rec.sleeps,
		"each failed write pauses with backoff before the next")
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

func TestPause_AdvancesTheHeartbeatEverySliceAndHoldsNothing(t *testing.T) {
	rec := &recorder{}
	c, _ := reliabilityConsumer(t, nil, rec)
	c.pauseSlice = 5 * time.Second
	c.lastActivity.Store(time.Now().Add(-time.Hour).UnixNano())
	require.True(t, c.pause(context.Background(), 12*time.Second))
	assert.Equal(t, []time.Duration{5 * time.Second, 5 * time.Second, 2 * time.Second}, rec.sleeps)
	assert.NoError(t, c.Healthy(time.Minute), "every slice advances the heartbeat")
	assert.Empty(t, rec.calls, "the holder, not the pause, keeps the messages in hand below the gate")
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
	c, logs := reliabilityConsumer(t, func(ctx context.Context, _ goredis.XMessage) error {
		<-ctx.Done()
		// A dial cut short by the deadline surfaces as a network timeout.
		return &net.OpError{Op: "dial", Net: "tcp", Err: ctx.Err()}
	}, rec)
	c.SetHandlerTimeout(20 * time.Millisecond)
	c.processOne(context.Background(), msg("1-0"))
	assert.Equal(t, readPathSchedule[1:], rec.sleeps, "a handler overrun never pauses the consumer, it is retried inline")
	assert.Zero(t, strings.Count(logs.String(), logInfraPause))
	assert.Empty(t, rec.calls, "the read path leaves an overrun for the reclaim sweep: no dead letter, no ACK")
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
	c.settleReclaimed(context.Background(), msg("1-0"))
	assert.Equal(t, []string{"deliveries"}, rec.calls, "the 4th delivery stays pending: no dead letter, no ACK")

	rec = &recorder{deliveries: 5}
	c, _ = reliabilityConsumer(t, failing, rec)
	c.settleReclaimed(context.Background(), msg("1-0"))
	assert.Equal(t, []string{"deliveries", "xadd", "ack:1-0"}, rec.calls, "the 5th delivery is dead-lettered, then acknowledged")
	require.Len(t, rec.deadLetters, 1)
	p := payloadOf(t, rec.deadLetters[0])
	assert.Equal(t, model.DeadLetterKindTransientExhausted, p.FailureKind)
	assert.Equal(t, int64(5), p.DeliveryCount)
}

func TestSettleReclaimed_AcksAHandledMessageAtOnce(t *testing.T) {
	rec := &recorder{}
	c, _ := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error { return nil }, rec)
	c.settleReclaimed(context.Background(), msg("1-0"))
	assert.Equal(t, []string{"ack:1-0"}, rec.calls,
		"a handled reclaimed message is acknowledged at once, never held back for the rest of its page")
}

func TestSettleReclaimed_PermanentDeadLetterCountsAtLeastOneDelivery(t *testing.T) {
	rec := &recorder{deliveries: 0} // the delivery counter could not be read
	c, _ := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error { return events.ErrPermanent }, rec)
	c.settleReclaimed(context.Background(), msg("1-0"))
	require.Len(t, rec.deadLetters, 1)
	assert.Equal(t, int64(1), payloadOf(t, rec.deadLetters[0]).DeliveryCount)
}

// liveAck records "ack:<id>" when the ACK runs with a live context and
// "ack-cancelled:<id>" when its context has already ended.
func liveAck(rec *recorder) func(context.Context, string) error {
	return func(ctx context.Context, id string) error {
		if ctx.Err() != nil {
			rec.record("ack-cancelled:" + id)
			return ctx.Err()
		}
		rec.record("ack:" + id)
		return nil
	}
}

func TestHandledMessage_AckedEvenWhenShutdownBeganDuringTheHandler(t *testing.T) {
	paths := map[string]func(*StreamConsumer, context.Context, goredis.XMessage){
		"read path":    (*StreamConsumer).processOne,
		"reclaim path": func(c *StreamConsumer, ctx context.Context, m goredis.XMessage) { c.settleReclaimed(ctx, m) },
	}
	for name, settle := range paths {
		t.Run(name, func(t *testing.T) {
			rec := &recorder{}
			ctx, cancel := context.WithCancel(context.Background())
			c, _ := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error { cancel(); return nil }, rec)
			c.ackFn = liveAck(rec)
			settle(c, ctx, msg("1-0"))
			assert.Equal(t, []string{"ack:1-0"}, rec.calls, "a handled message is acknowledged although shutdown began")
		})
	}
}

func TestDeadLetterAndAck_AcksWhenShutdownFollowsTheWrite(t *testing.T) {
	rec := &recorder{}
	ctx, cancel := context.WithCancel(context.Background())
	c, logs := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error { return events.ErrPermanent }, rec)
	c.deadLetterFn = func(context.Context, map[string]any) error { rec.record("xadd"); cancel(); return nil }
	c.ackFn = liveAck(rec)
	var dropCtxErr error
	WithOnDrop(func(dctx context.Context, _ goredis.XMessage, _ error) {
		dropCtxErr = dctx.Err()
		rec.record("drop")
	})(c)

	c.processOne(ctx, msg("1-0"))

	assert.Equal(t, []string{"xadd", "ack:1-0", "drop"}, rec.calls,
		"once the dead letter is written, a shutdown no longer stops its ACK, so no second dead letter follows")
	assert.NoError(t, dropCtxErr, "the drop handler runs with a live context")
	assert.Equal(t, 1, strings.Count(logs.String(), logDeadLettered))
}

func TestPauseAndBackoff_NonPositiveSettingsNeverSpin(t *testing.T) {
	c, _ := reliabilityConsumer(t, nil, &recorder{})
	c.pauseSlice, c.infraBackoffBase, c.infraBackoffCap = 0, 0, -time.Second
	assert.Equal(t, defaultInfraBackoffBase, c.infraBackoff(1))
	assert.Equal(t, defaultInfraBackoffCap, c.infraBackoff(10))

	var sleeps []time.Duration
	c.sleepFn = func(_ context.Context, d time.Duration) bool {
		sleeps = append(sleeps, d)
		return len(sleeps) < 100 // ends a pause that never advances
	}
	require.True(t, c.pause(context.Background(), 12*time.Second))
	assert.Equal(t, []time.Duration{5 * time.Second, 5 * time.Second, 2 * time.Second}, sleeps)
}

func TestProcessOne_PermanentErrorAtTheDeadlineIsDeadLetteredAsPermanent(t *testing.T) {
	rec := &recorder{}
	c, _ := reliabilityConsumer(t, func(ctx context.Context, _ goredis.XMessage) error {
		<-ctx.Done()
		return fmt.Errorf("%w: gave up at the deadline: %w", events.ErrPermanent, ctx.Err())
	}, rec)
	c.SetHandlerTimeout(20 * time.Millisecond)
	c.processOne(context.Background(), msg("1-0"))
	assert.Equal(t, []string{"xadd", "ack:1-0"}, rec.calls)
	require.Len(t, rec.deadLetters, 1)
	assert.Equal(t, model.DeadLetterKindPermanent, payloadOf(t, rec.deadLetters[0]).FailureKind)
}

func TestWithInfraClassifier_ServiceOutagePausesTheConsumer(t *testing.T) {
	errGraphDown := errors.New("graph database unavailable")
	rec := &recorder{}
	calls := 0
	c, logs := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error {
		calls++
		if calls == 1 {
			return fmt.Errorf("load topology: %w", errGraphDown)
		}
		return nil
	}, rec)
	WithInfraClassifier(func(err error) bool { return errors.Is(err, errGraphDown) })(c)

	c.processOne(context.Background(), msg("1-0"))

	assert.Equal(t, 2, calls)
	assert.Equal(t, []string{"ack:1-0"}, rec.calls, "paused, never dead-lettered, acknowledged once handled")
	assert.Equal(t, []time.Duration{time.Second}, rec.sleeps)
	assert.Equal(t, 1, strings.Count(logs.String(), logInfraPause))
}

func TestDeadLetter_CarriesTheMessageTenant(t *testing.T) {
	rec := &recorder{}
	c, _ := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error { return events.ErrPermanent }, rec)
	c.processOne(context.Background(), goredis.XMessage{ID: "1-0", Values: map[string]any{"tenant_id": "acme", "n": 7}})
	assert.Equal(t, "acme", rec.deadLetters[0]["tenant_id"])
	assert.Equal(t, map[string]string{"tenant_id": "acme", "n": "7"}, payloadOf(t, rec.deadLetters[0]).Fields)
}

// deadLettersIn returns the consumer.dead_letter:v1 entries for one test
// stream. It never fails the test, so it is safe off the test goroutine.
func deadLettersIn(ctx context.Context, rc *goredis.Client, stream string) ([]goredis.XMessage, error) {
	all, err := rc.XRange(ctx, streams.ConsumerDeadLetterV1, "-", "+").Result()
	if err != nil {
		return nil, err
	}
	var mine []goredis.XMessage
	for _, e := range all {
		var p events.ConsumerDeadLetter
		if json.Unmarshal([]byte(e.Values["payload"].(string)), &p) == nil && p.OriginalStream == stream {
			mine = append(mine, e)
		}
	}
	return mine, nil
}

// deadLettersFor returns the consumer.dead_letter:v1 entries for one test
// stream and deletes them when the test ends.
func deadLettersFor(t *testing.T, rc *goredis.Client, stream string) []goredis.XMessage {
	t.Helper()
	ctx := context.Background()
	mine, err := deadLettersIn(ctx, rc, stream)
	require.NoError(t, err)
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

	peerCalls, peerDone := sweepingPeer(ctx, rc, stream, group)

	require.NoError(t, a.readAndProcess(ctx))
	cancel()
	<-peerDone
	assert.Equal(t, 4, attempts)
	assert.Equal(t, []int64{1, 1, 1, 1}, seenDeliveries, "pauses never add a delivery")
	assert.Zero(t, peerCalls.Load(), "the peer never took the paused message")
	pending, err := rc.XPending(context.Background(), stream, group).Result()
	require.NoError(t, err)
	assert.Zero(t, pending.Count)
	assert.Empty(t, deadLettersFor(t, rc, stream))
}

// sweepingPeer runs a peer replica's reclaim sweep (min idle 300 ms) every
// 150 ms until ctx ends. It returns how many messages the peer handled and a
// channel closed once the sweeping goroutine has returned.
func sweepingPeer(ctx context.Context, rc *goredis.Client, stream, group string) (*atomic.Int32, <-chan struct{}) {
	return sweepingPeerEvery(ctx, rc, stream, group, 300*time.Millisecond, 150*time.Millisecond)
}

// sweepingPeerEvery is sweepingPeer with the peer's reclaim gate and sweep
// period given.
func sweepingPeerEvery(ctx context.Context, rc *goredis.Client, stream, group string, gate, every time.Duration) (*atomic.Int32, <-chan struct{}) {
	var calls atomic.Int32
	b := NewStreamConsumer(rc, stream, group, func(context.Context, goredis.XMessage) error { calls.Add(1); return nil },
		discardLog(), WithReclaimMinIdle(gate))
	b.SetService("pkg-redis-test")
	b.consumerName = "peer"
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			_ = b.reclaimPending(ctx)
			time.Sleep(every)
		}
	}()
	return &calls, done
}

func TestStreamConsumer_InlineRetry_PeerSweepDoesNotTakeMessage(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := fmt.Sprintf("test-inline-retry-hold-%d", time.Now().UnixNano())
	group := "test-group"
	t.Cleanup(func() { rc.Del(context.Background(), stream) })
	require.NoError(t, rc.XGroupCreateMkStream(ctx, stream, group, "0").Err())
	require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"k": "v"}}).Err())

	// Each attempt alone stays inside the peer's 400 ms gate, but the two
	// attempts and the retry delay between them (150+100+300 ms) do not: only
	// the holder, re-claiming the message every 100 ms, keeps the peer's sweep
	// away.
	attempts := 0
	a := NewStreamConsumer(rc, stream, group, func(context.Context, goredis.XMessage) error {
		attempts++
		if attempts == 1 {
			time.Sleep(150 * time.Millisecond)
			return errors.New("transient")
		}
		time.Sleep(300 * time.Millisecond)
		return nil
	}, discardLog())
	a.SetService("pkg-redis-test")
	a.pauseSlice = 100 * time.Millisecond

	peerCalls, peerDone := sweepingPeerEvery(ctx, rc, stream, group, 400*time.Millisecond, 25*time.Millisecond)

	require.NoError(t, a.readAndProcess(ctx))
	cancel()
	<-peerDone
	assert.Equal(t, 2, attempts)
	assert.Zero(t, peerCalls.Load(), "the peer never took the message during its inline retry")
	pending, err := rc.XPending(context.Background(), stream, group).Result()
	require.NoError(t, err)
	assert.Zero(t, pending.Count)
	assert.Empty(t, deadLettersFor(t, rc, stream))
}

func TestStreamConsumer_InlineRetry_LeftPendingSiblingStillReachesTheSweep(t *testing.T) {
	rc := internalRedisClient(t)
	ctx := context.Background()
	stream := fmt.Sprintf("test-inline-retry-sibling-%d", time.Now().UnixNano())
	group := "test-group"
	const gate = 600 * time.Millisecond
	t.Cleanup(func() { rc.Del(ctx, stream) })
	require.NoError(t, rc.XGroupCreateMkStream(ctx, stream, group, "0").Err())

	// "left" fails both read-path attempts and succeeds once reclaimed; every
	// "retried" message fails its first attempt and succeeds on the inline retry.
	attempts := map[string]int{}
	c := NewStreamConsumer(rc, stream, group, func(_ context.Context, m goredis.XMessage) error {
		attempts[m.ID]++
		if m.Values["k"] == "left" && attempts[m.ID] <= 2 {
			return errors.New("transient")
		}
		if m.Values["k"] == "retried" && attempts[m.ID] == 1 {
			return errors.New("transient")
		}
		return nil
	}, discardLog(), WithReclaimMinIdle(gate))
	c.SetService("pkg-redis-test")
	c.pauseSlice = 20 * time.Millisecond // the holder runs several times during each retried message

	left, err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"k": "left"}}).Result()
	require.NoError(t, err)
	require.NoError(t, c.readAndProcess(ctx))
	require.Equal(t, 2, attempts[left], "left pending for the reclaim sweep after its inline retry")
	leftSettled := time.Now()

	// Other messages keep retrying inline, more often than the gate, for
	// longer than the gate.
	retries := 0
	for time.Since(leftSettled) < gate+300*time.Millisecond {
		require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"k": "retried"}}).Err())
		require.NoError(t, c.readAndProcess(ctx))
		retries++
	}
	require.Greater(t, retries, 3)

	p, err := rc.XPendingExt(ctx, &goredis.XPendingExtArgs{Stream: stream, Group: group, Start: left, End: left, Count: 1}).Result()
	require.NoError(t, err)
	require.Len(t, p, 1)
	assert.GreaterOrEqual(t, p[0].Idle, time.Since(leftSettled)-100*time.Millisecond,
		"the left-pending message keeps aging while other messages retry inline")

	require.NoError(t, c.reclaimPending(ctx))
	assert.Equal(t, 3, attempts[left], "the reclaim sweep claims the left-pending message once it has been idle for the gate")
	pending, err := rc.XPending(ctx, stream, group).Result()
	require.NoError(t, err)
	assert.Zero(t, pending.Count)
	assert.Empty(t, deadLettersFor(t, rc, stream))
}

func TestStreamConsumer_InfraPause_LeftPendingSiblingStillReachesTheSweep(t *testing.T) {
	rc := internalRedisClient(t)
	ctx := context.Background()
	stream := fmt.Sprintf("test-infra-pause-left-pending-%d", time.Now().UnixNano())
	group := "test-group"
	const gate = 600 * time.Millisecond
	t.Cleanup(func() { rc.Del(ctx, stream) })
	require.NoError(t, rc.XGroupCreateMkStream(ctx, stream, group, "0").Err())

	// "left" fails both read-path attempts and succeeds once reclaimed; every
	// "paused" message hits an outage once, pauses, then succeeds.
	attempts := map[string]int{}
	c := NewStreamConsumer(rc, stream, group, func(_ context.Context, m goredis.XMessage) error {
		attempts[m.ID]++
		if m.Values["k"] == "left" && attempts[m.ID] <= 2 {
			return errors.New("transient")
		}
		if m.Values["k"] == "paused" && attempts[m.ID] == 1 {
			return infraRefused()
		}
		return nil
	}, discardLog(), WithReclaimMinIdle(gate))
	c.SetService("pkg-redis-test")
	c.infraBackoffBase, c.pauseSlice = 100*time.Millisecond, 20*time.Millisecond

	left, err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"k": "left"}}).Result()
	require.NoError(t, err)
	require.NoError(t, c.readAndProcess(ctx))
	require.Equal(t, 2, attempts[left], "left pending for the reclaim sweep after its inline retry")
	leftSettled := time.Now()

	// Other messages pause on an outage, more often than the gate, for longer
	// than the gate.
	pauses := 0
	for time.Since(leftSettled) < gate+300*time.Millisecond {
		require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"k": "paused"}}).Err())
		require.NoError(t, c.readAndProcess(ctx))
		pauses++
	}
	require.Greater(t, pauses, 3)

	p, err := rc.XPendingExt(ctx, &goredis.XPendingExtArgs{Stream: stream, Group: group, Start: left, End: left, Count: 1}).Result()
	require.NoError(t, err)
	require.Len(t, p, 1)
	assert.GreaterOrEqual(t, p[0].Idle, time.Since(leftSettled)-100*time.Millisecond,
		"the left-pending message keeps aging while other messages pause")

	require.NoError(t, c.reclaimPending(ctx))
	assert.Equal(t, 3, attempts[left], "the reclaim sweep claims the left-pending message once it has been idle for the gate")
	pending, err := rc.XPending(ctx, stream, group).Result()
	require.NoError(t, err)
	assert.Zero(t, pending.Count)
	assert.Empty(t, deadLettersFor(t, rc, stream))
}

func TestStreamConsumer_SlowHandlers_PeerSweepDoesNotTakeBatchSibling(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := fmt.Sprintf("test-slow-handlers-sibling-%d", time.Now().UnixNano())
	group := "test-group"
	t.Cleanup(func() { rc.Del(context.Background(), stream) })
	require.NoError(t, rc.XGroupCreateMkStream(ctx, stream, group, "0").Err())
	for _, k := range []string{"slow-1", "slow-2", "sibling"} {
		require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"k": k}}).Err())
	}

	// All three arrive in one read batch. The two slow handlers succeed, each
	// within its deadline, but together they keep the sibling waiting 800 ms,
	// twice the peer's 400 ms gate, before its first attempt.
	var a *StreamConsumer
	var siblingDeliveries []int64
	a = NewStreamConsumer(rc, stream, group, func(hctx context.Context, m goredis.XMessage) error {
		if m.Values["k"] == "sibling" {
			siblingDeliveries = append(siblingDeliveries, a.deliveryCount(hctx, m.ID))
			return nil
		}
		time.Sleep(400 * time.Millisecond)
		return nil
	}, discardLog())
	a.SetService("pkg-redis-test")
	a.pauseSlice = 100 * time.Millisecond

	peerCalls, peerDone := sweepingPeerEvery(ctx, rc, stream, group, 400*time.Millisecond, 25*time.Millisecond)

	require.NoError(t, a.readAndProcess(ctx))
	cancel()
	<-peerDone
	assert.Equal(t, []int64{1}, siblingDeliveries, "the sibling is still on its first delivery when its turn comes")
	assert.Zero(t, peerCalls.Load(), "the peer never took a message this consumer had in hand")
	pending, err := rc.XPending(context.Background(), stream, group).Result()
	require.NoError(t, err)
	assert.Zero(t, pending.Count)
}

func TestHold_RestartsIdleOfPendingAndSkipsAcknowledged(t *testing.T) {
	rc := internalRedisClient(t)
	ctx := context.Background()
	stream := fmt.Sprintf("test-hold-acked-%d", time.Now().UnixNano())
	group := "test-group"
	t.Cleanup(func() { rc.Del(ctx, stream) })
	require.NoError(t, rc.XGroupCreateMkStream(ctx, stream, group, "0").Err())
	acked, err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"k": "acked"}}).Result()
	require.NoError(t, err)
	held, err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"k": "held"}}).Result()
	require.NoError(t, err)

	c := NewStreamConsumer(rc, stream, group, nil, discardLog())
	c.SetService("pkg-redis-test")
	_, err = rc.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: group, Consumer: c.consumerName, Streams: []string{stream, ">"}, Count: 10,
	}).Result()
	require.NoError(t, err)
	require.NoError(t, rc.XAck(ctx, stream, group, acked).Err())
	time.Sleep(200 * time.Millisecond)

	// A re-claim that read the set before the first message settled still
	// names it; XCLAIM skips an acknowledged id.
	c.hold(ctx, []string{acked, held})

	p, err := rc.XPendingExt(ctx, &goredis.XPendingExtArgs{Stream: stream, Group: group, Start: "-", End: "+", Count: 10}).Result()
	require.NoError(t, err)
	require.Len(t, p, 1, "the acknowledged message is not pending again")
	assert.Equal(t, held, p[0].ID)
	assert.Less(t, p[0].Idle, 100*time.Millisecond, "the pending message's idle time restarted")
	assert.Equal(t, int64(1), p[0].RetryCount, "a hold counts no delivery")
}

// infraRefused is a connection-level failure: Classify reads it as an outage.
func infraRefused() error { return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED} }

func TestStreamConsumer_InfraPause_PeerSweepDoesNotTakeBatchSibling(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := fmt.Sprintf("test-infra-pause-sibling-%d", time.Now().UnixNano())
	group := "test-group"
	t.Cleanup(func() { rc.Del(context.Background(), stream) })
	require.NoError(t, rc.XGroupCreateMkStream(ctx, stream, group, "0").Err())
	first, err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"k": "first"}}).Result()
	require.NoError(t, err)
	require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"k": "second"}}).Err())

	// Both messages arrive in one read batch. The first pauses on an outage
	// for 400+800+1600 ms, far past the peer's 300 ms reclaim gate, while the
	// second waits its turn in the same batch.
	var a *StreamConsumer
	firstAttempts := 0
	var secondDeliveries []int64
	a = NewStreamConsumer(rc, stream, group, func(hctx context.Context, m goredis.XMessage) error {
		if m.ID == first {
			firstAttempts++
			if firstAttempts <= 3 {
				return infraRefused()
			}
			return nil
		}
		secondDeliveries = append(secondDeliveries, a.deliveryCount(hctx, m.ID))
		return nil
	}, discardLog())
	a.SetService("pkg-redis-test")
	a.infraBackoffBase, a.pauseSlice = 400*time.Millisecond, 100*time.Millisecond

	peerCalls, peerDone := sweepingPeer(ctx, rc, stream, group)

	require.NoError(t, a.readAndProcess(ctx))
	cancel()
	<-peerDone
	assert.Equal(t, 4, firstAttempts)
	assert.Equal(t, []int64{1}, secondDeliveries, "the sibling is still on its first delivery when its turn comes")
	assert.Zero(t, peerCalls.Load(), "the peer never took the sibling while the first message paused")
	pending, err := rc.XPending(context.Background(), stream, group).Result()
	require.NoError(t, err)
	assert.Zero(t, pending.Count)
	assert.Empty(t, deadLettersFor(t, rc, stream))
}

func TestStreamConsumer_ReclaimPath_HandledMessageAckedWhileALaterOnePauses(t *testing.T) {
	rc := internalRedisClient(t)
	ctx := context.Background()
	stream := fmt.Sprintf("test-reclaim-ack-at-once-%d", time.Now().UnixNano())
	group := "test-group"
	t.Cleanup(func() { rc.Del(ctx, stream) })
	require.NoError(t, rc.XGroupCreateMkStream(ctx, stream, group, "0").Err())
	handled, err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"k": "handled"}}).Result()
	require.NoError(t, err)
	require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"k": "paused"}}).Err())
	// Both messages are left pending by a consumer that is gone.
	_, err = rc.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: group, Consumer: "abandoned-consumer", Streams: []string{stream, ">"}, Count: 10,
	}).Result()
	require.NoError(t, err)

	// One reclaim page holds both: the first is handled, the second pauses
	// twice on an outage. Each attempt on the second records whether the
	// first is still pending.
	var handledStillPending []bool
	attempts := 0
	c := NewStreamConsumer(rc, stream, group, func(hctx context.Context, m goredis.XMessage) error {
		if m.ID == handled {
			return nil
		}
		attempts++
		p, perr := rc.XPendingExt(hctx, &goredis.XPendingExtArgs{
			Stream: stream, Group: group, Start: handled, End: handled, Count: 1,
		}).Result()
		require.NoError(t, perr)
		handledStillPending = append(handledStillPending, len(p) == 1)
		if attempts <= 2 {
			return infraRefused()
		}
		return nil
	}, discardLog(), WithReclaimMinIdle(0))
	c.SetService("pkg-redis-test")
	c.infraBackoffBase, c.pauseSlice = 50*time.Millisecond, 50*time.Millisecond

	require.NoError(t, c.reclaimPending(ctx))
	assert.Equal(t, []bool{false, false, false}, handledStillPending,
		"the handled message left the PEL before the outage on its page-mate ended")
	pending, err := rc.XPending(ctx, stream, group).Result()
	require.NoError(t, err)
	assert.Zero(t, pending.Count)
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
