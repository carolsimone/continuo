package redis

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

// MessageHandler is a callback invoked for each message read from a stream
type MessageHandler func(ctx context.Context, msg goredis.XMessage) error

// DropHandler is an optional callback invoked when the consumer abandons a
// message it could not process — a permanent handler error, or a transient one
// that persisted to its last delivery. It fires once a dead-lettered message is
// acknowledged, carrying the message and the cause, so the owning service can
// finalize any in-flight state it committed for that message (which the
// consumer leaves dangling, since dead-lettering leaves the service's own store
// untouched). The ctx it receives is detached from the service's shutdown
// cancellation and bounded by the consumer's handler timeout, so a shutdown
// that begins after the dead letter was written does not cancel the
// notification, and a slow handler cannot delay shutdown beyond that bound. It is best-effort housekeeping,
// never on the message-processing critical path: it is invoked with panic
// recovery and its outcome does not affect the ACK.
type DropHandler func(ctx context.Context, msg goredis.XMessage, cause error)

// StreamConsumer is a generic Redis Streams consumer that delegates message
// processing to a MessageHandler callback. It reads the stream's key,
// Key(streamName), and, while streams.LegacyKeys is true and the unprefixed
// key holds its group, LegacyKey(streamName) too, with one read loop and one
// reclaim loop per key. It never creates a stream or a group: the streams-init
// step does.
type StreamConsumer struct {
	consumerConfig

	// key is the Redis key this loop reads, acknowledges, claims and inspects.
	// NewStreamConsumer sets it to Key(streamName); Start gives each further
	// key it reads a loop of its own (see onKey).
	key string

	// keyLoops holds the loops Start runs for keys other than key, so Healthy
	// can check their heartbeats too. It is nil until Start finds such a key.
	keyLoops atomic.Pointer[[]*StreamConsumer]

	// handlerMu, when set, serializes handler invocations across the loops of
	// a consumer that processes strictly serially (workerCount 1), so reading
	// a second key never runs its handler concurrently with itself. Start sets
	// it when it runs more than one loop; it is nil otherwise.
	handlerMu *sync.Mutex

	// ackFn acknowledges a single resolved message, returning an error when the
	// XACK itself failed (the message then stays in the PEL). It defaults to
	// ackOne (a real XACK) and is a seam so the lane-scheduling logic can be
	// unit-tested without a live Redis connection.
	ackFn func(ctx context.Context, id string) error

	// lastActivity is the unix-nano timestamp of the most recent unit of read-
	// loop progress, stored via atomic.Int64 so Healthy can be polled from the
	// HTTP health handler's goroutine without racing the loop. It advances once
	// per loop iteration, once per handler attempt (see safeInvoke) and once per
	// slice of an infrastructure pause (see pause), so a batch of messages, a
	// single legitimately-slow handler or a long outage keeps the heartbeat
	// fresh rather than freezing it for the whole readAndProcess call.
	// It advances regardless of outcome — including a failed read during a Redis
	// outage — because the point is distinguishing "the loop is alive and
	// retrying" from "the loop is wedged or has exited," not "the last read
	// succeeded." A Redis outage alone must never read as unhealthy here: the
	// read loop already retries indefinitely, so flagging that as unhealthy
	// would just add readiness-flap noise on top of a condition the consumer is
	// already handling correctly.
	lastActivity atomic.Int64

	// inHand holds the ids of the messages this loop has read or claimed and
	// not yet settled.
	inHand inHandSet
	// Seams over Redis and the clock; NewStreamConsumer wires the real ones.
	deadLetterFn func(ctx context.Context, values map[string]any) error
	holdFn       func(ctx context.Context, ids []string)
	holdTicker   func(d time.Duration) (ticks <-chan time.Time, stop func())
	deliveriesFn func(ctx context.Context, id string) int64
	sleepFn      func(ctx context.Context, d time.Duration) bool
	nowFn        func() time.Time
}

// consumerConfig is what every loop of one consumer shares: the stream, the
// group, the handler and the options. onKey copies it whole into the loop of
// each further key.
type consumerConfig struct {
	client        *goredis.Client
	streamName    string
	consumerGroup string
	consumerName  string
	handler       MessageHandler
	logger        *slog.Logger
	observer      Observer

	// reclaimMinIdle is the reclaim gate set by WithReclaimMinIdle, used in
	// place of the derived one only when reclaimMinIdleSet is true (see
	// reclaimGate).
	reclaimMinIdle    time.Duration
	reclaimMinIdleSet bool

	// workerCount is the number of parallel processing lanes. The default is 1,
	// which processes each stream strictly serially.
	// When >1, messages are sharded across workerCount lanes by a hash of their
	// aggregate key so messages for one aggregate stay strictly ordered while
	// distinct aggregates process in parallel.
	workerCount int
	// aggregateKeyField is the message Values field whose value identifies the
	// aggregate (e.g. "schedule_id"). Messages with the same value land on the
	// same worker lane. An empty/absent value hashes to a stable lane so order
	// is never violated, only parallelism is forgone for that message.
	aggregateKeyField string

	// onDrop, when set, is invoked once a dead-lettered message is
	// acknowledged (see DropHandler). It is a seam for the owning service to
	// close out in-flight state the abandoned message left behind. When nil, an
	// abandoned message is dead-lettered and acknowledged with no further
	// notification.
	onDrop DropHandler

	// noDeadLetters, set by WithoutDeadLetters, leaves a message the consumer
	// would dead-letter pending instead, logged, for the next sweep.
	noDeadLetters bool

	// handlerTimeout bounds each handler invocation with a context deadline.
	// It is DefaultHandlerTimeout unless WithHandlerTimeout/SetHandlerTimeout
	// set another positive value before Start, and is read-only once the loop
	// runs. HeartbeatBudget derives the liveness bound from it, and reclaimGate
	// the reclaim gate unless one was set explicitly.
	handlerTimeout time.Duration

	// service names the consuming service; it is stamped as the producer of
	// every dead letter this consumer writes. Start refuses to run without it.
	service string
	// infraClassifiers recognise outages of service-specific dependencies on
	// top of the ones Classify knows.
	infraClassifiers []InfraClassifier
	// infraBackoffBase doubles per pause after an infrastructure error, up to
	// infraBackoffCap. pauseSlice bounds each sleep inside a pause, so the
	// heartbeat is refreshed at least that often, and is the period at which
	// the holder re-claims the messages in hand (see holdWhileInHand).
	infraBackoffBase time.Duration
	infraBackoffCap  time.Duration
	pauseSlice       time.Duration
	// startup bounds how long Start waits for the consumer group to exist
	// (DefaultStartupPolicy).
	startup StartupPolicy
}

// ConsumerOption tunes optional behaviour on a StreamConsumer.
type ConsumerOption func(*StreamConsumer)

// reclaimMargin is how far the derived reclaim gate (see reclaimGate) sits
// above the handler timeout. A message this consumer has in hand never reaches
// the gate, because the holder re-claims it every pauseSlice (see
// holdWhileInHand). The gate is what a message nobody holds waits out before a
// sweep takes it: one a crashed consumer left behind, or one this consumer
// left pending after a transient failure. Keeping the gate above the handler
// timeout means that, should the holder's re-claims fail, a handler invocation
// that starts on a freshly delivered message still ends before a peer's sweep
// may take that message.
const reclaimMargin = time.Minute

// WithReclaimMinIdle sets the reclaim gate explicitly: the minimum idle time a
// pending entry must have accumulated before this consumer's reclaim sweep may
// claim it. The value replaces the gate derived from the handler timeout (see
// reclaimGate), whatever handler timeout the consumer is given. 0 disables the
// gate, which lets a test exercise the reclaim path inside a single process.
// The holder re-claims the messages in hand every pauseSlice, so they are
// protected from a peer's sweep only while the gate is longer than pauseSlice:
// a gate at or below it lets a peer take a message this consumer still holds.
func WithReclaimMinIdle(d time.Duration) ConsumerOption {
	return func(c *StreamConsumer) {
		c.reclaimMinIdle = d
		c.reclaimMinIdleSet = true
	}
}

// WithWorkerPool enables bounded parallel processing across n lanes, sharding
// messages by a hash of the aggregateKeyField value so that all messages for a
// given aggregate remain strictly ordered (same key → same lane → FIFO) while
// distinct aggregates process concurrently. n is the number of lanes and
// aggregateKeyField is the message Values field that names the aggregate (for
// example "schedule_id"). n <= 1 is the default and processes strictly serially,
// so a binding opts into parallelism deliberately.
//
// Per-aggregate serialization in the write store (e.g. SELECT … FOR UPDATE on a
// run) means n>1 only buys throughput across aggregates — which is exactly the
// hot path — so callers should size n against the number of concurrently active
// aggregates, not the raw message rate.
func WithWorkerPool(n int, aggregateKeyField string) ConsumerOption {
	return func(c *StreamConsumer) {
		c.workerCount = n
		c.aggregateKeyField = aggregateKeyField
	}
}

// WithHandlerTimeout bounds each handler invocation with a deadline of d.
// d <= 0 keeps the current timeout (DefaultHandlerTimeout unless set).
func WithHandlerTimeout(d time.Duration) ConsumerOption {
	return func(c *StreamConsumer) {
		if d > 0 {
			c.handlerTimeout = d
		}
	}
}

// WithInfraClassifier adds a classifier for a service-specific dependency
// whose failures mean an outage rather than a bad message.
func WithInfraClassifier(f InfraClassifier) ConsumerOption {
	return func(c *StreamConsumer) { c.infraClassifiers = append(c.infraClassifiers, f) }
}

// WithOnDrop registers a DropHandler invoked once a dead-lettered message is
// acknowledged (see the onDrop field and DropHandler). Pass it when the
// handler commits in-flight state before it can fail — e.g. an in-flight
// status row — so that state is not orphaned when the message is abandoned.
// Without it, abandoned messages are dead-lettered with no notification.
func WithOnDrop(fn DropHandler) ConsumerOption {
	return func(c *StreamConsumer) { c.onDrop = fn }
}

// WithoutDeadLetters makes the consumer never dead-letter: a message whose
// handler fails permanently, or still fails on its last delivery, stays
// pending and is retried on every sweep. It is for consumers of the
// dead-letter streams themselves, whose dead letters would feed the stream
// they read.
func WithoutDeadLetters() ConsumerOption {
	return func(c *StreamConsumer) { c.noDeadLetters = true }
}

// SetHandlerTimeout sets the per-handler deadline before Start; d <= 0 keeps
// the current one. Callers that receive an already-constructed consumer (e.g.
// from a per-stream binding factory) use it instead of WithHandlerTimeout. The
// heartbeat budget and the derived reclaim gate follow the new timeout. The
// write happens-before the Start goroutine, so no synchronisation is required.
func (c *StreamConsumer) SetHandlerTimeout(d time.Duration) {
	if d > 0 {
		c.handlerTimeout = d
	}
}

// SetService names the service this consumer runs in. It must be called
// before Start: the name is the producer of every dead letter it writes.
func (c *StreamConsumer) SetService(name string) { c.service = name }

// HeartbeatBudget is the staleness bound to pass to Healthy: the handler
// timeout plus heartbeatMargin, derived from the timeout the consumer runs
// with. Each handler attempt and each slice of an infrastructure pause advance
// the heartbeat, so a live consumer stays inside it.
func (c *StreamConsumer) HeartbeatBudget() time.Duration { return c.handlerTimeout + heartbeatMargin }

// reclaimGate is the minimum idle time a pending entry must have before this
// consumer's reclaim sweep claims it: the WithReclaimMinIdle value when one was
// given, otherwise the handler timeout plus reclaimMargin. It is computed on
// every use, so a handler timeout set after construction moves the gate too.
func (c *StreamConsumer) reclaimGate() time.Duration {
	if c.reclaimMinIdleSet {
		return c.reclaimMinIdle
	}
	return c.handlerTimeout + reclaimMargin
}

// consumerName derives a stable, per-pod consumer name. Reusing the same name
// across process restarts means the consumer group registry does not grow an
// orphaned entry every restart; the restarted process re-attaches to its own
// PEL instead of leaking a dead consumer whose pending entries only the reclaim
// sweep would recover. The hostname is the pod identity under Kubernetes; if it
// is unavailable the name is time-seeded instead, so the consumer still starts.
func consumerName(consumerGroup string) string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return fmt.Sprintf("%s-%d", consumerGroup, time.Now().UnixNano())
	}
	return fmt.Sprintf("%s-%s", consumerGroup, host)
}

// NewStreamConsumer creates a new StreamConsumer
func NewStreamConsumer(
	client *goredis.Client,
	streamName, consumerGroup string,
	handler MessageHandler,
	logger *slog.Logger,
	opts ...ConsumerOption,
) *StreamConsumer {
	c := &StreamConsumer{
		consumerConfig: consumerConfig{
			client:           client,
			streamName:       streamName,
			consumerGroup:    consumerGroup,
			consumerName:     consumerName(consumerGroup),
			handler:          handler,
			logger:           logger,
			observer:         nopObserver{},
			workerCount:      1,
			handlerTimeout:   DefaultHandlerTimeout,
			infraBackoffBase: defaultInfraBackoffBase,
			infraBackoffCap:  defaultInfraBackoffCap,
			pauseSlice:       defaultPauseSlice,
			startup:          DefaultStartupPolicy(),
		},
		key: Key(streamName),
	}
	c.bindKeySeams()
	c.deadLetterFn = c.xaddDeadLetter
	c.holdTicker = func(d time.Duration) (<-chan time.Time, func()) {
		t := time.NewTicker(d)
		return t.C, t.Stop
	}
	c.sleepFn = func(ctx context.Context, d time.Duration) bool { return sleepCtx(ctx, d) == nil }
	c.nowFn = time.Now
	for _, opt := range opts {
		opt(c)
	}
	if c.workerCount < 1 {
		c.workerCount = 1
	}
	// Seed lastActivity at construction time (rather than leaving it zero until
	// the first loop iteration) so a health probe that fires in the window
	// between construction and the Start goroutine actually being scheduled
	// sees "just started," not "stalled since the epoch."
	c.lastActivity.Store(time.Now().UnixNano())
	return c
}

// bindKeySeams wires the seams that act on c.key to c's own Redis calls.
func (c *StreamConsumer) bindKeySeams() {
	c.ackFn = c.ackOne
	c.holdFn = c.hold
	c.deliveriesFn = c.deliveryCount
}

// onKey returns a loop of this consumer over key: the same configuration,
// handler-serialization lock and key-independent seams, with its own in-hand
// set, heartbeat and the seams that act on key.
func (c *StreamConsumer) onKey(key string) *StreamConsumer {
	l := &StreamConsumer{
		consumerConfig: c.consumerConfig,
		key:            key,
		handlerMu:      c.handlerMu,
		deadLetterFn:   c.deadLetterFn,
		holdTicker:     c.holdTicker,
		sleepFn:        c.sleepFn,
		nowFn:          c.nowFn,
	}
	l.bindKeySeams()
	l.lastActivity.Store(time.Now().UnixNano())
	return l
}

// reclaimInterval is how often the consumer re-scans the PEL for messages
// abandoned by other consumer instances (crash recovery) and for messages whose
// handler failed transiently. The read path retries a transient error once
// in-process (readPathSchedule) before leaving the message for this sweep. A
// sweep takes only entries idle for at least the consumer's reclaim gate
// (reclaimGate), so a message that keeps failing is retried by the first sweep
// after it has been idle that long: no more often than the larger of this
// interval and the gate.
const reclaimInterval = 2 * time.Minute

// staleConsumerIdle is how long a zero-pending consumer must have been idle
// before cleanupStaleConsumers deletes its registry entry. A live consumer
// re-reads at most every read block, so anything idle this long with nothing
// pending is a consumer left behind by a replaced pod (a Kubernetes rollout
// gives each new pod a fresh hostname, hence a new consumer name). Set well
// above the reclaim interval so a peer mid-sweep is never mistaken for dead.
const staleConsumerIdle = 2 * reclaimInterval

// DefaultHandlerTimeout bounds every handler invocation unless the consumer is
// given another timeout.
const DefaultHandlerTimeout = 30 * time.Second

// heartbeatMargin is added to the handler timeout to form HeartbeatBudget.
const heartbeatMargin = 2 * time.Minute

// The infrastructure-pause defaults. infraBackoff and pause also fall back to
// them when a consumer carries a non-positive value, so a pause always ends.
const (
	defaultInfraBackoffBase = time.Second
	defaultInfraBackoffCap  = 60 * time.Second
	defaultPauseSlice       = 5 * time.Second
)

// maxDeliveries is the delivery on which a message whose handler still fails
// transiently is dead-lettered. The count is the pending entry's delivery
// counter: the first read is delivery 1, each reclaim adds one. Infrastructure
// pauses never add to it, since they retry the handler in place (see run), and
// neither do the holder's re-claims (see hold).
const maxDeliveries = 5

// readPathSchedule retries a transient error once, quickly, on first delivery;
// after that the message waits for the reclaim sweep. reclaimSchedule runs a
// reclaimed message once per sweep, so a failing message is retried no more
// often than the larger of reclaimInterval and the consumer's reclaim gate.
var (
	readPathSchedule = []time.Duration{0, 100 * time.Millisecond}
	reclaimSchedule  = []time.Duration{0}
)

const (
	// logDeadLettered is logged once per message the consumer abandons, after
	// its dead letter is written.
	logDeadLettered = "Message dead-lettered — ACKing to drop from PEL"
	logInfraPause   = "Infrastructure error — pausing consumer"
)

// onDropped notifies the registered DropHandler that a dead-lettered message
// has been acknowledged, so the owning service can finalize in-flight state the
// message left behind. It is nil-safe (no handler → no-op) and panic-safe (a
// panicking handler is recovered and logged rather than unwinding into the
// consumer loop); the message is already dead-lettered and acknowledged when it
// runs, so a failing notification changes nothing about either.
func (c *StreamConsumer) onDropped(ctx context.Context, msg goredis.XMessage, cause error) {
	if c.onDrop == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("Drop handler panicked — recovered",
				"stream", c.streamName, "message_id", msg.ID, "panic", r)
		}
	}()
	c.onDrop(ctx, msg, cause)
}

// safeInvoke calls the handler once under the handler deadline, with panic
// recovery: a recovered panic becomes a plain error, so one poison message
// cannot crash the process. deadlineHit reports that the handler's own
// deadline ended the call while the consumer's context was still live.
func (c *StreamConsumer) safeInvoke(ctx context.Context, msg goredis.XMessage) (deadlineHit bool, err error) {
	// Advanced per attempt, so a batch or a slow handler keeps the liveness
	// heartbeat fresh: once before waiting for the handler lock, so a loop
	// waiting while another key's loop runs its handler (at most one handler
	// timeout) stays inside HeartbeatBudget, and again once it holds the lock.
	c.lastActivity.Store(time.Now().UnixNano())
	if c.handlerMu != nil && c.workerCount <= 1 {
		c.handlerMu.Lock()
		defer c.handlerMu.Unlock()
		c.lastActivity.Store(time.Now().UnixNano())
	}
	hctx, cancel := context.WithTimeout(withSourceKey(ctx, c.dedupSuffix()), c.handlerTimeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("Handler panicked — recovered to keep the consumer alive",
				"stream", c.streamName, "message_id", msg.ID, "panic", r)
			err = fmt.Errorf("handler panic: %v", r)
		}
		deadlineHit = err != nil && ctx.Err() == nil && errors.Is(hctx.Err(), context.DeadlineExceeded)
	}()
	return false, c.handler(hctx, msg)
}

// classify maps a handler error to its class. A handler that ran out its own
// deadline is transient whatever error the cut-off produced, so a message that
// can never finish in time is dead-lettered after maxDeliveries instead of
// pausing its stream forever. An explicit ErrPermanent still wins.
func (c *StreamConsumer) classify(err error, deadlineHit bool) ErrorClass {
	if deadlineHit && !errors.Is(err, events.ErrPermanent) {
		return ClassTransient
	}
	return Classify(err, c.infraClassifiers...)
}

// attempt runs the handler once per schedule entry and stops at the first
// success or the first error that is not transient. When ctx ends it returns
// ctx's error and the caller leaves the message pending.
func (c *StreamConsumer) attempt(ctx context.Context, msg goredis.XMessage, schedule []time.Duration) (ErrorClass, error) {
	class, err := ClassTransient, error(nil)
	for i, delay := range schedule {
		if delay > 0 {
			if !c.sleepFn(ctx, delay) {
				return ClassTransient, ctx.Err()
			}
			c.logger.Warn("Retrying transient handler error",
				"stream", c.streamName, "message_id", msg.ID, "attempt", i+1, "previous_error", err)
		}
		var deadlineHit bool
		started := time.Now()
		deadlineHit, err = c.safeInvoke(ctx, msg)
		if ctx.Err() == nil {
			result := "ok"
			if err != nil {
				result = c.classify(err, deadlineHit).String()
			}
			c.observer.Handled(c.streamName, c.consumerGroup, result, time.Since(started))
		}
		if err == nil {
			return ClassTransient, nil
		}
		if ctx.Err() != nil {
			return ClassTransient, ctx.Err()
		}
		if deadlineHit {
			c.logger.Error("Handler exceeded its timeout",
				"stream", c.streamName, "message_id", msg.ID, "timeout", c.handlerTimeout, "error", err)
		}
		if class = c.classify(err, deadlineHit); class != ClassTransient {
			return class, err
		}
	}
	return class, err
}

// run invokes the handler for msg. Transient errors are retried inline per
// schedule. Infrastructure errors are retried for as long as they last, with
// capped exponential backoff and without counting a delivery. The result is
// nil on success; otherwise the class and error of the last attempt, or ctx's
// error once the service is stopping.
func (c *StreamConsumer) run(ctx context.Context, msg goredis.XMessage, schedule []time.Duration) (ErrorClass, error) {
	for pauses := 1; ; pauses++ {
		class, err := c.attempt(ctx, msg, schedule)
		if err == nil || ctx.Err() != nil || class != ClassInfrastructure {
			return class, err
		}
		delay := c.infraBackoff(pauses)
		c.logger.Warn(logInfraPause, "stream", c.streamName, "group", c.consumerGroup,
			"message_id", msg.ID, "pause", pauses, "retry_in", delay, "error", err)
		c.observer.Paused(c.streamName, c.consumerGroup)
		if !c.pause(ctx, delay) {
			return ClassTransient, ctx.Err()
		}
	}
}

// infraBackoff is the n-th pause after an infrastructure error:
// infraBackoffBase doubled n-1 times, capped at infraBackoffCap. A
// non-positive base or cap counts as its default, so the result is always
// positive.
func (c *StreamConsumer) infraBackoff(n int) time.Duration {
	base, ceiling := c.infraBackoffBase, c.infraBackoffCap
	if base <= 0 {
		base = defaultInfraBackoffBase
	}
	if ceiling <= 0 {
		ceiling = defaultInfraBackoffCap
	}
	d := base
	for i := 1; i < n && d < ceiling; i++ {
		d *= 2
	}
	return min(d, ceiling)
}

// pause waits d. Every pauseSlice (defaultPauseSlice when non-positive) it
// advances the heartbeat, so liveness never mistakes the wait for a wedge. The
// paused message stays in hand, so the holder keeps it, and the rest of its
// batch or page, below the reclaim gate (see holdWhileInHand). It returns
// false once ctx ends.
func (c *StreamConsumer) pause(ctx context.Context, d time.Duration) bool {
	slice := c.pauseSlice
	if slice <= 0 {
		slice = defaultPauseSlice
	}
	for remaining := d; remaining > 0; {
		c.lastActivity.Store(time.Now().UnixNano())
		step := min(remaining, slice)
		if !c.sleepFn(ctx, step) {
			return false
		}
		remaining -= step
	}
	return true
}

// inHandSet is the set of message ids a consumer has read or claimed and not
// yet settled. Worker lanes settle concurrently with the holder reading it, so
// it is mutex-guarded. The zero value is an empty set.
type inHandSet struct {
	mu  sync.Mutex
	ids map[string]struct{}
}

// add puts every message of msgs in the set and returns their ids.
func (s *inHandSet) add(msgs []goredis.XMessage) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ids == nil {
		s.ids = make(map[string]struct{}, len(msgs))
	}
	ids := make([]string, 0, len(msgs))
	for _, m := range msgs {
		s.ids[m.ID] = struct{}{}
		ids = append(ids, m.ID)
	}
	return ids
}

// remove takes ids out of the set; an id that is not in it is ignored.
func (s *inHandSet) remove(ids ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		delete(s.ids, id)
	}
}

// snapshot returns the ids in the set, sorted.
func (s *inHandSet) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.ids))
	for id := range s.ids {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// holdWhileInHand runs process over msgs, a batch this consumer has just read
// or a page it has just claimed. Each message is in hand from now until it
// settles: acknowledged, dead-lettered and acknowledged, or left pending for
// the reclaim sweep. Meanwhile a holder goroutine re-claims exactly the
// messages still in hand every pauseSlice (see hold). None of them therefore
// grows idle enough for a peer's reclaim sweep to take it, whether it is
// running, paused on an outage, or waiting behind slow handlers in its batch or
// page. A message that has settled is out of the set, so the holder leaves it
// alone: one left pending after a transient failure keeps aging until the
// sweep takes it at the gate. A re-claim that read the set just before a
// message settled may touch that message once more; an acknowledged one is no
// longer pending and XCLAIM skips it. The holder stops when process returns or
// ctx ends; a message still in hand then stays pending.
func (c *StreamConsumer) holdWhileInHand(ctx context.Context, msgs []goredis.XMessage, process func()) {
	if len(msgs) == 0 {
		return
	}
	ids := c.inHand.add(msgs)
	defer c.inHand.remove(ids...)
	stop, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		c.holdInHand(ctx, stop)
	}()
	defer func() {
		close(stop)
		<-stopped
	}()
	process()
}

// holdInHand re-claims the messages in hand every pauseSlice
// (defaultPauseSlice when non-positive) until stop closes or ctx ends.
func (c *StreamConsumer) holdInHand(ctx context.Context, stop <-chan struct{}) {
	slice := c.pauseSlice
	if slice <= 0 {
		slice = defaultPauseSlice
	}
	ticks, stopTicks := c.holdTicker(slice)
	defer stopTicks()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticks:
			if ids := c.inHand.snapshot(); len(ids) > 0 {
				c.holdFn(ctx, ids)
			}
		}
	}
}

// hold re-claims ids for this consumer with XCLAIM … JUSTID, restarting each
// entry's idle time without counting a delivery. XCLAIM skips an id that is no
// longer pending, so it never re-creates an acknowledged entry. It is
// best-effort: a failure only shortens the protection, so it is logged unless
// the consumer is stopping.
func (c *StreamConsumer) hold(ctx context.Context, ids []string) {
	err := c.client.XClaimJustID(ctx, &goredis.XClaimArgs{
		Stream: c.key, Group: c.consumerGroup, Consumer: c.consumerName, MinIdle: 0, Messages: ids,
	}).Err()
	if err != nil && ctx.Err() == nil {
		c.logger.Warn("Could not refresh the idle time of the messages in hand",
			"stream", c.streamName, "key", c.key, "count", len(ids), "error", err)
	}
}

// settleContext detaches ctx from shutdown for the steps that follow a decided
// outcome — the ACK of a handled or dead-lettered message and the drop
// notification — so a shutdown that begins after the decision cannot leave the
// message pending, to be handled or dead-lettered a second time. The handler
// timeout bounds those steps, so shutdown still completes.
func (c *StreamConsumer) settleContext(ctx context.Context) (context.Context, context.CancelFunc) {
	d := c.handlerTimeout
	if d <= 0 {
		d = DefaultHandlerTimeout
	}
	return context.WithTimeout(context.WithoutCancel(ctx), d)
}

// xaddDeadLetter appends one entry to the key of consumer.dead_letter:v1 with
// no length cap; the dead-letter-controller's trim loop bounds the stream. It
// never creates the stream: while it is missing the write fails, and
// deadLetterAndAck keeps the message pending and retries.
func (c *StreamConsumer) xaddDeadLetter(ctx context.Context, values map[string]any) error {
	err := c.client.XAdd(ctx, &goredis.XAddArgs{Stream: Key(streams.ConsumerDeadLetterV1), NoMkStream: true, Values: values}).Err()
	return MissingStreamError(Key(streams.ConsumerDeadLetterV1), err)
}

// deadLetterAndAck writes msg to consumer.dead_letter:v1 and acknowledges it
// only after that write succeeded. While the write fails the message stays
// pending and the consumer pauses, retrying the write (never the handler).
// logDeadLettered is logged once the write succeeded; the drop handler fires
// once the acknowledgement succeeded. Both the acknowledgement and the drop
// handler run on settleContext, so a shutdown after the write neither leaves
// the message pending to be dead-lettered again nor skips the notification.
func (c *StreamConsumer) deadLetterAndAck(ctx context.Context, msg goredis.XMessage, kind model.DeadLetterKind, cause error, deliveries int64) {
	values, err := events.ConsumerDeadLetterFields(events.DeadLetteredMessage{
		TenantID: tenantOf(msg), Producer: c.service, OccurredAt: c.nowFn(),
		Stream: c.streamName, Group: c.consumerGroup, MessageID: msg.ID, Key: c.key,
		Fields: stringFields(msg.Values), FailureKind: kind, Error: cause.Error(), DeliveryCount: deliveries,
	})
	if err != nil {
		c.logger.Error("Could not encode dead letter — message stays pending", "stream", c.streamName, "message_id", msg.ID, "error", err)
		return
	}
	for writes := 1; ; writes++ {
		werr := c.deadLetterFn(ctx, values)
		if werr == nil {
			break
		}
		if ctx.Err() != nil {
			return
		}
		delay := c.infraBackoff(writes)
		c.logger.Error("Dead-letter write failed — message stays pending",
			"stream", c.streamName, "message_id", msg.ID, "retry_in", delay, "error", werr)
		if !c.pause(ctx, delay) {
			return
		}
	}
	c.logger.Error(logDeadLettered, "stream", c.streamName, "group", c.consumerGroup, "message_id", msg.ID,
		"failure_kind", string(kind), "deliveries", deliveries, "dead_letter_event_id", values["event_id"], "error", cause)
	c.observer.DeadLettered(c.streamName, c.consumerGroup, kind)
	sctx, cancel := c.settleContext(ctx)
	defer cancel()
	if err := c.ackFn(sctx, msg.ID); err != nil {
		return
	}
	c.onDropped(sctx, msg, cause)
}

// tenantOf returns the tenant_id a message carries, or the default tenant.
func tenantOf(msg goredis.XMessage) string {
	if t, ok := msg.Values["tenant_id"].(string); ok && t != "" {
		return t
	}
	return events.DefaultTenantID
}

// stringFields copies a message's fields as strings. XREADGROUP returns every
// value as a string; any other value is formatted with %v.
func stringFields(values map[string]any) map[string]string {
	out := make(map[string]string, len(values))
	for k, v := range values {
		if s, ok := v.(string); ok {
			out[k] = s
			continue
		}
		out[k] = fmt.Sprint(v)
	}
	return out
}

// Start consumes the stream until ctx is cancelled. It first waits for the
// consumer group (see waitForGroups) and never creates a stream or a group: it
// returns an error when the group is still missing once its startup policy is
// spent, and at once on an answer waiting cannot change (a key of the wrong
// type). It then runs one read loop and one reclaim loop per key it found and
// returns nil once ctx ends, or the error that ended a loop: a NOGROUP read,
// meaning the group was removed while the consumer ran.
func (c *StreamConsumer) Start(ctx context.Context) error {
	if c.service == "" {
		return fmt.Errorf("stream consumer %q/%q has no service name: call SetService before Start", c.streamName, c.consumerGroup)
	}
	keys, err := c.waitForGroups(ctx)
	if err != nil || len(keys) == 0 {
		return err
	}
	c.observer.Watch(c.streamName, c.consumerGroup)
	if len(keys) == 1 {
		return c.consume(ctx)
	}

	if c.workerCount <= 1 {
		c.handlerMu = &sync.Mutex{}
	}
	others := make([]*StreamConsumer, 0, len(keys)-1)
	for _, key := range keys[1:] {
		others = append(others, c.onKey(key))
	}
	c.keyLoops.Store(&others)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, len(others))
	for _, l := range others {
		go func() {
			err := l.consume(ctx)
			if err != nil {
				cancel()
			}
			errs <- err
		}()
	}
	err = c.consume(ctx)
	cancel()
	all := []error{err}
	for range others {
		all = append(all, <-errs)
	}
	return errors.Join(all...)
}

// consume runs this loop's read loop and reclaim loop over c.key until ctx is
// cancelled (it then returns nil) or a read finds the group gone.
func (c *StreamConsumer) consume(ctx context.Context) error {
	c.logger.Info("Starting consumer",
		"stream", c.streamName,
		"key", c.key,
		"group", c.consumerGroup,
		"consumer", c.consumerName,
		"workers", c.workerCount,
	)

	// Reclaim pending messages left by previous consumer instances that
	// crashed before ACKing (crash recovery), then delete the now-drained
	// registry entries those instances left behind.
	if err := c.reclaimPending(ctx); err != nil {
		c.logger.Error("Failed to reclaim pending messages", "stream", c.streamName, "key", c.key, "error", err)
	}
	c.cleanupStaleConsumers(ctx, staleConsumerIdle)

	reclaimTicker := time.NewTicker(reclaimInterval)
	defer reclaimTicker.Stop()

	for {
		// Recorded once per iteration, before the blocking work below, and
		// regardless of what that work returns. A failed read during a Redis
		// outage still advances this — the loop attempting and logging the
		// failure *is* the liveness signal; Healthy only needs to distinguish
		// that from a goroutine that stopped iterating altogether (wedged in a
		// call that never returns, or exited without going through Start's
		// normal ctx.Done() path).
		c.lastActivity.Store(time.Now().UnixNano())
		select {
		case <-ctx.Done():
			return nil
		case <-reclaimTicker.C:
			if err := c.reclaimPending(ctx); err != nil {
				c.logger.Error("Periodic reclaim pending failed", "stream", c.streamName, "key", c.key, "error", err)
			}
			c.cleanupStaleConsumers(ctx, staleConsumerIdle)
		default:
			err := c.readAndProcess(ctx)
			if errors.Is(err, errGroupGone) {
				c.logger.Error("Consumer group no longer exists — stopping the consumer",
					"stream", c.streamName, "key", c.key, "group", c.consumerGroup, "error", err)
				return err
			}
			if err != nil {
				c.logger.Error("Error in read loop", "stream", c.streamName, "key", c.key, "error", err)
				time.Sleep(3 * time.Second)
			}
		}
	}
}

// waitForGroups returns the keys this consumer reads: Key(streamName) once it
// holds the consumer group, followed by LegacyKey(streamName) when
// streams.LegacyKeys is true and that key holds the group too. A missing
// legacy key, or one without the group, is left out. It polls XINFO GROUPS
// with the backoff of c.startup until the group exists, its budget is spent
// or ctx ends (it then returns no keys and no error). It advances the
// heartbeat on every poll, so a consumer waiting through a Redis outage reads
// as alive. An answer waiting cannot change (a key of the wrong type, an
// unknown command) ends the wait at once.
func (c *StreamConsumer) waitForGroups(ctx context.Context) ([]string, error) {
	policy := c.startup
	deadline := time.Now().Add(policy.Timeout)
	delay := max(policy.InitialDelay, 10*time.Millisecond)
	for {
		c.lastActivity.Store(time.Now().UnixNano())
		keys, err := c.groupKeys(ctx)
		if err == nil && len(keys) > 0 {
			return keys, nil
		}
		if ctx.Err() != nil {
			return nil, nil
		}
		if isPermanentBootstrapError(err) {
			c.logger.Error("Permanent consumer-group bootstrap failure — not retrying (surfacing to health)",
				"stream", c.streamName, "key", c.key, "group", c.consumerGroup, "error", err)
			return nil, fmt.Errorf("permanent consumer-group bootstrap failure for stream %q group %q: %w",
				c.key, c.consumerGroup, err)
		}
		if time.Now().Add(delay).After(deadline) {
			if err != nil {
				return nil, fmt.Errorf("could not confirm consumer group %q on %q within %s: %w",
					c.consumerGroup, c.key, policy.Timeout, err)
			}
			return nil, fmt.Errorf("consumer group %q on %q does not exist; the streams-init step creates it",
				c.consumerGroup, c.key)
		}
		if err != nil {
			c.logger.Warn("Could not read the consumer groups — retrying",
				"stream", c.streamName, "key", c.key, "group", c.consumerGroup, "retry_in", delay, "error", err)
		} else {
			c.logger.Warn("Consumer group does not exist yet — waiting for the streams-init step",
				"stream", c.streamName, "key", c.key, "group", c.consumerGroup, "retry_in", delay)
		}
		if !c.sleepFn(ctx, delay) {
			return nil, nil
		}
		delay = min(delay*2, max(policy.MaxDelay, delay))
	}
}

// groupKeys reports the keys that hold the consumer group, Key(streamName)
// first; it returns none while that key does not hold it.
func (c *StreamConsumer) groupKeys(ctx context.Context) ([]string, error) {
	held, err := c.holdsGroup(ctx, c.key)
	if err != nil || !held {
		return nil, err
	}
	keys := []string{c.key}
	if !streams.LegacyKeys {
		return keys, nil
	}
	legacy := LegacyKey(c.streamName)
	held, err = c.holdsGroup(ctx, legacy)
	if err != nil {
		return nil, err
	}
	if held {
		keys = append(keys, legacy)
	}
	return keys, nil
}

// holdsGroup reports whether key holds the consumer group. A missing key holds
// none.
func (c *StreamConsumer) holdsGroup(ctx context.Context, key string) (bool, error) {
	groups, err := c.client.XInfoGroups(ctx, key).Result()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			return false, nil
		}
		return false, err
	}
	for _, g := range groups {
		if g.Name == c.consumerGroup {
			return true, nil
		}
	}
	return false, nil
}

// Healthy reports whether the read loop has made progress within maxStale. It
// returns nil whenever the loop is cycling — including throughout a Redis
// outage, since the loop's own retry-with-backoff already handles that case
// and a transient dial error is not itself a liveness failure, and including
// while a legitimately-slow-but-bounded handler is in flight, since each
// handler attempt advances the heartbeat before running (see safeInvoke). A
// non-nil error means the goroutine has stopped making progress: a handler
// that ignores its (deadline-bounded) context and never returns, or a
// goroutine that exited some way other than Start's normal ctx.Done() return.
// That is exactly the failure mode an HTTP liveness probe cannot see on its
// own — the process and its HTTP server stay up while a consumer goroutine is
// dead — so callers should wire this into a liveness probe (see pkg/liveness'
// AddWorkerProbe). Pass HeartbeatBudget as maxStale: it exceeds the handler
// timeout by a margin, so in-flight work and infrastructure pauses never trip
// liveness while a true wedge still does.
func (c *StreamConsumer) Healthy(maxStale time.Duration) error {
	if err := c.heartbeatFresh(maxStale); err != nil {
		return err
	}
	if others := c.keyLoops.Load(); others != nil {
		for _, l := range *others {
			if err := l.heartbeatFresh(maxStale); err != nil {
				return err
			}
		}
	}
	return nil
}

// heartbeatFresh reports whether this loop has made progress within maxStale.
func (c *StreamConsumer) heartbeatFresh(maxStale time.Duration) error {
	// lastActivity is seeded in NewStreamConsumer and onKey, so it is never the
	// zero value for a real loop; a zero-value consumer (only constructed in
	// tests) reads as "1970" and therefore stalled, which is the correct
	// unhealthy answer for something that never ran.
	lastAt := time.Unix(0, c.lastActivity.Load())
	if age := time.Since(lastAt); age > maxStale {
		return fmt.Errorf("stream consumer %q/%q on %q: read loop stalled — no activity for %s (last at %s)",
			c.streamName, c.consumerGroup, c.key, age.Round(time.Second), lastAt.Format(time.RFC3339))
	}
	return nil
}

// permanentBootstrapErrorPrefixes are Redis server-error *code* prefixes that no
// retry can ever clear, because no timing scenario makes them transient: the
// stream key exists as a non-stream type (WRONGTYPE), or the server does not
// implement the command at all (unknown command / subcommand). These are
// structural misconfigurations.
//
// Auth-class errors (WRONGPASS / NOAUTH / NOPERM) are deliberately NOT here:
// they can be transient — a password rotation race, or ACL propagation lag —
// and classifying them permanent would return Start, flip liveness, and
// crashloop the whole consumer fleet during a recoverable auth blip. They fall
// through to the transient path instead, so an operator's fix is picked up on
// the next retry with no pod restart. Every network/connection error and every
// transient server state (connection refused, i/o timeout, LOADING,
// CLUSTERDOWN, MASTERDOWN, TRYAGAIN, …) is likewise transient.
var permanentBootstrapErrorPrefixes = []string{
	"WRONGTYPE",
	"unknown command",
	"unknown subcommand",
}

// isPermanentBootstrapError reports whether an error reading the consumer
// groups at startup is a permanent misconfiguration that retrying will never fix. It matches against
// the Redis RESP error-code prefix via goredis.HasErrorPrefix, which first
// unwraps to the underlying server error (so our fmt.Errorf wrapping is
// transparent) and only matches genuine reply-error codes — a network error, or
// a wrapped/aggregated error whose text merely happens to contain one of these
// tokens, is never misclassified as permanent.
func isPermanentBootstrapError(err error) bool {
	if err == nil {
		return false
	}
	for _, p := range permanentBootstrapErrorPrefixes {
		if goredis.HasErrorPrefix(err, p) {
			return true
		}
	}
	return false
}

// reclaimPending claims and reprocesses messages left in the pending entry list
// (PEL) by consumers other than this one — typically a previous instance that
// crashed before ACKing. Only entries idle for at least the reclaim gate
// (reclaimGate) are eligible. A live replica's messages in hand never are,
// because its holder keeps re-claiming them (see holdWhileInHand), so a
// periodic sweep does not steal a peer's in-flight message.
//
// Each claimed page is held while its messages are settled one by one by
// settleReclaimed, so an entry waiting for its turn is not taken by a peer.
// Handler invocations here are **single-shot** for transient errors
// (reclaimSchedule): a PEL entry either landed here because a prior owner
// already burned its inline retry budget on the read path, or because that
// owner crashed. Re-running the read path's retry schedule inside the sweep
// would (a) head-of-line-block the read loop, and (b) duplicate work for the
// common case where a single attempt under the new owner already succeeds. If
// the single attempt fails, the entry stays in the PEL until a later sweep
// finds it idle for at least the reclaim gate, so it is retried no more often
// than the larger of reclaimInterval and the gate, until its delivery count
// reaches maxDeliveries and it is dead-lettered. A permanent error is
// dead-lettered at once; an infrastructure error pauses the sweep on that
// message until the dependency answers. Every message is acknowledged as soon
// as it is settled — a handled one at once, a dead-lettered one after its dead
// letter is written — so a pause on one message never holds finished work in
// the PEL.
//
// Implementation note: XAUTOCLAIM (Redis 6.2+) claims a whole page of up to 100
// entries in one cursor-paged command, rather than one XPENDING plus an XCLAIM
// per entry.
func (c *StreamConsumer) reclaimPending(ctx context.Context) error {
	gate := c.reclaimGate()
	cursor := "0-0"
	for {
		msgs, next, err := c.client.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
			Stream:   c.key,
			Group:    c.consumerGroup,
			Consumer: c.consumerName,
			MinIdle:  gate,
			Start:    cursor,
			Count:    100,
		}).Result()
		if err != nil {
			return fmt.Errorf("XAUTOCLAIM failed: %w", err)
		}

		if len(msgs) > 0 {
			c.logger.Warn("Reclaiming pending messages from previous consumers",
				"stream", c.streamName,
				"key", c.key,
				"count", len(msgs),
				"min_idle", gate,
			)
		}

		c.holdWhileInHand(ctx, msgs, func() {
			for _, msg := range msgs {
				if ctx.Err() != nil {
					return
				}
				c.settleReclaimed(ctx, msg)
			}
		})
		if ctx.Err() != nil {
			return nil
		}

		if next == "0-0" {
			return nil
		}
		cursor = next
	}
}

// settleReclaimed runs the handler for one reclaimed message — once, or for as
// long as an infrastructure error lasts — and settles it: acknowledged at once
// on success, even when shutdown began during the handler; dead-lettered then
// acknowledged on a permanent failure, or on a transient one on delivery
// maxDeliveries or later; otherwise left pending for the next sweep. A
// message whose handling is cut short by shutdown stays pending. Whatever the
// outcome, the message leaves the in-hand set when settleReclaimed returns.
func (c *StreamConsumer) settleReclaimed(ctx context.Context, msg goredis.XMessage) {
	defer c.inHand.remove(msg.ID)
	msg, ok := c.admit(ctx, msg)
	if !ok {
		return
	}
	class, err := c.run(ctx, msg, reclaimSchedule)
	switch {
	case err == nil:
		c.ackHandled(ctx, msg.ID)
		return
	case ctx.Err() != nil:
		return
	case class == ClassPermanent:
		if c.noDeadLetters {
			c.logger.Error("Failure on a consumer that never dead-letters — leaving in PEL for next sweep",
				"stream", c.streamName, "message_id", msg.ID, "deliveries", c.deliveriesFn(ctx, msg.ID), "error", err)
			return
		}
		// An unreadable delivery counter reads 0; the message was delivered at
		// least once.
		c.deadLetterAndAck(ctx, msg, model.DeadLetterKindPermanent, err, max(c.deliveriesFn(ctx, msg.ID), 1))
		return
	}
	n := c.deliveriesFn(ctx, msg.ID)
	if n >= maxDeliveries {
		if c.noDeadLetters {
			c.logger.Error("Failure on a consumer that never dead-letters — leaving in PEL for next sweep",
				"stream", c.streamName, "message_id", msg.ID, "deliveries", n, "error", err)
			return
		}
		c.deadLetterAndAck(ctx, msg, model.DeadLetterKindTransientExhausted, err, n)
		return
	}
	c.logger.Error("Reclaimed message still failing — leaving in PEL for next sweep",
		"stream", c.streamName, "message_id", msg.ID, "deliveries", n, "max_deliveries", maxDeliveries, "error", err)
}

// ackHandled acknowledges a message whose handler succeeded, on settleContext,
// so a shutdown that began during the handler does not leave it pending to be
// handled again.
func (c *StreamConsumer) ackHandled(ctx context.Context, id string) {
	sctx, cancel := c.settleContext(ctx)
	defer cancel()
	_ = c.ackFn(sctx, id)
}

// deliveryCount returns the PEL delivery counter for a single message — how many
// times it has been delivered or claimed without being ACKed. A read failure
// returns 0, which is deliberately conservative: it never dead-letters a
// message early, so a transiently-unreadable PEL just leaves the message for
// the next sweep instead of abandoning live work.
func (c *StreamConsumer) deliveryCount(ctx context.Context, id string) int64 {
	pend, err := c.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: c.key,
		Group:  c.consumerGroup,
		Start:  id,
		End:    id,
		Count:  1,
	}).Result()
	if err != nil {
		c.logger.Warn("Could not read PEL delivery count — not dead-lettering this sweep",
			"stream", c.streamName, "key", c.key, "message_id", id, "error", err)
		return 0
	}
	if len(pend) == 0 {
		return 0
	}
	return pend[0].RetryCount
}

// cleanupStaleConsumers deletes consumer-group registry entries left by previous
// process incarnations. XAUTOCLAIM moves an old consumer's pending entries to
// this one but never removes the now-empty consumer, so without this the
// registry would grow one dead entry per pod replacement (each rollout/reschedule
// gives the new pod a fresh hostname, hence a new consumer name). A consumer is
// deleted only when it is not this one, has no pending entries (its work is
// fully drained or already reclaimed), and has been idle longer than minIdle —
// the idle gate keeps a freshly-registered peer that has not yet read from being
// reaped. Deleting a live peer that is momentarily empty is harmless: it
// re-registers on its next read. Failures are logged and skipped; cleanup is
// best-effort housekeeping, never on the message-processing critical path.
func (c *StreamConsumer) cleanupStaleConsumers(ctx context.Context, minIdle time.Duration) {
	consumers, err := c.client.XInfoConsumers(ctx, c.key, c.consumerGroup).Result()
	if err != nil {
		c.logger.Warn("Could not list consumers for cleanup", "stream", c.streamName, "key", c.key, "error", err)
		return
	}
	for _, cons := range consumers {
		if cons.Name == c.consumerName || cons.Pending > 0 || cons.Idle < minIdle {
			continue
		}
		if err := c.client.XGroupDelConsumer(ctx, c.key, c.consumerGroup, cons.Name).Err(); err != nil {
			c.logger.Warn("Failed to delete stale consumer",
				"stream", c.streamName,
				"key", c.key,
				"consumer", cons.Name,
				"error", err,
			)
			continue
		}
		c.logger.Info("Removed drained stale consumer",
			"stream", c.streamName,
			"key", c.key,
			"consumer", cons.Name,
			"idle", cons.Idle,
		)
	}
}

// errGroupGone marks a read that found the consumer group missing: the group
// or its key was removed while the consumer ran. The consumer never re-creates
// it, so the loop stops.
var errGroupGone = errors.New("consumer group no longer exists")

// readAndProcess reads up to one batch of new entries from c.key and settles
// each of them. It returns an error wrapping errGroupGone when the key no
// longer holds the group.
func (c *StreamConsumer) readAndProcess(ctx context.Context) error {
	read, err := c.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    c.consumerGroup,
		Consumer: c.consumerName,
		Streams:  []string{c.key, ">"},
		Count:    10,
		Block:    1 * time.Second,
	}).Result()

	if err != nil {
		if err == goredis.Nil {
			return nil
		}
		if strings.Contains(err.Error(), "NOGROUP") {
			return fmt.Errorf("%w: group %q on %q: %w", errGroupGone, c.consumerGroup, c.key, err)
		}
		return fmt.Errorf("failed to read from stream: %w", err)
	}

	for _, stream := range read {
		msgs := stream.Messages
		c.holdWhileInHand(ctx, msgs, func() {
			if c.workerCount <= 1 {
				c.processSerial(ctx, msgs)
			} else {
				c.processSharded(ctx, msgs)
			}
		})
	}

	return nil
}

// processSerial runs the handler over the batch one message at a time in stream
// order, ACKing each message the moment it resolves. This is the workerCount==1
// path.
func (c *StreamConsumer) processSerial(ctx context.Context, msgs []goredis.XMessage) {
	for _, msg := range msgs {
		c.processOne(ctx, msg)
	}
}

// processSharded fans the batch across workerCount lanes keyed by the aggregate
// key so messages for one aggregate stay strictly ordered (same key → same lane
// → processed in arrival order) while distinct aggregates run in parallel. Each
// lane drains its messages in order and ACKs each one as it resolves, so a
// finished message is removed from the PEL immediately rather than waiting for
// the slowest lane — a stuck lane can never hold another lane's committed work
// in the PEL long enough for a peer's reclaim sweep to reprocess it. The batch
// is still fully processed before this returns, so the read loop never runs
// ahead of completed work.
func (c *StreamConsumer) processSharded(ctx context.Context, msgs []goredis.XMessage) {
	if len(msgs) == 0 {
		return
	}

	lanes := make([][]goredis.XMessage, c.workerCount)
	for _, msg := range msgs {
		lane := c.laneFor(msg)
		lanes[lane] = append(lanes[lane], msg)
	}

	done := make(chan struct{}, c.workerCount)
	active := 0
	for i := range lanes {
		if len(lanes[i]) == 0 {
			continue
		}
		active++
		go func(laneMsgs []goredis.XMessage) {
			for _, msg := range laneMsgs {
				c.processOne(ctx, msg)
			}
			done <- struct{}{}
		}(lanes[i])
	}
	for ; active > 0; active-- {
		<-done
	}
}

// laneFor maps a message to a worker lane in [0, workerCount). All messages that
// carry the same aggregateKeyField value land on the same lane, which is what
// guarantees per-aggregate ordering. A missing or empty aggregate value hashes
// the empty string to a fixed lane, which forgoes parallelism for that message
// but never reorders it relative to its peers.
func (c *StreamConsumer) laneFor(msg goredis.XMessage) int {
	key, _ := msg.Values[c.aggregateKeyField].(string)
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(boundedWorkerCount(c.workerCount))) //nolint:gosec // G115: boundedWorkerCount floors its result at 1, so this int -> uint32 conversion never sees a negative value
}

// boundedWorkerCount clamps a worker-pool size to at least 1 before it is
// converted to uint32 for the lane-hashing modulo below. workerCount is
// already clamped to >=1 in NewStreamConsumer, so this is a defensive
// floor — it guarantees the int -> uint32 conversion can never see a
// negative value, regardless of how the field is set in the future.
func boundedWorkerCount(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

// processOne handles one newly read message and settles it: acknowledged on
// success, even when shutdown began during the handler; dead-lettered then
// acknowledged on a permanent error; otherwise left pending for the reclaim
// sweep. A message whose handling is cut short by shutdown stays pending.
// Whatever the outcome, the message leaves the in-hand set when processOne
// returns, so the holder no longer re-claims it.
//
// Acknowledging per message (rather than once per batch) preserves
// ack-after-success under the worker pool: a completed message leaves the PEL
// immediately, so a slow sibling — or a stuck lane when workerCount>1 — can
// never keep finished work pending long enough for another replica's reclaim
// sweep to pick it up again.
func (c *StreamConsumer) processOne(ctx context.Context, msg goredis.XMessage) {
	defer c.inHand.remove(msg.ID)
	msg, ok := c.admit(ctx, msg)
	if !ok {
		return
	}
	class, err := c.run(ctx, msg, readPathSchedule)
	switch {
	case err == nil:
		c.ackHandled(ctx, msg.ID)
	case ctx.Err() != nil:
		// Stopping: the message stays pending and is redelivered after restart.
	case class == ClassPermanent:
		if c.noDeadLetters {
			c.logger.Error("Permanent failure on a consumer that never dead-letters — leaving in PEL",
				"stream", c.streamName, "message_id", msg.ID, "error", err)
			return
		}
		c.deadLetterAndAck(ctx, msg, model.DeadLetterKindPermanent, err, 1)
	default:
		c.logger.Error("Message still failing after in-process retries — leaving in PEL for the reclaim sweep",
			"stream", c.streamName, "message_id", msg.ID, "error", err)
	}
}

// admit decides whether msg reaches the handler. An entry with no fields (one
// trimmed from the stream after it was delivered) and a redriven entry
// addressed to another group are acknowledged without a handler. It returns
// the message to handle, with any redrive fields removed.
func (c *StreamConsumer) admit(ctx context.Context, msg goredis.XMessage) (goredis.XMessage, bool) {
	if len(msg.Values) == 0 {
		c.logger.Warn("Pending entry no longer in the stream — acknowledging",
			"stream", c.streamName, "group", c.consumerGroup, "message_id", msg.ID)
		c.ackHandled(ctx, msg.ID)
		return msg, false
	}
	routed, ok := routeRedrive(msg, c.consumerGroup)
	if !ok {
		c.ackHandled(ctx, msg.ID)
		return msg, false
	}
	if _, redriven := msg.Values[RedrivenFromField]; redriven {
		c.logger.Info("Handling a redriven entry", "stream", c.streamName, "group", c.consumerGroup,
			"message_id", msg.ID, "redriven_from", msg.Values[RedrivenFromField])
	}
	return routed, true
}

// ackOne acknowledges a single message, logging and returning the error on
// failure. A failed XACK is non-fatal: the message stays in the PEL and the
// reclaim sweep redelivers it — the returned error lets deadLetterAndAck hold
// back the drop notification until the ACK is confirmed.
func (c *StreamConsumer) ackOne(ctx context.Context, id string) error {
	if err := c.client.XAck(ctx, c.key, c.consumerGroup, id).Err(); err != nil {
		c.logger.Error("Failed to ACK message",
			"stream", c.streamName,
			"key", c.key,
			"message_id", id,
			"error", err,
		)
		return err
	}
	return nil
}
