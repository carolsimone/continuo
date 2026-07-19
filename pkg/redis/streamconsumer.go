package redis

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
)

// MessageHandler is a callback invoked for each message read from a stream
type MessageHandler func(ctx context.Context, msg goredis.XMessage) error

// StreamConsumer is a generic Redis Streams consumer that delegates message
// processing to a MessageHandler callback
type StreamConsumer struct {
	client         *goredis.Client
	streamName     string
	consumerGroup  string
	consumerName   string
	handler        MessageHandler
	logger         *slog.Logger
	reclaimMinIdle time.Duration

	// workerCount is the number of parallel processing lanes. The default is 1,
	// which preserves strictly-serial per-stream processing (today's behaviour).
	// When >1, messages are sharded across workerCount lanes by a hash of their
	// aggregate key so messages for one aggregate stay strictly ordered while
	// distinct aggregates process in parallel.
	workerCount int
	// aggregateKeyField is the message Values field whose value identifies the
	// aggregate (e.g. "schedule_id"). Messages with the same value land on the
	// same worker lane. An empty/absent value hashes to a stable lane so order
	// is never violated, only parallelism is forgone for that message.
	aggregateKeyField string

	// ackFn acknowledges a single resolved message. It defaults to ackOne (a
	// real XACK) and is a seam so the lane-scheduling logic can be unit-tested
	// without a live Redis connection.
	ackFn func(ctx context.Context, id string)

	// lastActivity is the unix-nano timestamp of the most recent read-loop
	// iteration, stored via atomic.Int64 so Healthy can be polled from the HTTP
	// health handler's goroutine without racing Start's loop. It advances once
	// per iteration regardless of outcome — including a failed read during a
	// Redis outage — because the point is distinguishing "the loop is alive and
	// retrying" from "the loop is wedged or has exited," not "the last read
	// succeeded." A Redis outage alone must never read as unhealthy here: the
	// read loop already retries indefinitely, so flagging that as unhealthy
	// would just add readiness-flap noise on top of a condition the consumer is
	// already handling correctly.
	lastActivity atomic.Int64
}

// ConsumerOption tunes optional behaviour on a StreamConsumer.
type ConsumerOption func(*StreamConsumer)

// defaultReclaimMinIdle is the MinIdle gate applied to the periodic PEL sweep.
// At 30s it is large enough that a healthy peer replica's in-flight message —
// including its in-process retry budget — will never be stolen by another
// replica's sweep, and small enough that a crashed consumer's PEL entry is
// recovered well within the 2-minute reclaim cadence.
const defaultReclaimMinIdle = 30 * time.Second

// WithReclaimMinIdle overrides the minimum idle time a pending entry must have
// accumulated before this consumer's reclaim sweep is allowed to claim it. The
// default is conservative (30s) for production safety against multi-replica
// stealing; tests that exercise the reclaim path inside a single process
// typically pass 0 to disable the gate.
func WithReclaimMinIdle(d time.Duration) ConsumerOption {
	return func(c *StreamConsumer) { c.reclaimMinIdle = d }
}

// WithWorkerPool enables bounded parallel processing across n lanes, sharding
// messages by a hash of the aggregateKeyField value so that all messages for a
// given aggregate remain strictly ordered (same key → same lane → FIFO) while
// distinct aggregates process concurrently. n is the number of lanes and
// aggregateKeyField is the message Values field that names the aggregate (for
// example "schedule_id"). n <= 1 is the default and keeps today's exact
// strictly-serial behaviour, so a binding opts into parallelism deliberately.
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

// consumerName derives a stable, per-pod consumer name. Reusing the same name
// across process restarts means the consumer group registry does not grow an
// orphaned entry every restart; the restarted process re-attaches to its own
// PEL instead of leaking a dead consumer whose pending entries only the reclaim
// sweep would recover. The hostname is the pod identity under Kubernetes; if it
// is unavailable we fall back to a time-seeded name (the pre-existing
// behaviour) so the consumer still starts.
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
		client:         client,
		streamName:     streamName,
		consumerGroup:  consumerGroup,
		consumerName:   consumerName(consumerGroup),
		handler:        handler,
		logger:         logger,
		reclaimMinIdle: defaultReclaimMinIdle,
		workerCount:    1,
	}
	c.ackFn = c.ackOne
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

// reclaimInterval is how often the consumer re-scans the PEL for messages
// abandoned by *other* consumer instances (crash recovery). Transient handler
// errors are retried in-process by invokeWithRetry before falling through to
// the PEL, so this interval no longer bounds same-instance retry latency.
const reclaimInterval = 2 * time.Minute

// staleConsumerIdle is how long a zero-pending consumer must have been idle
// before cleanupStaleConsumers deletes its registry entry. A live consumer
// re-reads at most every read block, so anything idle this long with nothing
// pending is a consumer left behind by a replaced pod (a Kubernetes rollout
// gives each new pod a fresh hostname, hence a new consumer name). Set well
// above the reclaim interval so a peer mid-sweep is never mistaken for dead.
const staleConsumerIdle = 2 * reclaimInterval

// transientRetryBackoffs is the inline retry schedule applied to non-permanent
// handler errors before the consumer gives up and leaves the message un-ACKed
// for the periodic PEL sweep. One quick retry then park: a single bad message
// can stall its lane for at most this budget before falling through to the PEL,
// which the reclaim sweep redelivers. Keeping it short avoids head-of-line
// blocking behind a transiently-failing message.
var transientRetryBackoffs = []time.Duration{
	0,
	100 * time.Millisecond,
}

// safeInvoke calls the handler with panic recovery. A panicking handler would
// otherwise unwind through the consumer loop and kill the process; on restart
// the same message is re-delivered from the PEL and panics again — a permanent
// crash loop from one poison message. safeInvoke converts a recovered panic
// into a plain (non-permanent) error so the message stays in the PEL and is
// retried on the next sweep, exactly like any transient failure. The parser
// layer is the primary poison defense (it returns ErrPermanent before any
// panic can happen); this recover is defense in depth for future handlers.
func (c *StreamConsumer) safeInvoke(ctx context.Context, msg goredis.XMessage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("Handler panicked — recovered to keep the consumer alive",
				"stream", c.streamName,
				"message_id", msg.ID,
				"panic", r,
			)
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return c.handler(ctx, msg)
}

// invokeWithRetry calls the handler with bounded retries on transient errors.
// ErrPermanent short-circuits the loop; context cancellation aborts it. The
// final error (or nil) is returned to the caller, which decides whether to
// ACK based on the existing ErrPermanent classification.
func (c *StreamConsumer) invokeWithRetry(ctx context.Context, msg goredis.XMessage) error {
	var err error
	for attempt, delay := range transientRetryBackoffs {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			c.logger.Warn("Retrying transient handler error",
				"stream", c.streamName,
				"message_id", msg.ID,
				"attempt", attempt+1,
				"previous_error", err,
			)
		}
		err = c.safeInvoke(ctx, msg)
		if err == nil || errors.Is(err, events.ErrPermanent) {
			return err
		}
	}
	return err
}

// Start begins consuming messages from the Redis stream until the context is cancelled
func (c *StreamConsumer) Start(ctx context.Context) error {
	// Bootstrap the consumer group with the same "log and retry" resilience as
	// the read loop below, rather than returning on the first failure. Without
	// this, a Redis outage that happens to overlap process startup (a pod boots
	// while Redis is mid-restart, or a rollout races a brief Redis blip) makes
	// Start return a single error and exit for good — every caller in this repo
	// launches Start once in a goroutine and never calls it again on failure, so
	// that one-shot failure would permanently kill the consumer even though the
	// read loop it never reached is fully capable of surviving that same outage.
	for {
		c.lastActivity.Store(time.Now().UnixNano())
		err := c.ensureConsumerGroup(ctx)
		if err == nil {
			break
		}
		c.logger.Error("Failed to ensure consumer group at startup — retrying",
			"stream", c.streamName, "group", c.consumerGroup, "error", err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(3 * time.Second):
		}
	}
	c.logger.Info("Starting consumer",
		"stream", c.streamName,
		"group", c.consumerGroup,
		"consumer", c.consumerName,
		"workers", c.workerCount,
	)

	// Reclaim pending messages left by previous consumer instances that
	// crashed before ACKing (crash recovery), then delete the now-drained
	// registry entries those instances left behind.
	if err := c.reclaimPending(ctx); err != nil {
		c.logger.Error("Failed to reclaim pending messages", "stream", c.streamName, "error", err)
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
				c.logger.Error("Periodic reclaim pending failed", "stream", c.streamName, "error", err)
			}
			c.cleanupStaleConsumers(ctx, staleConsumerIdle)
		default:
			if err := c.readAndProcess(ctx); err != nil {
				c.logger.Error("Error in read loop", "error", err)
				time.Sleep(3 * time.Second)
			}
		}
	}
}

// Healthy reports whether the read loop has iterated within maxStale. It
// returns nil whenever the loop is cycling — including throughout a Redis
// outage, since the loop's own retry-with-backoff already handles that case
// and a transient dial error is not itself a liveness failure. A non-nil
// error means the goroutine has stopped making progress: wedged in a call
// that never returns control (no context deadline on the path that hung), or
// it exited some way other than Start's normal ctx.Done() return. That is
// exactly the failure mode an HTTP liveness probe cannot see on its own — the
// process and its HTTP server stay up while a consumer goroutine is dead — so
// callers should wire this into a liveness/readiness registry probe (see
// pkg/liveness) rather than relying on "is the HTTP server up."
func (c *StreamConsumer) Healthy(maxStale time.Duration) error {
	last := c.lastActivity.Load()
	if last == 0 {
		return fmt.Errorf("stream consumer %q/%q: read loop has not started", c.streamName, c.consumerGroup)
	}
	lastAt := time.Unix(0, last)
	if age := time.Since(lastAt); age > maxStale {
		return fmt.Errorf("stream consumer %q/%q: read loop stalled — no activity for %s (last at %s)",
			c.streamName, c.consumerGroup, age.Round(time.Second), lastAt.Format(time.RFC3339))
	}
	return nil
}

func (c *StreamConsumer) ensureConsumerGroup(ctx context.Context) error {
	err := c.client.XGroupCreateMkStream(ctx, c.streamName, c.consumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}
	return nil
}

// reclaimPending claims and reprocesses messages left in the pending entry list
// (PEL) by consumers other than this one — typically a previous instance that
// crashed before ACKing. Only entries idle for at least reclaimMinIdle are
// eligible; this prevents a parallel replica from stealing a peer's in-flight
// message during a periodic sweep.
//
// Handler invocations here are **single-shot**: a PEL entry either landed here
// because a prior owner already burned its inline retry budget on the read
// path, or because that owner crashed. Re-running the read path's retry
// schedule inside the sweep would (a) head-of-line-block the read loop, and
// (b) duplicate work for the common case where a single attempt under the new
// owner already succeeds. If the single attempt fails, the entry stays in the
// PEL and the next sweep (≤ reclaimInterval) becomes the retry cadence.
//
// Implementation note: XAUTOCLAIM (Redis 6.2+) replaces the older XPENDING +
// per-ID XCLAIM loop, collapsing 1+N round-trips into a single cursor-paged
// command per page of up to 100 entries.
func (c *StreamConsumer) reclaimPending(ctx context.Context) error {
	cursor := "0-0"
	for {
		msgs, next, err := c.client.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
			Stream:   c.streamName,
			Group:    c.consumerGroup,
			Consumer: c.consumerName,
			MinIdle:  c.reclaimMinIdle,
			Start:    cursor,
			Count:    100,
		}).Result()
		if err != nil {
			return fmt.Errorf("XAUTOCLAIM failed: %w", err)
		}

		if len(msgs) > 0 {
			c.logger.Warn("Reclaiming pending messages from previous consumers",
				"stream", c.streamName,
				"count", len(msgs),
				"min_idle", c.reclaimMinIdle,
			)
		}

		var ackIDs []string
		for _, msg := range msgs {
			if err := c.safeInvoke(ctx, msg); err != nil {
				if errors.Is(err, events.ErrPermanent) {
					c.logger.Error("Permanent handler error — ACKing to drop from PEL",
						"message_id", msg.ID,
						"error", err,
					)
					// fall through to ACK
				} else {
					c.logger.Error("Reclaimed message still failing — leaving in PEL for next sweep",
						"message_id", msg.ID,
						"error", err,
					)
					continue // no-ACK; next periodic sweep is the retry cadence
				}
			}
			ackIDs = append(ackIDs, msg.ID)
		}
		c.ackBatch(ctx, ackIDs)

		if next == "0-0" {
			return nil
		}
		cursor = next
	}
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
	consumers, err := c.client.XInfoConsumers(ctx, c.streamName, c.consumerGroup).Result()
	if err != nil {
		c.logger.Warn("Could not list consumers for cleanup", "stream", c.streamName, "error", err)
		return
	}
	for _, cons := range consumers {
		if cons.Name == c.consumerName || cons.Pending > 0 || cons.Idle < minIdle {
			continue
		}
		if err := c.client.XGroupDelConsumer(ctx, c.streamName, c.consumerGroup, cons.Name).Err(); err != nil {
			c.logger.Warn("Failed to delete stale consumer",
				"stream", c.streamName,
				"consumer", cons.Name,
				"error", err,
			)
			continue
		}
		c.logger.Info("Removed drained stale consumer",
			"stream", c.streamName,
			"consumer", cons.Name,
			"idle", cons.Idle,
		)
	}
}

func (c *StreamConsumer) readAndProcess(ctx context.Context) error {
	streams, err := c.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    c.consumerGroup,
		Consumer: c.consumerName,
		Streams:  []string{c.streamName, ">"},
		Count:    10,
		Block:    1 * time.Second,
	}).Result()

	if err != nil {
		if err == goredis.Nil {
			return nil
		}
		if strings.Contains(err.Error(), "NOGROUP") {
			return c.ensureConsumerGroup(ctx)
		}
		return fmt.Errorf("failed to read from stream: %w", err)
	}

	for _, stream := range streams {
		if c.workerCount <= 1 {
			c.processSerial(ctx, stream.Messages)
		} else {
			c.processSharded(ctx, stream.Messages)
		}
	}

	return nil
}

// processSerial runs the handler over the batch one message at a time in stream
// order, ACKing each message the moment it resolves. This is the workerCount==1
// path and is behaviourally identical to the original strictly-serial loop.
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

// processOne runs the read-path handler for one message and ACKs it as soon as
// it resolves to an ACK-able state. ACKing per message (rather than once per
// batch) is what preserves ack-after-success under the worker pool: a completed
// message leaves the PEL immediately, so a slow sibling — or a stuck lane when
// workerCount>1 — can never keep finished work pending long enough for another
// replica's reclaim sweep to pick it up again.
func (c *StreamConsumer) processOne(ctx context.Context, msg goredis.XMessage) {
	if c.shouldAck(ctx, msg) {
		c.ackFn(ctx, msg.ID)
	}
}

// ackOne acknowledges a single message, logging on failure. A failed XACK is
// non-fatal: the message stays in the PEL and the reclaim sweep redelivers it.
func (c *StreamConsumer) ackOne(ctx context.Context, id string) {
	if err := c.client.XAck(ctx, c.streamName, c.consumerGroup, id).Err(); err != nil {
		c.logger.Error("Failed to ACK message",
			"stream", c.streamName,
			"message_id", id,
			"error", err,
		)
	}
}

// shouldAck runs the read-path handler (with inline retry) for one message and
// reports whether it must be ACKed: true on success or a permanent error
// (drop the poison message), false on an exhausted transient failure (leave it
// in the PEL for the reclaim sweep).
func (c *StreamConsumer) shouldAck(ctx context.Context, msg goredis.XMessage) bool {
	err := c.invokeWithRetry(ctx, msg)
	if err == nil {
		return true
	}
	if errors.Is(err, events.ErrPermanent) {
		c.logger.Error("Permanent handler error — ACKing to drop from PEL",
			"message_id", msg.ID,
			"error", err,
		)
		return true
	}
	c.logger.Error("Message still failing after in-process retries", "message_id", msg.ID, "error", err)
	return false // no-ACK; PEL sweep remains the safety net
}

// ackBatch acknowledges a page of reclaimed message IDs in a single pipelined
// round trip. Only IDs the handler resolved (success or permanent drop) are
// passed in, so a transiently-failed reclaimed message is never acked here. The
// read path acks per message (processOne); reclaimed entries are already past
// their idle gate and processed single-shot within one sweep, so a per-page ack
// carries none of the read path's hold-finished-work-in-PEL risk.
func (c *StreamConsumer) ackBatch(ctx context.Context, ids []string) {
	if len(ids) == 0 {
		return
	}
	if err := c.client.XAck(ctx, c.streamName, c.consumerGroup, ids...).Err(); err != nil {
		c.logger.Error("Failed to ACK message batch",
			"stream", c.streamName,
			"count", len(ids),
			"error", err,
		)
	}
}
