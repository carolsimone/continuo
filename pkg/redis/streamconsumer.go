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

// DropHandler is an optional callback invoked when the consumer abandons a
// message it could not process — a permanent handler error, or a poison message
// that exhausted its redelivery budget. It fires at the moment the message is
// ACK-dropped, carrying the message and the cause, so the owning service can
// finalize any in-flight state it committed for that message (which the drop
// itself leaves dangling, since the consumer records no state of its own). It is
// best-effort housekeeping, never on the message-processing critical path: it is
// invoked with panic recovery and its outcome does not affect the ACK.
type DropHandler func(ctx context.Context, msg goredis.XMessage, cause error)

// droppedEntry pairs a message the consumer is abandoning with the cause, held
// until its ACK is confirmed so the DropHandler is only notified for a message
// that actually left the PEL.
type droppedEntry struct {
	msg   goredis.XMessage
	cause error
}

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

	// ackFn acknowledges a single resolved message, returning an error when the
	// XACK itself failed (the message then stays in the PEL). It defaults to
	// ackOne (a real XACK) and is a seam so the lane-scheduling logic can be
	// unit-tested without a live Redis connection.
	ackFn func(ctx context.Context, id string) error

	// lastActivity is the unix-nano timestamp of the most recent unit of read-
	// loop progress, stored via atomic.Int64 so Healthy can be polled from the
	// HTTP health handler's goroutine without racing the loop. It advances once
	// per loop iteration AND once per handler attempt (see safeInvoke), so a
	// batch of messages — or a single legitimately-slow handler — keeps the
	// heartbeat fresh rather than freezing it for the whole readAndProcess call.
	// It advances regardless of outcome — including a failed read during a Redis
	// outage — because the point is distinguishing "the loop is alive and
	// retrying" from "the loop is wedged or has exited," not "the last read
	// succeeded." A Redis outage alone must never read as unhealthy here: the
	// read loop already retries indefinitely, so flagging that as unhealthy
	// would just add readiness-flap noise on top of a condition the consumer is
	// already handling correctly.
	lastActivity atomic.Int64

	// onDrop, when set, is invoked whenever the consumer abandons a message it
	// could not process (permanent error or poison quarantine). It is a seam for
	// the owning service to close out in-flight state the dropped message left
	// behind. nil (the default) preserves the pre-existing behaviour: a dropped
	// message is logged and ACKed with no further notification.
	onDrop DropHandler

	// handlerTimeout, when > 0, bounds each handler invocation with a context
	// deadline so a genuinely-hung handler eventually returns control to the
	// loop (which then iterates and re-advances the heartbeat). It is set once
	// before Start via SetHandlerTimeout, so it is read-only for the lifetime of
	// the running loop and needs no synchronisation. The liveness heartbeat-
	// stale budget each caller passes to Healthy MUST be larger than this
	// timeout plus a margin, so legitimate in-flight work never trips liveness
	// while a true wedge (a handler that ignores ctx and never returns) still
	// trips within budget. 0 (the default) leaves the handler unbounded, which
	// preserves the pre-existing behaviour for callers that do not opt in.
	handlerTimeout time.Duration
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

// WithHandlerTimeout bounds each handler invocation with a context deadline of
// d (see the handlerTimeout field). Pass d <= 0 to leave handlers unbounded
// (the default). Callers that wire the consumer's heartbeat into a liveness
// probe should set this and choose a heartbeat-stale budget larger than d.
func WithHandlerTimeout(d time.Duration) ConsumerOption {
	return func(c *StreamConsumer) { c.handlerTimeout = d }
}

// WithOnDrop registers a DropHandler invoked whenever the consumer abandons a
// message it could not process (see the onDrop field and DropHandler). Pass it
// when the handler commits in-flight state before it can fail — e.g. an
// in-flight status row — so that state is not orphaned when the message is
// finally dropped. The default (no option) leaves drops unnotified, exactly as
// before.
func WithOnDrop(fn DropHandler) ConsumerOption {
	return func(c *StreamConsumer) { c.onDrop = fn }
}

// SetHandlerTimeout sets the per-handler context-deadline budget (see the
// handlerTimeout field). It must be called before Start — callers that receive
// an already-constructed consumer (e.g. from a per-stream binding factory) use
// this to opt in without threading an option through every factory. The write
// happens-before the Start goroutine, so no synchronisation is required.
func (c *StreamConsumer) SetHandlerTimeout(d time.Duration) { c.handlerTimeout = d }

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

// maxDeliveries bounds how many times a message may be redelivered through the
// reclaim (PEL) sweep before it is treated as poison and ACK-dropped. A message
// that keeps failing every sweep — a handler bug, a payload the handler can
// never process, or a handler that repeatedly exceeds its timeout (whose error
// is context.DeadlineExceeded, NOT events.ErrPermanent, so it would otherwise
// never be dropped) — would cycle in the PEL forever, never ACKed and never
// surfaced. Past this bound the reclaim path drops it with a loud error log
// (same visibility as an ErrPermanent drop) so the loop keeps making progress.
// The comparison is against the XAUTOCLAIM/PEL delivery counter.
const maxDeliveries = 5

// onDropped notifies the registered DropHandler that a message is being
// abandoned, so the owning service can finalize in-flight state the drop leaves
// behind. It is nil-safe (no handler → no-op) and panic-safe (a panicking
// handler is recovered and logged rather than unwinding into the consumer loop);
// the drop itself has already been decided and logged by the caller, so a
// failing notification never changes whether the message is ACKed.
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

// safeInvoke calls the handler with panic recovery. A panicking handler would
// otherwise unwind through the consumer loop and kill the process; on restart
// the same message is re-delivered from the PEL and panics again — a permanent
// crash loop from one poison message. safeInvoke converts a recovered panic
// into a plain (non-permanent) error so the message stays in the PEL and is
// retried on the next sweep, exactly like any transient failure. The parser
// layer is the primary poison defense (it returns ErrPermanent before any
// panic can happen); this recover is defense in depth for future handlers.
func (c *StreamConsumer) safeInvoke(ctx context.Context, msg goredis.XMessage) (err error) {
	// Advance the heartbeat at each handler attempt (not just once per read-loop
	// iteration) so a batch of up to Count messages — or a single legitimately-
	// slow handler such as agent-remediation's LLM calls — keeps the liveness
	// heartbeat fresh instead of going stale mid-work and getting the pod
	// restarted underneath in-flight work.
	c.lastActivity.Store(time.Now().UnixNano())

	// Bound the handler with a deadline when configured so a genuinely-hung
	// handler eventually returns control to the loop. The caller's heartbeat-
	// stale budget is deliberately larger than this timeout, so this bounds
	// legitimate work without tripping liveness; a handler that ignores ctx and
	// never returns still trips liveness once the heartbeat goes stale.
	if c.handlerTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.handlerTimeout)
		defer cancel()
	}

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
	//
	// A PERMANENT bootstrap error (a wrong-typed stream key, an ACL/auth denial,
	// an unknown command) is the exception: no amount of retrying clears it, so
	// looping forever would keep the pod ready+live while it consumes nothing
	// and never fires WorkerExited. Those are returned instead, so the caller's
	// WorkerExited(name, err) records the failure — readiness fails and it is
	// logged loudly — rather than silently masking the misconfiguration.
	for {
		c.lastActivity.Store(time.Now().UnixNano())
		err := c.ensureConsumerGroup(ctx)
		if err == nil {
			break
		}
		if isPermanentBootstrapError(err) {
			c.logger.Error("Permanent consumer-group bootstrap failure — not retrying (surfacing to health)",
				"stream", c.streamName, "group", c.consumerGroup, "error", err)
			return fmt.Errorf("permanent consumer-group bootstrap failure for stream %q group %q: %w",
				c.streamName, c.consumerGroup, err)
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
// AddWorkerProbe). maxStale MUST exceed the consumer's handlerTimeout (see
// WithHandlerTimeout / SetHandlerTimeout) plus a margin, so in-flight work
// never trips liveness while a true wedge still does.
func (c *StreamConsumer) Healthy(maxStale time.Duration) error {
	// lastActivity is seeded in NewStreamConsumer, so it is never the zero value
	// for a real consumer; a zero-value consumer (only constructed in tests)
	// reads as "1970" and therefore stalled, which is the correct unhealthy
	// answer for something that never ran.
	lastAt := time.Unix(0, c.lastActivity.Load())
	if age := time.Since(lastAt); age > maxStale {
		return fmt.Errorf("stream consumer %q/%q: read loop stalled — no activity for %s (last at %s)",
			c.streamName, c.consumerGroup, age.Round(time.Second), lastAt.Format(time.RFC3339))
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
// CLUSTERDOWN, MASTERDOWN, TRYAGAIN, …) is likewise transient. BUSYGROUP never
// reaches here because ensureConsumerGroup treats it as success.
var permanentBootstrapErrorPrefixes = []string{
	"WRONGTYPE",
	"unknown command",
	"unknown subcommand",
}

// isPermanentBootstrapError reports whether an ensureConsumerGroup error is a
// permanent misconfiguration that retrying will never fix. It matches against
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
		// Drops are collected and notified only after ackBatch confirms the ACK:
		// a failed batch XACK leaves every id in the PEL to be reprocessed, so
		// finalizing an abandoned message's in-flight state before then would
		// abandon a message that was never actually dropped.
		var dropped []droppedEntry
		for _, msg := range msgs {
			err := c.safeInvoke(ctx, msg)
			if err == nil {
				ackIDs = append(ackIDs, msg.ID)
				continue
			}
			if errors.Is(err, events.ErrPermanent) {
				c.logger.Error("Permanent handler error — ACKing to drop from PEL",
					"message_id", msg.ID,
					"error", err,
				)
				dropped = append(dropped, droppedEntry{msg: msg, cause: err})
				ackIDs = append(ackIDs, msg.ID)
				continue
			}
			// Transient failure. Quarantine a poison message that has been
			// redelivered past the bound so it cannot cycle in the PEL forever
			// — this is the general safety net that also catches a handler
			// which keeps timing out (DeadlineExceeded is not ErrPermanent).
			if n := c.deliveryCount(ctx, msg.ID); n > maxDeliveries {
				c.logger.Error("Poison message exceeded max deliveries — ACK-dropping to break the redelivery loop",
					"stream", c.streamName,
					"message_id", msg.ID,
					"deliveries", n,
					"max_deliveries", maxDeliveries,
					"handler_timeout", isHandlerTimeout(err),
					"error", err,
				)
				dropped = append(dropped, droppedEntry{msg: msg, cause: err})
				ackIDs = append(ackIDs, msg.ID)
				continue
			}
			c.logger.Error("Reclaimed message still failing — leaving in PEL for next sweep",
				"stream", c.streamName,
				"message_id", msg.ID,
				"handler_timeout", isHandlerTimeout(err),
				"error", err,
			)
			// no-ACK; next periodic sweep is the retry cadence
		}
		if c.ackBatch(ctx, ackIDs) == nil {
			for _, d := range dropped {
				c.onDropped(ctx, d.msg, d.cause)
			}
		}

		if next == "0-0" {
			return nil
		}
		cursor = next
	}
}

// deliveryCount returns the PEL delivery counter for a single message — how many
// times it has been delivered or claimed without being ACKed. A read failure
// returns 0, which is deliberately conservative: it never triggers a false
// quarantine, so a transiently-unreadable PEL just leaves the message for the
// next sweep instead of dropping live work.
func (c *StreamConsumer) deliveryCount(ctx context.Context, id string) int64 {
	pend, err := c.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: c.streamName,
		Group:  c.consumerGroup,
		Start:  id,
		End:    id,
		Count:  1,
	}).Result()
	if err != nil {
		c.logger.Warn("Could not read PEL delivery count — not quarantining this sweep",
			"stream", c.streamName, "message_id", id, "error", err)
		return 0
	}
	if len(pend) == 0 {
		return 0
	}
	return pend[0].RetryCount
}

// isHandlerTimeout reports whether an error came from the per-handler context
// deadline (see handlerTimeout / safeInvoke), so a handler that ran out of time
// is logged distinctly from a generic transient failure.
func isHandlerTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
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
//
// A drop (permanent error) is notified only after the ACK is confirmed: a failed
// XACK leaves the message in the PEL to be reprocessed, so finalizing its
// in-flight state then would abandon a message that was never actually dropped.
func (c *StreamConsumer) processOne(ctx context.Context, msg goredis.XMessage) {
	ack, dropCause := c.shouldAck(ctx, msg)
	if !ack {
		return
	}
	if err := c.ackFn(ctx, msg.ID); err != nil {
		return
	}
	if dropCause != nil {
		c.onDropped(ctx, msg, dropCause)
	}
}

// ackOne acknowledges a single message, logging and returning the error on
// failure. A failed XACK is non-fatal: the message stays in the PEL and the
// reclaim sweep redelivers it — the returned error lets the caller hold back a
// drop notification until the ACK is confirmed.
func (c *StreamConsumer) ackOne(ctx context.Context, id string) error {
	if err := c.client.XAck(ctx, c.streamName, c.consumerGroup, id).Err(); err != nil {
		c.logger.Error("Failed to ACK message",
			"stream", c.streamName,
			"message_id", id,
			"error", err,
		)
		return err
	}
	return nil
}

// shouldAck runs the read-path handler (with inline retry) for one message and
// reports whether it must be ACKed and, when the ACK is a drop, the cause to
// notify once the ACK is confirmed. The results are: (true, nil) on success,
// (true, err) on a permanent error (drop the poison message — the caller
// notifies after ACKing), (false, nil) on an exhausted transient failure (leave
// it in the PEL for the reclaim sweep).
func (c *StreamConsumer) shouldAck(ctx context.Context, msg goredis.XMessage) (bool, error) {
	err := c.invokeWithRetry(ctx, msg)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, events.ErrPermanent) {
		c.logger.Error("Permanent handler error — ACKing to drop from PEL",
			"message_id", msg.ID,
			"error", err,
		)
		return true, err
	}
	if isHandlerTimeout(err) {
		c.logger.Error("Handler exceeded its timeout — leaving in PEL for the reclaim sweep",
			"stream", c.streamName, "message_id", msg.ID, "error", err)
	} else {
		c.logger.Error("Message still failing after in-process retries", "message_id", msg.ID, "error", err)
	}
	return false, nil // no-ACK; PEL sweep remains the safety net (and eventually quarantines poison)
}

// ackBatch acknowledges a page of reclaimed message IDs in a single pipelined
// round trip, returning the error when the XACK failed. Only IDs the handler
// resolved (success or permanent drop) are passed in, so a transiently-failed
// reclaimed message is never acked here. The read path acks per message
// (processOne); reclaimed entries are already past their idle gate and processed
// single-shot within one sweep, so a per-page ack carries none of the read
// path's hold-finished-work-in-PEL risk. The returned error lets the caller hold
// back drop notifications until the ACK is confirmed.
func (c *StreamConsumer) ackBatch(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := c.client.XAck(ctx, c.streamName, c.consumerGroup, ids...).Err(); err != nil {
		c.logger.Error("Failed to ACK message batch",
			"stream", c.streamName,
			"count", len(ids),
			"error", err,
		)
		return err
	}
	return nil
}
