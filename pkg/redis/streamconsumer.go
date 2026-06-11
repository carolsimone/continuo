package redis

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"strings"
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
	for _, opt := range opts {
		opt(c)
	}
	if c.workerCount < 1 {
		c.workerCount = 1
	}
	return c
}

// reclaimInterval is how often the consumer re-scans the PEL for messages
// abandoned by *other* consumer instances (crash recovery). Transient handler
// errors are retried in-process by invokeWithRetry before falling through to
// the PEL, so this interval no longer bounds same-instance retry latency.
const reclaimInterval = 2 * time.Minute

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
	if err := c.ensureConsumerGroup(ctx); err != nil {
		return err
	}
	c.logger.Info("Starting consumer",
		"stream", c.streamName,
		"group", c.consumerGroup,
		"consumer", c.consumerName,
		"workers", c.workerCount,
	)

	// Reclaim pending messages left by previous consumer instances that
	// crashed before ACKing (crash recovery).
	if err := c.reclaimPending(ctx); err != nil {
		c.logger.Error("Failed to reclaim pending messages", "stream", c.streamName, "error", err)
	}

	reclaimTicker := time.NewTicker(reclaimInterval)
	defer reclaimTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-reclaimTicker.C:
			if err := c.reclaimPending(ctx); err != nil {
				c.logger.Error("Periodic reclaim pending failed", "stream", c.streamName, "error", err)
			}
		default:
			if err := c.readAndProcess(ctx); err != nil {
				c.logger.Error("Error in read loop", "error", err)
				time.Sleep(3 * time.Second)
			}
		}
	}
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
		var ackIDs []string
		if c.workerCount <= 1 {
			ackIDs = c.processSerial(ctx, stream.Messages)
		} else {
			ackIDs = c.processSharded(ctx, stream.Messages)
		}
		c.ackBatch(ctx, ackIDs)
	}

	return nil
}

// processSerial runs the handler over the batch one message at a time in stream
// order and returns the IDs that must be ACKed. This is the workerCount==1 path
// and is behaviourally identical to the original strictly-serial loop.
func (c *StreamConsumer) processSerial(ctx context.Context, msgs []goredis.XMessage) []string {
	var ackIDs []string
	for _, msg := range msgs {
		if c.shouldAck(ctx, msg) {
			ackIDs = append(ackIDs, msg.ID)
		}
	}
	return ackIDs
}

// processSharded fans the batch across workerCount lanes keyed by the aggregate
// key so messages for one aggregate stay strictly ordered (same key → same lane
// → processed in arrival order) while distinct aggregates run in parallel. Each
// lane drains its messages in order; the batch is fully processed before this
// returns, so the read loop never runs ahead of completed work and the existing
// ack-after-success / PEL semantics are preserved. Returns the IDs to ACK.
func (c *StreamConsumer) processSharded(ctx context.Context, msgs []goredis.XMessage) []string {
	if len(msgs) == 0 {
		return nil
	}

	lanes := make([][]goredis.XMessage, c.workerCount)
	for _, msg := range msgs {
		lane := c.laneFor(msg)
		lanes[lane] = append(lanes[lane], msg)
	}

	results := make([][]string, c.workerCount)
	done := make(chan int, c.workerCount)
	active := 0
	for i := range lanes {
		if len(lanes[i]) == 0 {
			continue
		}
		active++
		go func(idx int) {
			var laneAcks []string
			for _, msg := range lanes[idx] {
				if c.shouldAck(ctx, msg) {
					laneAcks = append(laneAcks, msg.ID)
				}
			}
			results[idx] = laneAcks
			done <- idx
		}(i)
	}
	for ; active > 0; active-- {
		<-done
	}

	var ackIDs []string
	for _, laneAcks := range results {
		ackIDs = append(ackIDs, laneAcks...)
	}
	return ackIDs
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
	return int(h.Sum32() % uint32(c.workerCount))
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

// ackBatch acknowledges all the given message IDs in a single pipelined round
// trip instead of one XACK per message. ack-after-success semantics are
// unchanged: only IDs that the handler resolved (success or permanent drop) are
// passed in, so a transiently-failed message is never acked here.
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
