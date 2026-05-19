package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

// NewStreamConsumer creates a new StreamConsumer
func NewStreamConsumer(
	client *goredis.Client,
	streamName, consumerGroup string,
	handler MessageHandler,
	logger *slog.Logger,
	opts ...ConsumerOption,
) *StreamConsumer {
	consumerName := fmt.Sprintf("%s-%d", consumerGroup, time.Now().UnixNano())
	c := &StreamConsumer{
		client:         client,
		streamName:     streamName,
		consumerGroup:  consumerGroup,
		consumerName:   consumerName,
		handler:        handler,
		logger:         logger,
		reclaimMinIdle: defaultReclaimMinIdle,
	}
	for _, opt := range opts {
		opt(c)
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
// for the periodic PEL sweep. Total budget ≈ 2.6s.
var transientRetryBackoffs = []time.Duration{
	0,
	100 * time.Millisecond,
	500 * time.Millisecond,
	2 * time.Second,
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
		err = c.handler(ctx, msg)
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
	c.logger.Info("Starting consumer", "stream", c.streamName, "group", c.consumerGroup)

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
// schedule inside the sweep would (a) head-of-line-block the read loop for up
// to ~2.6s per pending entry, and (b) duplicate work for the common case where
// a single attempt under the new owner already succeeds. If the single attempt
// fails, the entry stays in the PEL and the next sweep (≤ reclaimInterval)
// becomes the retry cadence.
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

		for _, msg := range msgs {
			if err := c.handler(ctx, msg); err != nil {
				if errors.Is(err, events.ErrPermanent) {
					c.logger.Error("Permanent handler error — ACKing to drop from PEL",
						"message_id", msg.ID,
						"error", err,
					)
					// fall through to XAck
				} else {
					c.logger.Error("Reclaimed message still failing — leaving in PEL for next sweep",
						"message_id", msg.ID,
						"error", err,
					)
					continue // no-ACK; next periodic sweep is the retry cadence
				}
			}
			if err := c.client.XAck(ctx, c.streamName, c.consumerGroup, msg.ID).Err(); err != nil {
				c.logger.Error("Failed to ACK reclaimed message",
					"message_id", msg.ID,
					"error", err,
				)
			}
		}

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
		for _, msg := range stream.Messages {
			if err := c.invokeWithRetry(ctx, msg); err != nil {
				if errors.Is(err, events.ErrPermanent) {
					c.logger.Error("Permanent handler error — ACKing to drop from PEL",
						"message_id", msg.ID,
						"error", err,
					)
					// fall through to XAck
				} else {
					c.logger.Error("Message still failing after in-process retries", "message_id", msg.ID, "error", err)
					continue // no-ACK; PEL sweep remains the safety net
				}
			}
			if err := c.client.XAck(ctx, c.streamName, c.consumerGroup, msg.ID).Err(); err != nil {
				c.logger.Error("Failed to ACK message", "message_id", msg.ID, "error", err)
			}
		}
	}

	return nil
}
