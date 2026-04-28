package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// MessageHandler is a callback invoked for each message read from a stream
type MessageHandler func(ctx context.Context, msg goredis.XMessage) error

// StreamConsumer is a generic Redis Streams consumer that delegates message
// processing to a MessageHandler callback
type StreamConsumer struct {
	client        *goredis.Client
	streamName    string
	consumerGroup string
	consumerName  string
	handler       MessageHandler
	logger        *slog.Logger
}

// NewStreamConsumer creates a new StreamConsumer
func NewStreamConsumer(
	client *goredis.Client,
	streamName, consumerGroup string,
	handler MessageHandler,
	logger *slog.Logger,
) *StreamConsumer {
	consumerName := fmt.Sprintf("%s-%d", consumerGroup, time.Now().UnixNano())
	return &StreamConsumer{
		client:        client,
		streamName:    streamName,
		consumerGroup: consumerGroup,
		consumerName:  consumerName,
		handler:       handler,
		logger:        logger,
	}
}

// reclaimInterval is how often the consumer re-scans the PEL for messages whose
// handler returned an error (and were therefore never ACKed). A periodic sweep
// ensures that transient handler failures do not leave messages stuck forever —
// without it, PEL entries are only reclaimed at service startup.
const reclaimInterval = 2 * time.Minute

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
// (PEL) by any consumer in the group. This covers messages delivered to a
// previous ephemeral consumer name that crashed before ACKing.
func (c *StreamConsumer) reclaimPending(ctx context.Context) error {
	pending, err := c.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: c.streamName,
		Group:  c.consumerGroup,
		Start:  "-",
		End:    "+",
		Count:  100,
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to get pending messages: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}

	c.logger.Warn("Reclaiming pending messages from previous consumers",
		"stream", c.streamName,
		"count", len(pending),
	)

	for _, p := range pending {
		msgs, err := c.client.XClaim(ctx, &goredis.XClaimArgs{
			Stream:   c.streamName,
			Group:    c.consumerGroup,
			Consumer: c.consumerName,
			MinIdle:  0,
			Messages: []string{p.ID},
		}).Result()
		if err != nil {
			c.logger.Error("Failed to claim message",
				"stream", c.streamName,
				"message_id", p.ID,
				"error", err,
			)
			continue
		}

		for _, msg := range msgs {
			if err := c.handler(ctx, msg); err != nil {
				c.logger.Error("Failed to process reclaimed message",
					"message_id", msg.ID,
					"error", err,
				)
				continue
			}
			if err := c.client.XAck(ctx, c.streamName, c.consumerGroup, msg.ID).Err(); err != nil {
				c.logger.Error("Failed to ACK reclaimed message",
					"message_id", msg.ID,
					"error", err,
				)
			}
		}
	}

	return nil
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
			if err := c.handler(ctx, msg); err != nil {
				c.logger.Error("Failed to process message", "message_id", msg.ID, "error", err)
				continue
			}
			if err := c.client.XAck(ctx, c.streamName, c.consumerGroup, msg.ID).Err(); err != nil {
				c.logger.Error("Failed to ACK message", "message_id", msg.ID, "error", err)
			}
		}
	}

	return nil
}
