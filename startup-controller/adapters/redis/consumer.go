package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/carolsimone/continuo/startup-controller/domain/command"
	"github.com/carolsimone/continuo/startup-controller/service/messagebus"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// Consumer consumes scheduler.started events from Redis Streams
type Consumer struct {
	client        *goredis.Client
	streamName    string
	consumerGroup string
	consumerName  string
	messageBus    *messagebus.MessageBus
	logger        *slog.Logger
}

// NewConsumer creates a new Redis stream consumer
func NewConsumer(
	client *goredis.Client,
	streamName string,
	consumerGroup string,
	messageBus *messagebus.MessageBus,
	logger *slog.Logger,
) (*Consumer, error) {
	consumerName := fmt.Sprintf("consumer-%s", uuid.New().String()[:8])

	c := &Consumer{
		client:        client,
		streamName:    streamName,
		consumerGroup: consumerGroup,
		consumerName:  consumerName,
		messageBus:    messageBus,
		logger:        logger,
	}

	// Create consumer group if it doesn't exist
	if err := c.createConsumerGroup(); err != nil {
		return nil, err
	}

	return c, nil
}

// createConsumerGroup creates the consumer group or logs if it already exists
func (c *Consumer) createConsumerGroup() error {
	ctx := context.Background()

	err := c.client.XGroupCreateMkStream(ctx, c.streamName, c.consumerGroup, "0").Err()
	if err != nil {
		if err.Error() == "BUSYGROUP Consumer Group name already exists" {
			c.logger.Debug("Consumer group already exists", "group", c.consumerGroup)
			return nil
		}
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	c.logger.Info("Consumer group created",
		"group", c.consumerGroup,
		"stream", c.streamName,
	)
	return nil
}

// Start begins consuming messages from the Redis stream
func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Info("Starting consumer",
		"name", c.consumerName,
		"stream", c.streamName,
		"group", c.consumerGroup,
	)

	if err := c.reclaimPending(ctx); err != nil {
		c.logger.Error("Failed to reclaim pending messages", "stream", c.streamName, "error", err)
	}

	// Main consumption loop
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Consumer context cancelled, stopping")
			return nil
		default:
			if err := c.readAndProcess(ctx); err != nil {
				c.logger.Error("Error in read loop", "error", err)
				time.Sleep(3 * time.Second)
			}
		}
	}
}

// reclaimPending claims and reprocesses messages left in the PEL by previous
// consumer instances that crashed before ACKing.
func (c *Consumer) reclaimPending(ctx context.Context) error {
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
			c.logger.Error("Failed to claim message", "message_id", p.ID, "error", err)
			continue
		}
		for _, msg := range msgs {
			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Error("Failed to process reclaimed message", "message_id", msg.ID, "error", err)
				continue
			}
			if err := c.client.XAck(ctx, c.streamName, c.consumerGroup, msg.ID).Err(); err != nil {
				c.logger.Error("Failed to ACK reclaimed message", "message_id", msg.ID, "error", err)
			}
		}
	}
	return nil
}

// readAndProcess reads new messages and processes them
func (c *Consumer) readAndProcess(ctx context.Context) error {
	streams, err := c.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    c.consumerGroup,
		Consumer: c.consumerName,
		Streams:  []string{c.streamName, ">"},
		Count:    10,
		Block:    1 * time.Second,
	}).Result()

	if err != nil {
		if err == goredis.Nil {
			// No new messages, this is normal
			return nil
		}
		// If the stream or consumer group was deleted (e.g. during test cleanup or Redis restart),
		// recreate the consumer group so we can resume consuming.
		if strings.Contains(err.Error(), "NOGROUP") {
			c.logger.Warn("Consumer group missing, recreating",
				"stream", c.streamName,
				"group", c.consumerGroup,
			)
			if createErr := c.createConsumerGroup(); createErr != nil {
				return fmt.Errorf("failed to recreate consumer group: %w", createErr)
			}
			return nil
		}
		return fmt.Errorf("failed to read from stream: %w", err)
	}

	for _, stream := range streams {
		for _, msg := range stream.Messages {
			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Error("Failed to process message",
					"message_id", msg.ID,
					"error", err,
				)
				// Don't ACK failed messages - they'll be retried
				continue
			}

			// ACK the message only after successful processing
			if err := c.client.XAck(ctx, c.streamName, c.consumerGroup, msg.ID).Err(); err != nil {
				c.logger.Error("Failed to ACK message",
					"message_id", msg.ID,
					"error", err,
				)
			}
		}
	}

	return nil
}

// processMessage processes a single message
func (c *Consumer) processMessage(ctx context.Context, msg goredis.XMessage) error {
	c.logger.Info("Processing message",
		"message_id", msg.ID,
		"values", msg.Values,
	)

	// Extract runner_id and schedule_name from message
	runnerIDStr, ok := msg.Values["runner_id"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid runner_id in message")
	}

	scheduleName, ok := msg.Values["schedule_name"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid schedule_name in message")
	}

	// Parse runner_id as UUID
	runnerID, err := uuid.Parse(runnerIDStr)
	if err != nil {
		return fmt.Errorf("invalid runner_id format: %w", err)
	}

	// Create InitializeScheduler command
	cmd := command.InitializeScheduler{
		RunnerID:     runnerID,
		ScheduleName: scheduleName,
	}

	// Handle command via message bus
	if err := c.messageBus.Handle(ctx, cmd); err != nil {
		return fmt.Errorf("failed to handle command: %w", err)
	}

	c.logger.Info("Successfully processed message",
		"message_id", msg.ID,
		"runner_id", runnerID,
		"schedule_name", scheduleName,
	)

	return nil
}
