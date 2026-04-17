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

// RerunConsumer consumes rerun:v1 events from Redis Streams.
type RerunConsumer struct {
	client        *goredis.Client
	streamName    string
	consumerGroup string
	consumerName  string
	messageBus    *messagebus.MessageBus
	logger        *slog.Logger
}

// NewRerunConsumer creates a new RerunConsumer.
func NewRerunConsumer(
	client *goredis.Client,
	streamName, consumerGroup string,
	messageBus *messagebus.MessageBus,
	logger *slog.Logger,
) (*RerunConsumer, error) {
	c := &RerunConsumer{
		client:        client,
		streamName:    streamName,
		consumerGroup: consumerGroup,
		consumerName:  fmt.Sprintf("rerun-consumer-%s", uuid.New().String()[:8]),
		messageBus:    messageBus,
		logger:        logger,
	}
	if err := c.createConsumerGroup(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *RerunConsumer) createConsumerGroup() error {
	ctx := context.Background()
	err := c.client.XGroupCreateMkStream(ctx, c.streamName, c.consumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("failed to create rerun consumer group: %w", err)
	}
	return nil
}

// Start begins consuming messages from the Redis stream.
func (c *RerunConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting rerun consumer",
		"stream", c.streamName,
		"group", c.consumerGroup,
		"name", c.consumerName,
	)
	if err := c.reclaimPending(ctx); err != nil {
		c.logger.Error("Failed to reclaim pending messages", "stream", c.streamName, "error", err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := c.readAndProcess(ctx); err != nil {
				c.logger.Error("Rerun consumer error", "error", err)
				time.Sleep(3 * time.Second)
			}
		}
	}
}

func (c *RerunConsumer) reclaimPending(ctx context.Context) error {
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

func (c *RerunConsumer) readAndProcess(ctx context.Context) error {
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
			c.logger.Warn("Rerun consumer group missing, recreating", "stream", c.streamName)
			_ = c.createConsumerGroup()
			return nil
		}
		return fmt.Errorf("rerun consumer read error: %w", err)
	}
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Error("Failed to process rerun message", "id", msg.ID, "error", err)
				continue
			}
			if err := c.client.XAck(ctx, c.streamName, c.consumerGroup, msg.ID).Err(); err != nil {
				c.logger.Error("Failed to ACK rerun message", "id", msg.ID, "error", err)
			}
		}
	}
	return nil
}

func (c *RerunConsumer) processMessage(ctx context.Context, msg goredis.XMessage) error {
	scheduleIDStr, _ := msg.Values["schedule_id"].(string)
	scheduleName, _ := msg.Values["schedule_name"].(string)
	schema, _ := msg.Values["schema_name"].(string)
	tableName, _ := msg.Values["table_name"].(string)
	serviceName, _ := msg.Values["service_name"].(string)

	scheduleID, err := uuid.Parse(scheduleIDStr)
	if err != nil {
		return fmt.Errorf("invalid schedule_id in rerun message: %w", err)
	}

	return c.messageBus.Handle(ctx, command.RerunNode{
		ScheduleID:   scheduleID,
		ScheduleName: scheduleName,
		SchemaName:   schema,
		TableName:    tableName,
		ServiceName:  serviceName,
	})
}
