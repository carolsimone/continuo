package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/carolsimone/continuo/dependency-controller/adapters/postgres"
	"github.com/carolsimone/continuo/dependency-controller/domain/command"
	"github.com/carolsimone/continuo/dependency-controller/domain/model"
	"github.com/carolsimone/continuo/dependency-controller/service/messagebus"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"
)

// Consumer consumes update.table events from Redis Streams
type Consumer struct {
	client        *goredis.Client
	streamName    string
	consumerGroup string
	consumerName  string
	messageBus    *messagebus.MessageBus
	db            *sqlx.DB
	logger        *slog.Logger
}

// NewConsumer creates a new Redis stream consumer
func NewConsumer(
	client *goredis.Client,
	streamName string,
	consumerGroup string,
	messageBus *messagebus.MessageBus,
	db *sqlx.DB,
	logger *slog.Logger,
) (*Consumer, error) {
	consumerName := fmt.Sprintf("consumer-%s", uuid.New().String()[:8])

	c := &Consumer{
		client:        client,
		streamName:    streamName,
		consumerGroup: consumerGroup,
		consumerName:  consumerName,
		messageBus:    messageBus,
		db:            db,
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
			} else {
				// Update message_processing state to 'acked' (best effort, non-blocking)
				c.updateMessageStateToAcked(ctx, msg.ID)
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

	// Extract fields from message
	taskIDStr, ok := msg.Values["task_id"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid task_id in message")
	}

	scheduleIDStr, ok := msg.Values["schedule_id"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid schedule_id in message")
	}

	scheduleName, ok := msg.Values["schedule_name"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid schedule_name in message")
	}

	serviceName, ok := msg.Values["service_name"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid service_name in message")
	}

	schema, ok := msg.Values["schema"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid schema in message")
	}

	tableName, ok := msg.Values["table_name"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid table_name in message")
	}

	status, ok := msg.Values["status"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid status in message")
	}

	// Parse UUIDs
	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		return fmt.Errorf("invalid task_id format: %w", err)
	}

	scheduleID, err := uuid.Parse(scheduleIDStr)
	if err != nil {
		return fmt.Errorf("invalid schedule_id format: %w", err)
	}

	// Create ProcessNodeStatus command
	cmd := command.ProcessNodeStatus{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: scheduleName,
		ServiceName:  serviceName,
		Schema:       schema,
		TableName:    tableName,
		Status:       status,
	}

	// Handle command via message bus with message ID
	if err := c.messageBus.HandleWithMessageID(ctx, cmd, msg.ID); err != nil {
		return fmt.Errorf("failed to handle command: %w", err)
	}

	c.logger.Info("Successfully processed message",
		"message_id", msg.ID,
		"task_id", taskID,
		"table_name", tableName,
		"status", status,
	)

	return nil
}

// updateMessageStateToAcked updates message_processing state to 'acked' after successful ACK
// This is best effort and non-blocking - errors are logged but don't fail the ACK
func (c *Consumer) updateMessageStateToAcked(ctx context.Context, messageID string) {
	msgProcRepo := postgres.NewMessageProcessingRepository(c.db, c.logger)

	// Get the message processing record
	msgProc, err := msgProcRepo.GetByMessageID(ctx, messageID)
	if err != nil {
		c.logger.Debug("Failed to get message processing record for ACK update",
			"message_id", messageID,
			"error", err,
		)
		return
	}

	// Update state to acked
	if err := msgProcRepo.UpdateState(ctx, msgProc.ID, model.MessageProcessingStateAcked); err != nil {
		c.logger.Debug("Failed to update message processing state to acked",
			"message_id", messageID,
			"message_processing_id", msgProc.ID,
			"error", err,
		)
		return
	}

	c.logger.Debug("Updated message processing state to acked",
		"message_id", messageID,
		"message_processing_id", msgProc.ID,
	)
}
