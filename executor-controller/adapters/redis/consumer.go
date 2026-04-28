package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/service/messagebus"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"
)

// Consumer consumes deployment events from Redis Streams using consumer groups
type Consumer struct {
	client                 *goredis.Client
	queryModelStream       string
	retryStream            string
	consumerGroup          string
	consumerName           string
	messageBus             *messagebus.MessageBus
	db                     *sqlx.DB
	cancelledSchedulesRepo postgres.CancelledSchedulesRepository
	logger                 *slog.Logger
	stopCh                 chan struct{}
}

// NewConsumer creates a new Redis stream consumer with consumer groups
func NewConsumer(
	client *goredis.Client,
	queryModelStream string,
	retryStream string,
	consumerGroup string,
	messageBus *messagebus.MessageBus,
	db *sqlx.DB,
	cancelledSchedulesRepo postgres.CancelledSchedulesRepository,
	logger *slog.Logger,
) (*Consumer, error) {
	consumerName := fmt.Sprintf("consumer-%s", uuid.New().String()[:8])

	c := &Consumer{
		client:                 client,
		queryModelStream:       queryModelStream,
		retryStream:            retryStream,
		consumerGroup:          consumerGroup,
		consumerName:           consumerName,
		messageBus:             messageBus,
		db:                     db,
		cancelledSchedulesRepo: cancelledSchedulesRepo,
		logger:                 logger,
		stopCh:                 make(chan struct{}),
	}

	// Create consumer groups for both streams
	if err := c.createConsumerGroups(); err != nil {
		return nil, err
	}

	return c, nil
}

// createConsumerGroups creates the consumer group or logs if it already exists for both streams
func (c *Consumer) createConsumerGroups() error {
	ctx := context.Background()

	// Create group for main stream
	err := c.client.XGroupCreateMkStream(ctx, c.queryModelStream, c.consumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("failed to create consumer group for %s: %w", c.queryModelStream, err)
	}

	// Create group for retry stream
	err = c.client.XGroupCreateMkStream(ctx, c.retryStream, c.consumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("failed to create consumer group for %s: %w", c.retryStream, err)
	}

	c.logger.Info("Consumer groups created/verified",
		"group", c.consumerGroup,
		"streams", []string{c.queryModelStream, c.retryStream},
	)
	return nil
}

// Start begins consuming messages from both Redis streams
func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Info("Starting consumer",
		"name", c.consumerName,
		"streams", []string{c.queryModelStream, c.retryStream},
		"group", c.consumerGroup,
	)

	// CRITICAL: Process pending messages first (crash recovery) for both streams
	if err := c.processPendingMessages(ctx, c.queryModelStream); err != nil {
		c.logger.Error("Error processing pending messages", "stream", c.queryModelStream, "error", err)
	}
	if err := c.processPendingMessages(ctx, c.retryStream); err != nil {
		c.logger.Error("Error processing pending messages", "stream", c.retryStream, "error", err)
	}

	// Main consumption loop
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Consumer context cancelled, stopping")
			return nil
		case <-c.stopCh:
			c.logger.Info("Consumer stop signal received")
			return nil
		default:
			if err := c.readAndProcess(ctx); err != nil {
				c.logger.Error("Error in read loop", "error", err)
				time.Sleep(3 * time.Second)
			}
		}
	}
}

// processPendingMessages processes pending messages for crash recovery
func (c *Consumer) processPendingMessages(ctx context.Context, streamName string) error {
	c.logger.Info("Checking for pending messages", "stream", streamName)

	// Get pending messages for this consumer group
	pending, err := c.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: streamName,
		Group:  c.consumerGroup,
		Start:  "-",
		End:    "+",
		Count:  100,
	}).Result()

	if err != nil {
		return fmt.Errorf("failed to get pending messages: %w", err)
	}

	if len(pending) == 0 {
		c.logger.Info("No pending messages found", "stream", streamName)
		return nil
	}

	c.logger.Warn("Found pending messages, processing for crash recovery",
		"stream", streamName,
		"count", len(pending),
	)

	// Claim and process each pending message
	for _, p := range pending {
		// Claim the message for this consumer
		msgs, err := c.client.XClaim(ctx, &goredis.XClaimArgs{
			Stream:   streamName,
			Group:    c.consumerGroup,
			Consumer: c.consumerName,
			MinIdle:  0, // Claim immediately
			Messages: []string{p.ID},
		}).Result()

		if err != nil {
			c.logger.Error("Failed to claim message",
				"stream", streamName,
				"message_id", p.ID,
				"error", err,
			)
			continue
		}

		for _, msg := range msgs {
			if err := c.processMessage(ctx, msg, streamName); err != nil {
				c.logger.Error("Failed to process pending message",
					"stream", streamName,
					"message_id", msg.ID,
					"error", err,
				)
			}
		}
	}

	return nil
}

// readAndProcess reads new messages from both streams and processes them
func (c *Consumer) readAndProcess(ctx context.Context) error {
	// XREADGROUP GROUP groupName consumerName BLOCK 1000 COUNT 10 STREAMS stream1 stream2 > >
	streams, err := c.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    c.consumerGroup,
		Consumer: c.consumerName,
		Streams:  []string{c.queryModelStream, c.retryStream, ">", ">"}, // Read from both streams
		Count:    10,
		Block:    1 * time.Second,
	}).Result()

	if err != nil {
		if err == goredis.Nil {
			return nil // No messages, this is normal
		}
		if strings.Contains(err.Error(), "NOGROUP") {
			c.logger.Warn("Consumer group missing, recreating",
				"streams", []string{c.queryModelStream, c.retryStream},
				"group", c.consumerGroup,
			)
			if createErr := c.createConsumerGroups(); createErr != nil {
				return fmt.Errorf("failed to recreate consumer groups: %w", createErr)
			}
			return nil
		}
		return fmt.Errorf("failed to read from streams: %w", err)
	}

	// Process each message
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			if err := c.processMessage(ctx, msg, stream.Stream); err != nil {
				c.logger.Error("Failed to process message",
					"stream", stream.Stream,
					"message_id", msg.ID,
					"error", err,
				)
				// Message remains in pending list for retry
			}
		}
	}
	return nil
}

// processMessage processes a single message from the stream
// CRITICAL: Only ACKs after successful processing (implements exactly-once semantics)
func (c *Consumer) processMessage(ctx context.Context, msg goredis.XMessage, streamName string) error {
	c.logger.Info("Processing message", "stream", streamName, "message_id", msg.ID)

	// Parse message into DeployJob command
	// Expected fields: schedule_id, schedule_name, service_name, schema_name, table_name, task_id, job_name, outbox_entry_id
	taskIDStr := getString(msg.Values, "task_id")
	scheduleIDStr := getString(msg.Values, "schedule_id")

	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		return fmt.Errorf("invalid task_id: %w", err)
	}

	scheduleID, err := uuid.Parse(scheduleIDStr)
	if err != nil {
		return fmt.Errorf("invalid schedule_id: %w", err)
	}

	// Extract outbox_entry_id for deduplication
	outboxEntryIDStr := getString(msg.Values, "outbox_entry_id")
	var outboxEntryID uuid.UUID
	hasOutboxID := false

	if outboxEntryIDStr != "" {
		outboxEntryID, err = uuid.Parse(outboxEntryIDStr)
		if err != nil {
			c.logger.Warn("Invalid outbox_entry_id in message, processing anyway",
				"message_id", msg.ID,
				"outbox_entry_id", outboxEntryIDStr,
				"error", err,
			)
		} else {
			hasOutboxID = true

			// Check if already processed
			var exists bool
			err = c.db.QueryRowContext(ctx,
				"SELECT EXISTS(SELECT 1 FROM processed_events WHERE outbox_entry_id = $1)",
				outboxEntryID,
			).Scan(&exists)

			if err != nil {
				return fmt.Errorf("failed to check processed events: %w", err)
			}

			if exists {
				c.logger.Warn("Duplicate event detected - already processed, ACKing and skipping",
					"outbox_entry_id", outboxEntryID,
					"task_id", taskID,
					"stream", streamName,
					"message_id", msg.ID,
				)
				// ACK the message since it was already processed
				if err := c.client.XAck(ctx, streamName, c.consumerGroup, msg.ID).Err(); err != nil {
					c.logger.Error("Failed to ACK duplicate message",
						"stream", streamName,
						"message_id", msg.ID,
						"error", err,
					)
					return err
				}
				return nil // Successfully skipped duplicate
			}
		}
	} else {
		c.logger.Warn("Missing outbox_entry_id in message, processing anyway (backward compatibility)",
			"message_id", msg.ID,
			"task_id", taskID,
		)
	}

	// Guard: drop the message if the schedule was cancelled.
	cancelled, err := c.cancelledSchedulesRepo.Exists(ctx, scheduleID)
	if err != nil {
		return fmt.Errorf("cancelled schedules check: %w", err)
	}
	if cancelled {
		c.logger.Info("Schedule cancelled — dropping deploy message",
			"schedule_id", scheduleID, "task_id", taskID)
		if c.client != nil { // nil in unit tests that construct Consumer via struct literal
			return c.client.XAck(ctx, streamName, c.consumerGroup, msg.ID).Err()
		}
		return nil
	}

	nodeTypeStr := getString(msg.Values, "node_type")
	nodeType, err := pkg_model.ParseNodeType(nodeTypeStr)
	if err != nil {
		c.logger.Error("Unknown node_type in stream message — ACKing and discarding",
			"stream", streamName,
			"message_id", msg.ID,
			"node_type", nodeTypeStr,
			"error", err,
		)
		// Permanent data error — ACK to prevent consumer group backlog
		_ = c.client.XAck(ctx, streamName, c.consumerGroup, msg.ID)
		return nil
	}

	// Parse task_retry_count; present on retry.task:v1 messages, absent on initial deploys (defaults to 0).
	taskRetryCount := 0
	if s := getString(msg.Values, "task_retry_count"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			taskRetryCount = n
		}
	}

	// Parse max_retries; present on retry.task:v1 messages that carry it.
	// 0 means "use service default" — deploy_handler applies the fallback.
	maxRetries := 0
	if s := getString(msg.Values, "max_retries"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxRetries = n
		}
	}

	cmd := command.DeployJob{
		TaskID:         taskID,
		ScheduleID:     scheduleID,
		ScheduleName:   getString(msg.Values, "schedule_name"),
		ServiceName:    getString(msg.Values, "service_name"),
		SchemaName:     getString(msg.Values, "schema_name"),
		TableName:      getString(msg.Values, "table_name"),
		JobName:        getString(msg.Values, "job_name"),
		NodeType:       nodeType,
		ImageTag:       getString(msg.Values, "image_tag"),
		TaskRetryCount: taskRetryCount,
		MaxRetries:     maxRetries,
	}

	// Handle via message bus (same handler works for both initial deploy and retry)
	if err := c.messageBus.Handle(ctx, cmd); err != nil {
		return fmt.Errorf("message bus failed: %w", err)
	}

	// Record successful processing if we have an outbox_entry_id
	if hasOutboxID {
		_, err = c.db.ExecContext(ctx,
			"INSERT INTO processed_events (outbox_entry_id) VALUES ($1) ON CONFLICT DO NOTHING",
			outboxEntryID,
		)
		if err != nil {
			c.logger.Error("Failed to record processed event",
				"outbox_entry_id", outboxEntryID,
				"error", err,
			)
			// Job created, don't return error to prevent retry
		}
	}

	// CRITICAL: Only XACK after successful handler completion
	// Use the correct stream name for ACK
	if err := c.client.XAck(ctx, streamName, c.consumerGroup, msg.ID).Err(); err != nil {
		c.logger.Error("Failed to ACK message",
			"stream", streamName,
			"message_id", msg.ID,
			"error", err,
		)
		return err
	}

	c.logger.Info("Successfully processed and ACKed message",
		"stream", streamName,
		"message_id", msg.ID,
	)
	return nil
}

// Stop signals the consumer to stop gracefully
func (c *Consumer) Stop() {
	c.logger.Info("Stopping consumer")
	close(c.stopCh)
}

// getString safely retrieves a string value from message values
func getString(values map[string]interface{}, key string) string {
	if val, ok := values[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}
