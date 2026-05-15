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

// Consumer consumes deployment events from a single Redis Stream using a consumer group.
type Consumer struct {
	client                 *goredis.Client
	stream                 string
	group                  string
	consumerName           string
	messageBus             *messagebus.MessageBus
	db                     *sqlx.DB
	cancelledSchedulesRepo postgres.CancelledSchedulesRepository
	logger                 *slog.Logger
	stopCh                 chan struct{}
}

// NewConsumer creates a new Redis stream consumer for a single stream and consumer group.
func NewConsumer(
	client *goredis.Client,
	consumerName string,
	group string,
	stream string,
	messageBus *messagebus.MessageBus,
	db *sqlx.DB,
	cancelledSchedulesRepo postgres.CancelledSchedulesRepository,
	logger *slog.Logger,
) (*Consumer, error) {
	c := &Consumer{
		client:                 client,
		stream:                 stream,
		group:                  group,
		consumerName:           consumerName,
		messageBus:             messageBus,
		db:                     db,
		cancelledSchedulesRepo: cancelledSchedulesRepo,
		logger:                 logger,
		stopCh:                 make(chan struct{}),
	}

	if err := c.createConsumerGroup(); err != nil {
		return nil, err
	}

	return c, nil
}

// createConsumerGroup creates the consumer group for the stream, or no-ops if it already exists.
func (c *Consumer) createConsumerGroup() error {
	ctx := context.Background()

	err := c.client.XGroupCreateMkStream(ctx, c.stream, c.group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("failed to create consumer group for %s: %w", c.stream, err)
	}

	c.logger.Info("Consumer group created/verified", "group", c.group, "stream", c.stream)
	return nil
}

// Start begins consuming messages from the Redis stream.
func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Info("Starting consumer",
		"name", c.consumerName,
		"stream", c.stream,
		"group", c.group,
	)

	// Process pending messages first for crash recovery.
	if err := c.processPendingMessages(ctx); err != nil {
		c.logger.Error("Error processing pending messages", "stream", c.stream, "error", err)
	}

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

// processPendingMessages claims and reprocesses messages that were delivered but never ACKed (crash recovery).
func (c *Consumer) processPendingMessages(ctx context.Context) error {
	c.logger.Info("Checking for pending messages", "stream", c.stream)

	pending, err := c.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: c.stream,
		Group:  c.group,
		Start:  "-",
		End:    "+",
		Count:  100,
	}).Result()

	if err != nil {
		return fmt.Errorf("failed to get pending messages: %w", err)
	}

	if len(pending) == 0 {
		c.logger.Info("No pending messages found", "stream", c.stream)
		return nil
	}

	c.logger.Warn("Found pending messages, processing for crash recovery",
		"stream", c.stream,
		"count", len(pending),
	)

	for _, p := range pending {
		msgs, err := c.client.XClaim(ctx, &goredis.XClaimArgs{
			Stream:   c.stream,
			Group:    c.group,
			Consumer: c.consumerName,
			MinIdle:  0,
			Messages: []string{p.ID},
		}).Result()

		if err != nil {
			c.logger.Error("Failed to claim message",
				"stream", c.stream,
				"message_id", p.ID,
				"error", err,
			)
			continue
		}

		for _, msg := range msgs {
			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Error("Failed to process pending message",
					"stream", c.stream,
					"message_id", msg.ID,
					"error", err,
				)
			}
		}
	}

	return nil
}

// readAndProcess reads new messages from the stream and processes them.
func (c *Consumer) readAndProcess(ctx context.Context) error {
	streams, err := c.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    c.group,
		Consumer: c.consumerName,
		Streams:  []string{c.stream, ">"},
		Count:    10,
		Block:    1 * time.Second,
	}).Result()

	if err != nil {
		if err == goredis.Nil {
			return nil
		}
		if strings.Contains(err.Error(), "NOGROUP") {
			c.logger.Warn("Consumer group missing, recreating", "stream", c.stream, "group", c.group)
			if createErr := c.createConsumerGroup(); createErr != nil {
				return fmt.Errorf("failed to recreate consumer group: %w", createErr)
			}
			return nil
		}
		return fmt.Errorf("failed to read from stream: %w", err)
	}

	for _, s := range streams {
		for _, msg := range s.Messages {
			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Error("Failed to process message",
					"stream", c.stream,
					"message_id", msg.ID,
					"error", err,
				)
				// Message remains in pending list for retry.
			}
		}
	}
	return nil
}

// processMessage processes a single message from the stream.
// Only ACKs after successful processing to implement at-least-once delivery semantics.
func (c *Consumer) processMessage(ctx context.Context, msg goredis.XMessage) error {
	c.logger.Info("Processing message", "stream", c.stream, "message_id", msg.ID)

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

	// Deduplication via outbox_entry_id.
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
					"stream", c.stream,
					"message_id", msg.ID,
				)
				if err := c.client.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
					c.logger.Error("Failed to ACK duplicate message",
						"stream", c.stream,
						"message_id", msg.ID,
						"error", err,
					)
					return err
				}
				return nil
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
			return c.client.XAck(ctx, c.stream, c.group, msg.ID).Err()
		}
		return nil
	}

	nodeTypeStr := getString(msg.Values, "node_type")
	nodeType, err := pkg_model.ParseNodeType(nodeTypeStr)
	if err != nil {
		c.logger.Error("Unknown node_type in stream message — ACKing and discarding",
			"stream", c.stream,
			"message_id", msg.ID,
			"node_type", nodeTypeStr,
			"error", err,
		)
		// Permanent data error — ACK to prevent consumer group backlog.
		_ = c.client.XAck(ctx, c.stream, c.group, msg.ID)
		return nil
	}

	// task_retry_count is present on retry.task:v1 messages; absent on initial deploys (defaults to 0).
	taskRetryCount := 0
	if s := getString(msg.Values, "task_retry_count"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			taskRetryCount = n
		}
	}

	// max_retries is present on retry.task:v1 messages; 0 means "use service default".
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

	if err := c.messageBus.Handle(ctx, cmd); err != nil {
		return fmt.Errorf("message bus failed: %w", err)
	}

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
			// Job was created; do not return an error to prevent a spurious retry.
		}
	}

	// ACK only after successful handler completion.
	if err := c.client.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
		c.logger.Error("Failed to ACK message",
			"stream", c.stream,
			"message_id", msg.ID,
			"error", err,
		)
		return err
	}

	c.logger.Info("Successfully processed and ACKed message",
		"stream", c.stream,
		"message_id", msg.ID,
	)
	return nil
}

// Stop signals the consumer to stop gracefully.
func (c *Consumer) Stop() {
	c.logger.Info("Stopping consumer")
	close(c.stopCh)
}

// getString safely retrieves a string value from message values.
func getString(values map[string]interface{}, key string) string {
	if val, ok := values[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}
