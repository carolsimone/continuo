package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/domain/command"
	"github.com/carolsimone/continuo/k8s-controller/service/messagebus"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// defaultConsumerMaxRetries is used when a message does not carry a max_retries field
// (backward-compat: old executor-controller versions did not publish it).
const defaultConsumerMaxRetries = 2

// Consumer consumes messages from a single Redis Stream using a consumer group.
type Consumer struct {
	client            *goredis.Client
	stream            string
	group             string
	consumerName      string
	messageBus        *messagebus.MessageBus
	defaultMaxRetries int  // fallback when message does not carry max_retries
	checkDelay        bool // when true, messages with a future check_after timestamp are re-circulated
	logger            *slog.Logger
	stopCh            chan struct{}
}

// NewConsumer creates a new single-stream consumer.
// Set checkDelay to true for the check.k8s:v1 stream so that messages whose
// check_after timestamp has not yet been reached are re-circulated.
func NewConsumer(
	client *goredis.Client,
	consumerName string,
	group string,
	stream string,
	messageBus *messagebus.MessageBus,
	defaultMaxRetries int,
	checkDelay bool,
	logger *slog.Logger,
) (*Consumer, error) {
	if defaultMaxRetries <= 0 {
		defaultMaxRetries = defaultConsumerMaxRetries
	}

	c := &Consumer{
		client:            client,
		stream:            stream,
		group:             group,
		consumerName:      consumerName,
		messageBus:        messageBus,
		defaultMaxRetries: defaultMaxRetries,
		checkDelay:        checkDelay,
		logger:            logger,
		stopCh:            make(chan struct{}),
	}

	if err := c.createConsumerGroup(context.Background()); err != nil {
		return nil, err
	}

	logger.Info("Consumer initialized",
		"stream", stream,
		"group", group,
		"consumer_name", consumerName,
		"check_delay", checkDelay,
	)

	return c, nil
}

// createConsumerGroup creates the consumer group for the stream, or no-ops if it already exists.
func (c *Consumer) createConsumerGroup(ctx context.Context) error {
	err := c.client.XGroupCreateMkStream(ctx, c.stream, c.group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("failed to create consumer group for %s: %w", c.stream, err)
	}

	c.logger.Info("Consumer group created/verified", "group", c.group, "stream", c.stream)
	return nil
}

// Start begins consuming messages from the Redis stream.
func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Info("Starting consumer", "stream", c.stream, "group", c.group)

	// Process pending messages for crash recovery.
	if err := c.processPendingMessages(ctx); err != nil {
		c.logger.Error("Failed to process pending messages",
			"stream", c.stream,
			"error", err,
		)
	}

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Consumer context cancelled, stopping")
			return ctx.Err()
		case <-c.stopCh:
			c.logger.Info("Consumer stop signal received, stopping")
			return nil
		default:
			if err := c.readAndProcess(ctx); err != nil {
				c.logger.Error("Error in read and process", "error", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

// processPendingMessages claims and reprocesses messages that were delivered but never ACKed (crash recovery).
func (c *Consumer) processPendingMessages(ctx context.Context) error {
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
		return nil
	}

	c.logger.Info("Processing pending messages",
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
			c.logger.Warn("Consumer group missing, recreating",
				"stream", c.stream,
				"group", c.group,
			)
			if createErr := c.createConsumerGroup(ctx); createErr != nil {
				return fmt.Errorf("failed to recreate consumer group: %w", createErr)
			}
			return nil
		}
		return fmt.Errorf("failed to read from stream: %w", err)
	}

	for _, s := range streams {
		for _, msg := range s.Messages {
			// For streams with check delay enabled, re-circulate messages whose
			// check_after timestamp has not yet been reached.
			if c.checkDelay && !c.isReadyForProcessing(msg) {
				c.client.XAdd(ctx, &goredis.XAddArgs{
					Stream: c.stream,
					Values: msg.Values,
				})
				c.client.XAck(ctx, c.stream, c.group, msg.ID)
				continue
			}

			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Error("Failed to process message",
					"stream", c.stream,
					"message_id", msg.ID,
					"error", err,
				)
				// Do not ACK failed messages — they remain in the pending list for retry.
				continue
			}
		}
	}

	return nil
}

// isReadyForProcessing checks whether a check message's check_after timestamp has elapsed.
// Returns true if the message should be processed now.
func (c *Consumer) isReadyForProcessing(msg goredis.XMessage) bool {
	checkAfterStr, ok := msg.Values["check_after"].(string)
	if !ok {
		// No check_after field — process immediately.
		return true
	}

	checkAfter, err := strconv.ParseInt(checkAfterStr, 10, 64)
	if err != nil {
		c.logger.Warn("Failed to parse check_after timestamp", "error", err)
		return true
	}

	return time.Now().Unix() >= checkAfter
}

// processMessage processes a single message from the stream.
// Only ACKs after successful handler completion to implement at-least-once delivery.
func (c *Consumer) processMessage(ctx context.Context, msg goredis.XMessage) error {
	c.logger.Debug("Processing message",
		"stream", c.stream,
		"message_id", msg.ID,
	)

	cmd, err := c.parseCommand(msg)
	if err != nil {
		c.logger.Error("Failed to parse message",
			"stream", c.stream,
			"message_id", msg.ID,
			"error", err,
		)
		// ACK malformed messages to prevent a reprocessing loop.
		c.client.XAck(ctx, c.stream, c.group, msg.ID)
		return nil
	}

	if err := c.messageBus.Handle(ctx, cmd); err != nil {
		return fmt.Errorf("message bus failed: %w", err)
	}

	if err := c.client.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
		c.logger.Error("Failed to ACK message",
			"stream", c.stream,
			"message_id", msg.ID,
			"error", err,
		)
		return err
	}

	c.logger.Debug("Successfully processed and ACKed message",
		"stream", c.stream,
		"message_id", msg.ID,
	)

	return nil
}

// parseCommand parses a Redis message into a CheckJobStatus command.
func (c *Consumer) parseCommand(msg goredis.XMessage) (command.CheckJobStatus, error) {
	taskIDStr, _ := msg.Values["task_id"].(string)
	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		return command.CheckJobStatus{}, fmt.Errorf("invalid task_id: %w", err)
	}

	scheduleIDStr, _ := msg.Values["schedule_id"].(string)
	scheduleID, err := uuid.Parse(scheduleIDStr)
	if err != nil {
		return command.CheckJobStatus{}, fmt.Errorf("invalid schedule_id: %w", err)
	}

	scheduleName, _ := msg.Values["schedule_name"].(string)
	serviceName, _ := msg.Values["service_name"].(string)
	schema, _ := msg.Values["schema_name"].(string)
	tableName, _ := msg.Values["table_name"].(string)
	jobName, _ := msg.Values["job_name"].(string)
	nodeType, _ := msg.Values["node_type"].(string)
	imageTag, _ := msg.Values["image_tag"].(string)

	// Parse retry count. check.k8s:v1 re-circulating messages carry "retry_count"
	// (from JobCheckRequest.ToMap()); node.deployed:v1 from executor-controller
	// carries "task_retry_count". Try "retry_count" first, fall back to
	// "task_retry_count" for backward compatibility.
	retryCountStr, _ := msg.Values["retry_count"].(string)
	if retryCountStr == "" {
		retryCountStr, _ = msg.Values["task_retry_count"].(string)
	}
	retryCount := int32(0)
	if n, err := strconv.ParseInt(retryCountStr, 10, 32); err == nil {
		retryCount = int32(n)
	}

	// Parse max_retries; fall back to the consumer's configured default when absent.
	maxRetries := int32(c.defaultMaxRetries)
	if s, _ := msg.Values["max_retries"].(string); s != "" {
		if n, err := strconv.ParseInt(s, 10, 32); err == nil && n > 0 {
			maxRetries = int32(n)
		}
	}

	// Serialize the raw message fields as the dedup payload stored in message_processing.
	payload, err := json.Marshal(msg.Values)
	if err != nil {
		payload = []byte("{}")
	}

	return command.CheckJobStatus{
		MessageID:    msg.ID,
		StreamName:   c.stream,
		Payload:      payload,
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: scheduleName,
		ServiceName:  serviceName,
		SchemaName:   schema,
		TableName:    tableName,
		JobName:      jobName,
		NodeType:     nodeType,
		ImageTag:     imageTag,
		RetryCount:   retryCount,
		MaxRetries:   maxRetries,
	}, nil
}

// Stop stops the consumer gracefully.
func (c *Consumer) Stop() {
	close(c.stopCh)
}
