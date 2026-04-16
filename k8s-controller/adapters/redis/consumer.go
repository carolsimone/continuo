package redis

import (
	"context"
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

// DualStreamConsumer consumes from both node.deployed:v1 and check.k8s:v1 streams
type DualStreamConsumer struct {
	client         *goredis.Client
	deployedStream string // node.deployed:v1
	checkStream    string // check.k8s:v1
	consumerGroup  string
	consumerName   string
	messageBus     *messagebus.MessageBus
	logger         *slog.Logger
	stopCh         chan struct{}
}

// NewDualStreamConsumer creates a new dual-stream consumer
func NewDualStreamConsumer(
	client *goredis.Client,
	deployedStream string,
	checkStream string,
	consumerGroup string,
	messageBus *messagebus.MessageBus,
	logger *slog.Logger,
) (*DualStreamConsumer, error) {
	consumerName := fmt.Sprintf("consumer-%s", uuid.New().String()[:8])

	consumer := &DualStreamConsumer{
		client:         client,
		deployedStream: deployedStream,
		checkStream:    checkStream,
		consumerGroup:  consumerGroup,
		consumerName:   consumerName,
		messageBus:     messageBus,
		logger:         logger,
		stopCh:         make(chan struct{}),
	}

	// Create consumer groups for both streams
	if err := consumer.createConsumerGroups(context.Background()); err != nil {
		return nil, err
	}

	logger.Info("Dual-stream consumer initialized",
		"deployed_stream", deployedStream,
		"check_stream", checkStream,
		"consumer_group", consumerGroup,
		"consumer_name", consumerName,
	)

	return consumer, nil
}

// createConsumerGroups creates consumer groups for both streams
func (c *DualStreamConsumer) createConsumerGroups(ctx context.Context) error {
	// Create group for deployed stream
	err := c.client.XGroupCreateMkStream(ctx, c.deployedStream, c.consumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("failed to create consumer group for %s: %w", c.deployedStream, err)
	}

	// Create group for check stream
	err = c.client.XGroupCreateMkStream(ctx, c.checkStream, c.consumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("failed to create consumer group for %s: %w", c.checkStream, err)
	}

	c.logger.Info("Consumer groups created/verified")
	return nil
}

// Start starts consuming from both streams
func (c *DualStreamConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting dual-stream consumer")

	// Process pending messages from both streams (crash recovery)
	if err := c.processPendingMessages(ctx, c.deployedStream); err != nil {
		c.logger.Error("Failed to process pending messages",
			"stream", c.deployedStream,
			"error", err,
		)
	}

	if err := c.processPendingMessages(ctx, c.checkStream); err != nil {
		c.logger.Error("Failed to process pending messages",
			"stream", c.checkStream,
			"error", err,
		)
	}

	// Main consumption loop
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

// processPendingMessages processes pending messages from a stream (crash recovery)
func (c *DualStreamConsumer) processPendingMessages(ctx context.Context, stream string) error {
	pending, err := c.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: stream,
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

	c.logger.Info("Processing pending messages",
		"stream", stream,
		"count", len(pending),
	)

	for _, p := range pending {
		msgs, err := c.client.XClaim(ctx, &goredis.XClaimArgs{
			Stream:   stream,
			Group:    c.consumerGroup,
			Consumer: c.consumerName,
			MinIdle:  0,
			Messages: []string{p.ID},
		}).Result()

		if err != nil {
			c.logger.Error("Failed to claim message",
				"stream", stream,
				"message_id", p.ID,
				"error", err,
			)
			continue
		}

		for _, msg := range msgs {
			if err := c.processMessage(ctx, msg, stream); err != nil {
				c.logger.Error("Failed to process pending message",
					"stream", stream,
					"message_id", msg.ID,
					"error", err,
				)
			}
		}
	}

	return nil
}

// readAndProcess reads messages from both streams and processes them
func (c *DualStreamConsumer) readAndProcess(ctx context.Context) error {
	// XREADGROUP GROUP group consumer BLOCK 1000 COUNT 10
	//   STREAMS node.deployed:v1 check.k8s:v1 > >
	streams, err := c.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    c.consumerGroup,
		Consumer: c.consumerName,
		Streams:  []string{c.deployedStream, c.checkStream, ">", ">"},
		Count:    10,
		Block:    1 * time.Second,
	}).Result()

	if err != nil {
		if err == goredis.Nil {
			// No messages, normal case
			return nil
		}
		if strings.Contains(err.Error(), "NOGROUP") {
			c.logger.Warn("Consumer group missing, recreating",
				"streams", []string{c.deployedStream, c.checkStream},
				"group", c.consumerGroup,
			)
			if createErr := c.createConsumerGroups(ctx); createErr != nil {
				return fmt.Errorf("failed to recreate consumer groups: %w", createErr)
			}
			return nil
		}
		return fmt.Errorf("failed to read from streams: %w", err)
	}

	for _, stream := range streams {
		for _, msg := range stream.Messages {
			// For check.k8s:v1 messages, check if ready for processing
			if stream.Stream == c.checkStream {
				if !c.isReadyForProcessing(msg) {
					// Not ready yet: re-publish to the end of the stream so it gets
					// picked up again after the delay, then ACK the original.
					c.client.XAdd(ctx, &goredis.XAddArgs{
						Stream: c.checkStream,
						Values: msg.Values,
					})
					c.client.XAck(ctx, stream.Stream, c.consumerGroup, msg.ID)
					continue
				}
			}

			if err := c.processMessage(ctx, msg, stream.Stream); err != nil {
				c.logger.Error("Failed to process message",
					"stream", stream.Stream,
					"message_id", msg.ID,
					"error", err,
				)
				// Don't ACK failed messages - they'll be retried
				continue
			}
		}
	}

	return nil
}

// isReadyForProcessing checks if a check message is ready for processing
func (c *DualStreamConsumer) isReadyForProcessing(msg goredis.XMessage) bool {
	checkAfterStr, ok := msg.Values["check_after"].(string)
	if !ok {
		// If no check_after field, process immediately
		return true
	}

	checkAfter, err := strconv.ParseInt(checkAfterStr, 10, 64)
	if err != nil {
		c.logger.Warn("Failed to parse check_after timestamp", "error", err)
		return true
	}

	return time.Now().Unix() >= checkAfter
}

// processMessage processes a single message
func (c *DualStreamConsumer) processMessage(ctx context.Context, msg goredis.XMessage, streamName string) error {
	c.logger.Debug("Processing message",
		"stream", streamName,
		"message_id", msg.ID,
	)

	// Parse message into CheckJobStatus command
	cmd, err := c.parseCommand(msg)
	if err != nil {
		c.logger.Error("Failed to parse message",
			"stream", streamName,
			"message_id", msg.ID,
			"error", err,
		)
		// ACK malformed messages to avoid reprocessing loop
		c.client.XAck(ctx, streamName, c.consumerGroup, msg.ID)
		return nil
	}

	// Handle via message bus
	if err := c.messageBus.Handle(ctx, cmd); err != nil {
		return fmt.Errorf("message bus failed: %w", err)
	}

	// CRITICAL: Only ACK after successful handler completion
	if err := c.client.XAck(ctx, streamName, c.consumerGroup, msg.ID).Err(); err != nil {
		c.logger.Error("Failed to ACK message",
			"stream", streamName,
			"message_id", msg.ID,
			"error", err,
		)
		return err
	}

	c.logger.Debug("Successfully processed and ACKed message",
		"stream", streamName,
		"message_id", msg.ID,
	)

	return nil
}

// parseCommand parses a Redis message into a CheckJobStatus command
func (c *DualStreamConsumer) parseCommand(msg goredis.XMessage) (command.CheckJobStatus, error) {
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
	schema, _ := msg.Values["schema"].(string)
	tableName, _ := msg.Values["table_name"].(string)
	jobName, _ := msg.Values["job_name"].(string)
	nodeType, _ := msg.Values["node_type"].(string)

	cmd := command.CheckJobStatus{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: scheduleName,
		ServiceName:  serviceName,
		Schema:       schema,
		TableName:    tableName,
		JobName:      jobName,
		NodeType:     nodeType,
	}

	// Extract outbox_entry_id for deduplication.
	// If absent (old executor-controller during rollout), leave nil — handler skips dedup.
	if outboxEntryIDStr, _ := msg.Values["outbox_entry_id"].(string); outboxEntryIDStr != "" {
		parsed, err := uuid.Parse(outboxEntryIDStr)
		if err != nil {
			c.logger.Warn("Invalid outbox_entry_id, skipping dedup",
				"raw", outboxEntryIDStr,
				"message_id", msg.ID,
			)
		} else {
			cmd.OutboxEntryID = &parsed
		}
	}

	return cmd, nil
}

// Stop stops the consumer
func (c *DualStreamConsumer) Stop() {
	close(c.stopCh)
}
