package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	goredis "github.com/redis/go-redis/v9"
)

// TaskExecutionRecordedConsumer consumes task.execution.recorded:v1 events and persists
// task execution records.
type TaskExecutionRecordedConsumer struct {
	client        *goredis.Client
	streamName    string
	consumerGroup string
	consumerName  string
	handler       *TaskExecutionRecordedHandler
	logger        *slog.Logger
	stopCh        chan struct{}
}

// NewTaskExecutionRecordedConsumer creates and initialises the consumer group.
func NewTaskExecutionRecordedConsumer(
	client *goredis.Client,
	streamName string,
	db *sqlx.DB,
	execRepo postgres.TaskExecutionRepository,
	logger *slog.Logger,
) (*TaskExecutionRecordedConsumer, error) {
	handler := NewTaskExecutionRecordedHandler(db, execRepo, logger)
	c := &TaskExecutionRecordedConsumer{
		client:        client,
		streamName:    streamName,
		consumerGroup: "state-task-execution-recorded",
		consumerName:  fmt.Sprintf("consumer-%s", uuid.New().String()[:8]),
		handler:       handler,
		logger:        logger,
		stopCh:        make(chan struct{}),
	}
	// Offset "0" — processes historical messages on first boot.
	err := client.XGroupCreateMkStream(context.Background(), streamName, c.consumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return nil, fmt.Errorf("create consumer group for %s: %w", streamName, err)
	}
	logger.Info("TaskExecutionRecordedConsumer group ready", "group", c.consumerGroup, "stream", streamName)
	return c, nil
}

// Start begins consuming. Blocks until ctx is cancelled or Stop() is called.
func (c *TaskExecutionRecordedConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting TaskExecutionRecordedConsumer", "consumer", c.consumerName)
	if err := c.processPending(ctx); err != nil {
		c.logger.Error("Error processing pending messages", "error", err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.stopCh:
			return nil
		default:
			if err := c.readAndProcess(ctx); err != nil {
				c.logger.Error("Consumer read error", "error", err)
				time.Sleep(3 * time.Second)
			}
		}
	}
}

// Stop signals the consumer to stop.
func (c *TaskExecutionRecordedConsumer) Stop() {
	close(c.stopCh)
}

func (c *TaskExecutionRecordedConsumer) processPending(ctx context.Context) error {
	pending, err := c.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: c.streamName, Group: c.consumerGroup, Start: "-", End: "+", Count: 100,
	}).Result()
	if err != nil {
		return fmt.Errorf("list pending: %w", err)
	}
	for _, p := range pending {
		msgs, err := c.client.XClaim(ctx, &goredis.XClaimArgs{
			Stream: c.streamName, Group: c.consumerGroup, Consumer: c.consumerName,
			MinIdle: 0, Messages: []string{p.ID},
		}).Result()
		if err != nil {
			c.logger.Error("Failed to claim pending message", "id", p.ID, "error", err)
			continue
		}
		for _, msg := range msgs {
			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Error("Failed to process pending message", "id", msg.ID, "error", err)
			}
		}
	}
	return nil
}

func (c *TaskExecutionRecordedConsumer) readAndProcess(ctx context.Context) error {
	streams, err := c.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: c.consumerGroup, Consumer: c.consumerName,
		Streams: []string{c.streamName, ">"}, Count: 10, Block: 1 * time.Second,
	}).Result()
	if err != nil {
		if err == goredis.Nil {
			return nil
		}
		if strings.Contains(err.Error(), "NOGROUP") {
			c.logger.Warn("Consumer group missing, recreating")
			return c.client.XGroupCreateMkStream(ctx, c.streamName, c.consumerGroup, "0").Err()
		}
		return fmt.Errorf("xreadgroup: %w", err)
	}
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Error("Failed to process message", "id", msg.ID, "error", err)
				// message stays pending for retry
			}
		}
	}
	return nil
}

// processMessage delegates to handler and ACKs only when handler says so.
func (c *TaskExecutionRecordedConsumer) processMessage(ctx context.Context, msg goredis.XMessage) error {
	shouldACK, err := c.handler.Handle(ctx, msg.ID, msg.Values)
	if err != nil {
		// Transient error — do NOT ACK; message stays pending for retry
		return fmt.Errorf("handle message %s: %w", msg.ID, err)
	}
	if shouldACK {
		return c.client.XAck(ctx, c.streamName, c.consumerGroup, msg.ID).Err()
	}
	return nil
}
