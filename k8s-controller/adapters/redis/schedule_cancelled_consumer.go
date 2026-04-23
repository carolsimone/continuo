package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

type ScheduleCancelledConsumer struct {
	client        *goredis.Client
	streamName    string
	consumerGroup string
	consumerName  string
	repo          postgres.CancelledSchedulesRepository
	logger        *slog.Logger
}

func NewScheduleCancelledConsumer(
	client *goredis.Client,
	streamName string,
	repo postgres.CancelledSchedulesRepository,
	logger *slog.Logger,
) (*ScheduleCancelledConsumer, error) {
	c := &ScheduleCancelledConsumer{
		client:        client,
		streamName:    streamName,
		consumerGroup: "k8s-schedule-cancelled",
		consumerName:  fmt.Sprintf("consumer-%s", uuid.New().String()[:8]),
		repo:          repo,
		logger:        logger,
	}
	err := client.XGroupCreateMkStream(context.Background(), streamName, c.consumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return nil, fmt.Errorf("create consumer group for %s: %w", streamName, err)
	}
	return c, nil
}

func (c *ScheduleCancelledConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting ScheduleCancelledConsumer", "stream", c.streamName)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := c.readAndProcess(ctx); err != nil {
				c.logger.Error("Consumer error", "error", err)
				time.Sleep(3 * time.Second)
			}
		}
	}
}

func (c *ScheduleCancelledConsumer) readAndProcess(ctx context.Context) error {
	streams, err := c.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: c.consumerGroup, Consumer: c.consumerName,
		Streams: []string{c.streamName, ">"}, Count: 10, Block: time.Second,
	}).Result()
	if err != nil {
		if err == goredis.Nil {
			return nil
		}
		if strings.Contains(err.Error(), "NOGROUP") {
			return c.client.XGroupCreateMkStream(ctx, c.streamName, c.consumerGroup, "0").Err()
		}
		return fmt.Errorf("xreadgroup: %w", err)
	}
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			idStr, _ := msg.Values["schedule_id"].(string)
			scheduleID, err := uuid.Parse(idStr)
			if err != nil {
				c.logger.Error("schedule.cancelled: invalid schedule_id — discarding", "id", idStr)
				_ = c.client.XAck(ctx, c.streamName, c.consumerGroup, msg.ID)
				continue
			}
			if err := c.repo.Insert(ctx, scheduleID); err != nil {
				c.logger.Error("Failed to insert cancelled schedule", "id", scheduleID, "error", err)
				continue
			}
			_ = c.client.XAck(ctx, c.streamName, c.consumerGroup, msg.ID)
		}
	}
	return nil
}
