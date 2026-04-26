package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
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
	group string,
	repo postgres.CancelledSchedulesRepository,
	logger *slog.Logger,
) (*ScheduleCancelledConsumer, error) {
	c := &ScheduleCancelledConsumer{
		client:        client,
		streamName:    streamName,
		consumerGroup: group,
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
	if err := c.reclaimPending(ctx); err != nil {
		c.logger.Error("Error reclaiming pending schedule.cancelled messages", "error", err)
	}
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

// reclaimPending claims and processes any schedule.cancelled:v1 messages that were
// delivered to a previous instance but never ACKed (crash recovery).
func (c *ScheduleCancelledConsumer) reclaimPending(ctx context.Context) error {
	pending, err := c.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: c.streamName,
		Group:  c.consumerGroup,
		Start:  "-",
		End:    "+",
		Count:  100,
	}).Result()
	if err != nil {
		if strings.Contains(err.Error(), "NOGROUP") {
			return nil
		}
		return fmt.Errorf("xpendingext: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}
	c.logger.Warn("Reclaiming pending schedule.cancelled messages", "count", len(pending))
	for _, p := range pending {
		msgs, claimErr := c.client.XClaim(ctx, &goredis.XClaimArgs{
			Stream:   c.streamName,
			Group:    c.consumerGroup,
			Consumer: c.consumerName,
			MinIdle:  0,
			Messages: []string{p.ID},
		}).Result()
		if claimErr != nil {
			c.logger.Error("Failed to claim pending message", "id", p.ID, "error", claimErr)
			continue
		}
		for _, msg := range msgs {
			idStr, _ := msg.Values["schedule_id"].(string)
			scheduleID, parseErr := uuid.Parse(idStr)
			if parseErr != nil {
				c.logger.Error("schedule.cancelled: invalid schedule_id in PEL — discarding", "id", idStr)
				_ = c.client.XAck(ctx, c.streamName, c.consumerGroup, msg.ID)
				continue
			}
			if insertErr := c.repo.Insert(ctx, scheduleID); insertErr != nil {
				c.logger.Error("Failed to insert pending cancelled schedule", "id", scheduleID, "error", insertErr)
				continue
			}
			_ = c.client.XAck(ctx, c.streamName, c.consumerGroup, msg.ID)
		}
	}
	return nil
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
