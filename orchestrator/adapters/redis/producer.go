package redis

import (
	"context"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"
)

// Producer publishes events to Redis streams
type Producer struct {
	client     *goredis.Client
	streamName string
	logger     *slog.Logger
}

// NewProducer creates a new Redis producer
func NewProducer(client *goredis.Client, streamName string, logger *slog.Logger) *Producer {
	return &Producer{
		client:     client,
		streamName: streamName,
		logger:     logger,
	}
}

// Publish publishes an event to the Redis stream
func (p *Producer) Publish(ctx context.Context, values map[string]interface{}) (string, error) {
	messageID, err := p.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: p.streamName,
		MaxLen: 10000,
		Approx: true,
		Values: values,
	}).Result()

	if err != nil {
		p.logger.Error("Failed to publish message to Redis stream",
			"stream", p.streamName,
			"error", err,
		)
		return "", fmt.Errorf("failed to publish message: %w", err)
	}

	p.logger.Debug("Published message to Redis",
		"stream", p.streamName,
		"message_id", messageID,
	)

	return messageID, nil
}
