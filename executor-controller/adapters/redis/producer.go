package redis

import (
	"context"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"
)

// Producer publishes events to Redis streams
type Producer struct {
	client *goredis.Client
	logger *slog.Logger
}

// NewProducer creates a new Redis producer
func NewProducer(client *goredis.Client, logger *slog.Logger) *Producer {
	return &Producer{
		client: client,
		logger: logger,
	}
}

// Publish publishes an event to the given Redis stream
func (p *Producer) Publish(ctx context.Context, stream string, values map[string]interface{}) (string, error) {
	messageID, err := p.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: stream,
		MaxLen: 10000,
		Approx: true,
		Values: values,
	}).Result()

	if err != nil {
		p.logger.Error("Failed to publish message to Redis stream",
			"stream", stream,
			"error", err,
		)
		return "", fmt.Errorf("failed to publish message: %w", err)
	}

	p.logger.Debug("Published message to Redis",
		"stream", stream,
		"message_id", messageID,
	)

	return messageID, nil
}
