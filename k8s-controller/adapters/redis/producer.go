package redis

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/k8s-controller/domain/event"

	goredis "github.com/redis/go-redis/v9"
)

// MultiProducer publishes events to multiple Redis streams
type MultiProducer struct {
	client       *goredis.Client
	checkStream  string // k8s.check:v1
	retryStream  string // task.retry:v1
	failedStream string // task.failed:v1
	logger       *slog.Logger
}

// NewMultiProducer creates a new multi-stream Redis producer
func NewMultiProducer(client *goredis.Client, checkStream, retryStream, failedStream string, logger *slog.Logger) *MultiProducer {
	return &MultiProducer{
		client:       client,
		checkStream:  checkStream,
		retryStream:  retryStream,
		failedStream: failedStream,
		logger:       logger,
	}
}

// PublishCheck publishes a JobCheckRequest event to the check stream
func (p *MultiProducer) PublishCheck(ctx context.Context, evt event.JobCheckRequest) (string, error) {
	messageID, err := p.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: p.checkStream,
		MaxLen: 10000,
		Approx: true,
		Values: evt.ToMap(),
	}).Result()

	if err != nil {
		p.logger.Error("Failed to publish check event",
			"stream", p.checkStream,
			"task_id", evt.TaskID,
			"error", err,
		)
		return "", fmt.Errorf("failed to publish check event: %w", err)
	}

	p.logger.Debug("Published check event",
		"stream", p.checkStream,
		"task_id", evt.TaskID,
		"message_id", messageID,
	)

	return messageID, nil
}

// PublishRetry publishes a TaskRetry event to the retry stream
func (p *MultiProducer) PublishRetry(ctx context.Context, evt event.TaskRetry) (string, error) {
	messageID, err := p.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: p.retryStream,
		MaxLen: 10000,
		Approx: true,
		Values: evt.ToMap(),
	}).Result()

	if err != nil {
		p.logger.Error("Failed to publish retry event",
			"stream", p.retryStream,
			"task_id", evt.TaskID,
			"error", err,
		)
		return "", fmt.Errorf("failed to publish retry event: %w", err)
	}

	p.logger.Debug("Published retry event",
		"stream", p.retryStream,
		"task_id", evt.TaskID,
		"retry_count", evt.RetryCount,
		"message_id", messageID,
	)

	return messageID, nil
}

// PublishFailed publishes a TaskFailed event to the failed stream
func (p *MultiProducer) PublishFailed(ctx context.Context, evt event.TaskFailed) (string, error) {
	messageID, err := p.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: p.failedStream,
		MaxLen: 10000,
		Approx: true,
		Values: evt.ToMap(),
	}).Result()

	if err != nil {
		p.logger.Error("Failed to publish failed event",
			"stream", p.failedStream,
			"task_id", evt.TaskID,
			"error", err,
		)
		return "", fmt.Errorf("failed to publish failed event: %w", err)
	}

	p.logger.Debug("Published failed event",
		"stream", p.failedStream,
		"task_id", evt.TaskID,
		"message_id", messageID,
	)

	return messageID, nil
}

// PublishToStream publishes a generic event to any specified stream
// Used by OutboxProcessor to publish events based on outbox entries
func (p *MultiProducer) PublishToStream(ctx context.Context, streamName string, values map[string]interface{}) (string, error) {
	messageID, err := p.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: streamName,
		MaxLen: 10000,
		Approx: true,
		Values: values,
	}).Result()

	if err != nil {
		p.logger.Error("Failed to publish event to stream",
			"stream", streamName,
			"error", err,
		)
		return "", fmt.Errorf("failed to publish event to stream %s: %w", streamName, err)
	}

	p.logger.Debug("Published event to stream",
		"stream", streamName,
		"message_id", messageID,
	)

	return messageID, nil
}
