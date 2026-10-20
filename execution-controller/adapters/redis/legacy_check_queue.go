package redis

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
	"github.com/carolsimone/continuo/pkg/delayqueue"
	goredis "github.com/redis/go-redis/v9"
)

// LegacyCheckQueue is the Redis-backed ports.LegacyCheckQueue.
type LegacyCheckQueue struct{ client *goredis.Client }

var _ ports.LegacyCheckQueue = (*LegacyCheckQueue)(nil)

// NewLegacyCheckQueue binds a LegacyCheckQueue to client.
func NewLegacyCheckQueue(client *goredis.Client) *LegacyCheckQueue {
	return &LegacyCheckQueue{client: client}
}

// Clear deletes the delay queue's clock and tickets.
func (q *LegacyCheckQueue) Clear(ctx context.Context) (int64, error) {
	n, err := q.client.Del(ctx, delayqueue.PendingKey, delayqueue.TicketsKey).Result()
	if err != nil {
		return 0, fmt.Errorf("delete the status-check delay queue: %w", err)
	}
	return n, nil
}
