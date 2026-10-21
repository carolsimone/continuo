package redis

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
	goredis "github.com/redis/go-redis/v9"
)

// The status-check delay queue an execution-controller of the previous version
// writes while it runs beside this one: a ZSET clock and a HASH of tickets.
const (
	// LegacyCheckPendingKey is the ZSET clock: member = a Job's name, score = the
	// check's due time (unix seconds).
	LegacyCheckPendingKey = "checkk8s:pending"
	// LegacyCheckTicketsKey is the HASH of pending tickets: field = a Job's name.
	LegacyCheckTicketsKey = "checkk8s:tickets"
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
	n, err := q.client.Del(ctx, LegacyCheckPendingKey, LegacyCheckTicketsKey).Result()
	if err != nil {
		return 0, fmt.Errorf("delete the status-check delay queue: %w", err)
	}
	return n, nil
}
