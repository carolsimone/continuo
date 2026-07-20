package delayqueue

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// Schedule enqueues (or re-schedules) a check for a Job into the delay queue:
// HSET the payload and ZADD the due time, keyed by jobName. The two writes go in
// one MULTI/EXEC so a reader never sees a ZSET member whose payload is missing.
// Re-scheduling an existing jobName is an in-place update of both structures.
func Schedule(ctx context.Context, client *goredis.Client, jobName, payloadJSON string, checkAfter int64) error {
	pipe := client.TxPipeline()
	pipe.HSet(ctx, TicketsKey, jobName, payloadJSON)
	pipe.ZAdd(ctx, PendingKey, goredis.Z{Score: float64(checkAfter), Member: jobName})
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("schedule delay-queue ticket for %q: %w", jobName, err)
	}
	return nil
}
