package delayqueue

import (
	"context"
	"encoding/json"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// ticket is the value stored in TicketsKey. It carries the source outbox row's
// entry ID alongside the check payload so the promoter can stamp outbox_entry_id
// on the stream message; that field lets the consumer's secondary dedup key
// suppress a replay if the same check is promoted more than once.
type ticket struct {
	EntryID string `json:"entry_id"`
	Payload string `json:"payload"`
}

// Schedule enqueues (or re-schedules) a check for a Job into the delay queue:
// HSET the ticket and ZADD the due time, keyed by jobName. The two writes go in
// one MULTI/EXEC, so either both apply or neither does — there is no state where
// the HSET landed but the ZADD did not (a reader never sees a ZSET member whose
// payload is missing, nor a payload with no due time). If EXEC never runs (e.g. a
// dropped connection) neither write applies and Schedule returns an error, so the
// source outbox row stays unprocessed and the publish is retried.
// Re-scheduling an existing jobName is an in-place update of both structures.
// entryID is the source outbox row's ID; it rides through to the stream so a
// promoted check can be deduped on outbox_entry_id.
func Schedule(ctx context.Context, client *goredis.Client, jobName, entryID, payloadJSON string, checkAfter int64) error {
	value, err := json.Marshal(ticket{EntryID: entryID, Payload: payloadJSON})
	if err != nil {
		return fmt.Errorf("marshal delay-queue ticket for %q: %w", jobName, err)
	}
	pipe := client.TxPipeline()
	pipe.HSet(ctx, TicketsKey, jobName, value)
	pipe.ZAdd(ctx, PendingKey, goredis.Z{Score: float64(checkAfter), Member: jobName})
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("schedule delay-queue ticket for %q: %w", jobName, err)
	}
	return nil
}
