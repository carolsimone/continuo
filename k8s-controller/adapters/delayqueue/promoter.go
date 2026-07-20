package delayqueue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

// promoteBatch bounds how many due tickets one script invocation moves, so the
// single-threaded Redis server is never blocked by an unbounded loop. PromoteDue
// loops until a partial batch drains the backlog.
const promoteBatch = 500

// promoteMaxLen is the Phase-1 MAXLEN cap, retained as defense-in-depth on the
// promoter's XADD (matches the publisher's streamMaxLen and the app-side cap).
const promoteMaxLen = 10000

// promoteScript atomically moves all due tickets (score <= now) from the ZSET
// into the stream, bounded by LIMIT. Because Redis runs the whole script
// uninterrupted, with multiple k8s-controller replicas each due job is promoted
// exactly once: the first replica's ZREM/HDEL means a concurrent run sees
// nothing to promote. Returns the number of due members processed.
//
//	KEYS[1] = pending (ZSET)  KEYS[2] = tickets (HASH)  KEYS[3] = stream
//	ARGV[1] = now (unix sec)  ARGV[2] = batch limit     ARGV[3] = stream maxlen
const promoteScript = `
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, job in ipairs(due) do
  local payload = redis.call('HGET', KEYS[2], job)
  if payload then
    -- No outbox_entry_id here (unlike the publisher's xaddArgs, which stamps it
    -- on every XADD): this write is keyed by JobName and idempotent, so a
    -- Processor-crash redelivery just re-runs HSET+ZADD in place rather than
    -- duplicating a ticket, and a consumer-crash PEL redelivery reuses the same
    -- Redis msg_id, which the primary (message_id, stream_name) dedup already
    -- catches. The secondary outbox_entry_id dedup key is unnecessary here.
    redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[3], '*', 'payload', payload)
    redis.call('HDEL', KEYS[2], job)
  end
  redis.call('ZREM', KEYS[1], job)
end
return #due
`

// Promoter moves due delay-queue tickets into the check.k8s:v1 stream on a
// ticker. It is the "clock hand" of the ZSET-as-clock design: the ZSET says
// when, the stream runs the work.
type Promoter struct {
	client *goredis.Client
	script *goredis.Script
	logger *slog.Logger
	stream string
	batch  int
	maxLen int
}

// NewPromoter builds a Promoter for the check.k8s delay queue.
func NewPromoter(client *goredis.Client, logger *slog.Logger) *Promoter {
	return &Promoter{
		client: client,
		script: goredis.NewScript(promoteScript),
		logger: logger,
		stream: streams.CheckK8sV1,
		batch:  promoteBatch,
		maxLen: promoteMaxLen,
	}
}

// PromoteDue moves every ticket due at `now` into the stream, draining in
// batches (loop again on a full batch, stop on a partial one). Returns the total
// number of due members processed.
func (p *Promoter) PromoteDue(ctx context.Context, now int64) (int, error) {
	total := 0
	for {
		n, err := p.script.Run(ctx, p.client,
			[]string{PendingKey, TicketsKey, p.stream},
			now, p.batch, p.maxLen,
		).Int()
		if err != nil {
			return total, fmt.Errorf("promote due check tickets: %w", err)
		}
		total += n
		if n < p.batch {
			return total, nil
		}
	}
}

// Run promotes due tickets every `tick` until ctx is cancelled. A failed tick is
// logged and retried on the next tick — the ZSET is durable, so nothing is lost.
func (p *Promoter) Run(ctx context.Context, tick time.Duration) error {
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if _, err := p.PromoteDue(ctx, time.Now().Unix()); err != nil {
				p.logger.Error("delay-queue promoter tick failed", "error", err)
			}
		}
	}
}
