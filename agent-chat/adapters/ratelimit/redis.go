package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/agent-chat/service/ports"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// keyPrefix namespaces the limiter's sorted sets so they never collide with
// other Redis keys in the shared instance.
const keyPrefix = "agent-chat:ratelimit:"

// slidingWindow atomically trims expired entries, counts the window, and admits
// the current attempt only if the count is under the limit. Doing it in one Lua
// script makes the check-and-record atomic across replicas.
//
// KEYS[1] = the per-user sorted set
// ARGV[1] = now in microseconds (the new entry's score)
// ARGV[2] = window start in microseconds (entries at/below this are expired)
// ARGV[3] = the per-window limit
// ARGV[4] = a unique member for this attempt
// ARGV[5] = key TTL in milliseconds
// returns 1 when admitted, 0 when over the limit.
var slidingWindow = goredis.NewScript(`
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, tonumber(ARGV[2]))
local count = redis.call('ZCARD', KEYS[1])
if count >= tonumber(ARGV[3]) then
  return 0
end
redis.call('ZADD', KEYS[1], tonumber(ARGV[1]), ARGV[4])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[5]))
return 1
`)

// Redis is a sliding-window rate limiter backed by a shared Redis instance, so
// the per-user limit is enforced globally across all agent-chat replicas.
type Redis struct {
	client    goredis.Scripter
	perMinute int
	window    time.Duration
	// now is the clock source, overridable in tests.
	now func() time.Time
}

var _ ports.RateLimiter = (*Redis)(nil)

// NewRedis creates a Redis-backed limiter allowing perMinute messages per user.
func NewRedis(client goredis.Scripter, perMinute int) *Redis {
	return &Redis{client: client, perMinute: perMinute, window: time.Minute, now: time.Now}
}

// Allow reports whether userID may send another message now. A non-nil error
// means the Redis backend failed; the caller decides the fail-open policy.
func (r *Redis) Allow(ctx context.Context, userID string) (bool, error) {
	now := r.now()
	nowMicro := now.UnixMicro()
	windowStart := now.Add(-r.window).UnixMicro()
	member := fmt.Sprintf("%d:%s", nowMicro, uuid.NewString())
	ttlMillis := r.window.Milliseconds()

	res, err := slidingWindow.Run(ctx, r.client, []string{keyPrefix + userID},
		nowMicro, windowStart, r.perMinute, member, ttlMillis).Int64()
	if err != nil {
		return false, fmt.Errorf("redis rate limit: %w", err)
	}
	return res == 1, nil
}
