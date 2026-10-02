package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// StartupPolicy bounds how long a service waits for Redis to answer PING at
// boot. The wait covers a Redis that is still starting, or whose Service name
// has not propagated to DNS yet, which is the normal state when a service's
// Deployment rolls out next to the Redis StatefulSet, or when a docker-compose
// service starts before its Redis container. An answer waiting cannot change
// (bad credentials, a disallowed command) is not retried: it fails at once.
type StartupPolicy struct {
	// Timeout is the wall-clock budget for the whole wait, attempts included,
	// before giving up with the last error.
	Timeout time.Duration
	// InitialDelay is the pause after the first failed attempt; it doubles per
	// attempt up to MaxDelay.
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

// DefaultStartupPolicy waits as long as the chart's bundled-mode wait-for-redis
// init container (100 tries, 3s apart), so an install path without that gate
// (BYO Redis, docker-compose) tolerates the same cold start without hiding a
// real outage for longer.
func DefaultStartupPolicy() StartupPolicy {
	return StartupPolicy{
		Timeout:      5 * time.Minute,
		InitialDelay: time.Second,
		MaxDelay:     10 * time.Second,
	}
}

// Pinger is the part of a Redis client the startup wait needs.
type Pinger interface {
	Ping(ctx context.Context) *goredis.StatusCmd
}

// WaitForRedis pings client until Redis answers, retrying unreachable-server
// failures with capped exponential backoff for up to policy.Timeout. It
// returns nil once a PING succeeds; an error wrapping context.Canceled when
// ctx is cancelled first (a shutdown signal during boot); the Redis error
// itself, at once, when Redis answers with an error waiting cannot fix; and
// otherwise, once the budget is spent, an error wrapping the last failure.
func WaitForRedis(ctx context.Context, client Pinger, policy StartupPolicy, logger *slog.Logger) error {
	ping := func(ctx context.Context) error { return client.Ping(ctx).Err() }
	return waitForPing(ctx, ping, policy, sleepCtx, logger)
}

// waitForPing calls ping until it succeeds, the policy timeout elapses, ctx is
// cancelled, or ping returns an error that waiting cannot fix. The timeout
// bounds the whole wait, including a ping that blocks, so the budget is
// wall-clock time. sleep is injected so the backoff is testable without real
// time.
func waitForPing(
	ctx context.Context,
	ping func(context.Context) error,
	policy StartupPolicy,
	sleep func(context.Context, time.Duration) error,
	logger *slog.Logger,
) error {
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, policy.Timeout)
	defer cancel()

	var waited time.Duration
	delay := policy.InitialDelay
	for attempt := 1; ; attempt++ {
		err := ping(ctx)
		if err == nil {
			if attempt > 1 {
				logger.Info("Redis reachable", "attempts", attempt, "waited", waited.String())
			}
			return nil
		}
		if !isStartupRetryable(err) {
			return fmt.Errorf("redis rejected the connection: %w", err)
		}
		if stopped := waitStopped(parent, ctx, err, waited, attempt); stopped != nil {
			return stopped
		}
		if waited+delay > policy.Timeout {
			return fmt.Errorf("redis still unreachable after %s (%d attempts): %w", waited, attempt, err)
		}
		logger.Warn("Redis not reachable yet, retrying",
			"attempt", attempt, "retry_in", delay.String(), "error", err)
		if serr := sleep(ctx, delay); serr != nil {
			if stopped := waitStopped(parent, ctx, err, waited, attempt); stopped != nil {
				return stopped
			}
			return fmt.Errorf("waiting for redis interrupted: %w (last error: %v)", serr, err)
		}
		waited += delay
		delay = min(delay*2, policy.MaxDelay)
	}
}

// waitStopped explains why the wait ended when its context is done: the caller
// cancelled it (shutdown) or the policy budget ran out. It returns nil while
// the wait is still live.
func waitStopped(parent, ctx context.Context, last error, waited time.Duration, attempt int) error {
	if ctx.Err() == nil {
		return nil
	}
	if parent.Err() != nil {
		return fmt.Errorf("waiting for redis interrupted: %w (last error: %v)", parent.Err(), last)
	}
	return fmt.Errorf("redis still unreachable after %s (%d attempts): %w", waited, attempt, last)
}

// transientReplyPrefixes are the Redis error replies a server sends while it
// cannot serve yet but will without any change on the client's side.
var transientReplyPrefixes = []string{
	"LOADING ",
	"BUSY ",
	"MASTERDOWN ",
	"TRYAGAIN ",
	"CLUSTERDOWN ",
	"ERR max number of clients reached",
}

// isStartupRetryable reports whether err means "Redis is not up yet" rather
// than an answer that waiting cannot change. Any failure that is not an error
// reply from the server (a refused dial, an unresolvable host, an EOF, a
// timeout) means no Redis answered, so it is retried. An error reply means a
// Redis did answer; only the replies it sends while still starting are
// retried, and the rest (NOAUTH, WRONGPASS, a renamed PING) fail at once.
func isStartupRetryable(err error) bool {
	var reply goredis.Error
	if !errors.As(err, &reply) {
		return true
	}
	msg := reply.Error()
	for _, prefix := range transientReplyPrefixes {
		if strings.HasPrefix(msg, prefix) {
			return true
		}
	}
	return false
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
