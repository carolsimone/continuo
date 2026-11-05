package redis

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/carolsimone/continuo/pkg/startupwait"
	goredis "github.com/redis/go-redis/v9"
)

// StartupPolicy bounds how long a service waits for Redis to answer PING at
// boot. The wait covers a Redis that is still starting, or whose Service name
// has not propagated to DNS yet, which is the normal state when a service's
// Deployment rolls out next to the Redis StatefulSet, or when a docker-compose
// service starts before its Redis container. An answer waiting cannot change
// (bad credentials, a disallowed command) is not retried: it fails at once.
type StartupPolicy = startupwait.Policy

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

// waitForPing calls ping under policy (see startupwait.Wait). sleep is injected
// so the backoff is testable without real time.
func waitForPing(
	ctx context.Context,
	ping func(context.Context) error,
	policy StartupPolicy,
	sleep func(context.Context, time.Duration) error,
	logger *slog.Logger,
) error {
	return startupwait.Wait(ctx, startupwait.Dependency{
		Name:      "Redis",
		Ping:      ping,
		Retryable: isStartupRetryable,
		Sleep:     sleep,
	}, policy, logger)
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
