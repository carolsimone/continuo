// Package startupwait waits at boot for a dependency a service cannot start
// without, so a dependency that is not reachable yet delays the start instead of
// crashing the process.
package startupwait

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Policy bounds the wait for a dependency.
type Policy struct {
	// Timeout is the wall-clock budget for the whole wait, attempts included,
	// before giving up with the last error.
	Timeout time.Duration
	// InitialDelay is the pause after the first failed attempt; it doubles per
	// attempt up to MaxDelay.
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

// Dependency is what a service waits for.
type Dependency struct {
	// Name names the dependency in logs and errors, such as "Postgres".
	Name string
	// Ping makes one attempt to reach the dependency.
	Ping func(context.Context) error
	// Retryable reports whether a failed attempt means the dependency is not
	// reachable yet, rather than an answer that waiting cannot change.
	Retryable func(error) bool
	// Sleep pauses between attempts and returns early when its context ends. It
	// defaults to real time; a test injects its own to run the backoff instantly.
	Sleep func(context.Context, time.Duration) error
}

// Wait calls dep.Ping until it succeeds, retrying failures dep.Retryable accepts
// with capped exponential backoff for up to policy.Timeout. It returns nil once
// a ping succeeds; an error wrapping context.Canceled when ctx is cancelled first
// (a shutdown signal during boot); an error wrapping the ping's error, at once,
// when dep.Retryable refuses it; and otherwise, once the budget is spent, an
// error wrapping the last failure. The timeout bounds the whole wait, including
// a ping that blocks, so the budget is wall-clock time.
func Wait(ctx context.Context, dep Dependency, policy Policy, logger *slog.Logger) error {
	name := strings.ToLower(dep.Name)
	sleep := dep.Sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, policy.Timeout)
	defer cancel()

	var waited time.Duration
	delay := policy.InitialDelay
	for attempt := 1; ; attempt++ {
		err := dep.Ping(ctx)
		if err == nil {
			if attempt > 1 {
				logger.Info(dep.Name+" reachable", "attempts", attempt, "waited", waited.String())
			}
			return nil
		}
		if !dep.Retryable(err) {
			return fmt.Errorf("%s rejected the connection: %w", name, err)
		}
		if stopped := waitStopped(parent, ctx, name, err, waited, attempt); stopped != nil {
			return stopped
		}
		if waited+delay > policy.Timeout {
			return fmt.Errorf("%s still unreachable after %s (%d attempts): %w", name, waited, attempt, err)
		}
		logger.Warn(dep.Name+" not reachable yet, retrying",
			"attempt", attempt, "retry_in", delay.String(), "error", err)
		if serr := sleep(ctx, delay); serr != nil {
			if stopped := waitStopped(parent, ctx, name, err, waited, attempt); stopped != nil {
				return stopped
			}
			return fmt.Errorf("waiting for %s interrupted: %w (last error: %v)", name, serr, err)
		}
		waited += delay
		delay = min(delay*2, policy.MaxDelay)
	}
}

// waitStopped explains why the wait ended when its context is done: the caller
// cancelled it (shutdown) or the policy budget ran out. It returns nil while
// the wait is still live.
func waitStopped(parent, ctx context.Context, name string, last error, waited time.Duration, attempt int) error {
	if ctx.Err() == nil {
		return nil
	}
	if parent.Err() != nil {
		return fmt.Errorf("waiting for %s interrupted: %w (last error: %v)", name, parent.Err(), last)
	}
	return fmt.Errorf("%s still unreachable after %s (%d attempts): %w", name, waited, attempt, last)
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
