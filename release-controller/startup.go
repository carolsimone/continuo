package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
)

// startupStepFirstDelay and startupStepMaxDelay bound the backoff between
// attempts of a one-time startup step that waits for a dependency.
const (
	startupStepFirstDelay = time.Second
	startupStepMaxDelay   = time.Minute
)

// runStartupStep runs a one-time startup step until it succeeds. A step that
// fails because Postgres or the object store is unreachable is retried with
// exponential backoff (1 s doubling to 60 s) while the startup gate keeps
// readiness false; any other failure is returned, because running the step
// again cannot change it.
func runStartupStep(ctx context.Context, logger *slog.Logger, name string, step func(context.Context) error) error {
	return retryWhileUnreachable(ctx, logger, name, step, startupStepFirstDelay, startupStepMaxDelay)
}

// retryWhileUnreachable is runStartupStep with the backoff bounds as arguments.
func retryWhileUnreachable(ctx context.Context, logger *slog.Logger, name string, step func(context.Context) error, first, maxDelay time.Duration) error {
	delay := first
	for {
		err := step(ctx)
		if err == nil {
			return nil
		}
		if pkgredis.Classify(err) != pkgredis.ClassInfrastructure {
			return fmt.Errorf("%s: %w", name, err)
		}
		logger.Warn("startup step is waiting for a dependency", "step", name, "retry_in", delay.String(), "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, maxDelay)
	}
}

// pythonKindCheckExitCode logs why the startup check for services still
// released as kind python failed and returns the exit code of the process. A
// shutdown requested during the check ends the process with 0, as it does while
// it waits for Postgres. A service on kind python is logged with its names; any
// other failure, such as a lost connection, is logged as a failed check. err is
// the non-nil error the check returned; the caller calls this only on a failure.
func pythonKindCheckExitCode(ctx context.Context, logger *slog.Logger, err error) int {
	var python *handlers.PythonKindServicesError
	switch {
	case errors.Is(err, context.Canceled) || ctx.Err() != nil:
		logger.Info("Shutdown requested during the check for services released as kind python")
		return 0
	case errors.As(err, &python):
		logger.Error("Refusing to start: services are still released as kind python",
			"services", python.Services, "error", err)
	default:
		logger.Error("Failed to check whether services are still released as kind python", "error", err)
	}
	return 1
}
