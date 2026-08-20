package lifecycle

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// ShutdownHandler is a function that performs cleanup on shutdown.
type ShutdownHandler func(ctx context.Context) error

// ApplicationLifecycle manages application startup and graceful shutdown.
//
// Shutdown proceeds in a defined order so in-flight work is not truncated:
//  1. stop intake — cancel the root context so background jobs stop reading
//     new work and return after the current unit of work;
//  2. drain in-flight — wait on the tracked WaitGroup for those goroutines to
//     return, bounded by the shutdown grace period;
//  3. close infra — run the registered shutdown handlers (HTTP/gRPC servers
//     and database or cache connections) against a LIVE context derived from
//     context.Background(), never the root context that was just cancelled.
type ApplicationLifecycle struct {
	shutdownHandlers []ShutdownHandler
	logger           *slog.Logger
	mu               sync.Mutex
	shuttingDown     bool

	// wg tracks background goroutines that must drain before infra is closed.
	wg sync.WaitGroup

	// done is closed once the full shutdown sequence has completed, so main can
	// block on it instead of racing a fixed sleep.
	done chan struct{}
}

// NewApplicationLifecycle creates a new lifecycle manager.
func NewApplicationLifecycle(logger *slog.Logger) *ApplicationLifecycle {
	return &ApplicationLifecycle{
		shutdownHandlers: make([]ShutdownHandler, 0),
		logger:           logger,
		done:             make(chan struct{}),
	}
}

// RegisterShutdownHandler registers a handler to be called on shutdown.
// Handlers are called in reverse order (LIFO).
func (al *ApplicationLifecycle) RegisterShutdownHandler(handler ShutdownHandler) {
	al.mu.Lock()
	defer al.mu.Unlock()
	al.shutdownHandlers = append(al.shutdownHandlers, handler)
}

// Go runs fn in a tracked goroutine. The shutdown sequence waits for every
// tracked goroutine to return (within the grace period) before closing infra.
func (al *ApplicationLifecycle) Go(fn func()) {
	al.wg.Add(1)
	go func() {
		defer al.wg.Done()
		fn()
	}()
}

// Done returns a channel closed when the shutdown sequence has fully completed.
func (al *ApplicationLifecycle) Done() <-chan struct{} {
	return al.done
}

// SetupSignalHandlers installs SIGTERM/SIGINT handlers that drive the ordered
// graceful shutdown. cancel cancels the root context (intake stop); grace bounds
// both the in-flight drain and the infra-close handlers.
func (al *ApplicationLifecycle) SetupSignalHandlers(cancel context.CancelFunc, grace time.Duration) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigCh
		al.logger.Info("Received signal, initiating shutdown", "signal", sig.String(), "grace", grace.String())
		al.Shutdown(cancel, grace)
	}()
}

// Shutdown performs the ordered graceful shutdown: stop intake, drain in-flight
// goroutines, then close infra against a live context. It is idempotent; the
// first caller runs the sequence and closes Done().
func (al *ApplicationLifecycle) Shutdown(cancel context.CancelFunc, grace time.Duration) {
	al.mu.Lock()
	if al.shuttingDown {
		al.mu.Unlock()
		return
	}
	al.shuttingDown = true
	al.mu.Unlock()

	al.logger.Info("Starting graceful shutdown")

	// 1. Stop intake: cancel the root context so background jobs stop reading
	//    and return after the current unit of work.
	cancel()

	// 2. Drain in-flight goroutines, bounded by the grace period.
	al.drain(grace)

	// 3. Close infra against a LIVE context so HTTP draining and connection
	//    teardown actually run instead of returning instantly on a dead ctx.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), grace)
	defer cancelShutdown()
	for i := len(al.shutdownHandlers) - 1; i >= 0; i-- {
		if err := al.shutdownHandlers[i](shutdownCtx); err != nil {
			al.logger.Error("Error in shutdown handler", "error", err, "index", i)
		}
	}

	al.logger.Info("Graceful shutdown completed")
	close(al.done)
}

// drain waits for tracked goroutines to return, up to the grace period.
func (al *ApplicationLifecycle) drain(grace time.Duration) {
	drained := make(chan struct{})
	go func() {
		al.wg.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		al.logger.Info("In-flight goroutines drained")
	case <-time.After(grace):
		al.logger.Warn("Drain grace period elapsed before all goroutines returned", "grace", grace.String())
	}
}
