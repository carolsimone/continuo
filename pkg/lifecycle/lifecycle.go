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
// Shutdown proceeds in a defined order so infra is closed only after
// in-flight goroutines have unwound:
//  1. stop intake — cancel the root context so background jobs stop reading
//     new work; the in-flight unit is aborted, not completed — a message
//     handler's next database call fails, its transaction rolls back, and
//     its message is left un-ACKed for redelivery, while a non-transactional
//     worker (the outbox processor, a ticker loop) simply returns;
//  2. drain in-flight — wait on the tracked WaitGroup for those goroutines to
//     return, bounded by half the shutdown grace period;
//  3. close infra — run the registered shutdown handlers (HTTP/gRPC servers
//     and database or cache connections) against a LIVE context derived from
//     context.Background(), never the root context that was just cancelled.
//
// Steps 2 and 3 share a single deadline set to the grace period, so the
// whole sequence is bounded by grace, not 2x grace: the drain is capped at
// half the budget, which guarantees infra teardown always retains at least
// the other half, live, even when the drain used its entire share.
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
//
// Go refuses to start once shutdown has begun. sync.WaitGroup requires that
// any Add with a positive delta happen-before the Wait it is meant to be
// counted by; without this guard a Go call racing a concurrent Shutdown can
// violate that contract — either panicking ("WaitGroup misuse: Add called
// concurrently with Wait") or starting a goroutine that escapes the drain
// entirely and keeps running while shutdown handlers close infra under it.
func (al *ApplicationLifecycle) Go(fn func()) {
	al.mu.Lock()
	if al.shuttingDown {
		al.mu.Unlock()
		al.logger.Warn("Not starting tracked goroutine: shutdown already in progress")
		return
	}
	al.wg.Add(1)
	al.mu.Unlock()
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
// the whole sequence — the in-flight drain (capped at half of grace) and the
// infra-close handlers together.
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
// goroutines, then close infra against a live context. The whole sequence is
// bounded by grace. It is idempotent; the first caller runs the sequence and
// closes Done().
func (al *ApplicationLifecycle) Shutdown(cancel context.CancelFunc, grace time.Duration) {
	al.mu.Lock()
	if al.shuttingDown {
		al.mu.Unlock()
		return
	}
	al.shuttingDown = true
	al.mu.Unlock()

	al.logger.Info("Starting graceful shutdown")

	// The whole sequence — drain plus infra teardown — shares one deadline
	// so total shutdown time is bounded by grace, not 2x grace.
	deadline := time.Now().Add(grace)

	// 1. Stop intake: cancel the root context so background jobs stop reading
	//    new work. The in-flight unit is aborted, not completed — a message
	//    handler's next database call fails, its transaction rolls back, and
	//    its message is left un-ACKed for redelivery, while a
	//    non-transactional worker (the outbox processor, a ticker loop)
	//    simply returns.
	cancel()

	// 2. Drain in-flight goroutines. Capped at half the budget so infra
	//    teardown is never starved: handlers that close HTTP servers and
	//    connection pools must still get a live context with time left on
	//    it, even if the drain used its entire share.
	al.drain(grace / 2)

	// 3. Close infra against a LIVE context, sharing the same deadline set
	//    above, so HTTP draining and connection teardown actually run
	//    instead of returning instantly on a dead ctx.
	shutdownCtx, cancelShutdown := context.WithDeadline(context.Background(), deadline)
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
