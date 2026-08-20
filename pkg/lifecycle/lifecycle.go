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

// defaultGrace substitutes for a non-positive grace passed to Shutdown. A
// zero or negative grace would otherwise yield a deadline already in the
// past, handing shutdown handlers a context that is already done — the exact
// dead-context failure this package exists to prevent.
const defaultGrace = 15 * time.Second

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
// Step 3's context runs to a deadline fixed at Shutdown entry, while step 2
// is capped independently at half the grace budget; together they keep the
// total sequence within grace, not 2x grace — for handlers that honor their
// context. A Close()-style handler that ignores ctx is not itself bounded by
// this deadline.
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
//
// RegisterShutdownHandler refuses to register once shutdown has begun.
// Shutdown snapshots the handler slice under al.mu and never reads the field
// again afterward, so a handler registered after that snapshot would never
// run; refusing it loudly is preferable to a caller believing cleanup is
// wired up when it silently is not. This is symmetric with Go's refusal of
// late tracked-goroutine starts, and for the same underlying reason: both
// guard a field Shutdown reads under al.mu against an unsynchronized
// concurrent write.
func (al *ApplicationLifecycle) RegisterShutdownHandler(handler ShutdownHandler) {
	al.mu.Lock()
	defer al.mu.Unlock()
	if al.shuttingDown {
		al.logger.Warn("Not registering shutdown handler: shutdown already in progress")
		return
	}
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
	if grace <= 0 {
		al.logger.Warn("Non-positive shutdown grace, substituting default", "grace", grace.String(), "default", defaultGrace.String())
		grace = defaultGrace
	}

	al.mu.Lock()
	if al.shuttingDown {
		al.mu.Unlock()
		return
	}
	al.shuttingDown = true
	// Snapshot the handler slice under the same lock that guards the
	// shuttingDown flag and RegisterShutdownHandler's append, so every append
	// either happens-before this snapshot or is refused by
	// RegisterShutdownHandler outright — never racing this read. Iterating
	// al.shutdownHandlers directly here (as opposed to this snapshot) would
	// be an unsynchronized read racing that synchronized write.
	handlers := make([]ShutdownHandler, len(al.shutdownHandlers))
	copy(handlers, al.shutdownHandlers)
	al.mu.Unlock()

	al.logger.Info("Starting graceful shutdown")

	// deadline bounds step 3 (infra teardown, below); step 2 (drain) is capped
	// independently at half of grace via time.After. Together — for handlers
	// that honor their context — they keep the total sequence within grace,
	// not 2x grace.
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
	for i := len(handlers) - 1; i >= 0; i-- {
		if err := handlers[i](shutdownCtx); err != nil {
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
