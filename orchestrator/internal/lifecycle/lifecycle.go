package lifecycle

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// ShutdownHandler is a function that performs cleanup on shutdown
type ShutdownHandler func(ctx context.Context) error

// ApplicationLifecycle manages application startup and graceful shutdown
type ApplicationLifecycle struct {
	shutdownHandlers []ShutdownHandler
	logger           *slog.Logger
	mu               sync.Mutex
	shuttingDown     bool
}

// NewApplicationLifecycle creates a new lifecycle manager
func NewApplicationLifecycle(logger *slog.Logger) *ApplicationLifecycle {
	return &ApplicationLifecycle{
		shutdownHandlers: make([]ShutdownHandler, 0),
		logger:           logger,
	}
}

// RegisterShutdownHandler registers a handler to be called on shutdown
// Handlers are called in reverse order (LIFO)
func (al *ApplicationLifecycle) RegisterShutdownHandler(handler ShutdownHandler) {
	al.mu.Lock()
	defer al.mu.Unlock()
	al.shutdownHandlers = append(al.shutdownHandlers, handler)
}

// SetupSignalHandlers sets up signal handlers for graceful shutdown
func (al *ApplicationLifecycle) SetupSignalHandlers(ctx context.Context, cancel context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigCh
		al.logger.Info("Received signal, initiating shutdown", "signal", sig.String())
		cancel()
		al.Shutdown(ctx)
	}()
}

// Shutdown performs graceful shutdown of the application
// Calls all registered shutdown handlers in reverse order (LIFO)
func (al *ApplicationLifecycle) Shutdown(ctx context.Context) {
	al.mu.Lock()
	if al.shuttingDown {
		al.mu.Unlock()
		return
	}
	al.shuttingDown = true
	al.mu.Unlock()

	al.logger.Info("Starting graceful shutdown")

	// Call handlers in reverse order (LIFO)
	for i := len(al.shutdownHandlers) - 1; i >= 0; i-- {
		if err := al.shutdownHandlers[i](ctx); err != nil {
			al.logger.Error("Error in shutdown handler", "error", err, "index", i)
		}
	}

	al.logger.Info("Graceful shutdown completed")
}
