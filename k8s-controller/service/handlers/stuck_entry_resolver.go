package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
)

// StuckEntryRepository provides the read and remediation operations needed by
// StuckEntryResolver against the k8s_outbox table. These operations are not
// part of the canonical pkg/outbox.Repository because they are operational
// tooling specific to the stuck-entry recovery path.
type StuckEntryRepository interface {
	// GetStuckEntries returns outbox entries whose retry budget is exhausted and
	// that have been in pending state longer than stuckThresholdSeconds.
	// Implementations must use FOR UPDATE SKIP LOCKED.
	GetStuckEntries(ctx context.Context, limit int, stuckThresholdSeconds int) ([]*pkgoutbox.Entry, error)

	// ForceMarkFailed marks an entry as failed with a provided error message,
	// also setting processed_at. Used only by the stuck-entry resolver.
	ForceMarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error
}

// ResolverConfig holds configuration for the StuckEntryResolver
type ResolverConfig struct {
	CheckIntervalSeconds  int
	StuckThresholdSeconds int
	BatchSize             int
	MaxResolveAttempts    int
}

// StuckEntryResolver detects and resolves outbox entries stuck in pending state
type StuckEntryResolver struct {
	outboxRepo     StuckEntryRepository
	config         *ResolverConfig
	logger         *slog.Logger
	resolveTracker map[uuid.UUID]int // Tracks resolve attempt count per entry
	mu             sync.Mutex        // Protects resolveTracker
}

// NewStuckEntryResolver creates a new StuckEntryResolver
func NewStuckEntryResolver(
	outboxRepo StuckEntryRepository,
	config *ResolverConfig,
	logger *slog.Logger,
) *StuckEntryResolver {
	return &StuckEntryResolver{
		outboxRepo:     outboxRepo,
		config:         config,
		logger:         logger,
		resolveTracker: make(map[uuid.UUID]int),
	}
}

// Run starts the stuck entry resolver loop
func (r *StuckEntryResolver) Run(ctx context.Context) error {
	// If check interval is 0 or negative, resolver is disabled
	if r.config.CheckIntervalSeconds <= 0 {
		r.logger.Info("Stuck entry resolver is disabled", "check_interval", r.config.CheckIntervalSeconds)
		return nil
	}

	ticker := time.NewTicker(time.Duration(r.config.CheckIntervalSeconds) * time.Second)
	defer ticker.Stop()

	r.logger.Info("Starting stuck entry resolver",
		"check_interval_seconds", r.config.CheckIntervalSeconds,
		"stuck_threshold_seconds", r.config.StuckThresholdSeconds,
		"batch_size", r.config.BatchSize,
		"max_resolve_attempts", r.config.MaxResolveAttempts,
	)

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Stuck entry resolver context cancelled, stopping")
			return ctx.Err()
		case <-ticker.C:
			if err := r.ResolveStuckEntries(ctx); err != nil {
				r.logger.Error("Failed to resolve stuck entries", "error", err)
			}
		}
	}
}

// ResolveStuckEntries fetches and processes a batch of stuck entries
func (r *StuckEntryResolver) ResolveStuckEntries(ctx context.Context) error {
	// Step 1: Fetch stuck entries
	entries, err := r.outboxRepo.GetStuckEntries(ctx, r.config.BatchSize, r.config.StuckThresholdSeconds)
	if err != nil {
		return fmt.Errorf("failed to fetch stuck entries: %w", err)
	}

	if len(entries) == 0 {
		return nil // Nothing to resolve
	}

	r.logger.Warn("Found stuck outbox entries",
		"count", len(entries),
		"threshold_seconds", r.config.StuckThresholdSeconds,
	)

	// Step 2: Attempt to resolve each entry
	for _, entry := range entries {
		if err := r.resolveEntry(ctx, entry); err != nil {
			r.logger.Error("Failed to resolve stuck entry",
				"entry_id", entry.ID,
				"aggregate_id", entry.AggregateID,
				"error", err,
			)
		}
	}

	return nil
}

// resolveEntry attempts to mark a single entry as failed
func (r *StuckEntryResolver) resolveEntry(ctx context.Context, entry *pkgoutbox.Entry) error {
	entryID := entry.ID
	aggregateID := entry.AggregateID

	// Get current attempt count
	r.mu.Lock()
	attempts := r.resolveTracker[entryID]
	r.resolveTracker[entryID] = attempts + 1
	currentAttempts := attempts + 1
	r.mu.Unlock()

	// Build error message
	originalError := ""
	if entry.ErrorMessage != nil {
		originalError = *entry.ErrorMessage
	}
	if originalError == "" {
		originalError = "unknown error"
	}

	errorMessage := fmt.Sprintf(
		"STUCK_ENTRY_RESOLVED: Entry exceeded max_retries (%d >= %d). Original error: %s. Resolved at attempt %d.",
		entry.RetryCount,
		entry.MaxRetries,
		originalError,
		currentAttempts,
	)

	// Attempt to force mark as failed
	if err := r.outboxRepo.ForceMarkFailed(ctx, entryID, errorMessage); err != nil {
		// Check if we've exceeded max resolve attempts
		if currentAttempts >= r.config.MaxResolveAttempts {
			r.escalateAlert(entryID, aggregateID, currentAttempts, err)
		}
		return fmt.Errorf("failed to force mark entry as failed: %w", err)
	}

	// Success - remove from tracker and log
	r.mu.Lock()
	delete(r.resolveTracker, entryID)
	r.mu.Unlock()

	r.logger.Warn("Successfully resolved stuck entry",
		"entry_id", entryID,
		"aggregate_id", aggregateID,
		"attempt", currentAttempts,
		"retry_count", entry.RetryCount,
		"max_retries", entry.MaxRetries,
	)

	return nil
}

// escalateAlert logs a critical alert for entries that cannot be auto-resolved
func (r *StuckEntryResolver) escalateAlert(entryID, aggregateID uuid.UUID, attempts int, err error) {
	r.logger.Error("CRITICAL: Stuck entry cannot be auto-resolved",
		"alert_level", "CRITICAL",
		"action_required", "MANUAL_DATABASE_INTERVENTION",
		"entry_id", entryID,
		"aggregate_id", aggregateID,
		"resolve_attempts", attempts,
		"error", err,
		"recommended_action", fmt.Sprintf("UPDATE k8s_outbox SET status='failed', error_message='Manual intervention required', processed_at=NOW() WHERE id='%s'", entryID),
	)
}
