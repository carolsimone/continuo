// pkg/outbox/processor.go
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/jmoiron/sqlx"
)

// Publisher performs whatever side effects an outbox entry represents.
// For event-only services (state, orchestrator), implementations simply XADD
// entry.Payload to entry.StreamName. For services with multi-step semantics
// (executor: K8s deploy + multiple XADDs), the implementation is service-aware.
type Publisher interface {
	Publish(ctx context.Context, entry *Entry) error
}

// TerminalFailureHook runs once per entry when its retry budget is exhausted,
// just before MarkFailed. Best-effort: errors are logged, not propagated.
// Returning nil from the constructor (Processor.OnTerminalFailure unset) means
// "no-op," which is what state/orchestrator/k8s use; executor uses it to
// publish task.status.updated:v1=FAILED and node.updated:v1=FAILED for
// dispatch-exhaustion cases.
type TerminalFailureHook func(ctx context.Context, entry *Entry, cause error) error

// ProcessorConfig groups the optional knobs.
type ProcessorConfig struct {
	Tick      time.Duration // poll interval; default 1s
	BatchSize int           // max rows per batch; default 100
}

// Processor owns the poll loop. Each tick:
//  1. Begins a tx.
//  2. GetPendingBatch with FOR UPDATE SKIP LOCKED.
//  3. For each entry: call Publisher.Publish; on success MarkProcessed;
//     on error if retry budget remains call IncrementRetry, otherwise call
//     the TerminalFailureHook (best-effort) and MarkFailed.
//  4. Commits.
type Processor struct {
	db                *sqlx.DB
	tableName         string
	publisher         Publisher
	onTerminalFailure TerminalFailureHook
	logger            *slog.Logger
	tick              time.Duration
	batchSize         int
}

func NewProcessor(
	db *sqlx.DB,
	tableName string,
	publisher Publisher,
	onTerminalFailure TerminalFailureHook,
	logger *slog.Logger,
	cfg ProcessorConfig,
) *Processor {
	tick := cfg.Tick
	if tick == 0 {
		tick = time.Second
	}
	batchSize := cfg.BatchSize
	if batchSize == 0 {
		batchSize = 100
	}
	return &Processor{
		db:                db,
		tableName:         tableName,
		publisher:         publisher,
		onTerminalFailure: onTerminalFailure,
		logger:            logger,
		tick:              tick,
		batchSize:         batchSize,
	}
}

func (p *Processor) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.tick)
	defer ticker.Stop()
	p.logger.Info("Starting outbox processor", "table", p.tableName, "tick", p.tick)
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Outbox processor stopped", "table", p.tableName)
			return ctx.Err()
		case <-ticker.C:
			if err := p.ProcessBatch(ctx); err != nil {
				p.logger.Error("Outbox batch failed", "table", p.tableName, "error", err)
			}
		}
	}
}

// ProcessBatch runs one cycle. Exported for tests.
func (p *Processor) ProcessBatch(ctx context.Context) error {
	tx, err := p.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	repo := NewPostgresRepository(tx, p.tableName, p.logger)
	entries, err := repo.GetPendingBatch(ctx, p.batchSize)
	if err != nil {
		return fmt.Errorf("get pending batch: %w", err)
	}
	if len(entries) == 0 {
		return tx.Commit()
	}

	for _, entry := range entries {
		pubErr := p.publisher.Publish(ctx, entry)
		if pubErr == nil {
			if err := repo.MarkProcessed(ctx, entry.ID); err != nil {
				p.logger.Error("Mark processed failed", "entry_id", entry.ID, "error", err)
			}
			continue
		}
		p.logger.Error("Publish failed", "entry_id", entry.ID, "event_type", entry.EventType, "error", pubErr)
		// Permanent errors (events.ErrPermanent) bypass the retry budget. Retrying
		// a deterministic-failure payload would burn the budget for no benefit and
		// leave the task/schedule in limbo longer than necessary.
		permanent := errors.Is(pubErr, pkgevents.ErrPermanent)
		// retry_count is the count of past failures; +1 to evaluate "if we retry now, would this be the Nth attempt?"
		if !permanent && entry.RetryCount+1 < entry.MaxRetries {
			if err := repo.IncrementRetry(ctx, entry.ID); err != nil {
				p.logger.Error("Increment retry failed", "entry_id", entry.ID, "error", err)
			}
			continue
		}
		// Terminal: either ErrPermanent or budget exhausted.
		if p.onTerminalFailure != nil {
			if hookErr := p.onTerminalFailure(ctx, entry, pubErr); hookErr != nil {
				p.logger.Error("Terminal failure hook failed", "entry_id", entry.ID, "error", hookErr)
			}
		}
		if err := repo.MarkFailed(ctx, entry.ID, pubErr.Error()); err != nil {
			p.logger.Error("Mark failed failed", "entry_id", entry.ID, "error", err)
		}
	}

	err = tx.Commit()
	tx = nil
	if err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// Sentinel for tests / callers that want to detect a particular failure
// without string-matching the wrapped error.
var ErrPublisherFailed = errors.New("publisher failed")
