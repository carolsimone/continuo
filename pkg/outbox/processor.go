// pkg/outbox/processor.go
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// Publisher performs whatever side effects an outbox entry represents.
// For event-only services (state, orchestrator), implementations simply XADD
// entry.Payload to entry.StreamName. For services with multi-step semantics
// (executor: K8s deploy + multiple XADDs), the implementation is service-aware.
type Publisher interface {
	Publish(ctx context.Context, entry *Entry) error
}

// BatchPublisher is an optional capability a Publisher may also implement to
// dispatch a whole batch in one network round trip (e.g. a Redis pipeline of
// XADDs). PublishBatch returns one error per entry, positionally aligned with
// entries; a nil at index i means entry i succeeded. The processor uses this
// path automatically when the publisher implements it, and falls back to
// per-entry Publish otherwise — so multi-step publishers (executor) keep their
// sequential semantics untouched.
//
// Ordering guarantee: implementations MUST issue the per-entry side effects in
// the slice order they receive, over a single connection, so per-aggregate FIFO
// (already enforced by the SELECT order in GetPendingBatch) is preserved.
type BatchPublisher interface {
	PublishBatch(ctx context.Context, entries []*Entry) []error
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
	// PerAggregateFIFO publishes rows sharing an aggregate_id in creation order
	// (a later row waits until the earlier one is processed). Off by default.
	PerAggregateFIFO bool
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
	repoOpts          []Option

	// lastActivity is the unix-nano timestamp of the most recent unit of Run
	// progress (a poll tick, and each drained batch), stored via atomic.Int64 so
	// Healthy can be polled from a health-probe goroutine without racing Run.
	// It backs the liveness heartbeat that catches a wedged-but-not-exited
	// processor — the outbox analogue of StreamConsumer's heartbeat — which
	// RegisterWorker/WorkerExited alone (goroutine-exit only) cannot see.
	lastActivity atomic.Int64
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
	var repoOpts []Option
	if cfg.PerAggregateFIFO {
		repoOpts = append(repoOpts, WithPerAggregateOrdering())
	}
	p := &Processor{
		db:                db,
		tableName:         tableName,
		publisher:         publisher,
		onTerminalFailure: onTerminalFailure,
		logger:            logger,
		tick:              tick,
		batchSize:         batchSize,
		repoOpts:          repoOpts,
	}
	// Seed the heartbeat at construction so a probe firing before Run is
	// scheduled sees "just started," not "stalled since the epoch."
	p.lastActivity.Store(time.Now().UnixNano())
	return p
}

func (p *Processor) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.tick)
	defer ticker.Stop()
	p.logger.Info("Starting outbox processor", "table", p.tableName, "tick", p.tick)
	for {
		// Advance the heartbeat once per poll tick, so an idle-but-live loop
		// stays healthy; drain advances it again per batch under load.
		p.lastActivity.Store(time.Now().UnixNano())
		select {
		case <-ctx.Done():
			p.logger.Info("Outbox processor stopped", "table", p.tableName)
			return ctx.Err()
		case <-ticker.C:
			p.drain(ctx)
		}
	}
}

// Healthy reports whether Run has made progress within maxStale. It returns nil
// while the loop is ticking (idle or draining); a non-nil error means the Run
// goroutine has stopped making progress — wedged in a call that never returns,
// the outbox analogue of a dead consumer. Wire it into a liveness probe (see
// pkg/liveness AddWorkerProbe). maxStale MUST be comfortably above the poll
// tick so an idle processor is never flagged.
func (p *Processor) Healthy(maxStale time.Duration) error {
	lastAt := time.Unix(0, p.lastActivity.Load())
	if age := time.Since(lastAt); age > maxStale {
		return fmt.Errorf("outbox processor %q: Run loop stalled — no activity for %s (last at %s)",
			p.tableName, age.Round(time.Second), lastAt.Format(time.RFC3339))
	}
	return nil
}

// drain processes batches back-to-back as long as each one comes back full
// (len == batchSize), which signals more pending rows are waiting. This lets a
// burst of thousands of pending rows clear within the same tick instead of one
// batch per tick, so publishing throughput is bounded by Postgres+Redis rather
// than the tick interval. It stops on a short batch (backlog drained), an error,
// or context cancellation, yielding control back to the ticker.
func (p *Processor) drain(ctx context.Context) {
	for {
		// Advance the heartbeat per batch so a large multi-batch backlog drain
		// (which can run well beyond one tick) keeps liveness fresh instead of
		// freezing it at the tick timestamp for the whole drain.
		p.lastActivity.Store(time.Now().UnixNano())
		processed, err := p.processBatchOnce(ctx)
		if err != nil {
			p.logger.Error("Outbox batch failed", "table", p.tableName, "error", err)
			return
		}
		if processed < p.batchSize {
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// ProcessBatch runs one cycle. Exported for tests.
func (p *Processor) ProcessBatch(ctx context.Context) error {
	_, err := p.processBatchOnce(ctx)
	return err
}

// processBatchOnce runs exactly one cycle and reports how many rows it claimed
// (the GetPendingBatch result size), so the drain loop can tell a full batch
// (more work waiting) from a short one (backlog drained).
func (p *Processor) processBatchOnce(ctx context.Context) (int, error) {
	tx, err := p.db.BeginTxx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	repo := NewPostgresRepository(tx, p.tableName, p.logger, p.repoOpts...)
	entries, err := repo.GetPendingBatch(ctx, p.batchSize)
	if err != nil {
		return 0, fmt.Errorf("get pending batch: %w", err)
	}
	claimed := len(entries)
	if claimed == 0 {
		return 0, tx.Commit()
	}

	// publish dispatches the whole batch (pipelined when the publisher supports
	// it) and returns one error per entry, positionally aligned with entries.
	pubErrs := p.publish(ctx, entries)

	// Successes are flipped to 'processed' in a single UPDATE; only the failed
	// subset takes per-row retry/fail handling. This keeps the common all-success
	// path at one round trip for the status flip.
	processedIDs := make([]uuid.UUID, 0, len(entries))
	for i, entry := range entries {
		pubErr := pubErrs[i]
		if pubErr == nil {
			processedIDs = append(processedIDs, entry.ID)
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

	if err := repo.MarkProcessedBatch(ctx, processedIDs); err != nil {
		p.logger.Error("Mark processed batch failed", "table", p.tableName, "count", len(processedIDs), "error", err)
	}

	err = tx.Commit()
	tx = nil
	if err != nil {
		return 0, fmt.Errorf("commit tx: %w", err)
	}
	return claimed, nil
}

// publish dispatches the batch and returns one error per entry, aligned with
// entries by index. When the publisher implements BatchPublisher the whole batch
// goes out in one pipelined round trip; otherwise each entry is published
// sequentially, preserving the per-aggregate FIFO order already imposed by
// GetPendingBatch.
func (p *Processor) publish(ctx context.Context, entries []*Entry) []error {
	if batch, ok := p.publisher.(BatchPublisher); ok {
		errs := batch.PublishBatch(ctx, entries)
		if len(errs) == len(entries) {
			return errs
		}
		// A misbehaving BatchPublisher returned a mismatched slice; fall back to
		// per-entry publish rather than risk a panic on index access.
		p.logger.Error("BatchPublisher returned mismatched error slice; falling back to per-entry publish",
			"table", p.tableName, "entries", len(entries), "errors", len(errs))
	}
	errs := make([]error, len(entries))
	for i, entry := range entries {
		errs[i] = p.publisher.Publish(ctx, entry)
	}
	return errs
}

// Sentinel for tests / callers that want to detect a particular failure
// without string-matching the wrapped error.
var ErrPublisherFailed = errors.New("publisher failed")
