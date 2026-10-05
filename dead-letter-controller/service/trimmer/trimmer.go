// Package trimmer trims every contract stream to what its consumer groups still
// need, quarantining entries a lagging group still needs before the retention
// cap removes them.
package trimmer

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/repository"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/trim"
	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// Interval is how often the trim loop runs.
const Interval = 5 * time.Minute

// DefaultBudget bounds how many entries one run quarantines per stream; a run
// that reaches it trims only up to the first entry it did not store, and the
// next run continues.
const DefaultBudget = 50_000

// Config holds the trim loop's inputs: the retention cap, the per-stream
// quarantine budget, the contract's stream→groups map and the retired streams.
type Config struct {
	Retention time.Duration
	Budget    int
	Groups    map[string][]string
	Retired   []string
}

// Trimmer runs the trim loop.
type Trimmer struct {
	cfg       Config
	inspector ports.StreamInspector
	repo      repository.DeadLetterRepository
	lock      ports.TrimLock
	clock     ports.Clock
	obs       ports.TrimObserver
	logger    *slog.Logger
}

// New returns a Trimmer. A non-positive cfg.Budget selects DefaultBudget.
func New(cfg Config, inspector ports.StreamInspector, repo repository.DeadLetterRepository, lock ports.TrimLock,
	clock ports.Clock, obs ports.TrimObserver, logger *slog.Logger) *Trimmer {
	if cfg.Budget <= 0 {
		cfg.Budget = DefaultBudget
	}
	return &Trimmer{cfg: cfg, inspector: inspector, repo: repo, lock: lock, clock: clock, obs: obs, logger: logger}
}

// Run calls RunOnce now and then every interval until ctx ends.
func (t *Trimmer) Run(ctx context.Context, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		if err := t.RunOnce(ctx); err != nil && ctx.Err() == nil {
			t.logger.Error("Stream trim run failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// RunOnce trims every contract stream and deletes the retired ones, under the
// trim lock. A stream whose quarantine fails is left untrimmed; the run goes on
// with the others and returns the first error.
func (t *Trimmer) RunOnce(ctx context.Context) error {
	release, ok, err := t.lock.TryAcquire(ctx)
	if err != nil {
		return fmt.Errorf("trim lock: %w", err)
	}
	if !ok {
		t.logger.Debug("Another replica holds the trim lock — skipping this run")
		return nil
	}
	defer release()

	now := t.clock.Now()
	cutoff := trim.IDAt(now.Add(-t.cfg.Retention))
	streams := make([]string, 0, len(t.cfg.Groups))
	for s := range t.cfg.Groups {
		streams = append(streams, s)
	}
	sort.Strings(streams)

	var first error
	for _, s := range streams {
		if err := t.trimStream(ctx, s, t.cfg.Groups[s], cutoff, now); err != nil {
			t.logger.Error("Stream not trimmed", "stream", s, "error", err)
			if first == nil {
				first = err
			}
		}
	}
	for _, s := range t.cfg.Retired {
		deleted, err := t.inspector.DeleteIfExists(ctx, s)
		if err != nil {
			if first == nil {
				first = err
			}
			continue
		}
		if deleted {
			t.logger.Warn("Deleted a stream removed from the contract", "stream", s)
		}
	}
	if first == nil {
		t.obs.TrimSucceeded(now)
	}
	return first
}

func (t *Trimmer) trimStream(ctx context.Context, stream string, groups []string, cutoff trim.StreamID, now time.Time) error {
	snap, unknown, exists, err := t.inspector.Snapshot(ctx, stream, groups)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	for _, g := range unknown {
		t.logger.Warn("Consumer group not in the contract — ignored by trimming", "stream", stream, "group", g)
	}
	plan := trim.PlanTrim(snap, cutoff)
	minID := plan.MinID
	budget := t.cfg.Budget
	for _, w := range plan.Quarantine {
		stopAt, err := t.quarantine(ctx, stream, w, &budget, now)
		if err != nil {
			return err
		}
		if stopAt != nil && stopAt.Less(minID) {
			minID = *stopAt
		}
	}
	if minID.IsZero() {
		return nil
	}
	n, err := t.inspector.TrimBefore(ctx, stream, minID)
	if err != nil {
		return err
	}
	t.obs.Trimmed(stream, n)
	return nil
}

// quarantine stores what w.Group still needs below the cutoff, within the
// remaining budget. It returns the id of the first needed entry it did not
// store, or nil when it stored them all.
func (t *Trimmer) quarantine(ctx context.Context, stream string, w trim.Work, budget *int, now time.Time) (*trim.StreamID, error) {
	entries, err := t.inspector.NeededEntries(ctx, stream, w.Group, w.LastDelivered, w.Cutoff, *budget+1)
	if err != nil {
		return nil, err
	}
	var stopAt *trim.StreamID
	if len(entries) > *budget {
		id := entries[*budget].ID
		stopAt = &id
		entries = entries[:*budget]
	}
	if len(entries) == 0 {
		return stopAt, nil
	}
	dls := make([]deadletter.DeadLetter, 0, len(entries))
	for _, e := range entries {
		dls = append(dls, deadletter.DeadLetter{
			ID: uuid.New(), DedupKey: deadletter.QuarantineKey(stream, w.Group, e.ID.String()),
			Source: deadletter.SourceQuarantine, FailureKind: model.DeadLetterKindTrimmed, Stream: stream,
			Group: w.Group, OriginalMessageID: e.ID.String(),
			Error:  fmt.Sprintf("removed by the %s retention cap before group %s finished it", t.cfg.Retention, w.Group),
			Fields: e.Fields, Redrivable: len(e.Fields) > 0, OriginalAt: e.ID.Time(), RecordedAt: now,
			Status: deadletter.StatusOpen,
		})
	}
	n, err := t.repo.InsertBatch(ctx, dls)
	if err != nil {
		return nil, fmt.Errorf("quarantine %s/%s: %w", stream, w.Group, err)
	}
	*budget -= len(entries)
	t.obs.Quarantined(stream, w.Group, n)
	t.logger.Warn("Quarantined stream entries before trimming", "stream", stream, "group", w.Group,
		"entries", len(entries), "newly_stored", n)
	return stopAt, nil
}
