package retention

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/carolsimone/continuo/agent-chat/service/ports"
	"github.com/google/uuid"
)

// Repo is the narrow slice of ThreadRepository retention needs.
type Repo interface {
	ListIdleThreads(ctx context.Context, cutoff time.Time, limit int) ([]domain.Thread, error)
	ListMessages(ctx context.Context, threadID uuid.UUID) ([]domain.Message, error)
	// DeleteThreadIfIdle atomically deletes the thread only if it is still idle
	// (updated_at < idleSince), returning false if a message arrived after the
	// thread was selected and refreshed updated_at. This closes the race between
	// ListIdleThreads selecting a thread and the sweep deleting it.
	DeleteThreadIfIdle(ctx context.Context, id uuid.UUID, idleSince time.Time) (bool, error)
}

// Job deletes threads idle past the retention window, optionally archiving
// each to cold storage first.
type Job struct {
	repo          Repo
	archiver      ports.Archiver // nil = delete only
	retentionDays int
	logger        *slog.Logger
}

func NewJob(repo Repo, archiver ports.Archiver, retentionDays int, logger *slog.Logger) *Job {
	if logger == nil {
		logger = slog.Default()
	}
	return &Job{repo: repo, archiver: archiver, retentionDays: retentionDays, logger: logger}
}

// Run sweeps on every tick until ctx ends.
func (j *Job) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := j.Sweep(ctx); err != nil {
				j.logger.Error("retention sweep failed", "error", err)
			}
		}
	}
}

// Sweep processes idle threads in pages until none remain.
func (j *Job) Sweep(ctx context.Context) error {
	cutoff := time.Now().UTC().AddDate(0, 0, -j.retentionDays)
	for {
		idle, err := j.repo.ListIdleThreads(ctx, cutoff, 100)
		if err != nil {
			return err
		}
		if len(idle) == 0 {
			return nil
		}
		for _, th := range idle {
			if j.archiver != nil {
				msgs, err := j.repo.ListMessages(ctx, th.ID)
				if err != nil {
					return fmt.Errorf("load thread %s for archive: %w", th.ID, err)
				}
				if err := j.archiver.ArchiveThread(ctx, th, msgs); err != nil {
					return fmt.Errorf("archive thread %s: %w", th.ID, err)
				}
			}
			deleted, err := j.repo.DeleteThreadIfIdle(ctx, th.ID, cutoff)
			if err != nil {
				return err
			}
			if !deleted {
				// A message arrived after selection; the thread is active again.
				j.logger.Info("retention skipped now-active thread", "thread_id", th.ID.String())
				continue
			}
			j.logger.Info("retention removed thread", "thread_id", th.ID.String(), "archived", j.archiver != nil)
		}
	}
}
