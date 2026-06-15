package retention

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/agent-runner/domain"
	"github.com/carolsimone/continuo/agent-runner/service/ports"
	"github.com/google/uuid"
)

// Repo is the narrow slice of ThreadRepository retention needs.
type Repo interface {
	ListIdleThreads(ctx context.Context, cutoff time.Time, limit int) ([]domain.Thread, error)
	ListMessages(ctx context.Context, threadID uuid.UUID) ([]domain.Message, error)
	DeleteThread(ctx context.Context, id uuid.UUID) error
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
			if err := j.repo.DeleteThread(ctx, th.ID); err != nil {
				return err
			}
			j.logger.Info("retention removed thread", "thread_id", th.ID.String(), "archived", j.archiver != nil)
		}
	}
}
