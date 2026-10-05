package handlers

import (
	"context"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/repository"
	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

const expireBatch = 500

// Expirer deletes dead letters whose original message is past the replay
// horizon. They can no longer be redriven; each is logged as it goes.
type Expirer struct {
	repo   repository.DeadLetterRepository
	clock  ports.Clock
	obs    ports.Observer
	logger *slog.Logger
}

func NewExpirer(repo repository.DeadLetterRepository, clock ports.Clock, obs ports.Observer, logger *slog.Logger) *Expirer {
	return &Expirer{repo: repo, clock: clock, obs: obs, logger: logger}
}

// ExpireOnce deletes every expired dead letter, in batches, and returns how many.
func (e *Expirer) ExpireOnce(ctx context.Context) (int, error) {
	cutoff := e.clock.Now().Add(-model.ReplayHorizon)
	total := 0
	for {
		gone, err := e.repo.DeleteExpired(ctx, cutoff, expireBatch)
		if err != nil {
			return total, err
		}
		for _, dl := range gone {
			e.obs.Expired(dl)
			e.logger.Warn("Dead letter expired past the replay horizon — deleted", "id", dl.ID, "source", dl.Source,
				"stream", dl.Stream, "group", dl.Group, "status", dl.Status, "original_at", dl.OriginalAt)
		}
		total += len(gone)
		if len(gone) < expireBatch {
			return total, nil
		}
	}
}

// Run calls ExpireOnce every interval until ctx ends.
func (e *Expirer) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := e.ExpireOnce(ctx); err != nil && ctx.Err() == nil {
				e.logger.Error("Dead letter expiry failed", "error", err)
			}
		}
	}
}
