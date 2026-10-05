// Package handlers holds dead-letter-controller's use cases.
package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/repository"
	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	"github.com/google/uuid"
)

// Recorder stores dead letters. Storing one twice keeps the first row.
type Recorder struct {
	repo   repository.DeadLetterRepository
	obs    ports.Observer
	logger *slog.Logger
}

func NewRecorder(repo repository.DeadLetterRepository, obs ports.Observer, logger *slog.Logger) *Recorder {
	return &Recorder{repo: repo, obs: obs, logger: logger}
}

// Record stores dl, assigning an id when it has none. A store failure is
// returned so the consumer leaves the dead-letter entry pending.
func (r *Recorder) Record(ctx context.Context, dl deadletter.DeadLetter) error {
	if dl.ID == uuid.Nil {
		dl.ID = uuid.New()
	}
	inserted, err := r.repo.Insert(ctx, dl)
	if err != nil {
		return fmt.Errorf("store dead letter %s: %w", dl.DedupKey, err)
	}
	if inserted {
		r.obs.Recorded(dl)
		r.logger.Info("Dead letter stored", "id", dl.ID, "source", dl.Source, "stream", dl.Stream,
			"group", dl.Group, "failure_kind", dl.FailureKind, "redrivable", dl.Redrivable)
	}
	return nil
}
