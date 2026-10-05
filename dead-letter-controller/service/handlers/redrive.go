package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/serialization"
	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	"github.com/carolsimone/continuo/dead-letter-controller/service/uow"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
)

// Redriver republishes dead letters to their original stream through the
// outbox.
type Redriver struct {
	newUoW func() uow.UnitOfWork
	clock  ports.Clock
	obs    ports.Observer
	logger *slog.Logger
}

func NewRedriver(newUoW func() uow.UnitOfWork, clock ports.Clock, obs ports.Observer, logger *slog.Logger) *Redriver {
	return &Redriver{newUoW: newUoW, clock: clock, obs: obs, logger: logger}
}

// Redrive redrives every dead letter in ids, or none. An unknown id fails with
// ErrNotFound; an expired or non-redrivable one with ErrExpired or
// ErrNotRedrivable. One transaction marks each open dead letter redriven and
// writes its outbox row; an already redriven one is returned unchanged. The
// result follows the order of ids, without duplicates.
func (r *Redriver) Redrive(ctx context.Context, ids []uuid.UUID, actor, reason string) ([]deadletter.DeadLetter, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return nil, deadletter.ErrReasonRequired
	}
	ids = uniqueIDs(ids)
	if len(ids) == 0 {
		return nil, deadletter.ErrNoIDs
	}
	u := r.newUoW()
	if err := u.Begin(ctx); err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = u.Rollback()
		}
	}()

	rows, err := u.DeadLetters().LockForRedrive(ctx, ids)
	if err != nil {
		return nil, err
	}
	byID := make(map[uuid.UUID]deadletter.DeadLetter, len(rows))
	for _, dl := range rows {
		byID[dl.ID] = dl
	}
	var missing []string
	for _, id := range ids {
		if _, ok := byID[id]; !ok {
			missing = append(missing, id.String())
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%w: %s", deadletter.ErrNotFound, strings.Join(missing, ", "))
	}
	now := r.clock.Now()
	for _, id := range ids {
		if err := byID[id].CheckRedrive(now); err != nil {
			return nil, fmt.Errorf("dead letter %s: %w", id, err)
		}
	}

	out := make([]deadletter.DeadLetter, 0, len(ids))
	var redriven []deadletter.DeadLetter
	for _, id := range ids {
		dl := byID[id]
		if dl.MarkRedriven(actor, reason, now) {
			if err := u.DeadLetters().SaveRedrive(ctx, dl); err != nil {
				return nil, err
			}
			body, err := serialization.EncodeRedrive(dl)
			if err != nil {
				return nil, err
			}
			if err := u.Outbox().Create(ctx, &outbox.Entry{
				ID: uuid.New(), AggregateType: serialization.AggregateTypeDeadLetter, AggregateID: dl.ID,
				EventType: serialization.EventTypeRedrive, Payload: body, StreamName: dl.Stream, Status: "pending",
			}); err != nil {
				return nil, err
			}
			redriven = append(redriven, dl)
		}
		out = append(out, dl)
	}
	if err := u.Commit(); err != nil {
		return nil, err
	}
	committed = true
	for _, dl := range redriven {
		r.obs.Redriven(dl)
		r.logger.Info("Dead letter redriven", "id", dl.ID, "stream", dl.Stream, "group", dl.TargetGroup(),
			"actor", actor, "reason", reason)
	}
	return out, nil
}

func uniqueIDs(ids []uuid.UUID) []uuid.UUID {
	seen := make(map[uuid.UUID]bool, len(ids))
	out := ids[:0:0]
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}
