package handlers

import (
	"context"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/repository"
	"github.com/google/uuid"
)

// Query reads stored dead letters.
type Query struct {
	repo repository.DeadLetterRepository
}

func NewQuery(repo repository.DeadLetterRepository) *Query { return &Query{repo: repo} }

// List returns the dead letters f selects, newest first, and the number of
// open dead letters overall.
func (q *Query) List(ctx context.Context, f deadletter.Filter) ([]deadletter.DeadLetter, int64, error) {
	f, err := f.Normalize()
	if err != nil {
		return nil, 0, err
	}
	rows, err := q.repo.List(ctx, f)
	if err != nil {
		return nil, 0, err
	}
	open, err := q.repo.CountOpen(ctx)
	if err != nil {
		return nil, 0, err
	}
	return rows, open, nil
}

// Get returns one dead letter, or deadletter.ErrNotFound.
func (q *Query) Get(ctx context.Context, id uuid.UUID) (deadletter.DeadLetter, error) {
	return q.repo.Get(ctx, id)
}
