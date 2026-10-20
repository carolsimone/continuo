package handlers

import (
	"context"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/repository"
	"github.com/google/uuid"
)

// Query reads stored dead letters as they look under the current contract.
type Query struct {
	repo     repository.DeadLetterRepository
	contract deadletter.Contract
}

func NewQuery(repo repository.DeadLetterRepository, contract deadletter.Contract) *Query {
	return &Query{repo: repo, contract: contract}
}

// List returns the dead letters f selects, newest first, each with why it can
// never be redriven, and the number of open dead letters overall.
func (q *Query) List(ctx context.Context, f deadletter.Filter) ([]deadletter.View, int64, error) {
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
	views := make([]deadletter.View, 0, len(rows))
	for _, dl := range rows {
		views = append(views, dl.ViewUnder(q.contract))
	}
	return views, open, nil
}

// Get returns one dead letter with why it can never be redriven, or
// deadletter.ErrNotFound.
func (q *Query) Get(ctx context.Context, id uuid.UUID) (deadletter.View, error) {
	dl, err := q.repo.Get(ctx, id)
	if err != nil {
		return deadletter.View{}, err
	}
	return dl.ViewUnder(q.contract), nil
}
