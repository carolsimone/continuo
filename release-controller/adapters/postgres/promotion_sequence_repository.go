package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// PromotionSequenceRepository is the Postgres-backed implementation of
// repository.PromotionSequenceRepository over the single promotion_sequence
// row the release migrations create.
type PromotionSequenceRepository struct{ q Queryer }

// NewPromotionSequenceRepository constructs a PromotionSequenceRepository
// bound to the given Queryer. Pass the open *sqlx.Tx so the seq is taken in
// the same transaction as the writes that announce it.
func NewPromotionSequenceRepository(q Queryer) *PromotionSequenceRepository {
	return &PromotionSequenceRepository{q: q}
}

var _ repository.PromotionSequenceRepository = (*PromotionSequenceRepository)(nil)

// Next increments the sequence and returns the new value. The UPDATE keeps the
// row locked until the surrounding transaction ends.
func (p *PromotionSequenceRepository) Next(ctx context.Context) (int64, error) {
	var seq int64
	err := p.q.GetContext(ctx, &seq,
		`UPDATE promotion_sequence SET last_seq = last_seq + 1 WHERE id = 1 RETURNING last_seq`)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, errors.New("promotion_sequence has no row: the release migrations have not run")
	}
	if err != nil {
		return 0, fmt.Errorf("take next promotion seq: %w", err)
	}
	return seq, nil
}
