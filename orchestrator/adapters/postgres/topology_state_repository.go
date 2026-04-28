package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// TopologyStateRepository manages the singleton topology_state row in Postgres.
type TopologyStateRepository struct {
	db *sqlx.DB
}

// NewTopologyStateRepository creates a new TopologyStateRepository.
func NewTopologyStateRepository(db *sqlx.DB) *TopologyStateRepository {
	return &TopologyStateRepository{db: db}
}

// IncrementGeneration atomically increments topology_generation and returns the new value.
func (r *TopologyStateRepository) IncrementGeneration(ctx context.Context) (int64, error) {
	var next int64
	err := r.db.QueryRowxContext(ctx, `
		UPDATE topology_state
		SET topology_generation = topology_generation + 1,
		    updated_at = now()
		WHERE id = TRUE
		RETURNING topology_generation
	`).Scan(&next)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("increment topology_generation: topology_state singleton row missing — was V6 migration applied?")
		}
		return 0, fmt.Errorf("increment topology_generation: %w", err)
	}
	return next, nil
}

// GetGeneration returns the current topology_generation value.
func (r *TopologyStateRepository) GetGeneration(ctx context.Context) (int64, error) {
	var current int64
	err := r.db.QueryRowxContext(ctx, `
		SELECT topology_generation FROM topology_state WHERE id = TRUE
	`).Scan(&current)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("get topology_generation: topology_state singleton row missing — was V6 migration applied?")
		}
		return 0, fmt.Errorf("get topology_generation: %w", err)
	}
	return current, nil
}
