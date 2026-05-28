package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// CurrentProdRepository is the Postgres-backed implementation of
// repository.CurrentProdRepository. It owns the singleton current_prod row.
type CurrentProdRepository struct{ q Queryer }

// NewCurrentProdRepository constructs a CurrentProdRepository bound to the
// given Queryer. Pass *sqlx.DB for autocommit operations or *sqlx.Tx for
// transactional writes.
func NewCurrentProdRepository(q Queryer) *CurrentProdRepository {
	return &CurrentProdRepository{q: q}
}

var _ repository.CurrentProdRepository = (*CurrentProdRepository)(nil)

// Get returns the singleton CurrentProd. When no row exists yet (before the
// first promotion), a zero-value CurrentProd is returned — not an error.
func (c *CurrentProdRepository) Get(ctx context.Context) (*release.CurrentProd, error) {
	var row struct {
		ReleaseID string       `db:"release_id"`
		Topology  []byte       `db:"topology_snapshot"`
		UpdatedAt sql.NullTime `db:"updated_at"`
	}
	err := c.q.GetContext(ctx, &row,
		`SELECT release_id, topology_snapshot, updated_at FROM current_prod WHERE id = 1`)
	if errors.Is(err, sql.ErrNoRows) {
		return release.NewCurrentProd(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("select current_prod: %w", err)
	}
	var topo release.Topology
	if len(row.Topology) > 0 {
		if err := json.Unmarshal(row.Topology, &topo); err != nil {
			return nil, fmt.Errorf("unmarshal topology: %w", err)
		}
	}
	return release.RehydrateCurrentProd(row.ReleaseID, topo, row.UpdatedAt.Time), nil
}

// Upsert writes the current production state. The singleton id=1 row is
// inserted on first promotion and updated on every subsequent one.
func (c *CurrentProdRepository) Upsert(ctx context.Context, cp *release.CurrentProd) error {
	topoJSON, err := json.Marshal(cp.TopologySnapshot())
	if err != nil {
		return fmt.Errorf("marshal topology: %w", err)
	}
	_, err = c.q.ExecContext(ctx,
		`INSERT INTO current_prod (id, release_id, topology_snapshot, updated_at)
		 VALUES (1, $1, $2, $3)
		 ON CONFLICT (id) DO UPDATE SET
		   release_id = EXCLUDED.release_id,
		   topology_snapshot = EXCLUDED.topology_snapshot,
		   updated_at = EXCLUDED.updated_at`,
		cp.ReleaseID(), topoJSON, cp.UpdatedAt())
	if err != nil {
		return fmt.Errorf("upsert current_prod: %w", err)
	}
	return nil
}
