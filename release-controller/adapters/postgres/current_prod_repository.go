package postgres

import (
	"context"
	"database/sql"
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
// first promotion), a zero-value CurrentProd is returned — not an error. A row
// written before topology artifacts existed reads with a zero topology
// reference until release-controller's startup step writes its artifact.
func (c *CurrentProdRepository) Get(ctx context.Context) (*release.CurrentProd, error) {
	var row struct {
		ReleaseID      string         `db:"release_id"`
		TopologyURI    sql.NullString `db:"topology_uri"`
		TopologySHA256 sql.NullString `db:"topology_sha256"`
		NodeCount      sql.NullInt64  `db:"node_count"`
		PromotionSeq   int64          `db:"promotion_seq"`
		UpdatedAt      sql.NullTime   `db:"updated_at"`
	}
	err := c.q.GetContext(ctx, &row,
		`SELECT release_id, topology_uri, topology_sha256, node_count, promotion_seq, updated_at
		 FROM current_prod WHERE id = 1`)
	if errors.Is(err, sql.ErrNoRows) {
		return release.NewCurrentProd(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("select current_prod: %w", err)
	}
	ref := release.TopologyRef{
		URI:       row.TopologyURI.String,
		SHA256:    row.TopologySHA256.String,
		NodeCount: int(row.NodeCount.Int64),
	}
	return release.RehydrateCurrentProd(row.ReleaseID, ref, row.PromotionSeq, row.UpdatedAt.Time), nil
}

// Upsert writes the current production state. The singleton id=1 row is
// inserted on the first promotion and updated on every later write, which
// also clears the legacy topology_snapshot.
func (c *CurrentProdRepository) Upsert(ctx context.Context, cp *release.CurrentProd) error {
	ref := cp.Topology()
	_, err := c.q.ExecContext(ctx,
		`INSERT INTO current_prod (id, release_id, topology_uri, topology_sha256, node_count, promotion_seq, updated_at)
		 VALUES (1, $1, $2, $3, $4, $5, $6)
		 ON CONFLICT (id) DO UPDATE SET
		   release_id = EXCLUDED.release_id,
		   topology_uri = EXCLUDED.topology_uri,
		   topology_sha256 = EXCLUDED.topology_sha256,
		   node_count = EXCLUDED.node_count,
		   promotion_seq = EXCLUDED.promotion_seq,
		   updated_at = EXCLUDED.updated_at,
		   topology_snapshot = NULL`,
		cp.ReleaseID(),
		sql.NullString{String: ref.URI, Valid: ref.URI != ""},
		sql.NullString{String: ref.SHA256, Valid: ref.SHA256 != ""},
		sql.NullInt64{Int64: int64(ref.NodeCount), Valid: !ref.IsZero()},
		cp.PromotionSeq(),
		cp.UpdatedAt())
	if err != nil {
		return fmt.Errorf("upsert current_prod: %w", err)
	}
	return nil
}
