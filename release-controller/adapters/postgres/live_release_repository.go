package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// LiveReleaseRepository is the Postgres-backed implementation of
// repository.LiveReleaseRepository. It owns the singleton live_release row.
type LiveReleaseRepository struct{ q Queryer }

// NewLiveReleaseRepository constructs a LiveReleaseRepository bound to the
// given Queryer. Pass *sqlx.DB for autocommit operations or *sqlx.Tx for
// transactional writes.
func NewLiveReleaseRepository(q Queryer) *LiveReleaseRepository {
	return &LiveReleaseRepository{q: q}
}

var _ repository.LiveReleaseRepository = (*LiveReleaseRepository)(nil)

// Get returns the singleton LiveRelease. When no row exists yet (before the
// first promotion), a zero-value LiveRelease is returned — not an error. A row
// that references no topology artifact reads with a zero topology reference.
func (c *LiveReleaseRepository) Get(ctx context.Context) (*release.LiveRelease, error) {
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
		 FROM live_release WHERE id = 1`)
	if errors.Is(err, sql.ErrNoRows) {
		return release.NewLiveRelease(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("select live_release: %w", err)
	}
	ref := release.TopologyRef{
		URI:       row.TopologyURI.String,
		SHA256:    row.TopologySHA256.String,
		NodeCount: int(row.NodeCount.Int64),
	}
	return release.RehydrateLiveRelease(row.ReleaseID, ref, row.PromotionSeq, row.UpdatedAt.Time), nil
}

// Upsert records the live release. The singleton id=1 row is
// inserted on the first promotion and updated on every later write.
func (c *LiveReleaseRepository) Upsert(ctx context.Context, cp *release.LiveRelease) error {
	ref := cp.Topology()
	_, err := c.q.ExecContext(ctx,
		`INSERT INTO live_release (id, release_id, topology_uri, topology_sha256, node_count, promotion_seq, updated_at)
		 VALUES (1, $1, $2, $3, $4, $5, $6)
		 ON CONFLICT (id) DO UPDATE SET
		   release_id = EXCLUDED.release_id,
		   topology_uri = EXCLUDED.topology_uri,
		   topology_sha256 = EXCLUDED.topology_sha256,
		   node_count = EXCLUDED.node_count,
		   promotion_seq = EXCLUDED.promotion_seq,
		   updated_at = EXCLUDED.updated_at`,
		cp.ReleaseID(),
		sql.NullString{String: ref.URI, Valid: ref.URI != ""},
		sql.NullString{String: ref.SHA256, Valid: ref.SHA256 != ""},
		sql.NullInt64{Int64: int64(ref.NodeCount), Valid: !ref.IsZero()},
		cp.PromotionSeq(),
		cp.UpdatedAt())
	if err != nil {
		return fmt.Errorf("upsert live_release: %w", err)
	}
	return nil
}
