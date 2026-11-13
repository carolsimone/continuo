package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	repository "github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/carolsimone/continuo/run-controller/domain/seeds"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// ViewOutageRepository implements repository.ViewOutageRepository over
// view_outages. Statements run in the unit of work's transaction when one is
// open.
type ViewOutageRepository struct {
	db *sqlx.DB
	tx *sqlx.Tx
}

// Compile-time assertion that the adapter satisfies the domain port.
var _ repository.ViewOutageRepository = (*ViewOutageRepository)(nil)

// NewViewOutageRepository binds a ViewOutageRepository to tx; a nil tx runs
// every statement on the autocommit db.
func NewViewOutageRepository(db *sqlx.DB, tx *sqlx.Tx) *ViewOutageRepository {
	return &ViewOutageRepository{db: db, tx: tx}
}

func (r *ViewOutageRepository) conn() sqlx.ExtContext {
	if r.tx != nil {
		return r.tx
	}
	return r.db
}

// OpenForRebuilds inserts one outage, naming the seed, per rebuild task of
// the run (a task with a rebuild_seed_key) that is stamped for the seed or
// that the seed reaches over the run's release's edges through view,
// materialized_view and ephemeral nodes, the nodes a seed's load drops with
// its table. A view below two seeds is stamped for one of them, so the walk
// opens it whichever seed loads first. An outage already open keeps its since
// and takes the new state, seed and run.
func (r *ViewOutageRepository) OpenForRebuilds(ctx context.Context, tenantID string, runID uuid.UUID, seedKey topology.NodeKey,
	state seeds.OutageState, since time.Time) (int, error) {
	res, err := r.conn().ExecContext(ctx, `
		WITH RECURSIVE run_release AS (
		    SELECT release_id FROM runs WHERE tenant_id = $1 AND run_id = $2
		), reached(node_key) AS (
		    SELECT $3::text
		  UNION
		    SELECT e.downstream_key
		    FROM reached c
		    JOIN topology_edges e
		      ON e.tenant_id = $1 AND e.release_id = (SELECT release_id FROM run_release) AND e.upstream_key = c.node_key
		    JOIN topology_nodes n
		      ON n.tenant_id = e.tenant_id AND n.release_id = e.release_id AND n.node_key = e.downstream_key
		    WHERE n.materialized IN ('view', 'materialized_view', 'ephemeral')
		)
		INSERT INTO view_outages (tenant_id, node_key, state, since, seed_key, run_id)
		SELECT t.tenant_id, t.node_key, $4, $5, $3, t.run_id
		FROM tasks t
		WHERE t.tenant_id = $1 AND t.run_id = $2 AND t.node_key IS NOT NULL AND t.rebuild_seed_key <> ''
		  AND (t.rebuild_seed_key = $3 OR t.node_key IN (SELECT node_key FROM reached))
		ON CONFLICT (tenant_id, node_key) DO UPDATE SET
			state    = EXCLUDED.state,
			seed_key = EXCLUDED.seed_key,
			run_id   = EXCLUDED.run_id`,
		tenantID, runID, string(seedKey), string(state), since)
	if err != nil {
		return 0, fmt.Errorf("open the outages of the rebuilds of %s in run %s: %w", seedKey, runID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("open the outages of the rebuilds of %s in run %s: %w", seedKey, runID, err)
	}
	return int(n), nil
}

// Close deletes the node's outage and returns when it opened.
func (r *ViewOutageRepository) Close(ctx context.Context, tenantID string, key topology.NodeKey) (time.Time, bool, error) {
	var since time.Time
	err := sqlx.GetContext(ctx, r.conn(), &since, `
		DELETE FROM view_outages WHERE tenant_id = $1 AND node_key = $2 RETURNING since`, tenantID, string(key))
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("close the outage of %s: %w", key, err)
	}
	return since, true, nil
}

type viewOutageRow struct {
	NodeKey string    `db:"node_key"`
	State   string    `db:"state"`
	Since   time.Time `db:"since"`
	SeedKey string    `db:"seed_key"`
	RunID   uuid.UUID `db:"run_id"`
}

// Get reads the node's outage.
func (r *ViewOutageRepository) Get(ctx context.Context, tenantID string, key topology.NodeKey) (seeds.ViewOutage, bool, error) {
	var row viewOutageRow
	err := sqlx.GetContext(ctx, r.conn(), &row, `
		SELECT node_key, state, since, seed_key, run_id
		FROM view_outages WHERE tenant_id = $1 AND node_key = $2`, tenantID, string(key))
	if errors.Is(err, sql.ErrNoRows) {
		return seeds.ViewOutage{}, false, nil
	}
	if err != nil {
		return seeds.ViewOutage{}, false, fmt.Errorf("read the outage of %s: %w", key, err)
	}
	return seeds.ViewOutage{
		Key:     topology.NodeKey(row.NodeKey),
		State:   seeds.OutageState(row.State),
		Since:   row.Since,
		SeedKey: topology.NodeKey(row.SeedKey),
		RunID:   row.RunID,
	}, true, nil
}

// CountByState counts the outages of every tenant by state.
func (r *ViewOutageRepository) CountByState(ctx context.Context) (map[seeds.OutageState]int, error) {
	var rows []struct {
		State string `db:"state"`
		N     int    `db:"n"`
	}
	if err := sqlx.SelectContext(ctx, r.conn(), &rows, `
		SELECT state, count(*) AS n FROM view_outages GROUP BY state`); err != nil {
		return nil, fmt.Errorf("count the view outages: %w", err)
	}
	out := make(map[seeds.OutageState]int, len(rows))
	for _, row := range rows {
		out[seeds.OutageState(row.State)] = row.N
	}
	return out, nil
}

// DeleteAbsentFrom deletes the tenant's outages whose node the release lacks.
func (r *ViewOutageRepository) DeleteAbsentFrom(ctx context.Context, tenantID, releaseID string) (int64, error) {
	res, err := r.conn().ExecContext(ctx, `
		DELETE FROM view_outages o
		WHERE o.tenant_id = $1
		  AND NOT EXISTS (
		      SELECT 1 FROM topology_nodes n
		      WHERE n.tenant_id = o.tenant_id AND n.release_id = $2 AND n.node_key = o.node_key)`,
		tenantID, releaseID)
	if err != nil {
		return 0, fmt.Errorf("delete the view outages absent from release %s: %w", releaseID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("delete the view outages absent from release %s: %w", releaseID, err)
	}
	return n, nil
}
