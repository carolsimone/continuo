package postgres

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/carolsimone/continuo/state/domain/repository"
)

// LiveScheduleModelRepository implements repository.LiveScheduleModels over
// state's copy of the promoted topologies: each read joins the tenant's
// live_release row to that release's topology_nodes, in the transaction bound
// by WithTx when there is one, so a command never waits for a second
// connection while it holds its own, else on the autocommit db.
type LiveScheduleModelRepository struct {
	db *sqlx.DB
	tx *sqlx.Tx
}

var _ repository.LiveScheduleModels = (*LiveScheduleModelRepository)(nil)

// NewLiveScheduleModelRepository returns a LiveScheduleModelRepository over db,
// bound to no transaction.
func NewLiveScheduleModelRepository(db *sqlx.DB) *LiveScheduleModelRepository {
	return &LiveScheduleModelRepository{db: db}
}

// WithTx returns a LiveScheduleModelRepository over the same db that reads in
// tx; a nil tx reads on the autocommit db.
func (r *LiveScheduleModelRepository) WithTx(tx *sqlx.Tx) *LiveScheduleModelRepository {
	return &LiveScheduleModelRepository{db: r.db, tx: tx}
}

// conn is what every read runs on: the bound transaction, else the autocommit
// db.
func (r *LiveScheduleModelRepository) conn() sqlx.QueryerContext {
	if r.tx != nil {
		return r.tx
	}
	return r.db
}

// CountByScheduleName counts the live release's nodes per schedule name,
// leaving out the nodes whose schedule name is empty.
func (r *LiveScheduleModelRepository) CountByScheduleName(ctx context.Context, tenantID string) (map[string]int, error) {
	var rows []struct {
		ScheduleName string `db:"schedule_name"`
		Models       int    `db:"models"`
	}
	if err := sqlx.SelectContext(ctx, r.conn(), &rows, `
		SELECT n.schedule_name, count(*)::int AS models
		FROM live_release live
		JOIN topology_nodes n
		  ON n.tenant_id = live.tenant_id AND n.release_id = live.release_id
		WHERE live.tenant_id = $1 AND n.schedule_name <> ''
		GROUP BY n.schedule_name`, tenantID); err != nil {
		return nil, fmt.Errorf("count the live release's models per schedule name of tenant %s: %w", tenantID, err)
	}
	out := make(map[string]int, len(rows))
	for _, row := range rows {
		out[row.ScheduleName] = row.Models
	}
	return out, nil
}

// CountOf counts the live release's nodes of one schedule name through
// idx_topology_nodes_schedule. The empty name is no schedule's, so it counts 0.
func (r *LiveScheduleModelRepository) CountOf(ctx context.Context, tenantID, scheduleName string) (int, error) {
	if scheduleName == "" {
		return 0, nil
	}
	var models int
	if err := sqlx.GetContext(ctx, r.conn(), &models, `
		SELECT count(*)::int
		FROM live_release live
		JOIN topology_nodes n
		  ON n.tenant_id = live.tenant_id AND n.release_id = live.release_id AND n.schedule_name = $2
		WHERE live.tenant_id = $1`, tenantID, scheduleName); err != nil {
		return 0, fmt.Errorf("count the live release's models of schedule %q of tenant %s: %w", scheduleName, tenantID, err)
	}
	return models, nil
}
