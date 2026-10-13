package postgres

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// LegacyTopologyColumns is the Postgres-backed ports.LegacyTopologyColumns. It
// reads information_schema for the connection's current schema.
type LegacyTopologyColumns struct{ q Queryer }

// NewLegacyTopologyColumns binds a LegacyTopologyColumns to q.
func NewLegacyTopologyColumns(q Queryer) *LegacyTopologyColumns {
	return &LegacyTopologyColumns{q: q}
}

var _ ports.LegacyTopologyColumns = (*LegacyTopologyColumns)(nil)

// Present lists which of the three legacy topology columns exist, as
// "<table>.<column>" in name order.
func (c *LegacyTopologyColumns) Present(ctx context.Context) ([]string, error) {
	rows, err := c.q.QueryxContext(ctx, `
		SELECT table_name::text || '.' || column_name::text
		  FROM information_schema.columns
		 WHERE table_schema = current_schema()
		   AND (table_name::text, column_name::text) IN (('release_pipeline_runs', 'candidate_topology'),
		                                                 ('release_pipeline_runs', 'parsing_at_upgrade'),
		                                                 ('current_prod', 'topology_snapshot'))
		 ORDER BY 1`)
	if err != nil {
		return nil, fmt.Errorf("query legacy topology columns: %w", err)
	}
	defer func() { _ = rows.Close() }()

	present := []string{}
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scan legacy topology column: %w", err)
		}
		present = append(present, col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read legacy topology columns: %w", err)
	}
	return present, nil
}

// SchemaInitialized reports whether both release_pipeline_runs and current_prod
// exist in the current schema. It returns false for a reachable but
// un-migrated database, where every table and column is absent.
func (c *LegacyTopologyColumns) SchemaInitialized(ctx context.Context) (bool, error) {
	rows, err := c.q.QueryxContext(ctx, `
		SELECT count(*)
		  FROM information_schema.tables
		 WHERE table_schema = current_schema()
		   AND table_name::text IN ('release_pipeline_runs', 'current_prod')`)
	if err != nil {
		return false, fmt.Errorf("query release schema tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var n int
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			return false, fmt.Errorf("scan release schema table count: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("read release schema tables: %w", err)
	}
	return n == 2, nil
}
