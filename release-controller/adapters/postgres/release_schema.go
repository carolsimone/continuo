package postgres

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// ReleaseSchema is the Postgres-backed ports.ReleaseSchema. It reads
// information_schema for the connection's current schema.
type ReleaseSchema struct{ q Queryer }

// NewReleaseSchema binds a ReleaseSchema to q.
func NewReleaseSchema(q Queryer) *ReleaseSchema {
	return &ReleaseSchema{q: q}
}

var _ ports.ReleaseSchema = (*ReleaseSchema)(nil)

// Initialized reports whether the tables of the release checks, the live
// release and the production pointers (release_checks, live_release and
// production_pointers) all exist in the current schema.
func (s *ReleaseSchema) Initialized(ctx context.Context) (bool, error) {
	var n int
	row := s.q.QueryRowxContext(ctx, `
		SELECT count(*)
		  FROM information_schema.tables
		 WHERE table_schema = current_schema()
		   AND table_name::text IN ('release_checks', 'live_release', 'production_pointers')`)
	if err := row.Scan(&n); err != nil {
		return false, fmt.Errorf("read release schema tables: %w", err)
	}
	return n == 3, nil
}
