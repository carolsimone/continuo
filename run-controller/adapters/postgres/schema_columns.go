package postgres

import (
	"context"
	"fmt"

	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/run-controller/service/ports"
	"github.com/jmoiron/sqlx"
)

// SchemaColumns is the Postgres-backed ports.SchemaColumns. It reads
// information_schema for the connection's current schema.
type SchemaColumns struct{ db *sqlx.DB }

// NewSchemaColumns binds a SchemaColumns to db.
func NewSchemaColumns(db *sqlx.DB) *SchemaColumns { return &SchemaColumns{db: db} }

var _ ports.SchemaColumns = (*SchemaColumns)(nil)

// Missing returns the columns of want the current schema does not have, in
// the order given.
func (c *SchemaColumns) Missing(ctx context.Context, want []string) ([]string, error) {
	missing, err := pkgdb.MissingColumns(ctx, c.db, want)
	if err != nil {
		return nil, fmt.Errorf("run-controller database: %w", err)
	}
	return missing, nil
}
