package postgres

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/lib/pq"
)

// SchemaColumns is the Postgres-backed ports.SchemaColumns. It reads
// information_schema for the connection's current schema.
type SchemaColumns struct{ exec outbox.Executor }

// NewSchemaColumns binds a SchemaColumns to exec.
func NewSchemaColumns(exec outbox.Executor) *SchemaColumns { return &SchemaColumns{exec: exec} }

var _ ports.SchemaColumns = (*SchemaColumns)(nil)

// Missing returns the columns of want the current schema does not have, in
// the order given.
func (c *SchemaColumns) Missing(ctx context.Context, want []string) ([]string, error) {
	missing := []string{}
	if err := c.exec.SelectContext(ctx, &missing, `
		SELECT w.col
		  FROM unnest($1::text[]) WITH ORDINALITY AS w(col, ord)
		 WHERE NOT EXISTS (
		       SELECT 1
		         FROM information_schema.columns c
		        WHERE c.table_schema = current_schema()
		          AND c.table_name::text || '.' || c.column_name::text = w.col)
		 ORDER BY w.ord`, pq.Array(want)); err != nil {
		return nil, fmt.Errorf("read execution schema columns: %w", err)
	}
	return missing, nil
}
