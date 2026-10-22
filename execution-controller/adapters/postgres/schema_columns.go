package postgres

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/outbox"
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
	missing, err := pkgdb.MissingColumns(ctx, c.exec, want)
	if err != nil {
		return nil, fmt.Errorf("execution database: %w", err)
	}
	return missing, nil
}
