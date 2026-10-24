package handlers

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
)

// RequiredColumns are the columns execution-controller's code reads that an
// execution database not yet migrated lacks: what each attempt showed
// (deployments.facts), which attempt a deployment runs, the report last
// emitted for it and the guard table keyed by cancelled run.
var RequiredColumns = []string{"deployments.facts", "deployments.attempt", "deployments.last_report", "cancelled_runs.run_id"}

// ErrSchemaNotReady reports an execution database that lacks a column the
// running code reads.
var ErrSchemaNotReady = errors.New("the execution database is not migrated")

// CheckSchema returns ErrSchemaNotReady, naming each missing column, until
// every required column exists. With the bundled Postgres a new pod can start
// before the migration Job has applied; refusing to start until then keeps the
// code off a schema it cannot use.
func CheckSchema(ctx context.Context, cols ports.SchemaColumns) error {
	missing, err := cols.Missing(ctx, RequiredColumns)
	if err != nil {
		return fmt.Errorf("read the execution database schema: %w", err)
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s absent; execution-controller starts once the execution database migration "+
		"(the chart's db-init-migrate Job) has applied it", ErrSchemaNotReady, strings.Join(missing, ", "))
}
