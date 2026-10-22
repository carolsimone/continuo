package handlers

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/state/service/ports"
)

// RequiredColumns are the columns state's code reads that a state database not
// yet migrated lacks: the per-attempt rows (task_attempts) and the attempt
// counter on task_tracker.
var RequiredColumns = []string{"task_attempts.attempt", "task_tracker.attempt"}

// ErrSchemaNotReady reports a state database that lacks a column the running
// code reads.
var ErrSchemaNotReady = errors.New("the state database is not migrated")

// CheckSchema returns ErrSchemaNotReady, naming each missing column, until
// every required column exists. With the bundled Postgres a new pod can start
// before the migration Job has applied; refusing to start until then keeps the
// code off a schema it cannot use.
func CheckSchema(ctx context.Context, cols ports.SchemaColumns) error {
	missing, err := cols.Missing(ctx, RequiredColumns)
	if err != nil {
		return fmt.Errorf("read the state database schema: %w", err)
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s absent; state starts once the state database migration "+
		"(the chart's db-init-migrate Job) has applied it", ErrSchemaNotReady, strings.Join(missing, ", "))
}
