package handlers

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/state/service/ports"
)

// RequiredColumns are the columns state's code reads that a state database not
// yet migrated lacks: the per-attempt rows (task_attempts), the attempt counter
// on task_tracker, the timeout a release stores for each node, why a run
// failed when no task of it failed, when a stop was requested of an attempt,
// and the node leases and the wait for one. A migration applies whole, so a
// column also stands for what else its migration adds:
// task_attempts.stop_requested_at for the cancelling, stopping and stopped
// statuses, and the node lease columns for node_leases' release trigger and
// the values task_tracker.wait_reason accepts.
var RequiredColumns = []string{"task_attempts.attempt", "task_tracker.attempt", "topology_nodes.timeout_seconds",
	"scheduler_tracker.failure_reason", "task_attempts.stop_requested_at",
	"node_leases.node_key", "task_tracker.wait_reason", "task_tracker.lease_wait_since"}

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
