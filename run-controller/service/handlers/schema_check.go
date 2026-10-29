package handlers

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/run-controller/service/ports"
)

// RequiredColumns are the columns run-controller's code reads that a run-controller database not
// yet migrated lacks: the per-attempt rows (task_attempts), the attempt counter
// on tasks, the timeout a release stores for each node, why a run
// failed when no task of it failed, when a stop was requested of an attempt,
// the node leases and the wait for one, and the tenant of each run, task and
// attempt, the command requests that make a command run once per request id,
// and the schedules table with the slot a scheduled run records. A migration
// applies whole, so a column also stands for what else its migration adds:
// task_attempts.stop_requested_at for the cancelling, stopping and stopped
// statuses, the node lease columns for node_leases' release trigger and the
// values tasks.wait_reason accepts, the tenant columns for the keys that
// lead with tenant_id, command_requests.request_id for the command requests
// table, and runs.scheduled_for for the index that allows one run per
// schedule slot.
var RequiredColumns = []string{"task_attempts.attempt", "tasks.attempt", "topology_nodes.timeout_seconds",
	"runs.failure_reason", "task_attempts.stop_requested_at",
	"node_leases.node_key", "tasks.wait_reason", "tasks.lease_wait_since",
	"runs.tenant_id", "tasks.tenant_id", "task_attempts.tenant_id",
	"command_requests.request_id",
	"schedules.tenant_id", "schedules.name", "schedules.cron", "schedules.timezone", "schedules.description",
	"schedules.paused", "schedules.next_fire_at", "schedules.version", "schedules.updated_by", "schedules.updated_at",
	"runs.scheduled_for"}

// ErrSchemaNotReady reports a run-controller database that lacks a column the running
// code reads.
var ErrSchemaNotReady = errors.New("the run-controller database is not migrated")

// CheckSchema returns ErrSchemaNotReady, naming each missing column, until
// every required column exists. With the bundled Postgres a new pod can start
// before the migration Job has applied; refusing to start until then keeps the
// code off a schema it cannot use.
func CheckSchema(ctx context.Context, cols ports.SchemaColumns) error {
	missing, err := cols.Missing(ctx, RequiredColumns)
	if err != nil {
		return fmt.Errorf("read the run-controller database schema: %w", err)
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s absent; run-controller starts once the run-controller database migration "+
		"(the chart's db-init-migrate Job) has applied it", ErrSchemaNotReady, strings.Join(missing, ", "))
}
