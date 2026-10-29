package repository

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/projection"
)

// ErrRunNotFound is returned by a read of one run the tenant does not have.
// Another tenant's run is a run the tenant does not have.
var ErrRunNotFound = errors.New("run not found")

// RunDetailReader reads one run of a tenant for display.
type RunDetailReader interface {
	// GetRunDetail returns the tenant's run runID, or ErrRunNotFound.
	GetRunDetail(ctx context.Context, tenantID string, runID uuid.UUID) (projection.RunDetail, error)
}

// TaskDetailReader reads the tasks of a tenant's run for display.
type TaskDetailReader interface {
	// ListRunTasks returns one page of the tasks of the tenant's run runID,
	// newest first, only those in status when it is not nil, and how many
	// tasks match. A run the tenant does not have has no tasks.
	ListRunTasks(ctx context.Context, tenantID string, runID uuid.UUID, status *run.TaskStatus, limit, offset int) ([]projection.TaskDetail, int, error)
}

// NodeReader reads the history and catalog of a tenant's nodes.
type NodeReader interface {
	// ListNodeTasks returns the most recent tasks of a node applying
	// operation, newest run first; the tasks of model.OperationRun include
	// the node's full refreshes. limit is capped at 50, 0 meaning 50.
	ListNodeTasks(ctx context.Context, tenantID, serviceName, schemaName, tableName string, operation model.Operation, limit int) ([]*projection.NodeRun, error)

	// ListNodes returns one page of the node catalog of operation (the
	// catalog of model.OperationRun includes full refreshes) and how many
	// nodes match search (a case-insensitive exact match on the table name;
	// empty means all) and serviceName (exact; empty means all).
	ListNodes(ctx context.Context, tenantID, search, serviceName string, operation model.Operation, limit, offset int) ([]*projection.NodeSummary, int, error)

	// ListNodeNames returns the distinct table names of nodes that have run,
	// sorted, only serviceName's when it is not empty.
	ListNodeNames(ctx context.Context, tenantID, serviceName string) ([]string, error)

	// ListNodeServices returns the distinct service names of nodes that have
	// run, sorted.
	ListNodeServices(ctx context.Context, tenantID string) ([]string, error)
}

// LastRunReader reads the most recent run of each of a tenant's schedules.
type LastRunReader interface {
	// GetLastRunPerSchedule returns the tenant's most recent run per schedule
	// name.
	GetLastRunPerSchedule(ctx context.Context, tenantID string) (map[string]LastRunSummary, error)
}
