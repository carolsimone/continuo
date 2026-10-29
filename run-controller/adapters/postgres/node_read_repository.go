package postgres

import (
	"context"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/projection"
	"github.com/carolsimone/continuo/run-controller/domain/repository"
)

var _ repository.NodeReader = (*NodeReadRepository)(nil)

// NodeReadRepository reads the history and catalog of a tenant's nodes. Each
// read goes through the node-run repository bound to the tenant it is given.
type NodeReadRepository struct {
	nodeRunsOf func(tenantID string) NodeRunRepository
}

// NewNodeReadRepository returns a NodeReadRepository; nodeRunsOf returns the
// node-run reads of one tenant.
func NewNodeReadRepository(nodeRunsOf func(tenantID string) NodeRunRepository) *NodeReadRepository {
	return &NodeReadRepository{nodeRunsOf: nodeRunsOf}
}

// ListNodeTasks returns the most recent tasks of a node, newest run first.
func (r *NodeReadRepository) ListNodeTasks(ctx context.Context, tenantID, serviceName, schemaName, tableName string, operation model.Operation, limit int) ([]*projection.NodeRun, error) {
	return r.nodeRunsOf(tenantID).List(ctx, serviceName, schemaName, tableName, operation, limit)
}

// ListNodes returns one page of the node catalog and how many nodes match.
func (r *NodeReadRepository) ListNodes(ctx context.Context, tenantID, search, serviceName string, operation model.Operation, limit, offset int) ([]*projection.NodeSummary, int, error) {
	return r.nodeRunsOf(tenantID).ListNodes(ctx, search, serviceName, operation, limit, offset)
}

// ListNodeNames returns the distinct table names of nodes that have run.
func (r *NodeReadRepository) ListNodeNames(ctx context.Context, tenantID, serviceName string) ([]string, error) {
	return r.nodeRunsOf(tenantID).ListNodeNames(ctx, serviceName)
}

// ListNodeServices returns the distinct service names of nodes that have run.
func (r *NodeReadRepository) ListNodeServices(ctx context.Context, tenantID string) ([]string, error) {
	return r.nodeRunsOf(tenantID).ListNodeServices(ctx)
}
