package runreads

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/projection"
	"github.com/carolsimone/continuo/state/domain/repository"
)

// NodeCatalogEntry is one row of the node catalog with the task timeout the
// node's tasks get.
type NodeCatalogEntry struct {
	projection.NodeSummary
	// Timeout is the task timeout of the node: the one the live release
	// declares for it, else the install default.
	Timeout time.Duration
	// TimeoutDeclared is true when the live release declares the timeout.
	TimeoutDeclared bool
}

// Nodes reads the history and catalog of the nodes of the tenant each read is
// given.
type Nodes struct {
	nodes          repository.NodeReader
	defaultTimeout time.Duration
}

// NewNodes returns a Nodes over nodes; defaultTimeout is the task timeout of a
// node whose live release declares none.
func NewNodes(nodes repository.NodeReader, defaultTimeout time.Duration) *Nodes {
	return &Nodes{nodes: nodes, defaultTimeout: defaultTimeout}
}

// ListNodeTasks returns the most recent tasks of a node of tenantID applying
// operation, newest run first. The repository caps limit at 50.
func (n *Nodes) ListNodeTasks(ctx context.Context, tenantID, serviceName, schemaName, tableName string, operation model.Operation, limit int) ([]*projection.NodeRun, error) {
	return n.nodes.ListNodeTasks(ctx, tenantID, serviceName, schemaName, tableName, operation, limit)
}

// ListNodes returns one page of tenantID's node catalog and how many nodes
// match. A limit below 1 becomes DefaultPageSize, one above MaxPageSize
// becomes MaxPageSize, and a negative offset becomes 0.
func (n *Nodes) ListNodes(ctx context.Context, tenantID, search, serviceName string, operation model.Operation, limit, offset int) ([]NodeCatalogEntry, int, error) {
	if limit < 1 {
		limit = DefaultPageSize
	}
	limit = min(limit, MaxPageSize)
	offset = max(offset, 0)

	rows, total, err := n.nodes.ListNodes(ctx, tenantID, search, serviceName, operation, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defaults := run.TaskDefaults{Timeout: n.defaultTimeout}
	out := make([]NodeCatalogEntry, 0, len(rows))
	for _, r := range rows {
		timeout, declared := run.EffectiveTimeout(r.TimeoutSeconds, defaults)
		out = append(out, NodeCatalogEntry{NodeSummary: *r, Timeout: timeout, TimeoutDeclared: declared})
	}
	return out, total, nil
}

// ListNodeNames returns the distinct table names of tenantID's nodes that have
// run, only serviceName's when it is not empty.
func (n *Nodes) ListNodeNames(ctx context.Context, tenantID, serviceName string) ([]string, error) {
	return n.nodes.ListNodeNames(ctx, tenantID, serviceName)
}

// ListNodeServices returns the distinct service names of tenantID's nodes that
// have run.
func (n *Nodes) ListNodeServices(ctx context.Context, tenantID string) ([]string, error) {
	return n.nodes.ListNodeServices(ctx, tenantID)
}
