package runreads

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/projection"
	"github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/carolsimone/continuo/run-controller/domain/seeds"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
)

// NodeCatalogEntry is one row of the node list with the task timeout the
// node's tasks get.
type NodeCatalogEntry struct {
	projection.NodeSummary
	// Timeout is the task timeout of the node: the one the live release
	// declares for it, else the install default.
	Timeout time.Duration
	// TimeoutDeclared is true when the live release declares the timeout.
	TimeoutDeclared bool
}

// Nodes reads the history and node list of the nodes of the tenant each read is
// given.
type Nodes struct {
	nodes          repository.NodeReader
	outages        repository.ViewOutageReader
	defaultTimeout time.Duration
}

// NewNodes returns a Nodes over nodes and the view outages in outages;
// defaultTimeout is the task timeout of a node whose live release declares
// none.
func NewNodes(nodes repository.NodeReader, outages repository.ViewOutageReader, defaultTimeout time.Duration) *Nodes {
	return &Nodes{nodes: nodes, outages: outages, defaultTimeout: defaultTimeout}
}

// ViewOutage returns the open outage of tenantID's node that builds
// schemaName.tableName, a view a seed load left missing or stale; found is
// false when the node has none.
func (n *Nodes) ViewOutage(ctx context.Context, tenantID, schemaName, tableName string) (seeds.ViewOutage, bool, error) {
	return n.outages.Get(ctx, tenantID, topology.KeyOf(schemaName, tableName))
}

// ListNodeTasks returns the most recent tasks of a node of tenantID applying
// operation, newest run first. The repository caps limit at 50.
func (n *Nodes) ListNodeTasks(ctx context.Context, tenantID, serviceName, schemaName, tableName string, operation model.Operation, limit int) ([]*projection.NodeRun, error) {
	return n.nodes.ListNodeTasks(ctx, tenantID, serviceName, schemaName, tableName, operation, limit)
}

// ListNodes returns one page of tenantID's node list, the nodes of its live
// release, and how many nodes match; dormantOnly keeps only dormant nodes. A
// limit below 1 becomes DefaultPageSize, one above MaxPageSize becomes
// MaxPageSize, and a negative offset becomes 0.
func (n *Nodes) ListNodes(ctx context.Context, tenantID, search, serviceName string, operation model.Operation, dormantOnly bool, limit, offset int) ([]NodeCatalogEntry, int, error) {
	if limit < 1 {
		limit = DefaultPageSize
	}
	limit = min(limit, MaxPageSize)
	offset = max(offset, 0)

	rows, total, err := n.nodes.ListNodes(ctx, tenantID, search, serviceName, operation, dormantOnly, limit, offset)
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
