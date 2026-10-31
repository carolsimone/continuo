package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/catalog/domain"
)

// NodeQueryRepository implements the read-side (CQRS) queries that bypass aggregates.
// Used directly by gRPC handlers.
type NodeQueryRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

func NewNodeQueryRepository(client Neo4jClient, logger *slog.Logger) *NodeQueryRepository {
	return &NodeQueryRepository{client: client, logger: logger}
}

// getNodeCypher reads the tenant's :Node with one (service, schema, table)
// identity: a live node first, then the most recently retired, ties broken by
// the smallest unique_id. $include_inactive false matches live nodes only.
const getNodeCypher = `
MATCH (n:Node {tenant_id: $tenant_id, service_name: $service, schema_name: $schema, table_name: $table})
WHERE $include_inactive OR coalesce(n.active, true)
WITH n, NOT coalesce(n.active, true) AS inactive
RETURN n.node_type AS node_type, n.test_count AS test_count, inactive
ORDER BY inactive ASC, n.retired_at DESC, n.unique_id ASC
LIMIT 1`

// GetNode returns per-node topology metadata for one of the tenant's nodes,
// addressed by its (service, schema, table) identity. With
// domain.ActiveNodesOnly only a live node matches; with
// domain.IncludeInactiveNodes a retired node also matches (reported with
// Inactive = true) and a live match is preferred when both exist; among
// several retired matches the most recently retired wins, ties broken by the
// smallest unique_id. Returns domain.ErrNodeNotFound when no eligible node
// matches.
// test_count is read via intFieldPresent so a node predating test_count
// capture reports TestCountKnown = false rather than a misleading zero.
func (r *NodeQueryRepository) GetNode(ctx context.Context, tenantID, service, schema, table string, scope domain.NodeScope) (*domain.NodeMeta, error) {
	meta, err := inReadTx(ctx, r.client, tenantID, func(run TenantRunner) (*domain.NodeMeta, error) {
		result, err := run.Run(ctx, getNodeCypher, map[string]any{
			"service":          service,
			"schema":           schema,
			"table":            table,
			"include_inactive": scope == domain.IncludeInactiveNodes,
		})
		if err != nil {
			return nil, err
		}
		if !result.Next(ctx) {
			return nil, result.Err()
		}
		rec := result.Record()
		nodeType, _ := recordString(rec, "node_type")
		tc, tcKnown := intFieldPresent(rec, "test_count")
		return &domain.NodeMeta{
			NodeType:       nodeType,
			TestCount:      tc,
			TestCountKnown: tcKnown,
			Inactive:       recordBool(rec, "inactive"),
		}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("GetNode: %w", err)
	}
	if meta == nil {
		return nil, domain.ErrNodeNotFound
	}
	return meta, nil
}

// getNodeLocationCypher reads the source path and owning service of the
// tenant's live :Node.
const getNodeLocationCypher = `
MATCH (n:Node {tenant_id: $tenant_id, unique_id: $uid})
WHERE coalesce(n.active, true)
RETURN coalesce(n.original_file_path, '') AS file_path, n.service_name AS service_name`

// GetNodeLocation returns the project-relative source path and owning service
// of the tenant's node. An unknown or retired unique_id yields
// domain.ErrNodeNotFound: a retired node runs no source.
func (r *NodeQueryRepository) GetNodeLocation(ctx context.Context, tenantID, uniqueID string) (*domain.NodeLocation, error) {
	loc, err := inReadTx(ctx, r.client, tenantID, func(run TenantRunner) (*domain.NodeLocation, error) {
		result, err := run.Run(ctx, getNodeLocationCypher, map[string]any{"uid": uniqueID})
		if err != nil {
			return nil, err
		}
		if !result.Next(ctx) {
			return nil, result.Err()
		}
		rec := result.Record()
		fp, _ := recordString(rec, "file_path")
		svc, _ := recordString(rec, "service_name")
		return &domain.NodeLocation{FilePath: fp, ServiceName: svc}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("GetNodeLocation: %w", err)
	}
	if loc == nil {
		return nil, domain.ErrNodeNotFound
	}
	return loc, nil
}
