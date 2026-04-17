package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// TopologyRepository implements the write-side of topology.Repository.
type TopologyRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

func NewTopologyRepository(client Neo4jClient, logger *slog.Logger) *TopologyRepository {
	return &TopologyRepository{client: client, logger: logger}
}

// UpsertNode creates or updates a table node with its upstream dependencies.
func (r *TopologyRepository) UpsertNode(ctx context.Context, node *topology.TopologyNode) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	// Build query based on whether there are upstream dependencies
	var query string
	if len(node.Dependencies) == 0 {
		// No upstream dependencies - simpler query
		query = `
			MERGE (t:Table {schema_name: $schema_name, table_name: $table_name})
			ON CREATE SET
				t.service_name = $service_name,
				t.owner = $owner,
				t.schedule_name = $schedule_name,
				t.criticality = $criticality,
				t.node_type = $node_type,
				t.manifest_version = $manifest_version,
				t.created_at = datetime(),
				t.last_updated_at = datetime()
			ON MATCH SET
				t.service_name = $service_name,
				t.owner = $owner,
				t.schedule_name = $schedule_name,
				t.criticality = $criticality,
				t.node_type = $node_type,
				t.manifest_version = $manifest_version,
				t.last_updated_at = datetime()
			WITH t
			OPTIONAL MATCH (t)-[old:DEPENDS_ON]->()
			DELETE old
			RETURN t.table_name AS table_name
		`
	} else {
		// Has upstream dependencies
		query = `
			MERGE (t:Table {schema_name: $schema_name, table_name: $table_name})
			ON CREATE SET
				t.service_name = $service_name,
				t.owner = $owner,
				t.schedule_name = $schedule_name,
				t.criticality = $criticality,
				t.node_type = $node_type,
				t.manifest_version = $manifest_version,
				t.created_at = datetime(),
				t.last_updated_at = datetime()
			ON MATCH SET
				t.service_name = $service_name,
				t.owner = $owner,
				t.schedule_name = $schedule_name,
				t.criticality = $criticality,
				t.node_type = $node_type,
				t.manifest_version = $manifest_version,
				t.last_updated_at = datetime()
			WITH t
			OPTIONAL MATCH (t)-[old:DEPENDS_ON]->()
			DELETE old
			WITH t
			UNWIND $upstream_dependencies AS dep
			MERGE (upstream:Table {schema_name: dep.schema_name, table_name: dep.table_name})
			ON CREATE SET upstream.service_name = dep.service_name
			MERGE (t)-[:DEPENDS_ON]->(upstream)
			RETURN t.table_name AS table_name
		`
	}

	// Convert upstream dependencies to map format
	upstreamDeps := make([]map[string]interface{}, 0, len(node.Dependencies))
	for _, dep := range node.Dependencies {
		upstreamDeps = append(upstreamDeps, map[string]interface{}{
			"schema_name":  dep.SchemaName,
			"table_name":   dep.TableName,
			"service_name": dep.ServiceName,
		})
	}

	// If no upstream dependencies, use empty array
	if len(upstreamDeps) == 0 {
		upstreamDeps = []map[string]interface{}{}
	}

	params := map[string]interface{}{
		"table_name":            node.TableName,
		"schema_name":           node.SchemaName,
		"service_name":          node.ServiceName,
		"owner":                 node.Owner,
		"schedule_name":         node.ScheduleName,
		"criticality":           node.Criticality,
		"node_type":             node.NodeType,
		"manifest_version":      node.ManifestVersion,
		"upstream_dependencies": upstreamDeps,
	}

	result, err := session.Run(ctx, query, params)
	if err != nil {
		r.logger.Error("Failed to upsert node", "error", err, "schema", node.SchemaName, "table", node.TableName)
		return fmt.Errorf("failed to upsert node: %w", err)
	}

	if _, err := result.Consume(ctx); err != nil {
		return fmt.Errorf("failed to consume upsert result: %w", err)
	}

	return nil
}

// GetScheduleGraph returns all nodes and edges for a schedule.
// NOTE: This is a minimal implementation to satisfy the topology.Repository interface.
// The full graph read is handled by QueryRepository.
func (r *TopologyRepository) GetScheduleGraph(ctx context.Context, scheduleName string) ([]*topology.Node, []*topology.UpstreamDependency, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	nodeQuery := `
		MATCH (n:Table {schedule_name: $schedule_name})
		RETURN n.table_name AS table_name,
		       n.schema_name AS schema_name,
		       n.service_name AS service_name,
		       COALESCE(n.owner, "") AS owner,
		       n.schedule_name AS schedule_name,
		       COALESCE(n.criticality, "unspecified") AS criticality,
		       COALESCE(n.node_type, "") AS node_type,
		       COALESCE(n.manifest_version, "") AS manifest_version,
		       n.last_updated_at AS last_updated_at,
		       n.created_at AS created_at
	`
	nodeResult, err := session.Run(ctx, nodeQuery, map[string]interface{}{
		"schedule_name": scheduleName,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("GetScheduleGraph nodes: %w", err)
	}

	var nodes []*topology.Node
	for nodeResult.Next(ctx) {
		record := nodeResult.Record()
		n := &topology.Node{
			TableName:       safeString(recordValue(record, "table_name")),
			SchemaName:      safeString(recordValue(record, "schema_name")),
			ServiceName:     safeString(recordValue(record, "service_name")),
			Owner:           safeString(recordValue(record, "owner")),
			ScheduleName:    safeString(recordValue(record, "schedule_name")),
			NodeType:        safeString(recordValue(record, "node_type")),
			ManifestVersion: safeString(recordValue(record, "manifest_version")),
		}
		if ldt, ok := recordValue(record, "last_updated_at").(neo4j.LocalDateTime); ok {
			n.LastUpdatedAt = ldt.Time()
		}
		if ldt, ok := recordValue(record, "created_at").(neo4j.LocalDateTime); ok {
			n.CreatedAt = ldt.Time()
		}
		nodes = append(nodes, n)
	}
	if err := nodeResult.Err(); err != nil {
		return nil, nil, fmt.Errorf("GetScheduleGraph nodes iterate: %w", err)
	}

	depQuery := `
		MATCH (n:Table {schedule_name: $schedule_name})-[:DEPENDS_ON]->(u:Table)
		RETURN n.table_name AS from_table, n.schema_name AS from_schema,
		       u.table_name AS to_table, u.schema_name AS to_schema,
		       COALESCE(u.service_name, "") AS to_service
	`
	depResult, err := session.Run(ctx, depQuery, map[string]interface{}{
		"schedule_name": scheduleName,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("GetScheduleGraph deps: %w", err)
	}

	var deps []*topology.UpstreamDependency
	for depResult.Next(ctx) {
		record := depResult.Record()
		deps = append(deps, &topology.UpstreamDependency{
			TableName:   safeString(recordValue(record, "to_table")),
			SchemaName:  safeString(recordValue(record, "to_schema")),
			ServiceName: safeString(recordValue(record, "to_service")),
		})
	}
	if err := depResult.Err(); err != nil {
		return nil, nil, fmt.Errorf("GetScheduleGraph deps iterate: %w", err)
	}

	return nodes, deps, nil
}
