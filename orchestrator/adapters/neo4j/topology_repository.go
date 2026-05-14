package neo4jinfra

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/domain/topology"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// TopologyRepository implements the write-side of repository.TopologyRepository.
type TopologyRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

// Compile-time assertion that the neo4j adapter satisfies the domain port.
var _ repository.TopologyRepository = (*TopologyRepository)(nil)

func NewTopologyRepository(client Neo4jClient, logger *slog.Logger) *TopologyRepository {
	return &TopologyRepository{client: client, logger: logger}
}

// ApplySnapshot reconciles the current topology to exactly match the latest
// manifest snapshot. Nodes missing from the payload are retired from the
// current schedule graph, while historical nodes referenced by existing runs
// are preserved in Neo4j.
func (r *TopologyRepository) ApplySnapshot(ctx context.Context, nodes []*topology.TopologyNode, topologyGeneration int64) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin topology transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	currentNodes := make([]map[string]interface{}, 0, len(nodes))
	for _, node := range nodes {
		if err := r.upsertNodeTx(ctx, tx, node, topologyGeneration); err != nil {
			return err
		}
		currentNodes = append(currentNodes, map[string]interface{}{
			"schema_name": node.SchemaName,
			"table_name":  node.TableName,
		})
	}

	if err := r.retireMissingNodes(ctx, tx, currentNodes); err != nil {
		return err
	}
	if err := r.deleteInactiveOrphans(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit topology transaction: %w", err)
	}

	r.logger.Info("Applied topology snapshot", "node_count", len(nodes))
	return nil
}

// SetServiceMetadata writes the per-service metadata map onto the :TopologyRoot singleton node.
func (r *TopologyRepository) SetServiceMetadata(
	ctx context.Context,
	serviceMetadata map[string]map[string]string,
	topologyGeneration int64,
) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	payload, err := json.Marshal(serviceMetadata)
	if err != nil {
		return fmt.Errorf("marshal service_metadata: %w", err)
	}

	_, err = session.Run(ctx, `
		MERGE (root:TopologyRoot {id: 'singleton'})
		SET root.service_metadata = $service_metadata,
		    root.topology_generation = $topology_generation,
		    root.updated_at = datetime()
	`, map[string]interface{}{
		"service_metadata":    string(payload),
		"topology_generation": topologyGeneration,
	})
	return err
}

// UpsertNode creates or updates a table node with its upstream dependencies.
func (r *TopologyRepository) UpsertNode(ctx context.Context, node *topology.TopologyNode) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin node upsert transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := r.upsertNodeTx(ctx, tx, node, 0); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *TopologyRepository) upsertNodeTx(ctx context.Context, tx neo4j.ExplicitTransaction, node *topology.TopologyNode, topologyGeneration int64) error {
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
				t.image_tag = $image_tag,
				t.topology_generation = $topology_generation,
				t.active = true,
				t.retired_at = NULL,
				t.created_at = datetime(),
				t.last_updated_at = datetime()
			ON MATCH SET
				t.service_name = $service_name,
				t.owner = $owner,
				t.schedule_name = $schedule_name,
				t.criticality = $criticality,
				t.node_type = $node_type,
				t.manifest_version = $manifest_version,
				t.image_tag = $image_tag,
				t.topology_generation = $topology_generation,
				t.active = true,
				t.retired_at = NULL,
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
				t.image_tag = $image_tag,
				t.topology_generation = $topology_generation,
				t.active = true,
				t.retired_at = NULL,
				t.created_at = datetime(),
				t.last_updated_at = datetime()
			ON MATCH SET
				t.service_name = $service_name,
				t.owner = $owner,
				t.schedule_name = $schedule_name,
				t.criticality = $criticality,
				t.node_type = $node_type,
				t.manifest_version = $manifest_version,
				t.image_tag = $image_tag,
				t.topology_generation = $topology_generation,
				t.active = true,
				t.retired_at = NULL,
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
		"image_tag":             node.ImageTag,
		"topology_generation":   topologyGeneration,
		"upstream_dependencies": upstreamDeps,
	}

	result, err := tx.Run(ctx, query, params)
	if err != nil {
		r.logger.Error("Failed to upsert node", "error", err, "schema", node.SchemaName, "table", node.TableName)
		return fmt.Errorf("failed to upsert node: %w", err)
	}

	if _, err := result.Consume(ctx); err != nil {
		return fmt.Errorf("failed to consume upsert result: %w", err)
	}

	return nil
}

func (r *TopologyRepository) retireMissingNodes(
	ctx context.Context,
	tx neo4j.ExplicitTransaction,
	currentNodes []map[string]interface{},
) error {
	result, err := tx.Run(ctx, `
		MATCH (t:Table)
		WHERE COALESCE(t.active, true)
		  AND NOT any(node IN $current_nodes
		              WHERE node.schema_name = t.schema_name
		                AND node.table_name = t.table_name)
		SET t.active = false,
		    t.retired_at = datetime(),
		    t.last_updated_at = datetime()
		RETURN count(t) AS retired_count
	`, map[string]interface{}{
		"current_nodes": currentNodes,
	})
	if err != nil {
		return fmt.Errorf("failed to retire missing nodes: %w", err)
	}
	if result.Next(ctx) {
		if retired, ok := result.Record().Get("retired_count"); ok {
			r.logger.Info("Retired stale topology nodes", "count", retired)
		}
	}
	return result.Err()
}

func (r *TopologyRepository) deleteInactiveOrphans(ctx context.Context, tx neo4j.ExplicitTransaction) error {
	result, err := tx.Run(ctx, `
		MATCH (t:Table)
		WHERE COALESCE(t.active, true) = false
		  AND NOT EXISTS { MATCH (:Run)-[:EXECUTES]->(t) }
		DETACH DELETE t
		RETURN count(t) AS deleted_count
	`, nil)
	if err != nil {
		return fmt.Errorf("failed to delete inactive topology orphans: %w", err)
	}
	if result.Next(ctx) {
		if deleted, ok := result.Record().Get("deleted_count"); ok {
			r.logger.Info("Deleted inactive topology orphans", "count", deleted)
		}
	}
	return result.Err()
}

// GetScheduleGraph returns all nodes and edges for a schedule.
// NOTE: This is a minimal implementation to satisfy the repository.TopologyRepository interface.
// The full graph read is handled by OrchestratorQueryRepository.
func (r *TopologyRepository) GetScheduleGraph(ctx context.Context, scheduleName string) ([]*topology.Node, []*topology.UpstreamDependency, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	nodeQuery := `
		MATCH (n:Table {schedule_name: $schedule_name})
		WHERE COALESCE(n.active, true)
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
		WHERE COALESCE(n.active, true) AND COALESCE(u.active, true)
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
