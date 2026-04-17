package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// QueryRepository implements the read-side (CQRS) queries that bypass aggregates.
// Used directly by gRPC handlers.
type QueryRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

func NewQueryRepository(client Neo4jClient, logger *slog.Logger) *QueryRepository {
	return &QueryRepository{client: client, logger: logger}
}

// GetScheduleGraph returns all nodes and edges for a schedule,
// including cross-boundary upstream dependencies (e.g. seeds).
func (r *QueryRepository) GetScheduleGraph(ctx context.Context, scheduleName string) (*domain.ScheduleGraph, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	query := `
		MATCH (n:Table {schedule_name: $schedule_name})
		OPTIONAL MATCH (n)-[:DEPENDS_ON]->(u:Table)
		  WHERE u.schedule_name <> $schedule_name OR u.schedule_name IS NULL
		WITH collect(DISTINCT n) AS scheduleNodes,
		     collect(DISTINCT u) AS externalNodes,
		     collect(DISTINCT CASE WHEN u IS NOT NULL THEN {
		       from_id: n.service_name + '.' + n.schema_name + '.' + n.table_name,
		       to_id:   u.service_name + '.' + u.schema_name + '.' + u.table_name
		     } END) AS crossEdges
		UNWIND scheduleNodes AS n
		OPTIONAL MATCH (n)-[:DEPENDS_ON]->(m:Table {schedule_name: $schedule_name})
		WITH scheduleNodes, externalNodes, crossEdges,
		     collect(DISTINCT CASE WHEN m IS NOT NULL THEN {
		       from_id: n.service_name + '.' + n.schema_name + '.' + n.table_name,
		       to_id:   m.service_name + '.' + m.schema_name + '.' + m.table_name
		     } END) AS internalEdges
		RETURN scheduleNodes + externalNodes AS allNodes,
		       crossEdges + internalEdges AS allEdges
	`

	result, err := session.Run(ctx, query, map[string]interface{}{
		"schedule_name": scheduleName,
	})
	if err != nil {
		return nil, fmt.Errorf("GetScheduleGraph query failed: %w", err)
	}

	if !result.Next(ctx) {
		if err := result.Err(); err != nil {
			return nil, fmt.Errorf("GetScheduleGraph query error: %w", err)
		}
		return &domain.ScheduleGraph{}, nil
	}

	record := result.Record()

	rawNodes, _ := record.Get("allNodes")
	rawEdges, _ := record.Get("allEdges")

	nodes := make([]*domain.TableNode, 0)
	if nodeList, ok := rawNodes.([]interface{}); ok {
		for _, item := range nodeList {
			if item == nil {
				continue
			}
			nodeMap, ok := item.(neo4j.Node)
			if !ok {
				continue
			}
			props := nodeMap.Props
			node := &domain.TableNode{
				TableName:    safeString(props["table_name"]),
				SchemaName:   safeString(props["schema_name"]),
				ServiceName:  safeString(props["service_name"]),
				Owner:        safeString(props["owner"]),
				ScheduleName: safeString(props["schedule_name"]),
				Criticality:  domain.Criticality(safeString(props["criticality"])),
				NodeType:     safeString(props["node_type"]),
			}
			if lastUpdatedAtNeo, ok := props["last_updated_at"].(neo4j.LocalDateTime); ok {
				node.LastUpdatedAt = lastUpdatedAtNeo.Time()
			}
			if createdAtNeo, ok := props["created_at"].(neo4j.LocalDateTime); ok {
				node.CreatedAt = createdAtNeo.Time()
			}
			nodes = append(nodes, node)
		}
	}

	edges := make([]*domain.GraphEdge, 0)
	if edgeList, ok := rawEdges.([]interface{}); ok {
		for _, item := range edgeList {
			if item == nil {
				continue
			}
			edgeMap, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			fromID, _ := edgeMap["from_id"].(string)
			toID, _ := edgeMap["to_id"].(string)
			if fromID == "" || toID == "" {
				continue
			}
			edges = append(edges, &domain.GraphEdge{
				FromNodeID: fromID,
				ToNodeID:   toID,
			})
		}
	}

	r.logger.Debug("GetScheduleGraph", "schedule", scheduleName, "nodes", len(nodes), "edges", len(edges))
	return &domain.ScheduleGraph{Nodes: nodes, Edges: edges}, nil
}

// ListRuns returns completed runs for a schedule ordered by creation date descending.
func (r *QueryRepository) ListRuns(ctx context.Context, scheduleName string) ([]*domain.RunSummary, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	query := `
		MATCH (run:Run {schedule_name: $schedule_name})
		WHERE run.completed_at IS NOT NULL
		RETURN run.run_id AS run_id,
		       run.schedule_name AS schedule_name,
		       run.terminal_status AS terminal_status,
		       toString(run.created_at) AS created_at,
		       toString(run.completed_at) AS completed_at
		ORDER BY run.created_at DESC
	`
	result, err := session.Run(ctx, query, map[string]interface{}{"schedule_name": scheduleName})
	if err != nil {
		return nil, fmt.Errorf("ListRuns: %w", err)
	}

	var runs []*domain.RunSummary
	for result.Next(ctx) {
		record := result.Record()
		summary := &domain.RunSummary{
			RunID:          safeString(recordValue(record, "run_id")),
			ScheduleName:   safeString(recordValue(record, "schedule_name")),
			TerminalStatus: safeString(recordValue(record, "terminal_status")),
		}
		summary.CreatedAt = parseNeo4jTimestamp(safeString(recordValue(record, "created_at")))
		summary.CompletedAt = parseNeo4jTimestamp(safeString(recordValue(record, "completed_at")))
		runs = append(runs, summary)
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("ListRuns iterate: %w", err)
	}
	return runs, nil
}

// GetRunGraph returns nodes with their execution status and edges for a run.
func (r *QueryRepository) GetRunGraph(ctx context.Context, runID string) ([]*domain.TableNode, []*domain.GraphEdge, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	nodeQuery := `
		MATCH (:Run {run_id: $run_id})-[exec:EXECUTES]->(n:Table)
		RETURN n.table_name AS table_name,
		       n.schema_name AS schema_name,
		       n.service_name AS service_name,
		       COALESCE(n.owner, "") AS owner,
		       COALESCE(n.schedule_name, "") AS schedule_name,
		       COALESCE(n.criticality, "unspecified") AS criticality,
		       n.last_updated_at AS last_updated_at,
		       n.created_at AS created_at,
		       COALESCE(n.node_type, "") AS node_type,
		       COALESCE(exec.status, "") AS status
	`
	nodeResult, err := session.Run(ctx, nodeQuery, map[string]interface{}{"run_id": runID})
	if err != nil {
		return nil, nil, fmt.Errorf("GetRunGraph nodes: %w", err)
	}

	var nodes []*domain.TableNode
	for nodeResult.Next(ctx) {
		record := nodeResult.Record()
		node, err := recordToTableNode(record)
		if err != nil {
			return nil, nil, fmt.Errorf("GetRunGraph parse node: %w", err)
		}
		node.Status = safeString(recordValue(record, "status"))
		nodes = append(nodes, node)
	}
	if err := nodeResult.Err(); err != nil {
		return nil, nil, fmt.Errorf("GetRunGraph nodes iterate: %w", err)
	}

	edgeQuery := `
		MATCH (run:Run {run_id: $run_id})-[:EXECUTES]->(from_n:Table)-[:DEPENDS_ON]->(to_n:Table)
		WHERE EXISTS { MATCH (run)-[:EXECUTES]->(to_n) }
		RETURN from_n.service_name + '.' + from_n.schema_name + '.' + from_n.table_name AS from_node_id,
		       to_n.service_name + '.' + to_n.schema_name + '.' + to_n.table_name AS to_node_id
	`
	edgeResult, err := session.Run(ctx, edgeQuery, map[string]interface{}{"run_id": runID})
	if err != nil {
		return nil, nil, fmt.Errorf("GetRunGraph edges: %w", err)
	}

	var edges []*domain.GraphEdge
	for edgeResult.Next(ctx) {
		record := edgeResult.Record()
		fromNodeID := safeString(recordValue(record, "from_node_id"))
		toNodeID := safeString(recordValue(record, "to_node_id"))
		if fromNodeID == "" || toNodeID == "" {
			continue
		}
		edges = append(edges, &domain.GraphEdge{FromNodeID: fromNodeID, ToNodeID: toNodeID})
	}
	if err := edgeResult.Err(); err != nil {
		return nil, nil, fmt.Errorf("GetRunGraph edges iterate: %w", err)
	}

	return nodes, edges, nil
}

// parseNeo4jTimestamp parses a Neo4j datetime string into a time.Time.
func parseNeo4jTimestamp(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	if ts, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return ts
	}
	if ts, err := time.Parse("2006-01-02T15:04:05.999999999Z07:00", value); err == nil {
		return ts
	}
	return time.Time{}
}
