package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// OrchestratorQueryRepository implements the read-side (CQRS) queries that bypass aggregates.
// Used directly by gRPC handlers.
type OrchestratorQueryRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

func NewOrchestratorQueryRepository(client Neo4jClient, logger *slog.Logger) *OrchestratorQueryRepository {
	return &OrchestratorQueryRepository{client: client, logger: logger}
}

// GetScheduleGraph returns all nodes and edges for a schedule,
// including cross-boundary upstream dependencies (e.g. seeds).
func (r *OrchestratorQueryRepository) GetScheduleGraph(ctx context.Context, scheduleName string) (*domain.ScheduleGraph, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	query := `
        OPTIONAL MATCH (root:TopologyRoot {id: 'singleton'})
        WITH COALESCE(root.promotion_seq, 0) AS promotion_seq
        OPTIONAL MATCH (n:Table {schedule_name: $schedule_name})
        WHERE COALESCE(n.active, true)
        OPTIONAL MATCH (n)-[:DEPENDS_ON]->(u:Table)
          WHERE COALESCE(u.active, true)
            AND (u.schedule_name <> $schedule_name OR u.schedule_name IS NULL)
        WITH promotion_seq,
             collect(DISTINCT n) AS scheduleNodes,
             collect(DISTINCT u) AS externalNodes,
             collect(DISTINCT CASE WHEN u IS NOT NULL THEN {
               from_id: n.service_name + '.' + n.schema_name + '.' + n.table_name,
               to_id:   u.service_name + '.' + u.schema_name + '.' + u.table_name
             } END) AS crossEdges
        UNWIND (CASE WHEN size(scheduleNodes)=0 THEN [null] ELSE scheduleNodes END) AS n
        OPTIONAL MATCH (n)-[:DEPENDS_ON]->(m:Table {schedule_name: $schedule_name})
          WHERE COALESCE(m.active, true)
        WITH promotion_seq, scheduleNodes, externalNodes, crossEdges,
             collect(DISTINCT CASE WHEN m IS NOT NULL THEN {
               from_id: n.service_name + '.' + n.schema_name + '.' + n.table_name,
               to_id:   m.service_name + '.' + m.schema_name + '.' + m.table_name
             } END) AS internalEdges
        RETURN promotion_seq,
               scheduleNodes + externalNodes AS allNodes,
               crossEdges + internalEdges    AS allEdges
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

	var promotionSeq int64
	if v, ok := recordValue(record, "promotion_seq").(int64); ok {
		promotionSeq = v
	}

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
	return &domain.ScheduleGraph{Nodes: nodes, Edges: edges, PromotionSeq: promotionSeq}, nil
}

// GetGeneration returns the promotion seq of the live topology: the
// :TopologyRoot singleton's promotion_seq, written in the same transaction as
// the swap that made that topology live. It returns 0 before the first
// promotion. The schedule-graph cache keys on it.
func (r *OrchestratorQueryRepository) GetGeneration(ctx context.Context) (int64, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	result, err := session.Run(ctx, `
		OPTIONAL MATCH (root:TopologyRoot {id: 'singleton'})
		RETURN COALESCE(root.promotion_seq, 0) AS seq
	`, nil)
	if err != nil {
		return 0, fmt.Errorf("GetGeneration: %w", err)
	}
	if !result.Next(ctx) {
		if err := result.Err(); err != nil {
			return 0, fmt.Errorf("GetGeneration iterate: %w", err)
		}
		return 0, nil
	}
	if v, ok := recordValue(result.Record(), "seq").(int64); ok {
		return v, nil
	}
	return 0, nil
}

// ListActiveRuns returns ALL in-flight :Run nodes (completed_at IS NULL),
// ordered by schedule_name then newest-first (created_at DESC). The query
// does not deduplicate: multiple rows for the same schedule_name can appear
// when a concurrent-trigger invariant is violated, making the anomaly
// observable to the caller. Deduplication to the newest run per schedule is
// the responsibility of the caller (the run reconciler).
func (r *OrchestratorQueryRepository) ListActiveRuns(ctx context.Context) ([]*domain.ActiveRun, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	query := `
        MATCH (r:Run)
        WHERE r.completed_at IS NULL
        RETURN r.schedule_name AS schedule_name,
               r.run_id        AS run_id,
               COALESCE(r.promotion_seq, 0) AS promotion_seq
        ORDER BY r.schedule_name, r.created_at DESC
    `
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("ListActiveRuns: %w", err)
	}

	runs := make([]*domain.ActiveRun, 0)
	for result.Next(ctx) {
		record := result.Record()
		scheduleName := safeString(recordValue(record, "schedule_name"))
		runID := safeString(recordValue(record, "run_id"))
		var seq int64
		if v, ok := recordValue(record, "promotion_seq").(int64); ok {
			seq = v
		}
		runs = append(runs, &domain.ActiveRun{
			ScheduleName: scheduleName,
			RunID:        runID,
			PromotionSeq: seq,
		})
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("ListActiveRuns iterate: %w", err)
	}
	return runs, nil
}

// ListScheduleTopologies returns one row per schedule_name that has at least
// one active :Table, with node_count and the most recent last_updated_at
// across that schedule's nodes. Schedules with zero active nodes are omitted.
// Null schedule_name rows are defensively excluded.
func (r *OrchestratorQueryRepository) ListScheduleTopologies(ctx context.Context) ([]*domain.ScheduleTopologySummary, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	query := `
        MATCH (t:Table)
        WHERE COALESCE(t.active, true) AND t.schedule_name IS NOT NULL
        RETURN t.schedule_name        AS schedule_name,
               count(t)               AS node_count,
               max(t.last_updated_at) AS last_updated_at
        ORDER BY t.schedule_name
    `
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("ListScheduleTopologies: %w", err)
	}
	out := make([]*domain.ScheduleTopologySummary, 0)
	for result.Next(ctx) {
		record := result.Record()
		summary := &domain.ScheduleTopologySummary{
			ScheduleName: safeString(recordValue(record, "schedule_name")),
		}
		if v, ok := recordValue(record, "node_count").(int64); ok {
			summary.NodeCount = int(v)
		}
		// :Table.last_updated_at is written with Cypher `datetime()` (zoned),
		// which the Go driver returns as time.Time.
		// Test fixtures may use `localdatetime()` which arrives as
		// neo4j.LocalDateTime. Handle both so the gRPC field is populated in
		// production and tests.
		switch v := recordValue(record, "last_updated_at").(type) {
		case time.Time:
			summary.LastUpdatedAt = v
		case neo4j.LocalDateTime:
			summary.LastUpdatedAt = v.Time()
		}
		out = append(out, summary)
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("ListScheduleTopologies iterate: %w", err)
	}
	return out, nil
}

// GetNode returns per-node topology metadata for a single :Table, addressed by
// its (service, schema, table) identity. With domain.ActiveNodesOnly only an
// active :Table matches; with domain.IncludeInactiveNodes a :Table no longer
// active in the topology also matches (reported with Inactive = true) and an
// active match is preferred when both exist; among several inactive matches the
// most recently retired wins, ties broken by the smallest unique_id. Returns domain.ErrNodeNotFound
// when no eligible node matches.
// test_count is read via intFieldPresent so a node predating test_count
// capture reports TestCountKnown = false rather than a misleading zero.
func (r *OrchestratorQueryRepository) GetNode(ctx context.Context, service, schema, table string, scope domain.NodeScope) (*domain.NodeMeta, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	query := `
		MATCH (t:Table {service_name: $service, schema_name: $schema, table_name: $table})
		WHERE $include_inactive OR COALESCE(t.active, true)
		WITH t, NOT COALESCE(t.active, true) AS inactive
		RETURN t.node_type AS node_type, t.test_count AS test_count, inactive
		ORDER BY inactive ASC, t.retired_at DESC, t.unique_id ASC
		LIMIT 1
	`
	result, err := session.Run(ctx, query, map[string]interface{}{
		"service":          service,
		"schema":           schema,
		"table":            table,
		"include_inactive": scope == domain.IncludeInactiveNodes,
	})
	if err != nil {
		return nil, fmt.Errorf("GetNode query failed: %w", err)
	}
	if !result.Next(ctx) {
		if err := result.Err(); err != nil {
			return nil, fmt.Errorf("GetNode query error: %w", err)
		}
		return nil, domain.ErrNodeNotFound
	}
	rec := result.Record()
	nodeType, _ := rec.Get("node_type")
	inactiveVal, _ := rec.Get("inactive")
	tc, tcKnown := intFieldPresent(rec, "test_count")
	nt, _ := nodeType.(string)
	inactive, _ := inactiveVal.(bool)
	return &domain.NodeMeta{
		NodeType:       nt,
		TestCount:      tc,
		TestCountKnown: tcKnown,
		Inactive:       inactive,
	}, nil
}

// GetNodeLocation returns the node's project-relative source path and owning
// service. An unknown or inactive unique_id yields domain.ErrNodeNotFound.
func (r *OrchestratorQueryRepository) GetNodeLocation(ctx context.Context, uniqueID string) (*domain.NodeLocation, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	query := `
		MATCH (t:Table {unique_id: $uid})
		WHERE COALESCE(t.active, true)
		RETURN COALESCE(t.original_file_path, '') AS file_path, t.service_name AS service_name
	`
	result, err := session.Run(ctx, query, map[string]interface{}{"uid": uniqueID})
	if err != nil {
		return nil, fmt.Errorf("GetNodeLocation query failed: %w", err)
	}
	if !result.Next(ctx) {
		if err := result.Err(); err != nil {
			return nil, fmt.Errorf("GetNodeLocation query error: %w", err)
		}
		return nil, domain.ErrNodeNotFound
	}
	rec := result.Record()
	fp, _ := rec.Get("file_path")
	svc, _ := rec.Get("service_name")
	fpStr, _ := fp.(string)
	svcStr, _ := svc.(string)
	return &domain.NodeLocation{FilePath: fpStr, ServiceName: svcStr}, nil
}
