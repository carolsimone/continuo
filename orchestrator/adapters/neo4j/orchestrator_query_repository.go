package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
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
	defer session.Close(ctx)

	query := `
        OPTIONAL MATCH (root:TopologyRoot {id: 'singleton'})
        WITH COALESCE(root.topology_generation, 0) AS topology_generation
        OPTIONAL MATCH (n:Table {schedule_name: $schedule_name})
        WHERE COALESCE(n.active, true)
        OPTIONAL MATCH (n)-[:DEPENDS_ON]->(u:Table)
          WHERE COALESCE(u.active, true)
            AND (u.schedule_name <> $schedule_name OR u.schedule_name IS NULL)
        WITH topology_generation,
             collect(DISTINCT n) AS scheduleNodes,
             collect(DISTINCT u) AS externalNodes,
             collect(DISTINCT CASE WHEN u IS NOT NULL THEN {
               from_id: n.service_name + '.' + n.schema_name + '.' + n.table_name,
               to_id:   u.service_name + '.' + u.schema_name + '.' + u.table_name
             } END) AS crossEdges
        UNWIND (CASE WHEN size(scheduleNodes)=0 THEN [null] ELSE scheduleNodes END) AS n
        OPTIONAL MATCH (n)-[:DEPENDS_ON]->(m:Table {schedule_name: $schedule_name})
          WHERE COALESCE(m.active, true)
        WITH topology_generation, scheduleNodes, externalNodes, crossEdges,
             collect(DISTINCT CASE WHEN m IS NOT NULL THEN {
               from_id: n.service_name + '.' + n.schema_name + '.' + n.table_name,
               to_id:   m.service_name + '.' + m.schema_name + '.' + m.table_name
             } END) AS internalEdges
        RETURN topology_generation,
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

	var topologyGen int64
	if v, ok := recordValue(record, "topology_generation").(int64); ok {
		topologyGen = v
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
	return &domain.ScheduleGraph{Nodes: nodes, Edges: edges, TopologyGeneration: topologyGen}, nil
}

// ListRuns returns completed runs for a schedule ordered by creation date descending.
func (r *OrchestratorQueryRepository) ListRuns(ctx context.Context, scheduleName string) ([]*domain.RunSummary, error) {
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
func (r *OrchestratorQueryRepository) GetRunGraph(ctx context.Context, runID string) ([]*domain.TableNode, []*domain.GraphEdge, error) {
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

// GetRunTopologyGeneration returns the topology_generation stamped on the :Run
// node at Snapshot time. Returns 0 when the run does not exist OR when
// the property is unset (pre-tracking runs). The 0-vs-missing-vs-unset
// ambiguity is resolved at the service layer with a documented contract:
// 0 means "drift unknown".
func (r *OrchestratorQueryRepository) GetRunTopologyGeneration(ctx context.Context, runID string) (int64, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	query := `
		MATCH (r:Run {run_id: $run_id})
		RETURN COALESCE(r.topology_generation, 0) AS gen
	`
	result, err := session.Run(ctx, query, map[string]interface{}{"run_id": runID})
	if err != nil {
		return 0, fmt.Errorf("GetRunTopologyGeneration: %w", err)
	}
	if !result.Next(ctx) {
		if err := result.Err(); err != nil {
			return 0, fmt.Errorf("GetRunTopologyGeneration iterate: %w", err)
		}
		return 0, nil
	}
	raw, _ := result.Record().Get("gen")
	if v, ok := raw.(int64); ok {
		return v, nil
	}
	return 0, nil
}

// ListActiveRuns returns ALL in-flight :Run nodes (completed_at IS NULL),
// ordered by schedule_name then newest-first (created_at DESC). The query
// does not deduplicate: multiple rows for the same schedule_name can appear
// when a concurrent-trigger invariant is violated, making the anomaly
// observable to the caller. Deduplication to the newest run per schedule is
// the responsibility of the caller (RunQueryService).
func (r *OrchestratorQueryRepository) ListActiveRuns(ctx context.Context) ([]*domain.ActiveRun, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	query := `
        MATCH (r:Run)
        WHERE r.completed_at IS NULL
        RETURN r.schedule_name AS schedule_name,
               r.run_id        AS run_id,
               COALESCE(r.topology_generation, 0) AS topology_generation
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
		var gen int64
		if v, ok := recordValue(record, "topology_generation").(int64); ok {
			gen = v
		}
		runs = append(runs, &domain.ActiveRun{
			ScheduleName:       scheduleName,
			RunID:              runID,
			TopologyGeneration: gen,
		})
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("ListActiveRuns iterate: %w", err)
	}
	return runs, nil
}

// GetScheduleInitNodes runs all three scheduler-init queries in a single
// Neo4j read transaction to guarantee a consistent snapshot.
func (r *OrchestratorQueryRepository) GetScheduleInitNodes(ctx context.Context, scheduleName, runID string) (*run.ScheduleInitNodes, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	allNodes, err := r.getAllNodesInRun(ctx, tx, scheduleName, runID)
	if err != nil {
		return nil, err
	}
	rootNodes, err := r.getRootNodesInRun(ctx, tx, scheduleName, runID)
	if err != nil {
		return nil, err
	}
	seedNodes, err := r.getUpstreamSeedNodesInRun(ctx, tx, scheduleName, runID)
	if err != nil {
		return nil, err
	}

	return &run.ScheduleInitNodes{
		AllNodes:  allNodes,
		RootNodes: rootNodes,
		SeedNodes: seedNodes,
	}, nil
}

func (r *OrchestratorQueryRepository) getRootNodesInRun(ctx context.Context, tx neo4j.ExplicitTransaction, scheduleName, runID string) ([]*domain.TableNode, error) {
	query := `
		MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(t:Table {schedule_name: $schedule_name})
		WHERE NOT (t)-[:DEPENDS_ON]->(:Table {schedule_name: $schedule_name})
		RETURN
			t.schema_name AS schema_name,
			t.table_name AS table_name,
			t.service_name AS service_name,
			COALESCE(t.owner, "") AS owner,
			t.schedule_name AS schedule_name,
			COALESCE(t.criticality, "unspecified") AS criticality,
			t.last_updated_at AS last_updated_at,
			t.created_at AS created_at,
			COALESCE(t.node_type, "") AS node_type,
			COALESCE(e.task_id, "") AS task_id,
			COALESCE(e.manifest_version, "") AS manifest_version,
			COALESCE(e.image_tag, "") AS image_tag
		ORDER BY t.table_name
	`
	result, err := tx.Run(ctx, query, map[string]interface{}{
		"run_id":        runID,
		"schedule_name": scheduleName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query root nodes in run: %w", err)
	}
	return collectNodes(ctx, result)
}

func (r *OrchestratorQueryRepository) getUpstreamSeedNodesInRun(ctx context.Context, tx neo4j.ExplicitTransaction, scheduleName, runID string) ([]*domain.TableNode, error) {
	query := `
		MATCH (:Run {run_id: $run_id})-[:EXECUTES]->(t:Table {schedule_name: $schedule_name})
		MATCH (t)-[:DEPENDS_ON]->(s:Table {node_type: "dbt-seed"})
		MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(s)
		RETURN DISTINCT
			s.schema_name AS schema_name,
			s.table_name AS table_name,
			s.service_name AS service_name,
			COALESCE(s.owner, "") AS owner,
			s.schedule_name AS schedule_name,
			COALESCE(s.criticality, "unspecified") AS criticality,
			s.last_updated_at AS last_updated_at,
			s.created_at AS created_at,
			s.node_type AS node_type,
			COALESCE(e.task_id, "") AS task_id,
			COALESCE(e.manifest_version, "") AS manifest_version,
			COALESCE(e.image_tag, "") AS image_tag
		ORDER BY s.table_name
	`
	result, err := tx.Run(ctx, query, map[string]interface{}{
		"run_id":        runID,
		"schedule_name": scheduleName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query upstream seed nodes in run: %w", err)
	}
	return collectNodes(ctx, result)
}

func (r *OrchestratorQueryRepository) getAllNodesInRun(ctx context.Context, tx neo4j.ExplicitTransaction, scheduleName, runID string) ([]*domain.TableNode, error) {
	query := `
		CALL {
		    MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(t:Table {schedule_name: $schedule_name})
		    RETURN
		        t.schema_name   AS schema_name,
		        t.table_name    AS table_name,
		        t.service_name  AS service_name,
		        COALESCE(t.owner, "") AS owner,
		        t.schedule_name AS schedule_name,
		        COALESCE(t.criticality, "unspecified") AS criticality,
		        t.last_updated_at AS last_updated_at,
		        t.created_at AS created_at,
		        COALESCE(t.node_type, "") AS node_type,
		        COALESCE(e.task_id, "") AS task_id,
		        COALESCE(e.manifest_version, "") AS manifest_version,
		        COALESCE(e.image_tag, "") AS image_tag

		    UNION

		    MATCH (:Run {run_id: $run_id})-[:EXECUTES]->(t:Table {schedule_name: $schedule_name})
		    MATCH (t)-[:DEPENDS_ON]->(s:Table {node_type: "dbt-seed"})
		    MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(s)
		    RETURN
		        s.schema_name   AS schema_name,
		        s.table_name    AS table_name,
		        s.service_name  AS service_name,
		        COALESCE(s.owner, "") AS owner,
		        s.schedule_name AS schedule_name,
		        COALESCE(s.criticality, "unspecified") AS criticality,
		        s.last_updated_at AS last_updated_at,
		        s.created_at AS created_at,
		        s.node_type     AS node_type,
		        COALESCE(e.task_id, "") AS task_id,
		        COALESCE(e.manifest_version, "") AS manifest_version,
		        COALESCE(e.image_tag, "") AS image_tag
		}
		RETURN schema_name, table_name, service_name, owner, schedule_name, criticality, last_updated_at, created_at, node_type, task_id, manifest_version, image_tag
		ORDER BY table_name
	`
	result, err := tx.Run(ctx, query, map[string]interface{}{
		"run_id":        runID,
		"schedule_name": scheduleName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query all nodes in run: %w", err)
	}
	return collectNodes(ctx, result)
}

// ListScheduleTopologies returns one row per schedule_name that has at least
// one active :Table, with node_count and the most recent last_updated_at
// across that schedule's nodes. Schedules with zero active nodes are omitted.
// Null schedule_name rows are defensively excluded.
func (r *OrchestratorQueryRepository) ListScheduleTopologies(ctx context.Context) ([]*domain.ScheduleTopologySummary, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

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
		if v, ok := recordValue(record, "last_updated_at").(neo4j.LocalDateTime); ok {
			summary.LastUpdatedAt = v.Time()
		}
		out = append(out, summary)
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("ListScheduleTopologies iterate: %w", err)
	}
	return out, nil
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
