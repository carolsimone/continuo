package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
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

// ListRuns returns a page of completed runs for a schedule, newest-first, along
// with the total number of completed runs matching the schedule (independent of
// the page window) so callers can paginate. limit must be > 0 and offset >= 0;
// the gRPC handler clamps both before calling.
func (r *OrchestratorQueryRepository) ListRuns(ctx context.Context, scheduleName string, limit, offset int) ([]*domain.RunSummary, int, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	// count and page share the same MATCH so their filter can never drift.
	countQuery := `
		MATCH (run:Run {schedule_name: $schedule_name})
		WHERE run.completed_at IS NOT NULL
		RETURN count(run) AS total
	`
	countResult, err := session.Run(ctx, countQuery, map[string]interface{}{"schedule_name": scheduleName})
	if err != nil {
		return nil, 0, fmt.Errorf("ListRuns count: %w", err)
	}
	var total int
	if countResult.Next(ctx) {
		if v, ok := recordValue(countResult.Record(), "total").(int64); ok {
			total = int(v)
		}
	}
	if err := countResult.Err(); err != nil {
		return nil, 0, fmt.Errorf("ListRuns count iterate: %w", err)
	}

	pageQuery := `
		MATCH (run:Run {schedule_name: $schedule_name})
		WHERE run.completed_at IS NOT NULL
		RETURN run.run_id AS run_id,
		       run.schedule_name AS schedule_name,
		       run.terminal_status AS terminal_status,
		       toString(run.created_at) AS created_at,
		       toString(run.completed_at) AS completed_at
		ORDER BY run.created_at DESC
		SKIP $offset
		LIMIT $limit
	`
	result, err := session.Run(ctx, pageQuery, map[string]interface{}{
		"schedule_name": scheduleName,
		"offset":        int64(offset),
		"limit":         int64(limit),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("ListRuns: %w", err)
	}

	var runs []*domain.RunSummary
	for result.Next(ctx) {
		record := result.Record()
		summary := &domain.RunSummary{
			RunID:          safeString(recordValue(record, "run_id")),
			ScheduleName:   safeString(recordValue(record, "schedule_name")),
			TerminalStatus: safeString(recordValue(record, "terminal_status")),
		}
		summary.CreatedAt = r.parseNeo4jTimestamp("created_at", safeString(recordValue(record, "created_at")))
		summary.CompletedAt = r.parseNeo4jTimestamp("completed_at", safeString(recordValue(record, "completed_at")))
		runs = append(runs, summary)
	}
	if err := result.Err(); err != nil {
		return nil, 0, fmt.Errorf("ListRuns iterate: %w", err)
	}
	return runs, total, nil
}

// GetRunGraph returns nodes with their execution status and edges for a run.
func (r *OrchestratorQueryRepository) GetRunGraph(ctx context.Context, runID string) ([]*domain.TableNode, []*domain.GraphEdge, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

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
	defer func() { _ = session.Close(ctx) }()

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
	defer func() { _ = session.Close(ctx) }()

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
		// :Table.last_updated_at is written by TopologyRepository with Cypher
		// `datetime()` (zoned), which the Go driver returns as time.Time.
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

// parseNeo4jTimestamp parses a Neo4j RFC3339Nano datetime string into a
// time.Time. An empty value maps to the zero time silently (the field was
// never set); a non-empty but unparseable value is logged before falling back
// to the zero time, so corrupt timestamps surface in the logs instead of
// vanishing.
func (r *OrchestratorQueryRepository) parseNeo4jTimestamp(field, value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	ts, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		r.logger.Warn("Unparseable Neo4j timestamp — using zero time",
			"field", field, "value", value, "error", err)
		return time.Time{}
	}
	return ts
}

// GetNodeAncestry returns the node identified by uniqueID at depth 0 plus its
// transitive upstream ancestors (following OUTGOING :DEPENDS_ON edges, since the
// edge points downstream -> upstream), deduped to shallowest depth, ordered by
// last_changed_at DESC with unknown-provenance nodes last. maxDepth <= 0 walks
// the full closure; maxDepth > 0 caps the hop count. Returns domain.ErrNodeNotFound
// when uniqueID is not an active :Table.
func (r *OrchestratorQueryRepository) GetNodeAncestry(ctx context.Context, uniqueID string, maxDepth int) ([]*domain.NodeAncestor, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	// Cypher cannot parameterize the *1..N path-length bound, so interpolate the
	// validated integer (handler enforces 0..100). Unbounded when maxDepth <= 0.
	hop := "*1.."
	if maxDepth > 0 {
		hop = fmt.Sprintf("*1..%d", maxDepth)
	}
	query := fmt.Sprintf(`
		MATCH (n:Table {unique_id: $uid})
		// COALESCE treats a missing active flag as true to remain consistent with all
		// other topology queries in this repository; do not tighten to n.active = true.
		WHERE COALESCE(n.active, true)
		OPTIONAL MATCH path = (n)-[:DEPENDS_ON%s]->(anc:Table)
		// Constrain EVERY node on the path to active, not just the terminal ancestor:
		// retired :Table nodes are kept for run history but are not part of the current
		// topology, and an active node reachable only THROUGH a retired one is a severed
		// dependency. Mirrors the active-upstream filter in GetScheduleGraph.
		WHERE ALL(m IN nodes(path) WHERE COALESCE(m.active, true))
		WITH n, anc, min(length(path)) AS depth
		RETURN n AS self,
		       collect(CASE WHEN anc IS NULL THEN null ELSE {node: anc, depth: depth} END) AS ancestors
	`, hop)

	result, err := session.Run(ctx, query, map[string]interface{}{"uid": uniqueID})
	if err != nil {
		return nil, fmt.Errorf("GetNodeAncestry query failed: %w", err)
	}
	if !result.Next(ctx) {
		if err := result.Err(); err != nil {
			return nil, fmt.Errorf("GetNodeAncestry query error: %w", err)
		}
		return nil, domain.ErrNodeNotFound
	}
	record := result.Record()

	selfRaw, _ := record.Get("self")
	selfNode, ok := selfRaw.(neo4j.Node)
	if !ok {
		return nil, domain.ErrNodeNotFound
	}

	out := []*domain.NodeAncestor{ancestorFromProps(selfNode.Props, 0)}

	if ancRaw, ok := record.Get("ancestors"); ok {
		if ancList, ok := ancRaw.([]interface{}); ok {
			for _, item := range ancList {
				m, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				node, ok := m["node"].(neo4j.Node)
				if !ok {
					continue
				}
				depth := 0
				if d, ok := m["depth"].(int64); ok {
					depth = int(d)
				}
				out = append(out, ancestorFromProps(node.Props, depth))
			}
		}
	}

	// Order by last_changed_at DESC, unknown (nil) last. Neo4j orders nulls first
	// under DESC, so sort in Go instead.
	sort.SliceStable(out, func(i, j int) bool {
		ci, cj := out[i].LastChangedAt, out[j].LastChangedAt
		if ci == nil && cj == nil {
			return false
		}
		if ci == nil {
			return false
		}
		if cj == nil {
			return true
		}
		return ci.After(*cj)
	})
	return out, nil
}

// GetNode returns per-node topology metadata for a single active :Table,
// addressed by its (service, schema, table) identity. Returns
// domain.ErrNodeNotFound when no active node matches. test_count is read via
// intFieldPresent so a node predating test_count capture reports TestCountKnown
// = false rather than a misleading zero.
func (r *OrchestratorQueryRepository) GetNode(ctx context.Context, service, schema, table string) (*domain.NodeMeta, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	query := `
		MATCH (t:Table {service_name: $service, schema_name: $schema, table_name: $table})
		WHERE COALESCE(t.active, true)
		RETURN t.node_type AS node_type, t.test_count AS test_count
	`
	result, err := session.Run(ctx, query, map[string]interface{}{
		"service": service,
		"schema":  schema,
		"table":   table,
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
	tc, tcKnown := intFieldPresent(rec, "test_count")
	nt, _ := nodeType.(string)
	return &domain.NodeMeta{
		NodeType:       nt,
		TestCount:      tc,
		TestCountKnown: tcKnown,
	}, nil
}

func ancestorFromProps(props map[string]interface{}, depth int) *domain.NodeAncestor {
	a := &domain.NodeAncestor{
		UniqueID:      safeString(props["unique_id"]),
		SchemaName:    safeString(props["schema_name"]),
		TableName:     safeString(props["table_name"]),
		ServiceName:   safeString(props["service_name"]),
		NodeType:      safeString(props["node_type"]),
		Depth:         depth,
		FilePath:      safeString(props["original_file_path"]),
		LastCommitSHA: safeString(props["last_commit_sha"]),
		LastRepo:      safeString(props["last_repo"]),
		LastReleaseID: safeString(props["last_release_id"]),
	}
	switch v := props["last_changed_at"].(type) {
	case time.Time:
		t := v
		a.LastChangedAt = &t
	case neo4j.LocalDateTime:
		t := v.Time()
		a.LastChangedAt = &t
	}
	return a
}
