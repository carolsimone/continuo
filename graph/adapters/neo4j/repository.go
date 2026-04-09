package neo4j

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/graph/domain/model"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

var (
	// ErrNotFound is returned when a node is not found
	ErrNotFound = errors.New("node not found")
)

// GraphRepository defines the interface for graph operations
type GraphRepository interface {
	CreateNode(ctx context.Context, req *CreateNodeRequest) (*model.TableNode, error)
	UpdateNodeTimestamp(ctx context.Context, schemaName, tableName string) (*model.TableNode, error)
	GetStaleRootNodes(ctx context.Context, hoursThreshold int) ([]*model.TableNode, error)
	GetDownstreamDependencies(ctx context.Context, scheduleName, schemaName, tableName string, maxDepth int) ([]*model.Dependency, error)
	CheckUpstreamFreshness(ctx context.Context, schemaName, tableName string, freshnessMinutes int) (bool, []*model.StaleUpstream, error)
	GetScheduleGraph(ctx context.Context, scheduleName string) (*model.ScheduleGraph, error)
	GetScheduleInitNodes(ctx context.Context, scheduleName, runID string) (*model.ScheduleInitNodes, error)
	// Execution-state projection (write-through from dependency-controller)
	UpdateNodeStatus(ctx context.Context, scheduleName, schemaName, tableName, status, runID string) error
	GetReadyDownstream(ctx context.Context, scheduleName, schemaName, tableName, runID string) ([]*model.DownstreamNode, error)
	CheckScheduleCompletion(ctx context.Context, scheduleName, runID string) (isComplete bool, hasFailed bool, err error)
	// GetTransitiveDownstream returns all nodes that transitively depend on the given
	// node and are not in SUCCEEDED status. Used during re-run to determine reset scope.
	GetTransitiveDownstream(ctx context.Context, scheduleName, schemaName, tableName string) ([]*model.TableNode, error)
	SnapshotGraph(ctx context.Context, runID, scheduleName string) error
	FinalizeRun(ctx context.Context, runID, terminalStatus string) error
	ListRuns(ctx context.Context, scheduleName string) ([]*model.RunSummary, error)
	GetRunGraph(ctx context.Context, runID string) (*model.ScheduleGraph, error)
	DeleteExpiredRuns(ctx context.Context, retentionDays int) error
}

// CreateNodeRequest represents the parameters for creating a node
type CreateNodeRequest struct {
	TableName            string
	SchemaName           string
	ServiceName          string
	Owner                string
	ScheduleName         string
	Criticality          model.Criticality
	UpstreamDependencies []*model.UpstreamDependency
	NodeType             string // new
	ManifestVersion      string
}

// graphRepository implements GraphRepository
type graphRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

// NewGraphRepository creates a new graph repository
func NewGraphRepository(client Neo4jClient, logger *slog.Logger) GraphRepository {
	return &graphRepository{
		client: client,
		logger: logger,
	}
}

// CreateNode creates or updates a table node with dependencies
func (r *graphRepository) CreateNode(ctx context.Context, req *CreateNodeRequest) (*model.TableNode, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	// Build query based on whether there are upstream dependencies
	var query string
	if len(req.UpstreamDependencies) == 0 {
		// No upstream dependencies - simpler query
		query = `
			MERGE (t:Table {schema: $schema, table_name: $table_name})
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
			RETURN t.table_name AS table_name,
				   t.schema AS schema_name,
				   t.service_name AS service_name,
				   t.owner AS owner,
				   t.schedule_name AS schedule_name,
				   t.criticality AS criticality,
				   t.node_type AS node_type,
				   t.last_updated_at AS last_updated_at,
				   t.created_at AS created_at
		`
	} else {
		// Has upstream dependencies
		query = `
			MERGE (t:Table {schema: $schema, table_name: $table_name})
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
			MERGE (upstream:Table {schema: dep.schema_name, table_name: dep.table_name})
			ON CREATE SET upstream.service_name = dep.service_name
			MERGE (t)-[:DEPENDS_ON]->(upstream)
			RETURN t.table_name AS table_name,
				   t.schema AS schema_name,
				   t.service_name AS service_name,
				   t.owner AS owner,
				   t.schedule_name AS schedule_name,
				   t.criticality AS criticality,
				   t.node_type AS node_type,
				   t.last_updated_at AS last_updated_at,
				   t.created_at AS created_at
		`
	}

	// Convert upstream dependencies to map format
	upstreamDeps := make([]map[string]interface{}, 0, len(req.UpstreamDependencies))
	for _, dep := range req.UpstreamDependencies {
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
		"table_name":            req.TableName,
		"schema":                req.SchemaName,
		"service_name":          req.ServiceName,
		"owner":                 req.Owner,
		"schedule_name":         req.ScheduleName,
		"criticality":           req.Criticality.String(),
		"node_type":             req.NodeType,
		"manifest_version":      req.ManifestVersion,
		"upstream_dependencies": upstreamDeps,
	}

	result, err := session.Run(ctx, query, params)
	if err != nil {
		r.logger.Error("Failed to create node", "error", err, "schema", req.SchemaName, "table", req.TableName)
		return nil, fmt.Errorf("failed to create node: %w", err)
	}

	if result.Next(ctx) {
		record := result.Record()
		return r.recordToTableNode(record)
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("query execution error: %w", err)
	}

	return nil, ErrNotFound
}

// UpdateNodeTimestamp updates only the last_updated_at timestamp
func (r *graphRepository) UpdateNodeTimestamp(ctx context.Context, schemaName, tableName string) (*model.TableNode, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	query := `
		MATCH (t:Table {schema: $schema, table_name: $table_name})
		SET t.last_updated_at = datetime()
		RETURN t.table_name AS table_name,
			   t.schema AS schema_name,
			   t.service_name AS service_name,
			   t.owner AS owner,
			   t.schedule_name AS schedule_name,
			   t.criticality AS criticality,
			   COALESCE(t.node_type, "") AS node_type,
			   t.last_updated_at AS last_updated_at,
			   t.created_at AS created_at
	`

	params := map[string]interface{}{
		"schema":     schemaName,
		"table_name": tableName,
	}

	result, err := session.Run(ctx, query, params)
	if err != nil {
		r.logger.Error("Failed to update node timestamp", "error", err, "schema", schemaName, "table", tableName)
		return nil, fmt.Errorf("failed to update node timestamp: %w", err)
	}

	if result.Next(ctx) {
		record := result.Record()
		return r.recordToTableNode(record)
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("query execution error: %w", err)
	}

	return nil, ErrNotFound
}

// GetStaleRootNodes finds root nodes (no upstream deps) not updated in N hours
func (r *graphRepository) GetStaleRootNodes(ctx context.Context, hoursThreshold int) ([]*model.TableNode, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	query := `
		MATCH (t:Table)
		WHERE NOT (t)-[:DEPENDS_ON]->()
		  AND t.last_updated_at < datetime() - duration({hours: $hours})
		RETURN t.table_name AS table_name,
			   t.schema AS schema_name,
			   t.service_name AS service_name,
			   t.owner AS owner,
			   t.schedule_name AS schedule_name,
			   t.criticality AS criticality,
			   COALESCE(t.node_type, "") AS node_type,
			   t.last_updated_at AS last_updated_at,
			   t.created_at AS created_at
		ORDER BY t.last_updated_at ASC
	`

	params := map[string]interface{}{
		"hours": hoursThreshold,
	}

	result, err := session.Run(ctx, query, params)
	if err != nil {
		r.logger.Error("Failed to get stale root nodes", "error", err, "hours", hoursThreshold)
		return nil, fmt.Errorf("failed to get stale root nodes: %w", err)
	}

	nodes := make([]*model.TableNode, 0)
	for result.Next(ctx) {
		record := result.Record()
		node, err := r.recordToTableNode(record)
		if err != nil {
			r.logger.Warn("Failed to parse node record", "error", err)
			continue
		}
		nodes = append(nodes, node)
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("query execution error: %w", err)
	}

	r.logger.Debug("Found stale root nodes", "count", len(nodes), "hours_threshold", hoursThreshold)
	return nodes, nil
}

// GetDownstreamDependencies traverses downstream from a node
func (r *graphRepository) GetDownstreamDependencies(ctx context.Context, scheduleName, schemaName, tableName string, maxDepth int) ([]*model.Dependency, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	// Build depth constraint
	depthConstraint := "*1.."
	if maxDepth > 0 {
		depthConstraint = fmt.Sprintf("*1..%d", maxDepth)
	}

	query := fmt.Sprintf(`
		MATCH (start:Table {schedule_name: $schedule_name, schema: $schema, table_name: $table_name})
		MATCH path = (start)<-[:DEPENDS_ON%s]-(downstream:Table)
		RETURN DISTINCT downstream.table_name AS table_name,
			   downstream.schema AS schema_name,
			   downstream.service_name AS service_name,
			   downstream.owner AS owner,
			   downstream.schedule_name AS schedule_name,
			   downstream.criticality AS criticality,
			   COALESCE(downstream.node_type, "") AS node_type,
			   downstream.last_updated_at AS last_updated_at,
			   downstream.created_at AS created_at,
			   length(path) AS depth
		ORDER BY depth ASC, downstream.table_name ASC
	`, depthConstraint)

	params := map[string]interface{}{
		"schedule_name": scheduleName,
		"schema":        schemaName,
		"table_name":    tableName,
	}

	result, err := session.Run(ctx, query, params)
	if err != nil {
		r.logger.Error("Failed to get downstream dependencies", "error", err,
			"schedule", scheduleName, "schema", schemaName, "table", tableName)
		return nil, fmt.Errorf("failed to get downstream dependencies: %w", err)
	}

	dependencies := make([]*model.Dependency, 0)
	for result.Next(ctx) {
		record := result.Record()
		node, err := r.recordToTableNode(record)
		if err != nil {
			r.logger.Warn("Failed to parse node record", "error", err)
			continue
		}

		depth, ok := record.Get("depth")
		if !ok {
			r.logger.Warn("Depth not found in record")
			continue
		}

		depthInt, ok := depth.(int64)
		if !ok {
			r.logger.Warn("Depth is not an integer", "depth", depth)
			continue
		}

		dependencies = append(dependencies, &model.Dependency{
			Node:  node,
			Depth: int(depthInt),
		})
	}

	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("query execution error: %w", err)
	}

	r.logger.Debug("Found downstream dependencies", "count", len(dependencies),
		"schedule", scheduleName, "schema", schemaName, "table", tableName)
	return dependencies, nil
}

// CheckUpstreamFreshness checks if all upstream dependencies are fresh
func (r *graphRepository) CheckUpstreamFreshness(ctx context.Context, schemaName, tableName string, freshnessMinutes int) (bool, []*model.StaleUpstream, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	query := `
		MATCH (t:Table {schema: $schema, table_name: $table_name})-[:DEPENDS_ON]->(upstream:Table)
		WITH upstream,
			 duration.between(upstream.last_updated_at, datetime()).minutes AS minutes_stale
		WHERE minutes_stale > $freshness_minutes
		RETURN upstream.table_name AS table_name,
			   upstream.schema AS schema_name,
			   upstream.service_name AS service_name,
			   upstream.owner AS owner,
			   upstream.schedule_name AS schedule_name,
			   upstream.criticality AS criticality,
			   COALESCE(upstream.node_type, "") AS node_type,
			   upstream.last_updated_at AS last_updated_at,
			   upstream.created_at AS created_at,
			   toInteger(minutes_stale) AS minutes_since_update
		ORDER BY minutes_stale DESC
	`

	params := map[string]interface{}{
		"schema":            schemaName,
		"table_name":        tableName,
		"freshness_minutes": freshnessMinutes,
	}

	result, err := session.Run(ctx, query, params)
	if err != nil {
		r.logger.Error("Failed to check upstream freshness", "error", err,
			"schema", schemaName, "table", tableName)
		return false, nil, fmt.Errorf("failed to check upstream freshness: %w", err)
	}

	staleUpstreams := make([]*model.StaleUpstream, 0)
	for result.Next(ctx) {
		record := result.Record()
		node, err := r.recordToTableNode(record)
		if err != nil {
			r.logger.Warn("Failed to parse node record", "error", err)
			continue
		}

		minutesSinceUpdate, ok := record.Get("minutes_since_update")
		if !ok {
			r.logger.Warn("minutes_since_update not found in record")
			continue
		}

		minutesInt, ok := minutesSinceUpdate.(int64)
		if !ok {
			r.logger.Warn("minutes_since_update is not an integer", "minutes", minutesSinceUpdate)
			continue
		}

		staleUpstreams = append(staleUpstreams, &model.StaleUpstream{
			Node:               node,
			MinutesSinceUpdate: int(minutesInt),
		})
	}

	if err := result.Err(); err != nil {
		return false, nil, fmt.Errorf("query execution error: %w", err)
	}

	allFresh := len(staleUpstreams) == 0
	r.logger.Debug("Checked upstream freshness", "all_fresh", allFresh,
		"stale_count", len(staleUpstreams), "schema", schemaName, "table", tableName)

	return allFresh, staleUpstreams, nil
}

// GetScheduleGraph returns all nodes and edges for a schedule,
// including cross-boundary upstream dependencies (e.g. seeds).
func (r *graphRepository) GetScheduleGraph(ctx context.Context, scheduleName string) (*model.ScheduleGraph, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	query := `
		MATCH (n:Table {schedule_name: $schedule_name})
		OPTIONAL MATCH (n)-[:DEPENDS_ON]->(u:Table)
		  WHERE u.schedule_name <> $schedule_name OR u.schedule_name IS NULL
		WITH collect(DISTINCT n) AS scheduleNodes,
		     collect(DISTINCT u) AS externalNodes,
		     collect(DISTINCT CASE WHEN u IS NOT NULL THEN {
		       from_id: n.service_name + '.' + n.schema + '.' + n.table_name,
		       to_id:   u.service_name + '.' + u.schema + '.' + u.table_name
		     } END) AS crossEdges
		UNWIND scheduleNodes AS n
		OPTIONAL MATCH (n)-[:DEPENDS_ON]->(m:Table {schedule_name: $schedule_name})
		WITH scheduleNodes, externalNodes, crossEdges,
		     collect(DISTINCT CASE WHEN m IS NOT NULL THEN {
		       from_id: n.service_name + '.' + n.schema + '.' + n.table_name,
		       to_id:   m.service_name + '.' + m.schema + '.' + m.table_name
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
		return &model.ScheduleGraph{}, nil
	}

	record := result.Record()

	rawNodes, _ := record.Get("allNodes")
	rawEdges, _ := record.Get("allEdges")

	nodes := make([]*model.TableNode, 0)
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
			node := &model.TableNode{
				TableName:    safeString(props["table_name"]),
				SchemaName:   safeString(props["schema"]),
				ServiceName:  safeString(props["service_name"]),
				Owner:        safeString(props["owner"]),
				ScheduleName: safeString(props["schedule_name"]),
				Criticality:  model.Criticality(safeString(props["criticality"])),
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

	edges := make([]*model.GraphEdge, 0)
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
			edges = append(edges, &model.GraphEdge{
				FromNodeID: fromID,
				ToNodeID:   toID,
			})
		}
	}

	r.logger.Debug("GetScheduleGraph", "schedule", scheduleName, "nodes", len(nodes), "edges", len(edges))
	return &model.ScheduleGraph{Nodes: nodes, Edges: edges}, nil
}

// recordToTableNode converts a Neo4j record to a TableNode
func (r *graphRepository) recordToTableNode(record *neo4j.Record) (*model.TableNode, error) {
	tableName, _ := record.Get("table_name")
	schemaName, _ := record.Get("schema_name")
	serviceName, _ := record.Get("service_name")
	owner, _ := record.Get("owner")
	scheduleName, _ := record.Get("schedule_name")
	criticality, _ := record.Get("criticality")
	lastUpdatedAt, _ := record.Get("last_updated_at")
	createdAt, _ := record.Get("created_at")
	nodeType, _ := record.Get("node_type")

	node := &model.TableNode{
		TableName:    tableName.(string),
		SchemaName:   schemaName.(string),
		ServiceName:  serviceName.(string),
		Owner:        owner.(string),
		ScheduleName: scheduleName.(string),
		Criticality:  model.Criticality(criticality.(string)),
		NodeType:     safeString(nodeType),
	}

	// Convert Neo4j datetime to Go time.Time
	if lastUpdatedAtNeo, ok := lastUpdatedAt.(neo4j.LocalDateTime); ok {
		node.LastUpdatedAt = lastUpdatedAtNeo.Time()
	}

	if createdAtNeo, ok := createdAt.(neo4j.LocalDateTime); ok {
		node.CreatedAt = createdAtNeo.Time()
	}

	return node, nil
}

// safeString returns the string value of v, or "" if v is nil or not a string.
func safeString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func recordValue(record *neo4j.Record, key string) interface{} {
	value, _ := record.Get(key)
	return value
}

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

// GetScheduleInitNodes runs all three scheduler-init queries in a single
// Neo4j read transaction to guarantee a consistent snapshot.
func (r *graphRepository) GetScheduleInitNodes(ctx context.Context, scheduleName, runID string) (*model.ScheduleInitNodes, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

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

	return &model.ScheduleInitNodes{
		AllNodes:  allNodes,
		RootNodes: rootNodes,
		SeedNodes: seedNodes,
	}, nil
}

// Note: the private method names below match the plan. They accept
// neo4j.ExplicitTransaction and use tx.Run instead of session.Run.

func (r *graphRepository) getRootNodesInRun(ctx context.Context, tx neo4j.ExplicitTransaction, scheduleName, runID string) ([]*model.TableNode, error) {
	query := `
		MATCH (:Run {run_id: $run_id})-[:EXECUTES]->(t:Table {schedule_name: $schedule_name})
		WHERE NOT (t)-[:DEPENDS_ON]->(:Table {schedule_name: $schedule_name})
		RETURN
			t.schema AS schema_name,
			t.table_name AS table_name,
			t.service_name AS service_name,
			COALESCE(t.owner, "") AS owner,
			t.schedule_name AS schedule_name,
			COALESCE(t.criticality, "unspecified") AS criticality,
			t.last_updated_at AS last_updated_at,
			t.created_at AS created_at,
			COALESCE(t.node_type, "") AS node_type
		ORDER BY t.table_name
	`
	result, err := tx.Run(ctx, query, map[string]interface{}{
		"run_id":        runID,
		"schedule_name": scheduleName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query root nodes in run: %w", err)
	}
	return r.collectNodes(ctx, result)
}

func (r *graphRepository) getUpstreamSeedNodesInRun(ctx context.Context, tx neo4j.ExplicitTransaction, scheduleName, runID string) ([]*model.TableNode, error) {
	query := `
		MATCH (:Run {run_id: $run_id})-[:EXECUTES]->(t:Table {schedule_name: $schedule_name})
		MATCH (t)-[:DEPENDS_ON]->(s:Table {node_type: "dbt-seed"})
		MATCH (:Run {run_id: $run_id})-[:EXECUTES]->(s)
		RETURN DISTINCT
			s.schema AS schema_name,
			s.table_name AS table_name,
			s.service_name AS service_name,
			COALESCE(s.owner, "") AS owner,
			s.schedule_name AS schedule_name,
			COALESCE(s.criticality, "unspecified") AS criticality,
			s.last_updated_at AS last_updated_at,
			s.created_at AS created_at,
			s.node_type AS node_type
		ORDER BY s.table_name
	`
	result, err := tx.Run(ctx, query, map[string]interface{}{
		"run_id":        runID,
		"schedule_name": scheduleName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query upstream seed nodes in run: %w", err)
	}
	return r.collectNodes(ctx, result)
}

func (r *graphRepository) getAllNodesInRun(ctx context.Context, tx neo4j.ExplicitTransaction, scheduleName, runID string) ([]*model.TableNode, error) {
	query := `
		CALL {
		    MATCH (:Run {run_id: $run_id})-[:EXECUTES]->(t:Table {schedule_name: $schedule_name})
		    RETURN
		        t.schema        AS schema_name,
		        t.table_name    AS table_name,
		        t.service_name  AS service_name,
		        COALESCE(t.owner, "") AS owner,
		        t.schedule_name AS schedule_name,
		        COALESCE(t.criticality, "unspecified") AS criticality,
		        t.last_updated_at AS last_updated_at,
		        t.created_at AS created_at,
		        COALESCE(t.node_type, "") AS node_type

		    UNION

		    MATCH (:Run {run_id: $run_id})-[:EXECUTES]->(t:Table {schedule_name: $schedule_name})
		    MATCH (t)-[:DEPENDS_ON]->(s:Table {node_type: "dbt-seed"})
		    MATCH (:Run {run_id: $run_id})-[:EXECUTES]->(s)
		    RETURN
		        s.schema        AS schema_name,
		        s.table_name    AS table_name,
		        s.service_name  AS service_name,
		        COALESCE(s.owner, "") AS owner,
		        s.schedule_name AS schedule_name,
		        COALESCE(s.criticality, "unspecified") AS criticality,
		        s.last_updated_at AS last_updated_at,
		        s.created_at AS created_at,
		        s.node_type     AS node_type
		}
		RETURN schema_name, table_name, service_name, owner, schedule_name, criticality, last_updated_at, created_at, node_type
		ORDER BY table_name
	`
	result, err := tx.Run(ctx, query, map[string]interface{}{
		"run_id":        runID,
		"schedule_name": scheduleName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query all nodes in run: %w", err)
	}
	return r.collectNodes(ctx, result)
}

// UpdateNodeStatus sets the execution status on a Table node (write-through projection
// from dependency-controller; state service remains the authoritative source of truth).
func (r *graphRepository) UpdateNodeStatus(ctx context.Context, scheduleName, schemaName, tableName, status, runID string) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	query := `
		MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(t:Table)
		WHERE t.schema = $schema
		  AND t.table_name = $table_name
		  AND (t.schedule_name = $schedule_name OR t.node_type = 'dbt-seed')
		SET e.status = $status
	`

	result, err := session.Run(ctx, query, map[string]interface{}{
		"run_id":        runID,
		"schedule_name": scheduleName,
		"schema":        schemaName,
		"table_name":    tableName,
		"status":        status,
	})
	if err != nil {
		r.logger.Error("Failed to update node status via EXECUTES",
			"run_id", runID,
			"schedule_name", scheduleName,
			"schema", schemaName,
			"table_name", tableName,
			"status", status,
			"error", err,
		)
		return fmt.Errorf("failed to update node status: %w", err)
	}

	if _, err := result.Consume(ctx); err != nil {
		return fmt.Errorf("failed to consume update result: %w", err)
	}

	r.logger.Info("Updated node status via EXECUTES",
		"run_id", runID,
		"schedule_name", scheduleName,
		"schema", schemaName,
		"table_name", tableName,
		"status", status,
	)
	return nil
}

// GetReadyDownstream finds downstream nodes where ALL upstream dependencies have SUCCEEDED
// and the downstream node itself is PENDING or has no status yet.
func (r *graphRepository) GetReadyDownstream(ctx context.Context, scheduleName, schemaName, tableName, runID string) ([]*model.DownstreamNode, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	query := `
		MATCH (:Run {run_id: $run_id})-[snap:EXECUTES]->(downstream:Table {schedule_name: $schedule_name})
		WHERE (snap.status IS NULL OR snap.status = 'PENDING')
		  AND EXISTS {
			MATCH (downstream)-[:DEPENDS_ON]->(completed:Table)
			WHERE completed.schema = $schema AND completed.table_name = $table_name
		  }
		  AND NOT EXISTS {
			MATCH (downstream)-[:DEPENDS_ON]->(upstream:Table)
			WHERE NOT EXISTS {
				MATCH (:Run {run_id: $run_id})-[us:EXECUTES]->(upstream)
				WHERE us.status = 'SUCCEEDED'
			}
		  }
		RETURN downstream.service_name AS service_name,
		       downstream.schema AS schema_name,
		       downstream.table_name AS table_name,
		       COALESCE(downstream.node_type, "") AS node_type
	`

	result, err := session.Run(ctx, query, map[string]interface{}{
		"run_id":        runID,
		"schedule_name": scheduleName,
		"schema":        schemaName,
		"table_name":    tableName,
	})
	if err != nil {
		r.logger.Error("Failed to query ready downstream nodes",
			"run_id", runID,
			"schedule_name", scheduleName,
			"schema", schemaName,
			"table_name", tableName,
			"error", err,
		)
		return nil, fmt.Errorf("failed to query ready downstream nodes: %w", err)
	}

	var nodes []*model.DownstreamNode
	for result.Next(ctx) {
		record := result.Record()

		svcName, _ := record.Get("service_name")
		schemaVal, _ := record.Get("schema_name")
		tblName, _ := record.Get("table_name")
		nodeTypeRaw, _ := record.Get("node_type")

		nodeTypeStr := ""
		if s, ok := nodeTypeRaw.(string); ok {
			nodeTypeStr = s
		}

		nodes = append(nodes, &model.DownstreamNode{
			ServiceName: svcName.(string),
			SchemaName:  schemaVal.(string),
			TableName:   tblName.(string),
			NodeType:    nodeTypeStr,
		})
	}

	if err := result.Err(); err != nil {
		r.logger.Error("Error iterating downstream nodes",
			"run_id", runID,
			"schedule_name", scheduleName,
			"error", err,
		)
		return nil, fmt.Errorf("error iterating downstream nodes: %w", err)
	}

	r.logger.Info("Retrieved ready downstream nodes",
		"run_id", runID,
		"schedule_name", scheduleName,
		"schema", schemaName,
		"table_name", tableName,
		"count", len(nodes),
	)
	return nodes, nil
}

// CheckScheduleCompletion reports whether no node can change state anymore.
// isComplete=true when no node is RUNNING and no node is ready to trigger.
// hasFailed=true when at least one node has status FAILED.
func (r *graphRepository) CheckScheduleCompletion(ctx context.Context, scheduleName, runID string) (bool, bool, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	query := `
		MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(n:Table {schedule_name: $schedule_name})
		RETURN
			count(CASE
				WHEN e.status = 'RUNNING'
				THEN 1
				WHEN (e.status IS NULL OR e.status = 'PENDING')
				  AND NOT EXISTS {
					MATCH (n)-[:DEPENDS_ON]->(upstream:Table)
					WHERE EXISTS { MATCH (:Run {run_id: $run_id})-[:EXECUTES]->(upstream) }
					  AND NOT EXISTS {
						MATCH (:Run {run_id: $run_id})-[us:EXECUTES]->(upstream)
						WHERE us.status = 'SUCCEEDED'
					  }
				  }
				THEN 1 END) AS can_still_change,
			count(CASE WHEN e.status = 'FAILED' THEN 1 END) AS failed_count
	`

	result, err := session.Run(ctx, query, map[string]interface{}{
		"run_id":        runID,
		"schedule_name": scheduleName,
	})
	if err != nil {
		return false, false, fmt.Errorf("failed to check schedule completion: %w", err)
	}

	if !result.Next(ctx) {
		return false, false, fmt.Errorf("no result from CheckScheduleCompletion")
	}
	record := result.Record()

	canStillChange, _ := record.Get("can_still_change")
	failedCount, _ := record.Get("failed_count")

	isComplete := canStillChange.(int64) == 0
	hasFailed := failedCount.(int64) > 0

	r.logger.Info("Schedule completion check",
		"run_id", runID,
		"schedule_name", scheduleName,
		"can_still_change", canStillChange,
		"is_complete", isComplete,
		"has_failed", hasFailed,
	)
	return isComplete, hasFailed, result.Err()
}

func (r *graphRepository) SnapshotGraph(ctx context.Context, runID, scheduleName string) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	query := `
		MERGE (run:Run {run_id: $run_id})
		ON CREATE SET run.schedule_name = $schedule_name, run.created_at = datetime()
		WITH run
		CALL {
			WITH run
			MATCH (t:Table {schedule_name: $schedule_name})
			RETURN t AS node

			UNION

			WITH run
			MATCH (t:Table {schedule_name: $schedule_name})-[:DEPENDS_ON]->(s:Table {node_type: "dbt-seed"})
			RETURN s AS node
		}
		WITH DISTINCT run, node
		MERGE (run)-[e:EXECUTES]->(node)
		ON CREATE SET e.status = 'PENDING', e.manifest_version = COALESCE(node.manifest_version, '')
		RETURN count(e) AS edges_created
	`
	result, err := session.Run(ctx, query, map[string]interface{}{
		"run_id":        runID,
		"schedule_name": scheduleName,
	})
	if err != nil {
		return fmt.Errorf("SnapshotGraph: failed to create snapshot: %w", err)
	}
	if result.Next(ctx) {
		count, _ := result.Record().Get("edges_created")
		r.logger.Info("Graph snapshot created",
			"run_id", runID,
			"schedule_name", scheduleName,
			"edges", count,
		)
	}
	return result.Err()
}

func (r *graphRepository) FinalizeRun(ctx context.Context, runID, terminalStatus string) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	query := `
		MATCH (run:Run {run_id: $run_id})
		SET run.completed_at = datetime(),
		    run.terminal_status = $terminal_status
	`
	result, err := session.Run(ctx, query, map[string]interface{}{
		"run_id":          runID,
		"terminal_status": terminalStatus,
	})
	if err != nil {
		return fmt.Errorf("FinalizeRun: %w", err)
	}
	if _, err := result.Consume(ctx); err != nil {
		return fmt.Errorf("FinalizeRun consume: %w", err)
	}
	r.logger.Info("Run finalized", "run_id", runID, "terminal_status", terminalStatus)
	return nil
}

func (r *graphRepository) ListRuns(ctx context.Context, scheduleName string) ([]*model.RunSummary, error) {
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

	var runs []*model.RunSummary
	for result.Next(ctx) {
		record := result.Record()
		summary := &model.RunSummary{
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

func (r *graphRepository) GetRunGraph(ctx context.Context, runID string) (*model.ScheduleGraph, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	nodeQuery := `
		MATCH (:Run {run_id: $run_id})-[exec:EXECUTES]->(n:Table)
		RETURN n.table_name AS table_name,
		       n.schema AS schema_name,
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
		return nil, fmt.Errorf("GetRunGraph nodes: %w", err)
	}

	var nodes []*model.TableNode
	for nodeResult.Next(ctx) {
		record := nodeResult.Record()
		node, err := r.recordToTableNode(record)
		if err != nil {
			return nil, fmt.Errorf("GetRunGraph parse node: %w", err)
		}
		node.Status = safeString(recordValue(record, "status"))
		nodes = append(nodes, node)
	}
	if err := nodeResult.Err(); err != nil {
		return nil, fmt.Errorf("GetRunGraph nodes iterate: %w", err)
	}

	edgeQuery := `
		MATCH (run:Run {run_id: $run_id})-[:EXECUTES]->(from_n:Table)-[:DEPENDS_ON]->(to_n:Table)
		WHERE EXISTS { MATCH (run)-[:EXECUTES]->(to_n) }
		RETURN from_n.service_name + '.' + from_n.schema + '.' + from_n.table_name AS from_node_id,
		       to_n.service_name + '.' + to_n.schema + '.' + to_n.table_name AS to_node_id
	`
	edgeResult, err := session.Run(ctx, edgeQuery, map[string]interface{}{"run_id": runID})
	if err != nil {
		return nil, fmt.Errorf("GetRunGraph edges: %w", err)
	}

	var edges []*model.GraphEdge
	for edgeResult.Next(ctx) {
		record := edgeResult.Record()
		fromNodeID := safeString(recordValue(record, "from_node_id"))
		toNodeID := safeString(recordValue(record, "to_node_id"))
		if fromNodeID == "" || toNodeID == "" {
			continue
		}
		edges = append(edges, &model.GraphEdge{FromNodeID: fromNodeID, ToNodeID: toNodeID})
	}
	if err := edgeResult.Err(); err != nil {
		return nil, fmt.Errorf("GetRunGraph edges iterate: %w", err)
	}

	return &model.ScheduleGraph{Nodes: nodes, Edges: edges}, nil
}

func (r *graphRepository) DeleteExpiredRuns(ctx context.Context, retentionDays int) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	query := `
		MATCH (run:Run)
		WHERE run.completed_at IS NOT NULL
		  AND run.completed_at < datetime() - duration({days: $retention_days})
		DETACH DELETE run
	`
	result, err := session.Run(ctx, query, map[string]interface{}{"retention_days": retentionDays})
	if err != nil {
		return fmt.Errorf("DeleteExpiredRuns: %w", err)
	}
	summary, err := result.Consume(ctx)
	if err != nil {
		return fmt.Errorf("DeleteExpiredRuns consume: %w", err)
	}
	if deleted := summary.Counters().NodesDeleted(); deleted > 0 {
		r.logger.Info("Swept expired run nodes", "deleted", deleted, "retention_days", retentionDays)
	}
	return nil
}

// GetTransitiveDownstream returns all nodes that transitively depend on the given
// node (i.e. downstream in execution order) and are NOT in SUCCEEDED status.
// Edge direction: (child)-[:DEPENDS_ON]->(parent), so traversing in reverse
// from the start node finds all downstream dependents.
func (r *graphRepository) GetTransitiveDownstream(ctx context.Context, scheduleName, schemaName, tableName string) ([]*model.TableNode, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	query := `
		MATCH (start:Table {schedule_name: $schedule_name, schema: $schema, table_name: $table_name})
		MATCH (downstream:Table {schedule_name: $schedule_name})-[:DEPENDS_ON*1..]->(start)
		WHERE downstream.status IS NULL OR downstream.status <> 'SUCCEEDED'
		RETURN DISTINCT
			downstream.table_name    AS table_name,
			downstream.schema        AS schema_name,
			coalesce(downstream.service_name, '')  AS service_name,
			downstream.node_type     AS node_type,
			downstream.status        AS status
	`
	result, err := session.Run(ctx, query, map[string]interface{}{
		"schedule_name": scheduleName,
		"schema":        schemaName,
		"table_name":    tableName,
	})
	if err != nil {
		return nil, fmt.Errorf("GetTransitiveDownstream query failed: %w", err)
	}

	var nodes []*model.TableNode
	for result.Next(ctx) {
		record := result.Record()
		node := &model.TableNode{}
		if v, _ := record.Get("table_name"); v != nil {
			node.TableName = v.(string)
		}
		if v, _ := record.Get("schema_name"); v != nil {
			node.SchemaName = v.(string)
		}
		if v, _ := record.Get("service_name"); v != nil {
			node.ServiceName = v.(string)
		}
		if v, _ := record.Get("node_type"); v != nil {
			node.NodeType = safeString(v)
		}
		if v, _ := record.Get("status"); v != nil {
			node.Status = safeString(v)
		}
		nodes = append(nodes, node)
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("GetTransitiveDownstream result error: %w", err)
	}
	return nodes, nil
}

// collectNodes drains a Neo4j result cursor into a []*model.TableNode slice using
// the existing recordToTableNode helper.
func (r *graphRepository) collectNodes(ctx context.Context, result neo4j.ResultWithContext) ([]*model.TableNode, error) {
	var nodes []*model.TableNode
	for result.Next(ctx) {
		node, err := r.recordToTableNode(result.Record())
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	if err := result.Err(); err != nil {
		return nil, err
	}
	return nodes, nil
}
