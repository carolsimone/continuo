package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// RunAggregateRepository implements run.AggregateRepository against Neo4j.
type RunAggregateRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

func NewRunAggregateRepository(client Neo4jClient, logger *slog.Logger) *RunAggregateRepository {
	return &RunAggregateRepository{client: client, logger: logger}
}

var _ run.AggregateRepository = (*RunAggregateRepository)(nil)

// Load rehydrates the aggregate with an operation-scoped subgraph.
func (r *RunAggregateRepository) Load(ctx context.Context, runID string, hint run.LoadHint) (*run.Run, error) {
	switch h := hint.(type) {
	case run.LoadHintFull:
		return r.loadFull(ctx, runID)
	case run.LoadHintNodeCompletion:
		return r.loadForCompletion(ctx, runID, h)
	case run.LoadHintResetDownstream:
		return r.loadForReset(ctx, runID, h)
	default:
		return nil, fmt.Errorf("RunAggregateRepository.Load: unknown hint type %T", hint)
	}
}

// loadFull loads all nodes and edges for the run.
func (r *RunAggregateRepository) loadFull(ctx context.Context, runID string) (*run.Run, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	result, err := session.Run(ctx, `
        MATCH (run:Run {run_id: $run_id})
        MATCH (run)-[e:EXECUTES]->(t:Table)
        RETURN
            run.schedule_name   AS schedule_name,
            run.terminal_status AS terminal_status,
            run.total_nodes     AS total_nodes,
            run.terminal_count  AS terminal_count,
            run.version         AS version,
            e.task_id           AS task_id,
            COALESCE(e.status, 'PENDING')         AS status,
            COALESCE(e.manifest_version, '')      AS manifest_version,
            COALESCE(e.image_tag, '')             AS image_tag,
            t.table_name    AS table_name,
            t.schema_name   AS schema_name,
            t.service_name  AS service_name,
            t.node_type     AS node_type,
            t.schedule_name AS schedule_name_t,
            [(t)-[:DEPENDS_ON]->(up:Table)<-[:EXECUTES]-(run) |
                {schema_name: up.schema_name, table_name: up.table_name, service_name: up.service_name}
            ] AS upstreams,
            [(down:Table)-[:DEPENDS_ON]->(t)<-[:EXECUTES]-(run) |
                {schema_name: down.schema_name, table_name: down.table_name, service_name: down.service_name}
            ] AS downstreams
    `, map[string]interface{}{"run_id": runID})
	if err != nil {
		return nil, fmt.Errorf("loadFull: %w", err)
	}
	return r.collectRunFromFlatRows(ctx, runID, result)
}

// loadForCompletion loads the subgraph needed to complete the target node.
// FAILED:    target + transitive downstream
// SUCCEEDED: target + immediate downstream + immediate downstream's upstreams (for unblocking decision)
func (r *RunAggregateRepository) loadForCompletion(ctx context.Context, runID string, h run.LoadHintNodeCompletion) (*run.Run, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	var query string
	if h.Status == "FAILED" {
		query = `
            MATCH (run:Run {run_id: $run_id})
            MATCH (run)-[:EXECUTES]->(target:Table {schema_name: $schema_name, table_name: $table_name})
            // Target + transitive downstream
            OPTIONAL MATCH (downstream:Table)-[:DEPENDS_ON*1..]->(target)
            WHERE (run)-[:EXECUTES]->(downstream)
            WITH run, collect(DISTINCT target) + collect(DISTINCT downstream) AS allNodes
            UNWIND allNodes AS t
            WITH run, t WHERE t IS NOT NULL
            MATCH (run)-[e:EXECUTES]->(t)
            RETURN
                run.schedule_name   AS schedule_name,
                run.terminal_status AS terminal_status,
                run.total_nodes     AS total_nodes,
                run.terminal_count  AS terminal_count,
                run.version         AS version,
                e.task_id           AS task_id,
                COALESCE(e.status, 'PENDING')         AS status,
                COALESCE(e.manifest_version, '')      AS manifest_version,
                COALESCE(e.image_tag, '')             AS image_tag,
                t.table_name    AS table_name,
                t.schema_name   AS schema_name,
                t.service_name  AS service_name,
                t.node_type     AS node_type,
                t.schedule_name AS schedule_name_t,
                [(t)-[:DEPENDS_ON]->(up:Table)<-[:EXECUTES]-(run) |
                    {schema_name: up.schema_name, table_name: up.table_name, service_name: up.service_name}
                ] AS upstreams,
                [(down:Table)-[:DEPENDS_ON]->(t)<-[:EXECUTES]-(run) |
                    {schema_name: down.schema_name, table_name: down.table_name, service_name: down.service_name}
                ] AS downstreams
        `
	} else {
		// SUCCEEDED / SKIPPED: target + immediate downstream + their upstreams
		query = `
            MATCH (run:Run {run_id: $run_id})
            MATCH (run)-[:EXECUTES]->(target:Table {schema_name: $schema_name, table_name: $table_name})
            OPTIONAL MATCH (down:Table)-[:DEPENDS_ON]->(target)
            WHERE (run)-[:EXECUTES]->(down)
            OPTIONAL MATCH (sibling:Table)<-[:DEPENDS_ON]-(down)
            WHERE (run)-[:EXECUTES]->(sibling)
            WITH run,
                 collect(DISTINCT target) +
                 collect(DISTINCT down)   +
                 collect(DISTINCT sibling) AS allNodes
            UNWIND allNodes AS t
            WITH run, t WHERE t IS NOT NULL
            MATCH (run)-[e:EXECUTES]->(t)
            RETURN
                run.schedule_name   AS schedule_name,
                run.terminal_status AS terminal_status,
                run.total_nodes     AS total_nodes,
                run.terminal_count  AS terminal_count,
                run.version         AS version,
                e.task_id           AS task_id,
                COALESCE(e.status, 'PENDING')         AS status,
                COALESCE(e.manifest_version, '')      AS manifest_version,
                COALESCE(e.image_tag, '')             AS image_tag,
                t.table_name    AS table_name,
                t.schema_name   AS schema_name,
                t.service_name  AS service_name,
                t.node_type     AS node_type,
                t.schedule_name AS schedule_name_t,
                [(t)-[:DEPENDS_ON]->(up:Table)<-[:EXECUTES]-(run) |
                    {schema_name: up.schema_name, table_name: up.table_name, service_name: up.service_name}
                ] AS upstreams,
                [(down2:Table)-[:DEPENDS_ON]->(t)<-[:EXECUTES]-(run) |
                    {schema_name: down2.schema_name, table_name: down2.table_name, service_name: down2.service_name}
                ] AS downstreams
        `
	}

	result, err := session.Run(ctx, query, map[string]interface{}{
		"run_id":      runID,
		"schema_name": h.Key.SchemaName,
		"table_name":  h.Key.TableName,
	})
	if err != nil {
		return nil, fmt.Errorf("loadForCompletion: %w", err)
	}
	return r.collectRunFromFlatRows(ctx, runID, result)
}

// loadForReset loads the transitive downstream of the target node.
func (r *RunAggregateRepository) loadForReset(ctx context.Context, runID string, h run.LoadHintResetDownstream) (*run.Run, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	result, err := session.Run(ctx, `
        MATCH (run:Run {run_id: $run_id})
        MATCH (run)-[:EXECUTES]->(target:Table {schema_name: $schema_name, table_name: $table_name})
        OPTIONAL MATCH (downstream:Table)-[:DEPENDS_ON*1..]->(target)
        WHERE (run)-[:EXECUTES]->(downstream)
        WITH run, collect(DISTINCT target) + collect(DISTINCT downstream) AS allNodes
        UNWIND allNodes AS t
        WITH run, t WHERE t IS NOT NULL
        MATCH (run)-[e:EXECUTES]->(t)
        RETURN
            run.schedule_name   AS schedule_name,
            run.terminal_status AS terminal_status,
            run.total_nodes     AS total_nodes,
            run.terminal_count  AS terminal_count,
            run.version         AS version,
            e.task_id           AS task_id,
            COALESCE(e.status, 'PENDING')         AS status,
            COALESCE(e.manifest_version, '')      AS manifest_version,
            COALESCE(e.image_tag, '')             AS image_tag,
            t.table_name    AS table_name,
            t.schema_name   AS schema_name,
            t.service_name  AS service_name,
            t.node_type     AS node_type,
            t.schedule_name AS schedule_name_t,
            [(t)-[:DEPENDS_ON]->(up:Table)<-[:EXECUTES]-(run) |
                {schema_name: up.schema_name, table_name: up.table_name, service_name: up.service_name}
            ] AS upstreams,
            [(down:Table)-[:DEPENDS_ON]->(t)<-[:EXECUTES]-(run) |
                {schema_name: down.schema_name, table_name: down.table_name, service_name: down.service_name}
            ] AS downstreams
    `, map[string]interface{}{
		"run_id":      runID,
		"schema_name": h.Key.SchemaName,
		"table_name":  h.Key.TableName,
	})
	if err != nil {
		return nil, fmt.Errorf("loadForReset: %w", err)
	}
	return r.collectRunFromFlatRows(ctx, runID, result)
}

// Save persists the aggregate. Checks optimistic version. Writes only loaded nodes
// plus counters, version, and run status if finalized.
func (r *RunAggregateRepository) Save(ctx context.Context, agg *run.Run) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("Save: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	terminalStatus := ""
	if agg.Status.IsTerminal() {
		terminalStatus = string(agg.Status)
	}

	versionResult, err := tx.Run(ctx, `
        MATCH (run:Run {run_id: $run_id})
        WHERE run.version = $expected_version
        SET run.version         = $new_version,
            run.terminal_count  = $terminal_count,
            run.terminal_status = CASE WHEN $terminal_status <> '' THEN $terminal_status ELSE run.terminal_status END,
            run.completed_at    = CASE WHEN $terminal_status <> '' THEN datetime() ELSE run.completed_at END
        RETURN run.version AS new_version
    `, map[string]interface{}{
		"run_id":           agg.RunID,
		"expected_version": agg.Version - 1,
		"new_version":      agg.Version,
		"terminal_count":   agg.TerminalCount,
		"terminal_status":  terminalStatus,
	})
	if err != nil {
		return fmt.Errorf("Save: version check: %w", err)
	}
	if !versionResult.Next(ctx) {
		return run.ErrVersionConflict
	}
	if _, err := versionResult.Consume(ctx); err != nil {
		return fmt.Errorf("Save: consume version result: %w", err)
	}

	for _, n := range agg.Nodes() {
		_, err := tx.Run(ctx, `
            MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(t:Table)
            WHERE t.schema_name = $schema_name AND t.table_name = $table_name
            SET e.status = $status
        `, map[string]interface{}{
			"run_id":      agg.RunID,
			"schema_name": n.Key.SchemaName,
			"table_name":  n.Key.TableName,
			"status":      n.Status,
		})
		if err != nil {
			return fmt.Errorf("Save: update node %s.%s: %w", n.Key.SchemaName, n.Key.TableName, err)
		}
	}

	return tx.Commit(ctx)
}

// FinalizeRun stamps terminal_status and completed_at on the :Run node. Invoked
// from the run.finalized:v1 consumer to project state's authoritative scheduler
// outcome into Neo4j when the aggregate's internal completion path is not
// exercised (e.g. full-inherited rebases with no node.updated:v1 traffic).
func (r *RunAggregateRepository) FinalizeRun(ctx context.Context, runID, terminalStatus string) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	result, err := session.Run(ctx, `
        MATCH (run:Run {run_id: $run_id})
        SET run.terminal_status = $terminal_status,
            run.completed_at    = COALESCE(run.completed_at, datetime())
        RETURN run.run_id AS run_id
    `, map[string]interface{}{
		"run_id":          runID,
		"terminal_status": terminalStatus,
	})
	if err != nil {
		return fmt.Errorf("FinalizeRun: %w", err)
	}
	if _, err := result.Consume(ctx); err != nil {
		return fmt.Errorf("FinalizeRun consume: %w", err)
	}
	return nil
}

// DeleteExpiredRuns removes Run nodes older than retentionDays. Called by the sweeper.
func (r *RunAggregateRepository) DeleteExpiredRuns(ctx context.Context, retentionDays int) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	result, err := session.Run(ctx, `
        MATCH (run:Run)
        WHERE run.completed_at IS NOT NULL
          AND run.completed_at < datetime() - duration({days: $retention_days})
        DETACH DELETE run
    `, map[string]interface{}{"retention_days": retentionDays})
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

// collectRunFromFlatRows builds a *run.Run from a Neo4j result where each row
// represents a single node in the loaded subgraph.
func (r *RunAggregateRepository) collectRunFromFlatRows(
	ctx context.Context,
	runID string,
	result neo4j.ResultWithContext,
) (*run.Run, error) {
	var (
		scheduleName   string
		terminalStatus string
		totalNodes     int
		terminalCount  int
		version        int
		runMetaSet     bool
		nodes          []*run.RunNode
	)

	for result.Next(ctx) {
		rec := result.Record()

		if !runMetaSet {
			v, _ := rec.Get("schedule_name")
			scheduleName = safeString(v)
			v, _ = rec.Get("terminal_status")
			terminalStatus = safeString(v)
			v, _ = rec.Get("total_nodes")
			totalNodes = int(toInt64(v))
			v, _ = rec.Get("terminal_count")
			terminalCount = int(toInt64(v))
			v, _ = rec.Get("version")
			version = int(toInt64(v))
			runMetaSet = true
		}

		taskIDVal, _ := rec.Get("task_id")
		statusVal, _ := rec.Get("status")
		schemaVal, _ := rec.Get("schema_name")
		tableVal, _ := rec.Get("table_name")
		svcVal, _ := rec.Get("service_name")
		ntypeVal, _ := rec.Get("node_type")
		schedTVal, _ := rec.Get("schedule_name_t")
		mvVal, _ := rec.Get("manifest_version")
		itVal, _ := rec.Get("image_tag")
		upsRaw, _ := rec.Get("upstreams")
		downsRaw, _ := rec.Get("downstreams")

		nodeKey := run.NodeKey{
			ServiceName: safeString(svcVal),
			SchemaName:  safeString(schemaVal),
			TableName:   safeString(tableVal),
		}
		taskUUID, _ := uuid.Parse(safeString(taskIDVal))

		nodes = append(nodes, &run.RunNode{
			Key:             nodeKey,
			TaskID:          taskUUID,
			Status:          safeString(statusVal),
			ScheduleName:    safeString(schedTVal),
			NodeType:        safeString(ntypeVal),
			ManifestVersion: safeString(mvVal),
			ImageTag:        safeString(itVal),
			Upstreams:       extractNodeKeys(upsRaw),
			Downstreams:     extractNodeKeys(downsRaw),
		})
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("collectRunFromFlatRows: %w", err)
	}
	if !runMetaSet {
		return nil, fmt.Errorf("collectRunFromFlatRows: run %s not found", runID)
	}

	rebuilt := run.NewRun(runID, scheduleName, nodes)
	rebuilt.TotalNodes = totalNodes
	rebuilt.TerminalCount = terminalCount
	rebuilt.Version = version
	if terminalStatus != "" {
		rebuilt.Status = run.RunStatus(terminalStatus)
	} else if terminalCount > 0 {
		rebuilt.Status = run.RunStatusInProgress
	}
	return rebuilt, nil
}

func extractNodeKeys(raw interface{}) []run.NodeKey {
	list, ok := raw.([]interface{})
	if !ok {
		return nil
	}
	var keys []run.NodeKey
	for _, item := range list {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		keys = append(keys, run.NodeKey{
			ServiceName: safeString(m["service_name"]),
			SchemaName:  safeString(m["schema_name"]),
			TableName:   safeString(m["table_name"]),
		})
	}
	return keys
}

func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	default:
		return 0
	}
}
