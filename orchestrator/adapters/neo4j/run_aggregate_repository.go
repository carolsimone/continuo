package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	domainRun "github.com/carolsimone/continuo/orchestrator/domain/run"
	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// RunAggregateRepository implements domainRun.AggregateRepository against Neo4j.
type RunAggregateRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

func NewRunAggregateRepository(client Neo4jClient, logger *slog.Logger) *RunAggregateRepository {
	return &RunAggregateRepository{client: client, logger: logger}
}

var _ domainRun.AggregateRepository = (*RunAggregateRepository)(nil)

// Rehydrate reconstitutes the aggregate from persistent state for the given scope.
func (r *RunAggregateRepository) Rehydrate(ctx context.Context, runID string, scope domainRun.Scope) (*domainRun.Run, error) {
	switch s := scope.(type) {
	case domainRun.ScopeFull:
		return r.rehydrateFull(ctx, runID)
	case domainRun.ScopeNodeCompletion:
		return r.rehydrateForCompletion(ctx, runID, s)
	default:
		return nil, fmt.Errorf("RunAggregateRepository.Rehydrate: unknown scope type %T", scope)
	}
}

// rehydrateFull reads every node and edge for the run.
func (r *RunAggregateRepository) rehydrateFull(ctx context.Context, runID string) (*domainRun.Run, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	result, err := session.Run(ctx, `
        MATCH (run:Run {run_id: $run_id})
        MATCH (run)-[e:EXECUTES]->(t:Table)
        RETURN
            run.schedule_name                  AS schedule_name,
            run.terminal_status                AS terminal_status,
            COALESCE(run.operation,      '')   AS operation,
            COALESCE(run.total_nodes,    0)    AS total_nodes,
            COALESCE(run.terminal_count, 0)    AS terminal_count,
            COALESCE(run.failed_count,   0)    AS failed_count,
            COALESCE(run.version,        0)    AS version,
            e.task_id                          AS task_id,
            COALESCE(e.status, 'PENDING')      AS status,
            COALESCE(e.manifest_version, '')   AS manifest_version,
            COALESCE(e.image_tag, '')          AS image_tag,
            t.table_name                       AS table_name,
            t.schema_name                      AS schema_name,
            t.service_name                     AS service_name,
            t.node_type                        AS node_type,
            t.schedule_name                    AS schedule_name_t,
            [(t)-[:DEPENDS_ON]->(up:Table)<-[:EXECUTES]-(run) |
                {schema_name: up.schema_name, table_name: up.table_name, service_name: up.service_name}
            ] AS upstreams,
            [(down:Table)-[:DEPENDS_ON]->(t)<-[:EXECUTES]-(run) |
                {schema_name: down.schema_name, table_name: down.table_name, service_name: down.service_name}
            ] AS downstreams
    `, map[string]interface{}{"run_id": runID})
	if err != nil {
		return nil, fmt.Errorf("rehydrateFull: %w", err)
	}
	return r.collectRunFromFlatRows(ctx, runID, result)
}

// rehydrateForCompletion loads the subgraph needed to complete the target node.
// "immediate" = one DEPENDS_ON hop from the target; "transitive" = all hops.
//
//	FAILED:    target + transitive downstream
//	           (cascade-skip must reach every PENDING descendant).
//	SUCCEEDED: target + immediate downstream + each immediate downstream's
//	           upstreams (one DEPENDS_ON hop into the run on both sides of
//	           the downstream node), so the aggregate can evaluate
//	           "are all upstreams terminal?" for unblocking.
func (r *RunAggregateRepository) rehydrateForCompletion(ctx context.Context, runID string, s domainRun.ScopeNodeCompletion) (*domainRun.Run, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	var query string
	if s.Status == "FAILED" {
		query = `
            MATCH (run:Run {run_id: $run_id})
            MATCH (run)-[:EXECUTES]->(target:Table {service_name: $service_name, schema_name: $schema_name, table_name: $table_name})
            // Target + transitive downstream
            OPTIONAL MATCH (downstream:Table)-[:DEPENDS_ON*1..]->(target)
            WHERE (run)-[:EXECUTES]->(downstream)
            WITH run, collect(DISTINCT target) + collect(DISTINCT downstream) AS allNodes
            UNWIND allNodes AS t
            WITH run, t WHERE t IS NOT NULL
            MATCH (run)-[e:EXECUTES]->(t)
            RETURN
                run.schedule_name                  AS schedule_name,
                run.terminal_status                AS terminal_status,
                COALESCE(run.operation,      '')   AS operation,
                COALESCE(run.total_nodes,    0)    AS total_nodes,
                COALESCE(run.terminal_count, 0)    AS terminal_count,
                COALESCE(run.failed_count,   0)    AS failed_count,
                COALESCE(run.version,        0)    AS version,
                e.task_id                          AS task_id,
                COALESCE(e.status, 'PENDING')      AS status,
                COALESCE(e.manifest_version, '')   AS manifest_version,
                COALESCE(e.image_tag, '')          AS image_tag,
                t.table_name                       AS table_name,
                t.schema_name                      AS schema_name,
                t.service_name                     AS service_name,
                t.node_type                        AS node_type,
                t.schedule_name                    AS schedule_name_t,
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
            MATCH (run)-[:EXECUTES]->(target:Table {service_name: $service_name, schema_name: $schema_name, table_name: $table_name})
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
                run.schedule_name                  AS schedule_name,
                run.terminal_status                AS terminal_status,
                COALESCE(run.operation,      '')   AS operation,
                COALESCE(run.total_nodes,    0)    AS total_nodes,
                COALESCE(run.terminal_count, 0)    AS terminal_count,
                COALESCE(run.failed_count,   0)    AS failed_count,
                COALESCE(run.version,        0)    AS version,
                e.task_id                          AS task_id,
                COALESCE(e.status, 'PENDING')      AS status,
                COALESCE(e.manifest_version, '')   AS manifest_version,
                COALESCE(e.image_tag, '')          AS image_tag,
                t.table_name                       AS table_name,
                t.schema_name                      AS schema_name,
                t.service_name                     AS service_name,
                t.node_type                        AS node_type,
                t.schedule_name                    AS schedule_name_t,
                [(t)-[:DEPENDS_ON]->(up:Table)<-[:EXECUTES]-(run) |
                    {schema_name: up.schema_name, table_name: up.table_name, service_name: up.service_name}
                ] AS upstreams,
                [(down2:Table)-[:DEPENDS_ON]->(t)<-[:EXECUTES]-(run) |
                    {schema_name: down2.schema_name, table_name: down2.table_name, service_name: down2.service_name}
                ] AS downstreams
        `
	}

	result, err := session.Run(ctx, query, map[string]interface{}{
		"run_id":       runID,
		"service_name": s.Key.ServiceName,
		"schema_name":  s.Key.SchemaName,
		"table_name":   s.Key.TableName,
	})
	if err != nil {
		return nil, fmt.Errorf("rehydrateForCompletion: %w", err)
	}
	return r.collectRunFromFlatRows(ctx, runID, result)
}

// Save persists the aggregate. Checks optimistic version. Writes only loaded nodes
// plus counters, version, and run status if finalized.
func (r *RunAggregateRepository) Save(ctx context.Context, agg *domainRun.Run) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = session.Close(ctx) }()

	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("Save: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Normalize to the canonical lowercase terminal_status at the write boundary
	// so Save, FinalizeRun, and the snapshot writer all stamp the same form. The
	// in-memory aggregate vocabulary is uppercase; the stored projection the UI
	// reads is lowercase. storedTerminalStatus returns "" for non-terminal
	// statuses, which the CASE below treats as "do not stamp".
	terminalStatus := storedTerminalStatus(agg.Status)

	// COALESCE(run.version, 0) lets in-flight :Run nodes created before this
	// patch (no version property) match expected_version=0 on the first save.
	// terminal_status / completed_at are first-writer-wins so that state's
	// authoritative run.finalized:v1 projection cannot be overwritten by a
	// later aggregate save with a possibly-stale terminal_status.
	versionResult, err := tx.Run(ctx, `
        MATCH (run:Run {run_id: $run_id})
        WHERE COALESCE(run.version, 0) = $expected_version
        SET run.version         = $new_version,
            run.terminal_count  = $terminal_count,
            run.failed_count    = $failed_count,
            run.terminal_status = CASE
                WHEN run.terminal_status IS NOT NULL THEN run.terminal_status
                WHEN $terminal_status <> ''          THEN $terminal_status
                ELSE NULL
            END,
            run.completed_at = CASE
                WHEN run.completed_at IS NOT NULL THEN run.completed_at
                WHEN $terminal_status <> ''       THEN datetime()
                ELSE NULL
            END
        RETURN run.version AS new_version
    `, map[string]interface{}{
		"run_id":           agg.RunID,
		"expected_version": agg.Version - 1,
		"new_version":      agg.Version,
		"terminal_count":   agg.TerminalCount,
		"failed_count":     agg.FailedCount,
		"terminal_status":  terminalStatus,
	})
	if err != nil {
		return fmt.Errorf("Save: version check: %w", err)
	}
	if !versionResult.Next(ctx) {
		return domainRun.ErrVersionConflict
	}
	if _, err := versionResult.Consume(ctx); err != nil {
		return fmt.Errorf("Save: consume version result: %w", err)
	}

	// Persist every loaded node's status in a single round trip: one UNWIND over
	// the dirty set rather than one Cypher statement per node. The match keys on
	// the full (service_name, schema_name, table_name) identity so the status
	// lands on the correct :EXECUTES edge even when two services share a
	// schema.table name within the same run.
	nodeParams := make([]map[string]interface{}, 0, len(agg.Nodes()))
	for _, n := range agg.Nodes() {
		nodeParams = append(nodeParams, map[string]interface{}{
			"service": n.Key.ServiceName,
			"schema":  n.Key.SchemaName,
			"table":   n.Key.TableName,
			"status":  n.Status,
		})
	}
	if len(nodeParams) > 0 {
		if _, err := tx.Run(ctx, `
            UNWIND $nodes AS n
            MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(t:Table)
            WHERE t.service_name = n.service
              AND t.schema_name  = n.schema
              AND t.table_name   = n.table
            SET e.status = n.status
        `, map[string]interface{}{
			"run_id": agg.RunID,
			"nodes":  nodeParams,
		}); err != nil {
			return fmt.Errorf("Save: update node statuses: %w", err)
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
	defer func() { _ = session.Close(ctx) }()

	// Normalize to the canonical lowercase form so this writer agrees with Save
	// and the snapshot writer. state's run.finalized:v1 already emits lowercase
	// ("succeeded"/"failed"/"cancelled"); lowercasing here keeps the projection
	// consistent even if an upstream value ever arrives in another casing.
	result, err := session.Run(ctx, `
        MATCH (run:Run {run_id: $run_id})
        SET run.terminal_status = $terminal_status,
            run.completed_at    = COALESCE(run.completed_at, datetime())
        RETURN run.run_id AS run_id
    `, map[string]interface{}{
		"run_id":          runID,
		"terminal_status": strings.ToLower(terminalStatus),
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
	defer func() { _ = session.Close(ctx) }()

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

// collectRunFromFlatRows builds a *domainRun.Run from a Neo4j result where each
// row represents a single node in the loaded subgraph.
func (r *RunAggregateRepository) collectRunFromFlatRows(
	ctx context.Context,
	runID string,
	result neo4j.ResultWithContext,
) (*domainRun.Run, error) {
	var (
		scheduleName   string
		terminalStatus string
		operation      string
		totalNodes     int
		terminalCount  int
		failedCount    int
		version        int
		runMetaSet     bool
		nodes          []*domainRun.RunNode
	)

	for result.Next(ctx) {
		rec := result.Record()

		if !runMetaSet {
			v, _ := rec.Get("schedule_name")
			scheduleName = safeString(v)
			v, _ = rec.Get("terminal_status")
			terminalStatus = safeString(v)
			v, _ = rec.Get("operation")
			operation = safeString(v)
			v, _ = rec.Get("total_nodes")
			totalNodes = int(toInt64(v))
			v, _ = rec.Get("terminal_count")
			terminalCount = int(toInt64(v))
			v, _ = rec.Get("failed_count")
			failedCount = int(toInt64(v))
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

		nodeKey := domainRun.NodeKey{
			ServiceName: safeString(svcVal),
			SchemaName:  safeString(schemaVal),
			TableName:   safeString(tableVal),
		}
		taskUUID, _ := uuid.Parse(safeString(taskIDVal))

		// A "test" operation's rehydrated nodes carry no ups/downs: whole-DAG
		// test runs are a flat fan-out, so CompleteNode must not unblock or
		// cascade-skip anything based on the shared :Table DEPENDS_ON topology.
		ups := extractNodeKeys(upsRaw)
		downs := extractNodeKeys(downsRaw)
		if operation == string(pkgModel.OperationTest) {
			ups, downs = nil, nil
		}

		nodes = append(nodes, &domainRun.RunNode{
			Key:             nodeKey,
			TaskID:          taskUUID,
			Status:          safeString(statusVal),
			ScheduleName:    safeString(schedTVal),
			NodeType:        safeString(ntypeVal),
			ManifestVersion: safeString(mvVal),
			ImageTag:        safeString(itVal),
			Upstreams:       ups,
			Downstreams:     downs,
		})
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("collectRunFromFlatRows: %w", err)
	}
	if !runMetaSet {
		return nil, fmt.Errorf("collectRunFromFlatRows: run %s not found", runID)
	}

	rebuilt := domainRun.NewRun(runID, scheduleName, nodes)
	rebuilt.Operation = operation
	rebuilt.TotalNodes = totalNodes
	rebuilt.TerminalCount = terminalCount
	rebuilt.FailedCount = failedCount
	rebuilt.Version = version
	if s := runStatusFromStored(terminalStatus); s != "" {
		rebuilt.Status = s
	} else if terminalCount > 0 {
		rebuilt.Status = domainRun.RunStatusInProgress
	}
	return rebuilt, nil
}

func extractNodeKeys(raw interface{}) []domainRun.NodeKey {
	list, ok := raw.([]interface{})
	if !ok {
		return nil
	}
	var keys []domainRun.NodeKey
	for _, item := range list {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		keys = append(keys, domainRun.NodeKey{
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
