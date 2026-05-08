package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// RunRepository implements the write-side methods of run.Repository.
type RunRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

func NewRunRepository(client Neo4jClient, logger *slog.Logger) *RunRepository {
	return &RunRepository{client: client, logger: logger}
}

// UpdateNodeStatus sets the execution status on a Table node via the EXECUTES edge.
func (r *RunRepository) UpdateNodeStatus(ctx context.Context, runID, scheduleName, schema, tableName, status string) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	// Scope by Run EXECUTES edges + schema + table_name. No need to filter
	// by schedule_name since the run_id already identifies the run scope.
	query := `
		MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(t:Table)
		WHERE t.schema_name = $schema_name
		  AND t.table_name = $table_name
		SET e.status = $status
	`

	result, err := session.Run(ctx, query, map[string]interface{}{
		"run_id":      runID,
		"schema_name": schema,
		"table_name":  tableName,
		"status":      status,
	})
	if err != nil {
		r.logger.Error("Failed to update node status via EXECUTES",
			"run_id", runID,
			"schedule_name", scheduleName,
			"schema", schema,
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
		"schema", schema,
		"table_name", tableName,
		"status", status,
	)
	return nil
}

// GetReadyDownstream finds downstream nodes where ALL upstream dependencies have SUCCEEDED
// and the downstream node itself is PENDING or has no status yet.
// The query scopes by Run EXECUTES edges (not schedule_name) so that cross-schedule
// dependencies (e.g. seeds with schedule_name="seed" → models with a different schedule)
// are correctly resolved within the same run.
func (r *RunRepository) GetReadyDownstream(ctx context.Context, runID, scheduleName, schema, tableName string) ([]*run.DownstreamNode, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	query := `
		MATCH (:Run {run_id: $run_id})-[snap:EXECUTES]->(downstream:Table)
		WHERE (snap.status IS NULL OR snap.status = 'PENDING')
		  AND EXISTS {
			MATCH (downstream)-[:DEPENDS_ON]->(completed:Table)
			WHERE completed.schema_name = $schema_name AND completed.table_name = $table_name
		  }
		  AND NOT EXISTS {
			MATCH (downstream)-[:DEPENDS_ON]->(upstream:Table)
			WHERE NOT EXISTS {
				MATCH (:Run {run_id: $run_id})-[us:EXECUTES]->(upstream)
				WHERE us.status = 'SUCCEEDED'
			}
		  }
		RETURN downstream.service_name AS service_name,
		       downstream.schema_name AS schema_name,
		       downstream.table_name AS table_name,
		       COALESCE(downstream.node_type, "") AS node_type,
		       downstream.schedule_name AS schedule_name,
		       COALESCE(snap.manifest_version, "") AS manifest_version,
		       COALESCE(snap.image_tag, "") AS image_tag
	`

	result, err := session.Run(ctx, query, map[string]interface{}{
		"run_id":        runID,
		"schedule_name": scheduleName,
		"schema_name":   schema,
		"table_name":    tableName,
	})
	if err != nil {
		r.logger.Error("Failed to query ready downstream nodes",
			"run_id", runID,
			"schedule_name", scheduleName,
			"schema", schema,
			"table_name", tableName,
			"error", err,
		)
		return nil, fmt.Errorf("failed to query ready downstream nodes: %w", err)
	}

	var nodes []*run.DownstreamNode
	for result.Next(ctx) {
		record := result.Record()

		svcName, _ := record.Get("service_name")
		schemaVal, _ := record.Get("schema_name")
		tblName, _ := record.Get("table_name")
		nodeTypeRaw, _ := record.Get("node_type")
		schedNameRaw, _ := record.Get("schedule_name")
		manifestVersionRaw, _ := record.Get("manifest_version")
		imageTagRaw, _ := record.Get("image_tag")

		nodeTypeStr := ""
		if s, ok := nodeTypeRaw.(string); ok {
			nodeTypeStr = s
		}

		nodes = append(nodes, &run.DownstreamNode{
			ServiceName:     safeString(svcName),
			SchemaName:      safeString(schemaVal),
			TableName:       safeString(tblName),
			NodeType:        nodeTypeStr,
			ScheduleName:    safeString(schedNameRaw),
			ManifestVersion: safeString(manifestVersionRaw),
			ImageTag:        safeString(imageTagRaw),
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
		"schema", schema,
		"table_name", tableName,
		"count", len(nodes),
	)
	return nodes, nil
}

// CheckScheduleCompletion reports whether no node can change state anymore.
// isComplete=true when no node is RUNNING and no node is ready to trigger.
// hasFailed=true when at least one node has status FAILED.
func (r *RunRepository) CheckScheduleCompletion(ctx context.Context, runID, scheduleName string) (bool, bool, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	// Scope by Run EXECUTES edges (not schedule_name) so seeds and models
	// within the same run are both considered when deciding drain status.
	query := `
		MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(n:Table)
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
		"run_id": runID,
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

// FinalizeRun sets completed_at and terminal_status on a Run node.
func (r *RunRepository) FinalizeRun(ctx context.Context, runID, terminalStatus string) error {
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

// GetScheduleInitNodes runs all three scheduler-init queries in a single
// Neo4j read transaction to guarantee a consistent snapshot.
func (r *RunRepository) GetScheduleInitNodes(ctx context.Context, scheduleName, runID string) (*run.ScheduleInitNodes, error) {
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

func (r *RunRepository) getRootNodesInRun(ctx context.Context, tx neo4j.ExplicitTransaction, scheduleName, runID string) ([]*domain.TableNode, error) {
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

func (r *RunRepository) getUpstreamSeedNodesInRun(ctx context.Context, tx neo4j.ExplicitTransaction, scheduleName, runID string) ([]*domain.TableNode, error) {
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

func (r *RunRepository) getAllNodesInRun(ctx context.Context, tx neo4j.ExplicitTransaction, scheduleName, runID string) ([]*domain.TableNode, error) {
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

// GetTransitiveDownstream returns all nodes that transitively depend on the given
// node and are NOT in SUCCEEDED status.
func (r *RunRepository) GetTransitiveDownstream(ctx context.Context, scheduleName, schema, tableName string) ([]*domain.TableNode, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	query := `
		MATCH (start:Table {schedule_name: $schedule_name, schema_name: $schema_name, table_name: $table_name})
		WHERE COALESCE(start.active, true)
		MATCH (downstream:Table {schedule_name: $schedule_name})-[:DEPENDS_ON*1..]->(start)
		WHERE COALESCE(downstream.active, true)
		  AND (downstream.status IS NULL OR downstream.status <> 'SUCCEEDED')
		RETURN DISTINCT
			downstream.table_name    AS table_name,
			downstream.schema_name   AS schema_name,
			coalesce(downstream.service_name, '')  AS service_name,
			downstream.node_type     AS node_type,
			downstream.status        AS status
	`
	result, err := session.Run(ctx, query, map[string]interface{}{
		"schedule_name": scheduleName,
		"schema_name":   schema,
		"table_name":    tableName,
	})
	if err != nil {
		return nil, fmt.Errorf("GetTransitiveDownstream query failed: %w", err)
	}

	var nodes []*domain.TableNode
	for result.Next(ctx) {
		record := result.Record()
		node := &domain.TableNode{}
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

// GetNodeType returns the node_type property for a Table node.
func (r *RunRepository) GetNodeType(ctx context.Context, schema, tableName string) (string, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	result, err := session.Run(ctx, `
		MATCH (t:Table {schema_name: $schema_name, table_name: $table_name})
		RETURN COALESCE(t.node_type, '') AS node_type
		LIMIT 1
	`, map[string]interface{}{
		"schema_name": schema,
		"table_name":  tableName,
	})
	if err != nil {
		return "", fmt.Errorf("GetNodeType query failed: %w", err)
	}
	if result.Next(ctx) {
		if v, _ := result.Record().Get("node_type"); v != nil {
			return safeString(v), nil
		}
	}
	return "", fmt.Errorf("node not found: %s.%s", schema, tableName)
}

// GetNodeServiceName returns the service_name property for a Table node.
func (r *RunRepository) GetNodeServiceName(ctx context.Context, schema, tableName string) (string, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	result, err := session.Run(ctx, `
		MATCH (t:Table {schema_name: $schema_name, table_name: $table_name})
		RETURN COALESCE(t.service_name, '') AS service_name
		LIMIT 1
	`, map[string]interface{}{
		"schema_name": schema,
		"table_name":  tableName,
	})
	if err != nil {
		return "", fmt.Errorf("GetNodeServiceName query failed: %w", err)
	}
	if result.Next(ctx) {
		if v, _ := result.Record().Get("service_name"); v != nil {
			return safeString(v), nil
		}
	}
	return "", fmt.Errorf("node not found: %s.%s", schema, tableName)
}

// GetTaskIDForNode returns the task_id assigned on the EXECUTES edge for a specific
// Table node within a run.
func (r *RunRepository) GetTaskIDForNode(ctx context.Context, runID, serviceName, schemaName, tableName string) (string, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	result, err := session.Run(ctx, `
		MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(t:Table)
		WHERE t.service_name = $service_name AND t.schema_name = $schema_name AND t.table_name = $table_name
		RETURN e.task_id AS task_id
		LIMIT 1
	`, map[string]interface{}{
		"run_id":       runID,
		"service_name": serviceName,
		"schema_name":  schemaName,
		"table_name":   tableName,
	})
	if err != nil {
		return "", fmt.Errorf("GetTaskIDForNode query failed: %w", err)
	}
	if result.Next(ctx) {
		if v, _ := result.Record().Get("task_id"); v != nil {
			if id := safeString(v); id != "" {
				return id, nil
			}
		}
	}
	if err := result.Err(); err != nil {
		return "", fmt.Errorf("GetTaskIDForNode result error: %w", err)
	}
	return "", fmt.Errorf("GetTaskIDForNode: no task_id on EXECUTES edge for run=%s service=%s schema=%s table=%s", runID, serviceName, schemaName, tableName)
}

// GetSkippedDownstreamTaskIDs returns the task_ids of all transitively downstream
// Table nodes that currently have status=SKIPPED within the given run.
func (r *RunRepository) GetSkippedDownstreamTaskIDs(ctx context.Context, runID, schemaName, tableName string) ([]string, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	result, err := session.Run(ctx, `
		MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(target:Table)
		WHERE target.schema_name = $schema_name AND target.table_name = $table_name
		MATCH (downstream:Table)-[:DEPENDS_ON*1..]->(target)
		MATCH (:Run {run_id: $run_id})-[de:EXECUTES]->(downstream)
		WHERE de.status = 'SKIPPED'
		RETURN COALESCE(de.task_id, '') AS task_id
	`, map[string]interface{}{
		"run_id":      runID,
		"schema_name": schemaName,
		"table_name":  tableName,
	})
	if err != nil {
		return nil, fmt.Errorf("GetSkippedDownstreamTaskIDs query failed: %w", err)
	}

	var taskIDs []string
	for result.Next(ctx) {
		if v, _ := result.Record().Get("task_id"); v != nil {
			if id := safeString(v); id != "" {
				taskIDs = append(taskIDs, id)
			}
		}
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("GetSkippedDownstreamTaskIDs result error: %w", err)
	}
	return taskIDs, nil
}

// MarkPendingDownstreamSkipped finds all transitively downstream nodes in the run that
// are still PENDING, atomically marks them SKIPPED, and returns their identifying info.
func (r *RunRepository) MarkPendingDownstreamSkipped(ctx context.Context, runID, scheduleName, schemaName, tableName string) ([]*run.CascadedFailureNode, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	result, err := session.Run(ctx, `
		MATCH (:Run {run_id: $run_id})-[:EXECUTES]->(target:Table)
		WHERE target.schema_name = $schema_name AND target.table_name = $table_name
		MATCH (downstream:Table)-[:DEPENDS_ON*1..]->(target)
		MATCH (:Run {run_id: $run_id})-[de:EXECUTES]->(downstream)
		WHERE de.status = 'PENDING' OR de.status IS NULL
		SET de.status = 'SKIPPED'
		RETURN
			COALESCE(de.task_id, '')           AS task_id,
			downstream.schema_name              AS schema_name,
			downstream.table_name               AS table_name,
			COALESCE(downstream.service_name, '') AS service_name
	`, map[string]interface{}{
		"run_id":      runID,
		"schema_name": schemaName,
		"table_name":  tableName,
	})
	if err != nil {
		return nil, fmt.Errorf("MarkPendingDownstreamSkipped query failed: %w", err)
	}

	var nodes []*run.CascadedFailureNode
	for result.Next(ctx) {
		record := result.Record()
		n := &run.CascadedFailureNode{}
		if v, _ := record.Get("task_id"); v != nil {
			n.TaskID = safeString(v)
		}
		if v, _ := record.Get("schema_name"); v != nil {
			n.SchemaName = safeString(v)
		}
		if v, _ := record.Get("table_name"); v != nil {
			n.TableName = safeString(v)
		}
		if v, _ := record.Get("service_name"); v != nil {
			n.ServiceName = safeString(v)
		}
		nodes = append(nodes, n)
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("MarkPendingDownstreamSkipped result error: %w", err)
	}

	if len(nodes) > 0 {
		r.logger.Info("Marked pending downstream nodes as SKIPPED",
			"run_id", runID,
			"failed_table", tableName,
			"cascaded_count", len(nodes),
		)
	}
	return nodes, nil
}

// ResetSkippedDownstreamToPending resets all transitively downstream nodes that were
// cascade-skipped back to PENDING so they can be dispatched after a successful rerun.
func (r *RunRepository) ResetSkippedDownstreamToPending(ctx context.Context, runID, schemaName, tableName string) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	result, err := session.Run(ctx, `
		MATCH (:Run {run_id: $run_id})-[:EXECUTES]->(target:Table)
		WHERE target.schema_name = $schema_name AND target.table_name = $table_name
		MATCH (downstream:Table)-[:DEPENDS_ON*1..]->(target)
		MATCH (:Run {run_id: $run_id})-[de:EXECUTES]->(downstream)
		WHERE de.status = 'SKIPPED'
		SET de.status = 'PENDING'
		RETURN count(de) AS reset_count
	`, map[string]interface{}{
		"run_id":      runID,
		"schema_name": schemaName,
		"table_name":  tableName,
	})
	if err != nil {
		return fmt.Errorf("ResetSkippedDownstreamToPending query failed: %w", err)
	}

	var resetCount int64
	if result.Next(ctx) {
		if v, _ := result.Record().Get("reset_count"); v != nil {
			resetCount, _ = v.(int64)
		}
	}
	if err := result.Err(); err != nil {
		return fmt.Errorf("ResetSkippedDownstreamToPending result error: %w", err)
	}

	r.logger.Info("Reset cascade-skipped downstream nodes to PENDING",
		"run_id", runID,
		"table_name", tableName,
		"reset_count", resetCount,
	)
	return nil
}

// GetNodeEdgeData reads manifest_version and image_tag from the EXECUTES edge for a
// specific Table node within a run.
func (r *RunRepository) GetNodeEdgeData(ctx context.Context, runID, schemaName, tableName string) (string, string, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	result, err := session.Run(ctx, `
		MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(:Table {schema_name: $schema_name, table_name: $table_name})
		RETURN COALESCE(e.manifest_version, '') AS manifest_version, COALESCE(e.image_tag, '') AS image_tag
		LIMIT 1
	`, map[string]interface{}{
		"run_id":      runID,
		"schema_name": schemaName,
		"table_name":  tableName,
	})
	if err != nil {
		return "", "", fmt.Errorf("GetNodeEdgeData query failed: %w", err)
	}
	if result.Next(ctx) {
		record := result.Record()
		mv, _ := record.Get("manifest_version")
		it, _ := record.Get("image_tag")
		mvStr, itStr := safeString(mv), safeString(it)
		if err := result.Err(); err != nil {
			return "", "", fmt.Errorf("GetNodeEdgeData result error: %w", err)
		}
		return mvStr, itStr, nil
	}
	if err := result.Err(); err != nil {
		return "", "", fmt.Errorf("GetNodeEdgeData result error: %w", err)
	}
	return "", "", fmt.Errorf("GetNodeEdgeData: no EXECUTES edge for run=%s schema=%s table=%s", runID, schemaName, tableName)
}

// DeleteExpiredRuns removes Run nodes (and their EXECUTES edges) older than retentionDays.
func (r *RunRepository) DeleteExpiredRuns(ctx context.Context, retentionDays int) error {
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

// SnapshotSingleNodeRun creates a one-task :Run + :Task + :EXECUTES edge for a
// single-node run. It supports two metadata sources:
//
//   - "latest": reads :TopologyRoot for topology_generation + service_metadata and
//     reads :Table for per-node image_tag, manifest_version, and node_type.
//   - "snapshot_of_run": reads the :EXECUTES edge of the source :Run to inherit
//     the frozen image_tag and manifest_version, and inherits topology_generation +
//     service_metadata from the source :Run node.
//
// Returns run.ErrTargetNotFound when no matching :Table (latest) or no matching
// source :Run/:EXECUTES edge (stale) is found.
func (r *RunRepository) SnapshotSingleNodeRun(
	ctx context.Context,
	runID, scheduleName string,
	sourceRunID *uuid.UUID,
	serviceName, schemaName, tableName string,
	metadataSource string,
) (taskID, imageTag, manifestVersion, nodeType string, err error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	taskIDValue := uuid.New().String()

	params := map[string]interface{}{
		"run_id":        runID,
		"schedule_name": scheduleName,
		"service_name":  serviceName,
		"schema_name":   schemaName,
		"table_name":    tableName,
		"task_id":       taskIDValue,
	}

	var query string
	switch metadataSource {
	case "latest":
		params["source_run_id"] = nil
		query = `
			OPTIONAL MATCH (root:TopologyRoot {id: 'singleton'})
			WITH COALESCE(root.topology_generation, 0) AS topo_gen,
			     COALESCE(root.service_metadata, '{}') AS svc_meta
			MATCH (tbl:Table {service_name: $service_name,
			                  schema_name:  $schema_name,
			                  table_name:   $table_name})
			WHERE COALESCE(tbl.active, true)
			WITH topo_gen, svc_meta, tbl LIMIT 1
			MERGE (run:Run {run_id: $run_id})
			ON CREATE SET run.schedule_name      = $schedule_name,
			              run.created_at         = datetime(),
			              run.kind               = 'single_node_run',
			              run.topology_generation = topo_gen,
			              run.service_metadata    = svc_meta
			ON MATCH SET  run.kind = COALESCE(run.kind, 'single_node_run')
			WITH run, tbl, topo_gen, svc_meta
			MERGE (task:Task {task_id: $task_id})
			ON CREATE SET task.status       = 'PENDING',
			              task.service_name = $service_name,
			              task.schema_name  = $schema_name,
			              task.table_name   = $table_name
			WITH run, tbl, task
			MERGE (run)-[e:EXECUTES]->(tbl)
			ON CREATE SET e.status           = 'PENDING',
			              e.task_id          = $task_id,
			              e.image_tag        = COALESCE(tbl.image_tag, ''),
			              e.manifest_version = COALESCE(tbl.manifest_version, '')
			RETURN e.task_id          AS task_id,
			       e.image_tag        AS image_tag,
			       e.manifest_version AS manifest_version,
			       COALESCE(tbl.node_type, 'dbt-model') AS node_type
		`
	case "snapshot_of_run":
		if sourceRunID == nil {
			return "", "", "", "", fmt.Errorf("source_run_id required for stale mode")
		}
		params["source_run_id"] = sourceRunID.String()
		query = `
			MATCH (src:Run {run_id: $source_run_id})-[srcEdge:EXECUTES]->
			      (tbl:Table {service_name: $service_name,
			                  schema_name:  $schema_name,
			                  table_name:   $table_name})
			WITH src, srcEdge, tbl LIMIT 1
			MERGE (run:Run {run_id: $run_id})
			ON CREATE SET run.schedule_name      = $schedule_name,
			              run.created_at         = datetime(),
			              run.kind               = 'single_node_run',
			              run.topology_generation = src.topology_generation,
			              run.service_metadata    = src.service_metadata,
			              run.source_run_id       = $source_run_id
			ON MATCH SET  run.kind = COALESCE(run.kind, 'single_node_run')
			WITH run, tbl, src, srcEdge
			MERGE (task:Task {task_id: $task_id})
			ON CREATE SET task.status       = 'PENDING',
			              task.service_name = $service_name,
			              task.schema_name  = $schema_name,
			              task.table_name   = $table_name
			WITH run, tbl, task, srcEdge
			MERGE (run)-[e:EXECUTES]->(tbl)
			ON CREATE SET e.status           = 'PENDING',
			              e.task_id          = $task_id,
			              e.image_tag        = srcEdge.image_tag,
			              e.manifest_version = srcEdge.manifest_version
			RETURN e.task_id          AS task_id,
			       e.image_tag        AS image_tag,
			       e.manifest_version AS manifest_version,
			       COALESCE(tbl.node_type, 'dbt-model') AS node_type
		`
	default:
		return "", "", "", "", fmt.Errorf("SnapshotSingleNodeRun: invalid metadataSource %q", metadataSource)
	}

	result, err := session.Run(ctx, query, params)
	if err != nil {
		return "", "", "", "", fmt.Errorf("SnapshotSingleNodeRun: query failed: %w", err)
	}
	if !result.Next(ctx) {
		// No matching row → target not found in the chosen source.
		if rerr := result.Err(); rerr != nil {
			return "", "", "", "", fmt.Errorf("SnapshotSingleNodeRun: result error: %w", rerr)
		}
		return "", "", "", "", run.ErrTargetNotFound
	}
	rec := result.Record()
	taskID = safeString(recordValue(rec, "task_id"))
	imageTag = safeString(recordValue(rec, "image_tag"))
	manifestVersion = safeString(recordValue(rec, "manifest_version"))
	nodeType = safeString(recordValue(rec, "node_type"))
	if rerr := result.Err(); rerr != nil {
		return "", "", "", "", fmt.Errorf("SnapshotSingleNodeRun: result error: %w", rerr)
	}
	r.logger.Info("Single-node run snapshot created",
		"run_id", runID,
		"schedule_name", scheduleName,
		"metadata_source", metadataSource,
		"task_id", taskID,
		"image_tag", imageTag,
		"manifest_version", manifestVersion,
		"node_type", nodeType,
	)
	return taskID, imageTag, manifestVersion, nodeType, nil
}

// Snapshot is the unified per-run snapshot routine (umbrella §3). Plan/materialise:
//   - the selector inside params reads source/topology and returns a projection
//   - the materialiser writes :Run + per-task :EXECUTES edges in one Cypher tx
//
// Returns the projection unchanged so the caller can build outbox events
// (run.entries.dispatched:v1 with per-task Status + InheritedFromTaskID;
// query.model:v1 only for PENDING rows) without re-reading Neo4j.
func (r *RunRepository) Snapshot(ctx context.Context, params snapshot.Params) ([]snapshot.TaskProjection, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	var projection []snapshot.TaskProjection
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		var inner error
		projection, inner = params.Selector.SelectTasks(ctx, tx, params)
		if inner != nil {
			return nil, inner
		}
		if len(projection) == 0 {
			return nil, snapshot.ErrEmptyProjection
		}
		return nil, snapshot.Materialise(ctx, tx, params, projection)
	})
	if err != nil {
		return nil, err
	}
	r.logger.Info("Snapshot created",
		"run_id", params.RunID,
		"schedule_name", params.ScheduleName,
		"kind", params.Kind,
		"tasks", len(projection),
	)
	return projection, nil
}

// collectNodes drains a Neo4j result cursor into a []*domain.TableNode slice.
func collectNodes(ctx context.Context, result neo4j.ResultWithContext) ([]*domain.TableNode, error) {
	var nodes []*domain.TableNode
	for result.Next(ctx) {
		node, err := recordToTableNode(result.Record())
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

// recordToTableNode converts a Neo4j record to a domain.TableNode.
func recordToTableNode(record *neo4j.Record) (*domain.TableNode, error) {
	tableName, _ := record.Get("table_name")
	schemaName, _ := record.Get("schema_name")
	serviceName, _ := record.Get("service_name")
	owner, _ := record.Get("owner")
	scheduleName, _ := record.Get("schedule_name")
	criticality, _ := record.Get("criticality")
	lastUpdatedAt, _ := record.Get("last_updated_at")
	createdAt, _ := record.Get("created_at")
	nodeType, _ := record.Get("node_type")
	taskID, _ := record.Get("task_id")
	manifestVersion, _ := record.Get("manifest_version")
	imageTag, _ := record.Get("image_tag")

	node := &domain.TableNode{
		TableName:       safeString(tableName),
		SchemaName:      safeString(schemaName),
		ServiceName:     safeString(serviceName),
		Owner:           safeString(owner),
		ScheduleName:    safeString(scheduleName),
		Criticality:     domain.Criticality(safeString(criticality)),
		NodeType:        safeString(nodeType),
		TaskID:          safeString(taskID),
		ManifestVersion: safeString(manifestVersion),
		ImageTag:        safeString(imageTag),
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

// recordValue retrieves a value from a Neo4j record by key.
func recordValue(record *neo4j.Record, key string) interface{} {
	value, _ := record.Get(key)
	return value
}
