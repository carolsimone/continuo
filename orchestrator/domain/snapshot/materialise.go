package snapshot

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Materialise writes the :Run node and one :EXECUTES edge per projection entry
// in a single Neo4j transaction. Kind-agnostic — branching lives in selectors.
//
// :Run properties: run_id, schedule_name, kind, created_at, source_run_id?
//
//	(source_run_id is set only when params.SourceRunID is non-nil)
//
// :EXECUTES edge properties: task_id, status, image_tag, manifest_version,
//
//	inherited_from_task_id?
//
// Convention: edge.status is uppercase ("PENDING" / "SUCCEEDED"). The wire
// format that crosses to state (run.entries.dispatched:v1) uses lowercase;
// callers translate at that boundary. Inside Neo4j we keep the existing
// uppercase convention used by SnapshotGraph + UpdateNodeStatus.
//
// The MATCH on :Table uses (service_name, schema_name, table_name, schedule_name)
// to mirror the existing SnapshotGraph approach (handles cross-schedule seed
// dependencies — same Table FQN can appear under multiple schedule_names).
//
// Re-running with the same (run_id, projection) is idempotent — MERGE preserves
// existing rows. ON CREATE clauses on the edge ensure status/task_id/image_tag/
// manifest_version are stamped only on first write.
func Materialise(ctx context.Context, tx neo4j.ManagedTransaction, p Params, projection []TaskProjection) error {
	if len(projection) == 0 {
		return ErrEmptyProjection
	}

	// sourceRunIDParam is interface{} so a nil maps cleanly to a Cypher null.
	var sourceRunIDParam interface{}
	if p.SourceRunID != nil {
		sourceRunIDParam = p.SourceRunID.String()
	} else {
		sourceRunIDParam = nil
	}

	tasks := make([]map[string]interface{}, len(projection))
	for i, t := range projection {
		// inheritedFrom is interface{} so a nil maps cleanly to a Cypher null.
		var inheritedFrom interface{}
		if t.InheritedFromTaskID != nil {
			inheritedFrom = t.InheritedFromTaskID.String()
		}
		tasks[i] = map[string]interface{}{
			"task_id":                t.TaskID.String(),
			"service_name":           t.ServiceName,
			"schema_name":            t.SchemaName,
			"table_name":             t.TableName,
			"schedule_name":          t.ScheduleName,
			"initial_status":         t.InitialStatus,
			"image_tag":              t.ImageTag,
			"manifest_version":       t.ManifestVersion,
			"inherited_from_task_id": inheritedFrom,
		}
	}

	// :Run.topology_generation + :Run.service_metadata are stamped on CREATE so
	// the run is isolated from future topology changes. Source depends on whether
	// this is a derived run:
	//   - source_run_id IS NULL  (cron/trigger/latest-single-node)   → :TopologyRoot
	//   - source_run_id non-NULL (rerun/rebase/stale-single-node)    → source :Run
	// If source_run_id is set but the source :Run was pruned, fall back to
	// :TopologyRoot rather than fail (matches legacy SnapshotSingleNodeRun's
	// graceful behaviour for missing source nodes).
	const query = `
		OPTIONAL MATCH (root:TopologyRoot {id: 'singleton'})
		OPTIONAL MATCH (src:Run {run_id: $source_run_id})
		WITH root, src,
		     CASE
		         WHEN src IS NULL THEN COALESCE(root.topology_generation, 0)
		         ELSE src.topology_generation
		     END AS topo_gen,
		     CASE
		         WHEN src IS NULL THEN COALESCE(root.service_metadata, '{}')
		         ELSE src.service_metadata
		     END AS svc_meta
		MERGE (run:Run {run_id: $run_id})
		ON CREATE SET run.schedule_name      = $schedule_name,
		              run.created_at         = datetime(),
		              run.kind               = $kind,
		              run.topology_generation = topo_gen,
		              run.service_metadata    = svc_meta
		ON MATCH SET  run.kind = COALESCE(run.kind, $kind)
		FOREACH (_ IN CASE WHEN $source_run_id IS NULL THEN [] ELSE [1] END |
		    SET run.source_run_id = $source_run_id
		)
		WITH run
		UNWIND $tasks AS t
		MATCH (tbl:Table {service_name:  t.service_name,
		                  schema_name:   t.schema_name,
		                  table_name:    t.table_name,
		                  schedule_name: t.schedule_name})
		MERGE (run)-[e:EXECUTES]->(tbl)
		ON CREATE SET e.status           = t.initial_status,
		              e.task_id          = t.task_id,
		              e.image_tag        = t.image_tag,
		              e.manifest_version = t.manifest_version
		FOREACH (_ IN CASE WHEN t.inherited_from_task_id IS NULL THEN [] ELSE [1] END |
		    SET e.inherited_from_task_id = t.inherited_from_task_id
		)
		RETURN count(e) AS edges_created
	`

	result, err := tx.Run(ctx, query, map[string]interface{}{
		"run_id":        p.RunID,
		"schedule_name": p.ScheduleName,
		"kind":          p.Kind,
		"source_run_id": sourceRunIDParam,
		"tasks":         tasks,
	})
	if err != nil {
		return fmt.Errorf("Materialise: query failed: %w", err)
	}
	if !result.Next(ctx) {
		return fmt.Errorf("Materialise: no result")
	}
	count, _ := result.Record().Get("edges_created")
	// edges_created counts how many MATCH+MERGE'd edges the query found/created.
	// If it's less than projection length, some :Table MATCHes failed — meaning
	// callers projected a node that doesn't exist in latest topology with the
	// given schedule_name. Surface this as an error so the handler can react.
	if c, ok := count.(int64); ok && int(c) < len(projection) {
		return fmt.Errorf("Materialise: wrote %d edges, expected %d (likely missing :Table)", c, len(projection))
	}
	return result.Err()
}
