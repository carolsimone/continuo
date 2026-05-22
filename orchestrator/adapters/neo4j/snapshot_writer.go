package neo4jinfra

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// snapshotWriter implements snapshot.SnapshotWriter against a Neo4j managed
// transaction. The Cypher is identical to today's domain/snapshot.Materialise.
type snapshotWriter struct {
	tx neo4j.ManagedTransaction
}

func newSnapshotWriter(tx neo4j.ManagedTransaction) *snapshotWriter {
	return &snapshotWriter{tx: tx}
}

// WriteRunAndExecutesEdges writes the :Run node and one :EXECUTES edge per
// projection entry. Idempotent on rerun.
//
// :Run properties: run_id, schedule_name, kind, created_at, source_run_id?,
//
//	topology_generation, service_metadata, total_nodes, terminal_count,
//	version
//
// :EXECUTES edge:  task_id, status, image_tag, manifest_version,
//
//	inherited_from_task_id?
//
// topology_generation + service_metadata are stamped on CREATE from the source
// :Run if source_run_id is set, otherwise from :TopologyRoot. Falls back to
// :TopologyRoot if source :Run was pruned. total_nodes is set to len(projection),
// terminal_count initialized to 0, and version initialized to 0.
func (w *snapshotWriter) WriteRunAndExecutesEdges(ctx context.Context, p snapshot.Params, projection []snapshot.TaskProjection) error {
	if len(projection) == 0 {
		return snapshot.ErrEmptyProjection
	}
	var sourceRunIDParam interface{}
	if p.SourceRunID != nil {
		sourceRunIDParam = p.SourceRunID.String()
	} else {
		sourceRunIDParam = nil
	}
	tasks := make([]map[string]interface{}, len(projection))
	for i, t := range projection {
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
		              run.service_metadata    = svc_meta,
		              run.total_nodes         = $total_nodes,
		              run.terminal_count      = 0,
		              run.failed_count        = 0,
		              run.version             = 0,
		              run.terminal_status     = CASE WHEN $cancelled THEN 'cancelled' ELSE null END,
		              run.completed_at        = CASE WHEN $cancelled THEN datetime() ELSE null END
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
	result, err := w.tx.Run(ctx, query, map[string]interface{}{
		"run_id":        p.RunID,
		"schedule_name": p.ScheduleName,
		"kind":          p.Kind,
		"source_run_id": sourceRunIDParam,
		"tasks":         tasks,
		"total_nodes":   len(projection),
		"cancelled":     p.Cancelled,
	})
	if err != nil {
		return fmt.Errorf("snapshot_writer: query failed: %w", err)
	}
	if !result.Next(ctx) {
		return fmt.Errorf("snapshot_writer: no result")
	}
	count, _ := result.Record().Get("edges_created")
	if c, ok := count.(int64); ok && int(c) < len(projection) {
		return fmt.Errorf("snapshot_writer: wrote %d edges, expected %d (likely missing :Table)", c, len(projection))
	}
	return result.Err()
}

// NewSnapshotWriterForTest exposes newSnapshotWriter to package-external tests.
func NewSnapshotWriterForTest(tx neo4j.ManagedTransaction) snapshot.SnapshotWriter {
	return newSnapshotWriter(tx)
}
