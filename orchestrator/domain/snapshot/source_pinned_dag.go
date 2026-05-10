package snapshot

import (
	"context"
	"fmt"

	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// SourcePinnedDAG is the rerun selector. It produces a full DAG projection
// from the source :Run's frozen :EXECUTES set, with uniform OLD metadata.
//
// Behaviour:
//   - The target node (TargetService/Schema/Table) → InitialStatus=PENDING,
//     fresh task_id, source's pinned (image_tag, manifest_version), no inherit.
//   - Non-SUCCEEDED descendants of the target in source's :EXECUTES set
//     (typically SKIPPED — the cascade-skipped set when target failed) →
//     PENDING, fresh task_id, no inherit. Mirrors legacy rerun behaviour.
//   - Every other source task (incl. SUCCEEDED descendants of target and
//     unrelated SUCCEEDED rows) → inherited. InitialStatus = source's
//     stored status. Source's pinned metadata. InheritedFromTaskID points
//     to the root-resolved task_id (forwards when source row was itself
//     inherited).
//
// Returns ErrTargetNotFound if the target node is not in the source's
// :EXECUTES set.
type SourcePinnedDAG struct {
	TargetService string
	TargetSchema  string
	TargetTable   string
}

// fqnKey is a 3-tuple used as a map key for FQN lookups.
type fqnKey struct {
	service string
	schema  string
	table   string
}

// sourceTaskRow captures the per-task data we need from source's :EXECUTES set.
type sourceTaskRow struct {
	taskID            uuid.UUID
	scheduleName      string
	nodeType          string
	status            string // uppercase, from edge
	imageTag          string
	manifestVersion   string
	inheritedFromRoot *uuid.UUID // non-nil iff source row was itself inherited
}

func (s SourcePinnedDAG) SelectTasks(ctx context.Context, tx neo4j.ManagedTransaction, p Params) ([]TaskProjection, error) {
	if p.SourceRunID == nil {
		return nil, fmt.Errorf("SourcePinnedDAG: SourceRunID required")
	}
	if s.TargetService == "" || s.TargetSchema == "" || s.TargetTable == "" {
		return nil, fmt.Errorf("SourcePinnedDAG: target identity required")
	}

	source, err := readSourceTasks(ctx, tx, p.SourceRunID.String())
	if err != nil {
		return nil, err
	}

	target := fqnKey{service: s.TargetService, schema: s.TargetSchema, table: s.TargetTable}
	if _, ok := source[target]; !ok {
		return nil, ErrTargetNotFound
	}

	// Compute "rebase set": target + non-SUCCEEDED descendants of target in source's
	// :EXECUTES set (so we walk the source's pinned topology, not latest).
	rebaseFQNs := map[fqnKey]struct{}{target: {}}
	descendants, err := descendantsInSourceRun(ctx, tx, p.SourceRunID.String(), target)
	if err != nil {
		return nil, err
	}
	for _, d := range descendants {
		st, ok := source[d]
		if !ok {
			continue
		}
		// Only flip non-SUCCEEDED descendants. SUCCEEDED descendants stay
		// inherited (their result was already correct in source).
		if st.status != "SUCCEEDED" {
			rebaseFQNs[d] = struct{}{}
		}
	}

	var projection []TaskProjection
	for f, st := range source {
		if _, isRebased := rebaseFQNs[f]; isRebased {
			projection = append(projection, TaskProjection{
				TaskID:          uuid.New(),
				ServiceName:     f.service,
				SchemaName:      f.schema,
				TableName:       f.table,
				ScheduleName:    st.scheduleName,
				NodeType:        st.nodeType,
				InitialStatus:   "PENDING",
				ImageTag:        st.imageTag,
				ManifestVersion: st.manifestVersion,
				MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
			})
			continue
		}
		// Inherited: keep source's status + source's metadata + root-forwarded pointer.
		root := st.taskID
		if st.inheritedFromRoot != nil {
			root = *st.inheritedFromRoot
		}
		projection = append(projection, TaskProjection{
			TaskID:              uuid.New(),
			ServiceName:         f.service,
			SchemaName:          f.schema,
			TableName:           f.table,
			ScheduleName:        st.scheduleName,
			NodeType:            st.nodeType,
			InitialStatus:       st.status, // typically "SUCCEEDED"
			ImageTag:            st.imageTag,
			ManifestVersion:     st.manifestVersion,
			InheritedFromTaskID: &root,
			MaxRetries:          0, // inherited rows won't retry
		})
	}
	return projection, nil
}

// readSourceTasks loads source :Run's :EXECUTES set keyed by FQN.
func readSourceTasks(ctx context.Context, tx neo4j.ManagedTransaction, sourceRunID string) (map[fqnKey]sourceTaskRow, error) {
	const q = `
		MATCH (sr:Run {run_id: $source_run_id})-[se:EXECUTES]->(st:Table)
		RETURN st.service_name AS service,
		       st.schema_name  AS schema,
		       st.table_name   AS tbl,
		       se.task_id      AS task_id,
		       st.schedule_name AS schedule_name,
		       COALESCE(st.node_type, 'dbt-model') AS node_type,
		       COALESCE(se.status, 'PENDING')      AS status,
		       COALESCE(se.image_tag, '')          AS image_tag,
		       COALESCE(se.manifest_version, '')   AS manifest_version,
		       se.inherited_from_task_id           AS inherited_from
	`
	result, err := tx.Run(ctx, q, map[string]interface{}{"source_run_id": sourceRunID})
	if err != nil {
		return nil, fmt.Errorf("readSourceTasks: %w", err)
	}
	out := make(map[fqnKey]sourceTaskRow)
	for result.Next(ctx) {
		rec := result.Record()
		taskIDStr := stringField(rec, "task_id")
		taskID, _ := uuid.Parse(taskIDStr)
		f := fqnKey{
			service: stringField(rec, "service"),
			schema:  stringField(rec, "schema"),
			table:   stringField(rec, "tbl"),
		}
		st := sourceTaskRow{
			taskID:          taskID,
			scheduleName:    stringField(rec, "schedule_name"),
			nodeType:        stringField(rec, "node_type"),
			status:          stringField(rec, "status"),
			imageTag:        stringField(rec, "image_tag"),
			manifestVersion: stringField(rec, "manifest_version"),
		}
		if v, _ := rec.Get("inherited_from"); v != nil {
			if s, ok := v.(string); ok && s != "" {
				if u, perr := uuid.Parse(s); perr == nil {
					st.inheritedFromRoot = &u
				}
			}
		}
		out[f] = st
	}
	return out, result.Err()
}

// descendantsInSourceRun walks DEPENDS_ON (in :Table space) and restricts the
// result to nodes that are also in the source run's :EXECUTES set — preserves
// source's pinned topology even if latest topology has drifted.
func descendantsInSourceRun(ctx context.Context, tx neo4j.ManagedTransaction, sourceRunID string, start fqnKey) ([]fqnKey, error) {
	const q = `
		MATCH (start:Table {service_name: $svc, schema_name: $schema, table_name: $tbl})
		MATCH (d:Table)-[:DEPENDS_ON*1..]->(start)
		MATCH (sr:Run {run_id: $source_run_id})-[:EXECUTES]->(d)
		RETURN DISTINCT d.service_name AS svc, d.schema_name AS schema, d.table_name AS tbl
	`
	result, err := tx.Run(ctx, q, map[string]interface{}{
		"source_run_id": sourceRunID,
		"svc":           start.service, "schema": start.schema, "tbl": start.table,
	})
	if err != nil {
		return nil, fmt.Errorf("descendantsInSourceRun: %w", err)
	}
	var out []fqnKey
	for result.Next(ctx) {
		rec := result.Record()
		out = append(out, fqnKey{
			service: stringField(rec, "svc"),
			schema:  stringField(rec, "schema"),
			table:   stringField(rec, "tbl"),
		})
	}
	return out, result.Err()
}
