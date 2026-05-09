package snapshot

import (
	"context"
	"fmt"

	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// RebasePartition is the rebase selector (Feature 2). Given a terminal
// source :Run and the latest topology, it produces a projection that
// rebases everything that didn't succeed (plus its descendants and any
// new arrivals) and inherits the rest.
//
// Returns ErrEmptyProjection if the partition produces zero tasks (e.g.
// every source task in the drop_set and no new arrivals).
type RebasePartition struct{}

type latestTableRow struct {
	scheduleName    string
	nodeType        string
	imageTag        string
	manifestVersion string
}

func (RebasePartition) SelectTasks(ctx context.Context, tx neo4j.ManagedTransaction, p Params) ([]TaskProjection, error) {
	if p.SourceRunID == nil {
		return nil, fmt.Errorf("RebasePartition: SourceRunID required")
	}

	source, err := readSourceTasks(ctx, tx, p.SourceRunID.String())
	if err != nil {
		return nil, err
	}
	// "Latest" for a rebase = the source schedule's full DAG shape:
	// the source schedule's own :Tables, plus their transitive upstream
	// dependencies (typically seeds in other schedules). Tables that
	// happen to live in a touched seed schedule but are not in this DAG
	// are NOT in scope.
	latest, err := readLatestSourceDAG(ctx, tx, p.ScheduleName)
	if err != nil {
		return nil, err
	}

	// Pass 1: seed rebase_set with non-SUCCEEDED source tasks that still exist in latest.
	rebaseFQNs := map[fqnKey]struct{}{}
	for f, st := range source {
		if st.status != "SUCCEEDED" {
			if _, exists := latest[f]; exists {
				rebaseFQNs[f] = struct{}{}
			}
		}
	}
	// Pass 2: for each seeded FQN, add its descendants in LATEST topology.
	for f := range rebaseFQNs {
		descendants, err := descendantsInLatestTopology(ctx, tx, f)
		if err != nil {
			return nil, err
		}
		for _, d := range descendants {
			if _, exists := latest[d]; exists {
				rebaseFQNs[d] = struct{}{}
			}
		}
	}
	// Pass 3: new arrivals (in latest, not in source) join rebase_set.
	for f := range latest {
		if _, exists := source[f]; !exists {
			rebaseFQNs[f] = struct{}{}
		}
	}

	// Pass 4: emit projection — iterate latest (drop_set excluded by construction).
	var projection []TaskProjection
	for f, lt := range latest {
		if _, isRebased := rebaseFQNs[f]; isRebased {
			projection = append(projection, TaskProjection{
				TaskID:          uuid.New(),
				ServiceName:     f.service,
				SchemaName:      f.schema,
				TableName:       f.table,
				ScheduleName:    lt.scheduleName,
				NodeType:        lt.nodeType,
				InitialStatus:   "PENDING",
				ImageTag:        lt.imageTag,
				ManifestVersion: lt.manifestVersion,
				MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
			})
			continue
		}
		// Inherited iff source had this FQN as SUCCEEDED.
		if st, ok := source[f]; ok && st.status == "SUCCEEDED" {
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
				InitialStatus:       "SUCCEEDED",
				ImageTag:            st.imageTag,
				ManifestVersion:     st.manifestVersion,
				InheritedFromTaskID: &root,
				MaxRetries:          0,
			})
		}
	}

	if len(projection) == 0 {
		return nil, ErrEmptyProjection
	}
	return projection, nil
}

// readLatestSourceDAG returns every active :Table in the source schedule
// plus every active :Table that any of those tables transitively
// DEPENDS_ON (upstream seeds, possibly in other schedules). Tables that
// share a seed's schedule but are not part of this DAG are excluded.
func readLatestSourceDAG(ctx context.Context, tx neo4j.ManagedTransaction, scheduleName string) (map[fqnKey]latestTableRow, error) {
	if scheduleName == "" {
		return map[fqnKey]latestTableRow{}, nil
	}
	const q = `
		MATCH (root:Table {schedule_name: $sched})
		WHERE COALESCE(root.active, true)
		OPTIONAL MATCH (root)-[:DEPENDS_ON*0..]->(t:Table)
		WHERE COALESCE(t.active, true)
		WITH COLLECT(DISTINCT t) + COLLECT(DISTINCT root) AS all_tables
		UNWIND all_tables AS t
		WITH DISTINCT t
		RETURN t.service_name AS service,
		       t.schema_name  AS schema,
		       t.table_name   AS tbl,
		       t.schedule_name AS schedule_name,
		       COALESCE(t.node_type, 'dbt-model') AS node_type,
		       COALESCE(t.image_tag, '')          AS image_tag,
		       COALESCE(t.manifest_version, '')   AS manifest_version
	`
	result, err := tx.Run(ctx, q, map[string]interface{}{"sched": scheduleName})
	if err != nil {
		return nil, fmt.Errorf("readLatestSourceDAG: %w", err)
	}
	out := make(map[fqnKey]latestTableRow)
	for result.Next(ctx) {
		rec := result.Record()
		f := fqnKey{
			service: stringField(rec, "service"),
			schema:  stringField(rec, "schema"),
			table:   stringField(rec, "tbl"),
		}
		out[f] = latestTableRow{
			scheduleName:    stringField(rec, "schedule_name"),
			nodeType:        stringField(rec, "node_type"),
			imageTag:        stringField(rec, "image_tag"),
			manifestVersion: stringField(rec, "manifest_version"),
		}
	}
	return out, result.Err()
}

// descendantsInLatestTopology walks DEPENDS_ON in :Table space (not restricted
// to any source run) — the rebase needs to walk the LATEST topology since
// rebased tasks execute against latest.
func descendantsInLatestTopology(ctx context.Context, tx neo4j.ManagedTransaction, start fqnKey) ([]fqnKey, error) {
	const q = `
		MATCH (start:Table {service_name: $svc, schema_name: $schema, table_name: $tbl})
		MATCH (d:Table)-[:DEPENDS_ON*1..]->(start)
		WHERE COALESCE(d.active, true)
		RETURN DISTINCT d.service_name AS svc, d.schema_name AS schema, d.table_name AS tbl
	`
	result, err := tx.Run(ctx, q, map[string]interface{}{
		"svc": start.service, "schema": start.schema, "tbl": start.table,
	})
	if err != nil {
		return nil, fmt.Errorf("descendantsInLatestTopology: %w", err)
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
