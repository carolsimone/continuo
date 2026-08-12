package neo4jinfra

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// topologyReader implements snapshot.TopologyReader against a Neo4j managed
// transaction. Constructed by SnapshotTxRunner inside its ExecuteWrite callback
// so the caller's reads and the subsequent writer call commit in one Cypher tx.
type topologyReader struct {
	tx neo4j.ManagedTransaction
}

func newTopologyReader(tx neo4j.ManagedTransaction) *topologyReader {
	return &topologyReader{tx: tx}
}

func (r *topologyReader) LoadLatestSourceDAG(ctx context.Context, scheduleName string) (map[snapshot.FQN]snapshot.LatestTableRow, error) {
	if scheduleName == "" {
		return map[snapshot.FQN]snapshot.LatestTableRow{}, nil
	}
	// P1 fix: restored pre-refactor UNION semantics.
	// Returns every active :Table in the schedule PLUS the direct (one-hop)
	// dbt-seed upstreams of those tables.  Transitive walk was removed because
	// it picked up non-seed cross-schedule upstreams that HandleSchedulerStarted
	// never dispatches, causing the run to stall.
	const q = `
		CALL {
		    MATCH (t:Table {schedule_name: $schedule_name})
		    WHERE COALESCE(t.active, true)
		    RETURN t.schema_name   AS schema_name,
		           t.table_name    AS table_name,
		           t.service_name  AS service_name,
		           t.schedule_name AS schedule_name,
		           COALESCE(t.node_type, 'dbt-model')      AS node_type,
		           t.test_count                             AS test_count,
		           COALESCE(t.image_tag, '')                AS image_tag,
		           COALESCE(t.manifest_version, '')         AS manifest_version,
		           COALESCE(t.content_hash, '')             AS content_hash

		    UNION

		    MATCH (t:Table {schedule_name: $schedule_name})
		    WHERE COALESCE(t.active, true)
		    MATCH (t)-[:DEPENDS_ON]->(s:Table {node_type: "dbt-seed"})
		    WHERE COALESCE(s.active, true)
		    RETURN s.schema_name   AS schema_name,
		           s.table_name    AS table_name,
		           s.service_name  AS service_name,
		           s.schedule_name AS schedule_name,
		           s.node_type     AS node_type,
		           s.test_count                     AS test_count,
		           COALESCE(s.image_tag, '')        AS image_tag,
		           COALESCE(s.manifest_version, '') AS manifest_version,
		           COALESCE(s.content_hash, '')     AS content_hash
		}
		RETURN DISTINCT schema_name, table_name, service_name, schedule_name,
		                node_type, test_count, image_tag, manifest_version, content_hash
	`
	result, err := r.tx.Run(ctx, q, map[string]interface{}{"schedule_name": scheduleName})
	if err != nil {
		return nil, fmt.Errorf("topology_reader: LoadLatestSourceDAG: %w", err)
	}
	out := make(map[snapshot.FQN]snapshot.LatestTableRow)
	for result.Next(ctx) {
		rec := result.Record()
		schedName := stringField(rec, "schedule_name")
		f := snapshot.FQN{
			Service:      stringField(rec, "service_name"),
			Schema:       stringField(rec, "schema_name"),
			Table:        stringField(rec, "table_name"),
			ScheduleName: schedName,
		}
		tc, tcKnown := intFieldPresent(rec, "test_count")
		out[f] = snapshot.LatestTableRow{
			ScheduleName:    schedName,
			NodeType:        stringField(rec, "node_type"),
			TestCount:       tc,
			TestCountKnown:  tcKnown,
			ImageTag:        stringField(rec, "image_tag"),
			ManifestVersion: stringField(rec, "manifest_version"),
			ContentHash:     stringField(rec, "content_hash"),
		}
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("topology_reader: LoadLatestSourceDAG result: %w", err)
	}
	return out, nil
}

func (r *topologyReader) LoadSourceTasks(ctx context.Context, sourceRunID string) (map[snapshot.FQN]snapshot.SourceTaskRow, error) {
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
		       COALESCE(se.content_hash, '')       AS content_hash,
		       se.inherited_from_task_id           AS inherited_from
	`
	result, err := r.tx.Run(ctx, q, map[string]interface{}{"source_run_id": sourceRunID})
	if err != nil {
		return nil, fmt.Errorf("topology_reader: LoadSourceTasks: %w", err)
	}
	out := make(map[snapshot.FQN]snapshot.SourceTaskRow)
	for result.Next(ctx) {
		rec := result.Record()
		taskIDStr := stringField(rec, "task_id")
		taskID, _ := uuid.Parse(taskIDStr)
		f := snapshot.FQN{
			Service:      stringField(rec, "service"),
			Schema:       stringField(rec, "schema"),
			Table:        stringField(rec, "tbl"),
			ScheduleName: stringField(rec, "schedule_name"),
		}
		st := snapshot.SourceTaskRow{
			TaskID:          taskID,
			ScheduleName:    stringField(rec, "schedule_name"),
			NodeType:        stringField(rec, "node_type"),
			Status:          stringField(rec, "status"),
			ImageTag:        stringField(rec, "image_tag"),
			ManifestVersion: stringField(rec, "manifest_version"),
			ContentHash:     stringField(rec, "content_hash"),
		}
		if v, _ := rec.Get("inherited_from"); v != nil {
			if s, ok := v.(string); ok && s != "" {
				if u, perr := uuid.Parse(s); perr == nil {
					st.InheritedFromRoot = &u
				}
			}
		}
		out[f] = st
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("topology_reader: LoadSourceTasks result: %w", err)
	}
	return out, nil
}

func (r *topologyReader) DescendantsInLatestTopologyBatch(ctx context.Context, starts []snapshot.FQN) (map[snapshot.FQN][]snapshot.FQN, error) {
	// Transitive (`*1..`) DEPENDS_ON descendants in active :Table space, one row
	// per start with its descendants collected. Empty ScheduleName on a start
	// means "any schedule".
	const q = `
		UNWIND $starts AS s
		OPTIONAL MATCH (start:Table {service_name: s.svc, schema_name: s.schema, table_name: s.tbl})
		WHERE (s.sched = '' OR start.schedule_name = s.sched) AND COALESCE(start.active, true)
		OPTIONAL MATCH (d:Table)-[:DEPENDS_ON*1..]->(start)
		WHERE COALESCE(d.active, true)
		WITH s, collect(DISTINCT d) AS ds
		RETURN s AS start,
		       [d IN ds WHERE d IS NOT NULL |
		            {svc: d.service_name, schema: d.schema_name, tbl: d.table_name,
		             schedule_name: d.schedule_name}] AS descendants
	`
	return r.queryDescendantsBatch(ctx, "DescendantsInLatestTopologyBatch", q, starts, nil)
}

func (r *topologyReader) DescendantsInSourceRunBatch(ctx context.Context, sourceRunID string, starts []snapshot.FQN) (map[snapshot.FQN][]snapshot.FQN, error) {
	// Transitive DEPENDS_ON descendants restricted to the source run's :EXECUTES
	// set, one row per start with its descendants collected.
	const q = `
		UNWIND $starts AS s
		OPTIONAL MATCH (start:Table {service_name: s.svc, schema_name: s.schema, table_name: s.tbl})
		WHERE (s.sched = '' OR start.schedule_name = s.sched)
		OPTIONAL MATCH (d:Table)-[:DEPENDS_ON*1..]->(start)
		WHERE EXISTS { MATCH (:Run {run_id: $source_run_id})-[:EXECUTES]->(d) }
		WITH s, collect(DISTINCT d) AS ds
		RETURN s AS start,
		       [d IN ds WHERE d IS NOT NULL |
		            {svc: d.service_name, schema: d.schema_name, tbl: d.table_name,
		             schedule_name: d.schedule_name}] AS descendants
	`
	return r.queryDescendantsBatch(ctx, "DescendantsInSourceRunBatch", q, starts,
		map[string]interface{}{"source_run_id": sourceRunID})
}

func (r *topologyReader) ImmediateDescendantsInLatestTopologyBatch(ctx context.Context, starts []snapshot.FQN) (map[snapshot.FQN][]snapshot.FQN, error) {
	// One DEPENDS_ON hop (no `*1..`): direct dependents only, in active :Table space.
	const q = `
		UNWIND $starts AS s
		OPTIONAL MATCH (start:Table {service_name: s.svc, schema_name: s.schema, table_name: s.tbl})
		WHERE (s.sched = '' OR start.schedule_name = s.sched) AND COALESCE(start.active, true)
		OPTIONAL MATCH (d:Table)-[:DEPENDS_ON]->(start)
		WHERE COALESCE(d.active, true)
		WITH s, collect(DISTINCT d) AS ds
		RETURN s AS start,
		       [d IN ds WHERE d IS NOT NULL |
		            {svc: d.service_name, schema: d.schema_name, tbl: d.table_name,
		             schedule_name: d.schedule_name}] AS descendants
	`
	return r.queryDescendantsBatch(ctx, "ImmediateDescendantsInLatestTopologyBatch", q, starts, nil)
}

func (r *topologyReader) ImmediateDescendantsInSourceRunBatch(ctx context.Context, sourceRunID string, starts []snapshot.FQN) (map[snapshot.FQN][]snapshot.FQN, error) {
	// One DEPENDS_ON hop restricted to the source run's :EXECUTES set.
	const q = `
		UNWIND $starts AS s
		OPTIONAL MATCH (start:Table {service_name: s.svc, schema_name: s.schema, table_name: s.tbl})
		WHERE (s.sched = '' OR start.schedule_name = s.sched)
		OPTIONAL MATCH (d:Table)-[:DEPENDS_ON]->(start)
		WHERE EXISTS { MATCH (:Run {run_id: $source_run_id})-[:EXECUTES]->(d) }
		WITH s, collect(DISTINCT d) AS ds
		RETURN s AS start,
		       [d IN ds WHERE d IS NOT NULL |
		            {svc: d.service_name, schema: d.schema_name, tbl: d.table_name,
		             schedule_name: d.schedule_name}] AS descendants
	`
	return r.queryDescendantsBatch(ctx, "ImmediateDescendantsInSourceRunBatch", q, starts,
		map[string]interface{}{"source_run_id": sourceRunID})
}

// queryDescendantsBatch runs a batched descendant query that returns one row per
// start FQN (echoed back as `start`) plus a `descendants` list. It builds the
// $starts parameter from the input slice and keys the result map by the original
// start FQN. Every start in the input appears in the output (with a possibly-nil
// slice) so callers never see a missing key. extraParams carries query-specific
// bindings such as source_run_id.
func (r *topologyReader) queryDescendantsBatch(
	ctx context.Context,
	label, q string,
	starts []snapshot.FQN,
	extraParams map[string]interface{},
) (map[snapshot.FQN][]snapshot.FQN, error) {
	out := make(map[snapshot.FQN][]snapshot.FQN, len(starts))
	if len(starts) == 0 {
		return out, nil
	}
	startParams := make([]map[string]interface{}, 0, len(starts))
	for _, s := range starts {
		startParams = append(startParams, map[string]interface{}{
			"svc": s.Service, "schema": s.Schema, "tbl": s.Table, "sched": s.ScheduleName,
		})
		out[s] = nil // ensure every start is a key even with no descendants
	}
	params := map[string]interface{}{"starts": startParams}
	for k, v := range extraParams {
		params[k] = v
	}

	result, err := r.tx.Run(ctx, q, params)
	if err != nil {
		return nil, fmt.Errorf("topology_reader: %s: %w", label, err)
	}
	for result.Next(ctx) {
		rec := result.Record()
		start, ok := fqnFromMap(recordMap(rec, "start"))
		if !ok {
			continue
		}
		descRaw, _ := rec.Get("descendants")
		descList, _ := descRaw.([]interface{})
		var descendants []snapshot.FQN
		for _, item := range descList {
			if f, ok := fqnFromMap(item); ok {
				descendants = append(descendants, f)
			}
		}
		out[start] = descendants
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("topology_reader: %s result: %w", label, err)
	}
	return out, nil
}

// recordMap extracts a map field from a Neo4j record (used for the echoed-back
// `start` parameter, which the driver returns as a map[string]interface{}).
func recordMap(rec *neo4j.Record, k string) interface{} {
	v, _ := rec.Get(k)
	return v
}

// fqnFromMap builds an FQN from a map carrying svc/schema/tbl/schedule_name (or
// the echoed start's svc/schema/tbl/sched). Returns false when the value is not
// a map.
func fqnFromMap(raw interface{}) (snapshot.FQN, bool) {
	m, ok := raw.(map[string]interface{})
	if !ok {
		return snapshot.FQN{}, false
	}
	sched := mapString(m, "schedule_name")
	if sched == "" {
		sched = mapString(m, "sched")
	}
	return snapshot.FQN{
		Service:      mapString(m, "svc"),
		Schema:       mapString(m, "schema"),
		Table:        mapString(m, "tbl"),
		ScheduleName: sched,
	}, true
}

// mapString reads a string-typed map value, returning "" for nil/non-string.
func mapString(m map[string]interface{}, k string) string {
	if v, ok := m[k]; ok && v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func (r *topologyReader) LoadSingleLatestTable(ctx context.Context, fqn snapshot.FQN) (snapshot.LatestTableRow, bool, error) {
	// Empty ScheduleName means "any schedule" (SingleNode doesn't know it).
	const q = `
		MATCH (tbl:Table {service_name: $svc, schema_name: $schema, table_name: $tbl})
		WHERE ($sched = '' OR tbl.schedule_name = $sched) AND COALESCE(tbl.active, true)
		RETURN tbl.schedule_name AS schedule_name,
		       COALESCE(tbl.node_type, 'dbt-model') AS node_type,
		       tbl.test_count                       AS test_count,
		       COALESCE(tbl.image_tag, '')          AS image_tag,
		       COALESCE(tbl.manifest_version, '')   AS manifest_version,
		       COALESCE(tbl.content_hash, '')       AS content_hash
		LIMIT 1
	`
	result, err := r.tx.Run(ctx, q, map[string]interface{}{
		"svc": fqn.Service, "schema": fqn.Schema, "tbl": fqn.Table,
		"sched": fqn.ScheduleName,
	})
	if err != nil {
		return snapshot.LatestTableRow{}, false, fmt.Errorf("topology_reader: LoadSingleLatestTable: %w", err)
	}
	if !result.Next(ctx) {
		if rerr := result.Err(); rerr != nil {
			return snapshot.LatestTableRow{}, false, fmt.Errorf("topology_reader: LoadSingleLatestTable result: %w", rerr)
		}
		return snapshot.LatestTableRow{}, false, nil
	}
	rec := result.Record()
	tc, tcKnown := intFieldPresent(rec, "test_count")
	return snapshot.LatestTableRow{
		ScheduleName:    stringField(rec, "schedule_name"),
		NodeType:        stringField(rec, "node_type"),
		TestCount:       tc,
		TestCountKnown:  tcKnown,
		ImageTag:        stringField(rec, "image_tag"),
		ManifestVersion: stringField(rec, "manifest_version"),
		ContentHash:     stringField(rec, "content_hash"),
	}, true, nil
}

func (r *topologyReader) LoadSingleTableFromSourceRun(ctx context.Context, sourceRunID string, fqn snapshot.FQN) (snapshot.LatestTableRow, bool, error) {
	// Empty ScheduleName means "any schedule" (SingleNode doesn't know it).
	const q = `
		MATCH (src:Run {run_id: $source_run_id})-[srcEdge:EXECUTES]->
		      (tbl:Table {service_name: $svc, schema_name: $schema, table_name: $tbl})
		WHERE ($sched = '' OR tbl.schedule_name = $sched)
		RETURN tbl.schedule_name AS schedule_name,
		       COALESCE(tbl.node_type, 'dbt-model') AS node_type,
		       srcEdge.test_count                     AS test_count,
		       COALESCE(srcEdge.image_tag, '')        AS image_tag,
		       COALESCE(srcEdge.manifest_version, '') AS manifest_version,
		       COALESCE(srcEdge.content_hash, '')     AS content_hash
		LIMIT 1
	`
	result, err := r.tx.Run(ctx, q, map[string]interface{}{
		"source_run_id": sourceRunID,
		"svc":           fqn.Service, "schema": fqn.Schema, "tbl": fqn.Table,
		"sched": fqn.ScheduleName,
	})
	if err != nil {
		return snapshot.LatestTableRow{}, false, fmt.Errorf("topology_reader: LoadSingleTableFromSourceRun: %w", err)
	}
	if !result.Next(ctx) {
		if rerr := result.Err(); rerr != nil {
			return snapshot.LatestTableRow{}, false, fmt.Errorf("topology_reader: LoadSingleTableFromSourceRun result: %w", rerr)
		}
		return snapshot.LatestTableRow{}, false, nil
	}
	rec := result.Record()
	tc, tcKnown := intFieldPresent(rec, "test_count")
	return snapshot.LatestTableRow{
		ScheduleName:    stringField(rec, "schedule_name"),
		NodeType:        stringField(rec, "node_type"),
		TestCount:       tc,
		TestCountKnown:  tcKnown,
		ImageTag:        stringField(rec, "image_tag"),
		ManifestVersion: stringField(rec, "manifest_version"),
		ContentHash:     stringField(rec, "content_hash"),
	}, true, nil
}

func (r *topologyReader) SourceRunOperation(ctx context.Context, sourceRunID string) (string, error) {
	const q = `
		MATCH (r:Run {run_id: $source_run_id})
		RETURN COALESCE(r.operation, '') AS operation
	`
	result, err := r.tx.Run(ctx, q, map[string]interface{}{"source_run_id": sourceRunID})
	if err != nil {
		return "", fmt.Errorf("topology_reader: SourceRunOperation: %w", err)
	}
	if !result.Next(ctx) {
		if rerr := result.Err(); rerr != nil {
			return "", fmt.Errorf("topology_reader: SourceRunOperation result: %w", rerr)
		}
		return "", nil
	}
	return stringField(result.Record(), "operation"), nil
}

// stringField extracts a string-typed field from a Neo4j record, returning ""
// for nil or non-string values. Mirrors today's helper in domain/snapshot.
func stringField(rec *neo4j.Record, k string) string {
	if v, _ := rec.Get(k); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// intFieldPresent returns the int value of key and whether the field was
// present and non-null (a Neo4j property that does not exist reads as nil).
func intFieldPresent(rec *neo4j.Record, k string) (int, bool) {
	v, ok := rec.Get(k)
	if !ok || v == nil {
		return 0, false
	}
	return int(toInt64(v)), true
}

// NewTopologyReaderForTest exposes newTopologyReader to package-external tests
// (e.g. integration tests in *_test.go files outside this package). Production
// code should always go through SnapshotTxRunner.
func NewTopologyReaderForTest(tx neo4j.ManagedTransaction) snapshot.TopologyReader {
	return newTopologyReader(tx)
}
