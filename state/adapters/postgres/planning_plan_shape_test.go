package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// planScan is one scan of an EXPLAIN (FORMAT JSON) plan.
type planScan struct {
	NodeType string
	Relation string
	Index    string
}

// planScans returns every scan of query's plan, run with args on an analysed
// database with sequential scans disabled: a table the query can only read
// whole then still shows up as a Seq Scan, while a predicate an index serves
// shows up as that index.
func planScans(t *testing.T, query string, args ...any) []planScan {
	t.Helper()
	db := newTestDB(t)
	tx, err := db.Beginx()
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.Exec(`SET LOCAL enable_seqscan = off`)
	require.NoError(t, err)
	var raw string
	require.NoError(t, tx.Get(&raw, `EXPLAIN (FORMAT JSON) `+query, args...))
	var plans []struct {
		Plan map[string]any `json:"Plan"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &plans))
	var out []planScan
	var walk func(node map[string]any)
	walk = func(node map[string]any) {
		s := planScan{}
		s.NodeType, _ = node["Node Type"].(string)
		s.Relation, _ = node["Relation Name"].(string)
		s.Index, _ = node["Index Name"].(string)
		if s.Relation != "" || s.Index != "" {
			out = append(out, s)
		}
		children, _ := node["Plans"].([]any)
		for _, c := range children {
			if child, ok := c.(map[string]any); ok {
				walk(child)
			}
		}
	}
	for _, p := range plans {
		walk(p.Plan)
	}
	return out
}

func indexesOf(scans []planScan) []string {
	var out []string
	for _, s := range scans {
		if s.Index != "" {
			out = append(out, s.Index)
		}
	}
	return out
}

func seqScansOf(scans []planScan) []string {
	var out []string
	for _, s := range scans {
		if s.NodeType == "Seq Scan" {
			out = append(out, s.Relation)
		}
	}
	return out
}

// The hot queries of a run's attempt reports read topology_edges and
// tasks through their indexes only, on an analysed 2,000-node release
// and its run: a success's ready candidates, and a final failure's pending
// descendants.
func TestPlanShape_TaskGraphQueriesReadThroughIndexes(t *testing.T) {
	db := newTestDB(t)
	tenant := storeShape(t, db, dag2000)
	r := startScheduleRun(t, db, tenant, dag2000.schedule(), model.OperationRun)
	_, err := db.ExecContext(context.Background(), `ANALYZE topology_nodes, topology_edges, tasks`)
	require.NoError(t, err)
	args := []any{tenant, r.RunID(), "rel-1", "bench.n10_0000"}

	ready := planScans(t, postgres.ReadyCandidatesQueryForTest, args...)
	assert.Empty(t, seqScansOf(ready), "ready candidates: %v", ready)
	assert.Subset(t, indexesOf(ready),
		[]string{"topology_edges_pkey", "idx_topology_edges_downstream", "uq_tasks_run_node", "topology_nodes_pkey"},
		"ready candidates: %v", ready)

	descendants := planScans(t, postgres.PendingDescendantsQueryForTest, args...)
	assert.Empty(t, seqScansOf(descendants), "pending descendants: %v", descendants)
	assert.Subset(t, indexesOf(descendants), []string{"topology_edges_pkey", "uq_tasks_run_node"},
		"pending descendants: %v", descendants)
}

// A final failure's pending descendants come out of the walk itself: the query
// reads no table outside its recursive term, so a new run whose tasks the
// statistics estimate at one row cannot turn a join after the walk into a
// nested loop over the walk.
func TestPlanShape_PendingDescendantsReadNoTableOutsideTheWalk(t *testing.T) {
	db := newTestDB(t)
	tenant := storeShape(t, db, cascade2000)
	r := startScheduleRun(t, db, tenant, cascade2000.schedule(), model.OperationRun)

	var raw string
	require.NoError(t, db.Get(&raw, `EXPLAIN (FORMAT JSON) `+postgres.PendingDescendantsQueryForTest,
		tenant, r.RunID(), "rel-1", "bench.fail_root"))
	var plans []struct {
		Plan map[string]any `json:"Plan"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &plans))
	var outside []string
	var walk func(node map[string]any)
	walk = func(node map[string]any) {
		if name, _ := node["Subplan Name"].(string); name != "" {
			return
		}
		if rel, _ := node["Relation Name"].(string); rel != "" {
			outside = append(outside, rel)
		}
		children, _ := node["Plans"].([]any)
		for _, c := range children {
			if child, ok := c.(map[string]any); ok {
				walk(child)
			}
		}
	}
	for _, p := range plans {
		walk(p.Plan)
	}
	assert.Empty(t, outside, "tables read outside the walk")
}

// A rebase preview's duration read probes tasks by node identity and
// task_attempts by task, on an analysed 2,000-task run whose every task
// succeeded with one attempt: neither table is read whole.
func TestPlanShape_LastSuccessDurationsProbesTheNodeIdentity(t *testing.T) {
	db := newTestDB(t)
	tenant := storeShape(t, db, dag2000)
	r := startScheduleRun(t, db, tenant, dag2000.schedule(), model.OperationRun)
	succeedWithAttempts(t, db, r.RunID())
	_, err := db.ExecContext(context.Background(), `ANALYZE tasks, task_attempts`)
	require.NoError(t, err)
	services, schemas, tables := make([]string, 10), make([]string, 10), make([]string, 10)
	for i := range 10 {
		services[i], schemas[i], tables[i] = "bench-svc", "bench", fmt.Sprintf("n%02d_0000", i)
	}

	scans := planScans(t, postgres.LastSuccessDurationsQueryForTest, pq.Array(services), pq.Array(schemas), pq.Array(tables), tenant)
	assert.Empty(t, seqScansOf(scans), "last success durations: %v", scans)
	assert.Subset(t, indexesOf(scans), []string{"idx_tasks_node_identity", "task_attempts_pkey"},
		"last success durations: %v", scans)
}
