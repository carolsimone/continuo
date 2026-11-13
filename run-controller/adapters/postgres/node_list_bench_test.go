//go:build integration

package postgres_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
)

// nodeListShape is the history of a tenant whose live release holds a number
// of scheduled dbt models given by nodes, spread over nodeListServices
// services and nodeListSchedules schedules, plus one dormant dbt model per
// nodeListDormantEvery of them, which never ran. Every schedule ran
// runsPerNode times, and each run holds one run task per node of its
// schedule: one in twenty tasks failed, one in twenty needed a second
// attempt, and every task records its attempts.
type nodeListShape struct {
	name        string
	nodes       int
	runsPerNode int
}

const (
	nodeListServices     = 10
	nodeListSchedules    = 20
	nodeListDormantEvery = 50
)

// The shapes BenchmarkListNodes reads, from 27,500 tasks (A) to half a
// million (C and D): C and D hold the same tasks, D in four times as many
// nodes.
var nodeListShapes = []nodeListShape{
	{name: "A-2500x11", nodes: 2500, runsPerNode: 11},
	{name: "B-2500x50", nodes: 2500, runsPerNode: 50},
	{name: "C-2500x200", nodes: 2500, runsPerNode: 200},
	{name: "D-10000x50", nodes: 10000, runsPerNode: 50},
}

// BenchmarkListNodes measures the node list on each shape: "page" is the
// first page of every node, "badge" the one-row page that only reads the
// total, "node" the read of one node by its service and table name, "service"
// the first page of one service's nodes and "dormant" the first page of the
// dormant nodes. Each reports the median of its iterations as p50-ms.
//
//	go test -tags integration -run '^$' -bench BenchmarkListNodes -benchtime 10x ./adapters/postgres/
func BenchmarkListNodes(b *testing.B) {
	ctx := context.Background()
	db := newTestDB(b)
	for _, shape := range nodeListShapes {
		b.Run(shape.name, func(b *testing.B) {
			tenant := storeNodeListShape(b, db, shape)
			repo := postgres.NewNodeRunRepository(db, tenant, discardLogger())
			for _, read := range []struct {
				name            string
				search, service string
				dormantOnly     bool
				limit           int
			}{
				{name: "page", limit: 50},
				{name: "badge", limit: 1},
				{name: "node", search: "t_1003", service: "svc_3", limit: 50},
				{name: "service", service: "svc_3", limit: 50},
				{name: "dormant", dormantOnly: true, limit: 50},
			} {
				b.Run(read.name, func(b *testing.B) {
					took := make([]time.Duration, 0, b.N)
					b.ResetTimer()
					for range b.N {
						start := time.Now()
						rows, total, err := repo.ListNodes(ctx, read.search, read.service, model.OperationRun, read.dormantOnly, read.limit, 0)
						took = append(took, time.Since(start))
						require.NoError(b, err)
						require.NotEmpty(b, rows)
						require.Positive(b, total)
					}
					b.StopTimer()
					slices.Sort(took)
					b.ReportMetric(float64(took[len(took)/2].Microseconds())/1000, "p50-ms")
				})
			}
		})
	}
}

// storeNodeListShape stores shape as the live release and the history of a
// new tenant, analyzes the tables the node list reads and returns the tenant.
// The tenant's runs, tasks, attempts and releases are deleted when the
// benchmark ends.
func storeNodeListShape(b *testing.B, db *sqlx.DB, shape nodeListShape) string {
	b.Helper()
	ctx := context.Background()
	tenant := planningTenant(b, db)
	b.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM runs WHERE tenant_id = $1`, tenant)
	})
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO topology_releases (tenant_id, release_id, promotion_seq, topology_uri, topology_sha256)
		  VALUES ($1, 'rel-1', 1, 's3://bench/topology.json.gz', 'bench')`, []any{tenant}},
		{`INSERT INTO topology_nodes (tenant_id, release_id, node_key, unique_id, service_name, schema_name,
		                             table_name, node_type, schedule_name, timeout_seconds)
		  SELECT $1, 'rel-1', 'analytics.t_' || n, 'model.svc_' || (n % $3) || '.t_' || n, 'svc_' || (n % $3),
		         'analytics', 't_' || n, 'dbt-model', 'sched_' || (n % $4), CASE WHEN n % 3 = 0 THEN 600 END
		  FROM generate_series(0, $2::int - 1) AS n`,
			[]any{tenant, shape.nodes, nodeListServices, nodeListSchedules}},
		{`INSERT INTO topology_nodes (tenant_id, release_id, node_key, unique_id, service_name, schema_name,
		                             table_name, node_type)
		  SELECT $1, 'rel-1', 'analytics.d_' || n, 'model.svc_' || (n % $3) || '.d_' || n, 'svc_' || (n % $3),
		         'analytics', 'd_' || n, 'dbt-model'
		  FROM generate_series(0, $2::int / $4::int - 1) AS n`,
			[]any{tenant, shape.nodes, nodeListServices, nodeListDormantEvery}},
		{`INSERT INTO live_release (tenant_id, release_id, promotion_seq, updated_at) VALUES ($1, 'rel-1', 1, now())`,
			[]any{tenant}},
		{`INSERT INTO runs (run_id, tenant_id, schedule_name, status, created_at, started_at, completed_at,
		                   kind, operation, release_id, promotion_seq, total_task_count, terminal_task_count)
		  SELECT md5($1 || ':' || s || ':' || k)::uuid, $1, 'sched_' || s, 'succeeded', t.at,
		         t.at + interval '1 second', t.at + interval '10 minutes', 'cron', 'run', 'rel-1', 1, 0, 0
		  FROM generate_series(0, $2::int - 1) AS s, generate_series(0, $3::int - 1) AS k,
		       LATERAL (SELECT now() - make_interval(hours => k, mins => s % 60) AS at) t`,
			[]any{tenant, nodeListSchedules, shape.runsPerNode}},
		{`INSERT INTO tasks (task_id, run_id, tenant_id, created_at, service_name, schema_name, table_name,
		                    job_name, status, attempt, max_retries, operation, node_key, dispatched_at)
		  SELECT md5($1 || ':' || n || ':' || k)::uuid, md5($1 || ':' || (n % $4) || ':' || k)::uuid, $1, t.at,
		         'svc_' || (n % $3), 'analytics', 't_' || n, 'job-' || left(md5($1 || ':' || n || ':' || k), 20),
		         CASE WHEN (n + 7 * k) % 20 = 0 THEN 'failed' ELSE 'succeeded' END,
		         CASE WHEN (3 * n + k) % 20 = 1 THEN 1 ELSE 0 END, 2, 'run', 'analytics.t_' || n, t.at
		  FROM generate_series(0, $2::int - 1) AS n, generate_series(0, $5::int - 1) AS k,
		       LATERAL (SELECT now() - make_interval(hours => k, mins => (n % $4) % 60) AS at) t`,
			[]any{tenant, shape.nodes, nodeListServices, nodeListSchedules, shape.runsPerNode}},
		{`INSERT INTO task_attempts (task_id, tenant_id, attempt, job_name, status, dispatched_at, started_at, finished_at)
		  SELECT task_id, tenant_id, 0, job_name || '-0', CASE WHEN attempt = 1 THEN 'failed' ELSE status END,
		         created_at, created_at + interval '2 seconds',
		         created_at + make_interval(secs => 7 + (hashtext(task_id::text) & 255))
		  FROM tasks WHERE tenant_id = $1`, []any{tenant}},
		{`INSERT INTO task_attempts (task_id, tenant_id, attempt, job_name, status, dispatched_at, started_at, finished_at)
		  SELECT task_id, tenant_id, 1, job_name || '-1', status,
		         created_at + interval '1 minute', created_at + interval '62 seconds',
		         created_at + make_interval(secs => 67 + (hashtext(task_id::text) & 255))
		  FROM tasks WHERE tenant_id = $1 AND attempt = 1`, []any{tenant}},
	}
	for _, stmt := range statements {
		_, err := db.ExecContext(ctx, stmt.sql, stmt.args...)
		require.NoError(b, err)
	}
	_, err := db.ExecContext(ctx, `VACUUM ANALYZE runs, tasks, task_attempts, topology_nodes, live_release`)
	require.NoError(b, err)
	return tenant
}
