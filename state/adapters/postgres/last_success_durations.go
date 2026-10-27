package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
)

// lastSuccessDurationsQuery reads, for each node identity of the arrays $1
// (service), $2 (schema) and $3 (table), the duration of the latest succeeded
// attempt with a positive duration, of a succeeded task of the tenant $4 that
// is not a test.
// Each identity is one probe of idx_tasks_node_identity and its tasks'
// attempts one probe each of task_attempts_pkey; a node without such an
// attempt yields no row. An inherited task records no attempt, so it never
// answers.
const lastSuccessDurationsQuery = `
	SELECT n.service_name, n.schema_name, n.table_name, last.execution_seconds
	FROM unnest($1::text[], $2::text[], $3::text[]) AS n(service_name, schema_name, table_name)
	CROSS JOIN LATERAL (
		SELECT a.execution_seconds
		FROM tasks t
		JOIN task_attempts a ON a.tenant_id = t.tenant_id AND a.task_id = t.task_id
		WHERE t.tenant_id = $4
		  AND t.service_name = n.service_name
		  AND t.schema_name = n.schema_name
		  AND t.table_name = n.table_name
		  AND t.status = 'succeeded'
		  AND t.operation <> 'test'
		  AND a.status = 'succeeded'
		  AND a.execution_seconds > 0
		ORDER BY a.finished_at DESC NULLS LAST
		LIMIT 1
	) AS last`

type lastSuccessRow struct {
	ServiceName string  `db:"service_name"`
	SchemaName  string  `db:"schema_name"`
	TableName   string  `db:"table_name"`
	Seconds     float64 `db:"execution_seconds"`
}

// LastSuccessDurations implements repository.RunReadRepository in one round
// trip.
func (r *RunReadRepository) LastSuccessDurations(ctx context.Context, tenantID string, nodes []run.NodeID) (map[run.NodeID]time.Duration, error) {
	out := make(map[run.NodeID]time.Duration, len(nodes))
	if len(nodes) == 0 {
		return out, nil
	}
	services := make([]string, 0, len(nodes))
	schemas := make([]string, 0, len(nodes))
	tables := make([]string, 0, len(nodes))
	for _, n := range nodes {
		services = append(services, n.ServiceName)
		schemas = append(schemas, n.SchemaName)
		tables = append(tables, n.TableName)
	}
	var rows []lastSuccessRow
	if err := r.db.SelectContext(ctx, &rows, lastSuccessDurationsQuery,
		pq.Array(services), pq.Array(schemas), pq.Array(tables), tenantID); err != nil {
		return nil, fmt.Errorf("read the last successful durations of %d nodes: %w", len(nodes), err)
	}
	for _, row := range rows {
		id := run.NodeID{ServiceName: row.ServiceName, SchemaName: row.SchemaName, TableName: row.TableName}
		out[id] = time.Duration(row.Seconds * float64(time.Second))
	}
	return out, nil
}
