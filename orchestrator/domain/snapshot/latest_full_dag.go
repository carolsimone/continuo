package snapshot

import (
	"context"
	"fmt"

	pkgEvents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// LatestFullDAG is the cron / trigger selector. It returns every active :Table
// for the schedule plus the upstream dbt-seed :Tables those depend on (which
// typically live under a different schedule_name like "seed"). Each projected
// task is PENDING and gets its (image_tag, manifest_version) from the :Table
// node — the canonical "latest topology" pinning at snapshot time.
//
// Replicates the task-discovery query from the legacy SnapshotGraph cron path,
// extended to also return per-:Table node_type / image_tag / manifest_version
// so the projection carries everything the materialiser + downstream events need.
type LatestFullDAG struct{}

func (LatestFullDAG) SelectTasks(ctx context.Context, tx neo4j.ManagedTransaction, p Params) ([]TaskProjection, error) {
	const query = `
		CALL {
		    MATCH (t:Table {schedule_name: $schedule_name})
		    WHERE COALESCE(t.active, true)
		    RETURN t.schema_name   AS schema_name,
		           t.table_name    AS table_name,
		           t.service_name  AS service_name,
		           t.schedule_name AS schedule_name,
		           COALESCE(t.node_type, 'dbt-model')      AS node_type,
		           COALESCE(t.image_tag, '')                AS image_tag,
		           COALESCE(t.manifest_version, '')         AS manifest_version

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
		           COALESCE(s.image_tag, '')        AS image_tag,
		           COALESCE(s.manifest_version, '') AS manifest_version
		}
		RETURN DISTINCT schema_name, table_name, service_name, schedule_name,
		                node_type, image_tag, manifest_version
	`
	result, err := tx.Run(ctx, query, map[string]interface{}{"schedule_name": p.ScheduleName})
	if err != nil {
		return nil, fmt.Errorf("LatestFullDAG: %w", err)
	}
	var projection []TaskProjection
	for result.Next(ctx) {
		rec := result.Record()
		projection = append(projection, TaskProjection{
			TaskID:          uuid.New(),
			ServiceName:     stringField(rec, "service_name"),
			SchemaName:      stringField(rec, "schema_name"),
			TableName:       stringField(rec, "table_name"),
			ScheduleName:    stringField(rec, "schedule_name"),
			NodeType:        stringField(rec, "node_type"),
			InitialStatus:   "PENDING",
			ImageTag:        stringField(rec, "image_tag"),
			ManifestVersion: stringField(rec, "manifest_version"),
			MaxRetries:      pkgEvents.DefaultTaskMaxRetries,
		})
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("LatestFullDAG result: %w", err)
	}
	return projection, nil
}

// stringField extracts a string-typed field from a Neo4j record, returning ""
// for nil or non-string values. Shared by all selectors in this package.
func stringField(rec *neo4j.Record, k string) string {
	if v, _ := rec.Get(k); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
