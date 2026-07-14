package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// topoNode is one node of the fixed e2e DAG seeded into the topology.
type topoNode struct {
	table    string   // table_name; unique_id is "e2e_schema."+table
	service  string   // service-1|2|3
	schedule string   // schedule_name (tag)
	upstream []string // upstream table names (same schema)
}

// e2eDAG is the full topology the legacy graph-load path used to ingest. It is
// the single source of truth for e2e topology now that the ingest path is gone.
var e2eDAG = []topoNode{
	{"seed_table_1", "service-1", "seed", nil},
	{"seed_table_2", "service-1", "seed", nil},
	{"seed_table_3", "service-1", "seed", nil},
	{"table_a", "service-1", "e2e-schedule", []string{"seed_table_1"}},
	{"table_b", "service-1", "e2e-schedule", []string{"seed_table_2"}},
	{"table_c", "service-1", "e2e-schedule", []string{"seed_table_3"}},
	{"table_d", "service-3", "e2e-schedule", []string{"table_a", "table_b"}},
	{"table_e", "service-3", "e2e-schedule", []string{"table_b", "table_c"}},
	{"table_f", "service-3", "e2e-schedule", []string{"table_a", "table_c"}},
	{"table_g", "service-2", "e2e-schedule", []string{"table_d", "table_e"}},
	{"table_h", "service-2", "e2e-schedule", []string{"table_e", "table_f"}},
	{"table_i", "service-3", "e2e-schedule", []string{"table_g", "table_h"}},
	{"table_j", "service-3", "e2e-schedule", []string{"table_g", "table_h"}},
	{"ftable_a", "service-1", "e2e-schedule-failure", nil},
	{"ftable_b", "service-1", "e2e-schedule-failure", nil},
	{"ftable_c", "service-3", "e2e-schedule-failure", []string{"ftable_a", "ftable_b"}},
	{"ftable_d", "service-2", "e2e-schedule-failure", []string{"ftable_c"}},
	{"ftable_e", "service-2", "e2e-schedule-failure", []string{"ftable_c"}},
	{"ftable_f", "service-3", "e2e-schedule-failure", []string{"ftable_d", "ftable_e"}},
	{"ftable_g", "service-3", "e2e-schedule-failure", []string{"ftable_a"}},
	{"ftable_h", "service-2", "e2e-schedule-failure", []string{"ftable_g"}},
	{"rel_probe", "service-1", "rel-probe", nil},
}

const seedSchemaName = "e2e_schema"

// e2eTestCounts assigns a per-node dbt test count to selected nodes so the
// test-operation flows have topology to exercise: seed_table_1 and seed_table_3
// carry tests (test-operation runs dispatch them), while seed_table_2 has none
// (a single-node test on it is gated as no_tests). Nodes absent from this map
// default to 0, which is correct for every non-test run since test_count is
// only consulted for operation=test.
var e2eTestCounts = map[string]int{
	"seed_table_1": 2,
	"seed_table_3": 1,
}

// promotedNode mirrors orchestrator/domain/event.ReleasePromotedNode (the
// release.promoted:v1 topology element).
type promotedNode struct {
	UniqueID          string   `json:"unique_id"`
	SchemaName        string   `json:"schema_name"`
	TableName         string   `json:"table_name"`
	ServiceName       string   `json:"service_name"`
	NodeType          string   `json:"node_type"`
	ImageTag          string   `json:"image_tag"`
	Schedule          string   `json:"schedule"`
	UpstreamUniqueIDs []string `json:"upstream_unique_ids"`
	TestCount         int      `json:"test_count"`
}

// seedTopology installs the full e2e topology by publishing a release.promoted:v1
// event and waiting for the orchestrator to apply it. This reuses the production
// promotion path (the same handler a real release drives), so it establishes the
// complete topology state — Neo4j :Table/:DEPENDS_ON, the topology_generation +
// :TopologyRoot, the :Meta current_release pointer, and schedules.loaded ->
// schedule_catalog — without re-implementing any of it. It bypasses validation
// (release.promoted is post-validation), so it can seed any topology, including
// the intentionally-failing ftable_* DAGs whose runs fail at execution time.
//
// Per-service image_tag is read from the release-controller service_prod table
// so the seeded nodes carry the content-addressed tag the kind images actually
// have. Seed *data* (e2e_schema.seed_table_*) is materialized separately by
// setup.sh's `dbt seed` step; this only establishes the topology graph.
func seedTopology(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()

	// A prior blue/green test may have mutated service_prod; re-establish the
	// baseline pointers before reading each service's image tag.
	seedBaselineServiceProd(t, ctx, clients)

	imageTags := map[string]string{}
	for _, svc := range []string{"service-1", "service-2", "service-3"} {
		imageTags[svc] = readServiceImageTag(t, ctx, clients, svc)
	}

	scheduleSet := map[string]bool{}
	topology := make([]promotedNode, 0, len(e2eDAG))
	for _, n := range e2eDAG {
		scheduleSet[n.schedule] = true
		ups := make([]string, 0, len(n.upstream))
		for _, u := range n.upstream {
			ups = append(ups, seedSchemaName+"."+u)
		}
		// node_type drives the run reader: seed upstreams are pulled into a
		// model's run only when typed "dbt-seed". The e2e project's only seeds
		// are the seed_table_* files; everything else is a dbt model.
		nodeType := "dbt-model"
		if strings.HasPrefix(n.table, "seed_table") {
			nodeType = "dbt-seed"
		}
		topology = append(topology, promotedNode{
			UniqueID:          seedSchemaName + "." + n.table,
			SchemaName:        seedSchemaName,
			TableName:         n.table,
			ServiceName:       n.service,
			NodeType:          nodeType,
			ImageTag:          imageTags[n.service],
			Schedule:          n.schedule,
			UpstreamUniqueIDs: ups,
			TestCount:         e2eTestCounts[n.table],
		})
	}

	releaseID := "e2e-seed-" + uuid.NewString()[:8]
	payload, err := json.Marshal(map[string]any{
		"release_id": releaseID,
		"topology":   topology,
		"image_tags": imageTags,
	})
	require.NoError(t, err, "marshal release.promoted payload")

	require.NoError(t, clients.redisClient.XAdd(ctx, &goredis.XAddArgs{
		Stream: streams.ReleasePromotedV1,
		Values: map[string]any{"payload": string(payload)},
	}).Err(), "publish release.promoted:v1")

	// Wait until the orchestrator has applied the promotion: the Neo4j :Meta
	// current_release pointer flips to this release, and the schedule_catalog
	// reflects every seeded schedule (so the tests' ActivateSchedule/Trigger
	// calls find their schedules). Polling both proves the swap committed and
	// schedules.loaded propagated to the state service.
	wantSchedules := len(scheduleSet)
	pollUntil(t, ctx, 60*time.Second, 1*time.Second, func() (bool, error) {
		if neo4jScalarString(ctx, clients,
			`MATCH (m:Meta {key: 'current_release'}) RETURN m.release_id AS v`, nil) != releaseID {
			return false, nil
		}
		var count int
		if err := clients.stateDB.QueryRowContext(ctx,
			`SELECT COUNT(DISTINCT schedule_name) FROM schedule_catalog WHERE removed_at IS NULL`,
		).Scan(&count); err != nil {
			return false, nil
		}
		return count >= wantSchedules, nil
	}, fmt.Sprintf("timeout waiting for orchestrator to apply seeded topology (release %s)", releaseID))
}
