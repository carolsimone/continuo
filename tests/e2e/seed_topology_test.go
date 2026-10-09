package e2e

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/google/uuid"
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

// e2eTopology returns the fixed e2e DAG as topology artifact nodes, plus the
// schedules it declares, sorted. Per-service image_tag is read from the
// release-controller service_prod table so the nodes carry the
// content-addressed tag the kind images actually have.
func e2eTopology(t *testing.T, ctx context.Context, clients *testClients) ([]topologyartifact.Node, []string) {
	t.Helper()

	// A prior blue/green test may have mutated service_prod; re-establish the
	// baseline pointers before reading each service's image tag.
	seedBaselineServiceProd(t, ctx, clients)

	imageTags := map[string]string{}
	for _, svc := range []string{"service-1", "service-2", "service-3"} {
		imageTags[svc] = readServiceImageTag(t, ctx, clients, svc)
	}

	scheduleSet := map[string]bool{}
	nodes := make([]topologyartifact.Node, 0, len(e2eDAG))
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
		nodes = append(nodes, topologyartifact.Node{
			UniqueID:           seedSchemaName + "." + n.table,
			SchemaName:         seedSchemaName,
			TableName:          n.table,
			ResolvedRelationID: seedSchemaName + "." + n.table,
			ServiceName:        n.service,
			NodeType:           nodeType,
			ImageTag:           imageTags[n.service],
			Schedule:           n.schedule,
			UpstreamUniqueIDs:  ups,
			TestCount:          e2eTestCounts[n.table],
		})
	}
	schedules := make([]string, 0, len(scheduleSet))
	for s := range scheduleSet {
		schedules = append(schedules, s)
	}
	sort.Strings(schedules)
	return nodes, schedules
}

// seedTopology installs the full e2e topology through release-controller's
// announce-topology: release-controller writes the artifact, takes the next
// promotion seq and queues release.promoted:v2, and the orchestrator applies it
// on the path a promoted release takes — Neo4j :Table/:DEPENDS_ON, :Meta and
// :TopologyRoot, schedules.loaded -> schedule_catalog. It skips validation, so
// it can seed any topology, including the intentionally failing ftable_* DAGs
// whose runs fail at execution time. It returns the announcement, whose
// promotion seq new runs carry.
//
// Seed *data* (e2e_schema.seed_table_*) is materialized separately by
// setup.sh's `dbt seed` step; this only establishes the topology graph.
func seedTopology(t *testing.T, ctx context.Context, clients *testClients) announceResult {
	t.Helper()
	nodes, schedules := e2eTopology(t, ctx, clients)
	res := announceTopology(t, ctx, "e2e-seed-"+uuid.NewString()[:8], nodes)
	waitForAnnouncedTopology(t, ctx, clients, res, schedules)
	return res
}
