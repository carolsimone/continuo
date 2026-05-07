package neo4jinfra_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// neo4jURI returns the Neo4j bolt URI from the environment, defaulting to the
// docker-compose service name used in CI / local development.
func neo4jURI() string {
	if u := os.Getenv("NEO4J_URI"); u != "" {
		return u
	}
	return "bolt://neo4j:7687"
}

func neo4jUser() string {
	if u := os.Getenv("NEO4J_USER"); u != "" {
		return u
	}
	return "neo4j"
}

func neo4jPassword() string {
	if p := os.Getenv("NEO4J_PASSWORD"); p != "" {
		return p
	}
	return "atlas_password"
}

// newTestClient creates a Neo4j client for integration tests.
// Tests that call this will be skipped if Neo4j is unreachable.
func newTestClient(t *testing.T) neo4jinfra.Neo4jClient {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client, err := neo4jinfra.NewNeo4jClient(neo4jURI(), neo4jUser(), neo4jPassword(), logger)
	if err != nil {
		t.Skipf("Neo4j unavailable, skipping integration test: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}

// seedScheduleNodes creates a set of Table nodes in Neo4j for the given
// scheduleName and returns a cleanup function that removes them.
// Topology: nodeA → nodeB → nodeC  (DEPENDS_ON edges)
// nodeC is a dbt-seed so it gets included via the seed-path in SnapshotGraph.
func seedScheduleNodes(t *testing.T, ctx context.Context, client neo4jinfra.Neo4jClient, scheduleName string) func() {
	t.Helper()

	driver := client.(interface {
		NewSession(context.Context, neo4j.AccessMode) neo4j.SessionWithContext
	})
	session := driver.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	seed := fmt.Sprintf(`
		MERGE (a:Table {schema_name: 'test_schema', table_name: 'node_a_%s'})
		  ON CREATE SET a.service_name = 'svc1', a.schedule_name = $sched,
		                a.node_type = 'dbt-model', a.criticality = 'unspecified'
		  ON MATCH  SET a.schedule_name = $sched
		MERGE (b:Table {schema_name: 'test_schema', table_name: 'node_b_%s'})
		  ON CREATE SET b.service_name = 'svc1', b.schedule_name = $sched,
		                b.node_type = 'dbt-model', b.criticality = 'unspecified'
		  ON MATCH  SET b.schedule_name = $sched
		MERGE (c:Table {schema_name: 'test_schema', table_name: 'node_c_%s'})
		  ON CREATE SET c.service_name = 'svc1', c.schedule_name = $sched,
		                c.node_type = 'dbt-seed', c.criticality = 'unspecified'
		  ON MATCH  SET c.schedule_name = $sched
		MERGE (a)-[:DEPENDS_ON]->(b)
		MERGE (b)-[:DEPENDS_ON]->(c)
	`, scheduleName, scheduleName, scheduleName)

	result, err := session.Run(ctx, seed, map[string]interface{}{"sched": scheduleName})
	require.NoError(t, err)
	_, err = result.Consume(ctx)
	require.NoError(t, err)

	return func() {
		cleanSession := driver.NewSession(context.Background(), neo4j.AccessModeWrite)
		defer cleanSession.Close(context.Background())
		cleanup := fmt.Sprintf(`
			MATCH (n:Table)
			WHERE n.schedule_name = $sched
			   OR n.table_name IN ['node_a_%s','node_b_%s','node_c_%s']
			DETACH DELETE n
		`, scheduleName, scheduleName, scheduleName)
		r, _ := cleanSession.Run(context.Background(), cleanup, map[string]interface{}{"sched": scheduleName})
		if r != nil {
			_, _ = r.Consume(context.Background())
		}
		// Also clean up any Run nodes created for this test
		runClean := `MATCH (r:Run) WHERE r.schedule_name = $sched DETACH DELETE r`
		r2, _ := cleanSession.Run(context.Background(), runClean, map[string]interface{}{"sched": scheduleName})
		if r2 != nil {
			_, _ = r2.Consume(context.Background())
		}
	}
}

// isValidUUID returns true when s is a well-formed RFC 4122 UUID string.
func isValidUUID(s string) bool {
	_, err := uuid.Parse(s)
	return err == nil
}

// TestRunRepository_SnapshotGraph_ScopesExactlyToScheduleNodes verifies that when two
// Table nodes share (schema_name, table_name) but belong to different schedules,
// SnapshotGraph only creates EXECUTES edges for the target schedule's nodes.
//
// This guards against the two-phase snapshot bug: the listQuery filters by schedule, but
// the original mergeQuery matched only on (schema_name, table_name), so it would attach
// the run to nodes from other schedules sharing the same pair.
func TestRunRepository_SnapshotGraph_ScopesExactlyToScheduleNodes(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)

	uid := uuid.New().String()[:8]
	schedA := "sched_a_" + uid
	schedB := "sched_b_" + uid

	driver := client.(interface {
		NewSession(context.Context, neo4j.AccessMode) neo4j.SessionWithContext
	})

	// CREATE (not MERGE) to force two distinct Neo4j nodes with the same
	// (schema_name, table_name) but different service_name/schedule_name,
	// simulating a future topology where the MERGE key includes service_name.
	session := driver.NewSession(ctx, neo4j.AccessModeWrite)
	result, err := session.Run(ctx, `
		CREATE (:Table {
			schema_name:  'shared_schema',
			table_name:   'shared_table',
			service_name: 'svc_a',
			schedule_name: $sched_a,
			node_type:    'dbt-model',
			criticality:  'unspecified'
		})
		CREATE (:Table {
			schema_name:  'shared_schema',
			table_name:   'shared_table',
			service_name: 'svc_b',
			schedule_name: $sched_b,
			node_type:    'dbt-model',
			criticality:  'unspecified'
		})
	`, map[string]interface{}{"sched_a": schedA, "sched_b": schedB})
	require.NoError(t, err)
	_, err = result.Consume(ctx)
	require.NoError(t, err)
	session.Close(ctx)

	t.Cleanup(func() {
		cs := driver.NewSession(context.Background(), neo4j.AccessModeWrite)
		defer cs.Close(context.Background())
		r, _ := cs.Run(context.Background(), `
			MATCH (n:Table) WHERE n.schedule_name IN [$sa, $sb] DETACH DELETE n
		`, map[string]interface{}{"sa": schedA, "sb": schedB})
		if r != nil {
			_, _ = r.Consume(context.Background())
		}
		r2, _ := cs.Run(context.Background(), `
			MATCH (r:Run) WHERE r.schedule_name IN [$sa, $sb] DETACH DELETE r
		`, map[string]interface{}{"sa": schedA, "sb": schedB})
		if r2 != nil {
			_, _ = r2.Consume(context.Background())
		}
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	repo := neo4jinfra.NewRunRepository(client, logger)
	runID := uuid.New().String()

	require.NoError(t, repo.SnapshotGraph(ctx, runID, schedA, "cron", nil))

	// Count EXECUTES edges to nodes that do NOT belong to schedA — must be zero.
	sess2 := driver.NewSession(ctx, neo4j.AccessModeRead)
	defer sess2.Close(ctx)
	r2, err := sess2.Run(ctx, `
		MATCH (:Run {run_id: $run_id})-[:EXECUTES]->(n:Table)
		WHERE n.schedule_name <> $sched_a
		RETURN count(n) AS spurious_count
	`, map[string]interface{}{"run_id": runID, "sched_a": schedA})
	require.NoError(t, err)
	require.True(t, r2.Next(ctx))
	countRaw, _ := r2.Record().Get("spurious_count")
	spurious, _ := countRaw.(int64)
	assert.Equal(t, int64(0), spurious,
		"SnapshotGraph must not attach EXECUTES edges to nodes outside the target schedule")
}

// TestRunRepository_SnapshotGraph_IncludesCrossScheduleSeed verifies that a dbt-seed
// whose schedule_name differs from the snapshotted schedule is still included in the
// run (via the seed UNION branch) and receives a valid task_id on its EXECUTES edge.
//
// This guards against a partial-fix regression where schedule_name is added to the
// mergeQuery MATCH but not projected from listQuery / stored in assignments, causing
// the seed MATCH to filter on null and silently drop the seed.
func TestRunRepository_SnapshotGraph_IncludesCrossScheduleSeed(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)

	uid := uuid.New().String()[:8]
	mainSched := "main_sched_" + uid
	seedSched := "seed_sched_" + uid

	driver := client.(interface {
		NewSession(context.Context, neo4j.AccessMode) neo4j.SessionWithContext
	})
	session := driver.NewSession(ctx, neo4j.AccessModeWrite)
	result, err := session.Run(ctx, `
		MERGE (t:Table {schema_name: 'cs_schema', table_name: $t_table})
		  ON CREATE SET t.service_name = 'svc1', t.schedule_name = $main_sched,
		                t.node_type = 'dbt-model', t.criticality = 'unspecified'
		  ON MATCH  SET t.service_name = 'svc1', t.schedule_name = $main_sched
		MERGE (s:Table {schema_name: 'cs_schema', table_name: $s_table})
		  ON CREATE SET s.service_name = 'svc1', s.schedule_name = $seed_sched,
		                s.node_type = 'dbt-seed', s.criticality = 'unspecified'
		  ON MATCH  SET s.service_name = 'svc1', s.schedule_name = $seed_sched
		MERGE (t)-[:DEPENDS_ON]->(s)
	`, map[string]interface{}{
		"t_table":   "cs_model_" + uid,
		"s_table":   "cs_seed_" + uid,
		"main_sched": mainSched,
		"seed_sched": seedSched,
	})
	require.NoError(t, err)
	_, err = result.Consume(ctx)
	require.NoError(t, err)
	session.Close(ctx)

	t.Cleanup(func() {
		cs := driver.NewSession(context.Background(), neo4j.AccessModeWrite)
		defer cs.Close(context.Background())
		for _, q := range []string{
			`MATCH (n:Table) WHERE n.schedule_name IN [$a, $b] DETACH DELETE n`,
			`MATCH (r:Run) WHERE r.schedule_name IN [$a, $b] DETACH DELETE r`,
		} {
			r, _ := cs.Run(context.Background(), q, map[string]interface{}{"a": mainSched, "b": seedSched})
			if r != nil {
				_, _ = r.Consume(context.Background())
			}
		}
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	repo := neo4jinfra.NewRunRepository(client, logger)
	runID := uuid.New().String()

	require.NoError(t, repo.SnapshotGraph(ctx, runID, mainSched, "cron", nil))

	// The seed node (schedule_name=seedSched) must have an EXECUTES edge with a valid task_id.
	sess2 := driver.NewSession(ctx, neo4j.AccessModeRead)
	defer sess2.Close(ctx)
	r2, err := sess2.Run(ctx, `
		MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(n:Table {node_type: 'dbt-seed', schedule_name: $seed_sched})
		RETURN COALESCE(e.task_id, '') AS task_id
	`, map[string]interface{}{"run_id": runID, "seed_sched": seedSched})
	require.NoError(t, err)
	require.True(t, r2.Next(ctx), "cross-schedule seed must have an EXECUTES edge in the run")
	taskIDRaw, _ := r2.Record().Get("task_id")
	assert.True(t, isValidUUID(safeStringTest(taskIDRaw)),
		"cross-schedule seed must have a valid UUID task_id on its EXECUTES edge, got %q", taskIDRaw)
}

// TestRunRepository_SnapshotGraph_NoSpuriousEdgesToDuplicateSeeds verifies that when
// two services each have a dbt-seed with the same (schema_name, table_name), snapshotting
// one service's schedule creates an EXECUTES edge only to that service's seed node, not to
// the other service's seed.
//
// This extends ScopesExactlyToScheduleNodes to the seed branch of the listQuery.
func TestRunRepository_SnapshotGraph_NoSpuriousEdgesToDuplicateSeeds(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)

	uid := uuid.New().String()[:8]
	mainSched := "dseed_main_" + uid
	otherSched := "dseed_other_" + uid
	seedSchedA := "dseed_sa_" + uid
	seedSchedB := "dseed_sb_" + uid

	driver := client.(interface {
		NewSession(context.Context, neo4j.AccessMode) neo4j.SessionWithContext
	})

	// model_a (mainSched) -[:DEPENDS_ON]-> seed_shared (svc_a, seedSchedA)
	// model_b (otherSched) -[:DEPENDS_ON]-> seed_shared (svc_b, seedSchedB) — duplicate (schema, table)
	session := driver.NewSession(ctx, neo4j.AccessModeWrite)
	result, err := session.Run(ctx, `
		MERGE (ma:Table {schema_name: 'ds_schema', table_name: $ma_table})
		  ON CREATE SET ma.service_name = 'svc_a', ma.schedule_name = $main_sched,
		                ma.node_type = 'dbt-model', ma.criticality = 'unspecified'
		  ON MATCH  SET ma.service_name = 'svc_a', ma.schedule_name = $main_sched
		MERGE (mb:Table {schema_name: 'ds_schema', table_name: $mb_table})
		  ON CREATE SET mb.service_name = 'svc_b', mb.schedule_name = $other_sched,
		                mb.node_type = 'dbt-model', mb.criticality = 'unspecified'
		  ON MATCH  SET mb.service_name = 'svc_b', mb.schedule_name = $other_sched
		CREATE (sa:Table {schema_name: 'ds_schema', table_name: 'ds_shared_seed_' + $uid,
		                  service_name: 'svc_a', schedule_name: $seed_sched_a,
		                  node_type: 'dbt-seed', criticality: 'unspecified'})
		CREATE (sb:Table {schema_name: 'ds_schema', table_name: 'ds_shared_seed_' + $uid,
		                  service_name: 'svc_b', schedule_name: $seed_sched_b,
		                  node_type: 'dbt-seed', criticality: 'unspecified'})
		MERGE (ma)-[:DEPENDS_ON]->(sa)
		MERGE (mb)-[:DEPENDS_ON]->(sb)
	`, map[string]interface{}{
		"ma_table":    "ds_model_a_" + uid,
		"mb_table":    "ds_model_b_" + uid,
		"uid":         uid,
		"main_sched":  mainSched,
		"other_sched": otherSched,
		"seed_sched_a": seedSchedA,
		"seed_sched_b": seedSchedB,
	})
	require.NoError(t, err)
	_, err = result.Consume(ctx)
	require.NoError(t, err)
	session.Close(ctx)

	t.Cleanup(func() {
		cs := driver.NewSession(context.Background(), neo4j.AccessModeWrite)
		defer cs.Close(context.Background())
		for _, q := range []string{
			`MATCH (n:Table) WHERE n.schedule_name IN [$a, $b, $c, $d] DETACH DELETE n`,
			`MATCH (r:Run)   WHERE r.schedule_name IN [$a, $b, $c, $d] DETACH DELETE r`,
		} {
			r, _ := cs.Run(context.Background(), q, map[string]interface{}{
				"a": mainSched, "b": otherSched, "c": seedSchedA, "d": seedSchedB,
			})
			if r != nil {
				_, _ = r.Consume(context.Background())
			}
		}
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	repo := neo4jinfra.NewRunRepository(client, logger)
	runID := uuid.New().String()

	require.NoError(t, repo.SnapshotGraph(ctx, runID, mainSched, "cron", nil))

	sess2 := driver.NewSession(ctx, neo4j.AccessModeRead)
	defer sess2.Close(ctx)

	// svc_b's seed must NOT have an EXECUTES edge from this run.
	r2, err := sess2.Run(ctx, `
		MATCH (:Run {run_id: $run_id})-[:EXECUTES]->(n:Table {service_name: 'svc_b'})
		RETURN count(n) AS spurious_count
	`, map[string]interface{}{"run_id": runID})
	require.NoError(t, err)
	require.True(t, r2.Next(ctx))
	countRaw, _ := r2.Record().Get("spurious_count")
	spurious, _ := countRaw.(int64)
	assert.Equal(t, int64(0), spurious,
		"SnapshotGraph must not create EXECUTES edges to seeds belonging to another service")
}

// safeStringTest mirrors safeString for use in test assertions.
func safeStringTest(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// ── Helpers for SnapshotSingleNodeRun tests ──────────────────────────────────

// singleNodeRunFixture bundles the repo + raw client so test helpers can do
// direct Neo4j queries without going through the RunRepository API.
type singleNodeRunFixture struct {
	repo   *neo4jinfra.RunRepository
	client neo4jinfra.Neo4jClient
}

// newRunRepoWithNeo4j creates a RunRepository + raw client for integration tests
// and returns a cleanup function.
func newRunRepoWithNeo4j(t *testing.T) (*singleNodeRunFixture, func()) {
	t.Helper()
	client := newTestClient(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	repo := neo4jinfra.NewRunRepository(client, logger)
	cleanup := func() { _ = client.Close(context.Background()) }
	return &singleNodeRunFixture{repo: repo, client: client}, cleanup
}

// seedTopologyRoot upserts the :TopologyRoot singleton with given generation and metadata.
func seedTopologyRoot(t *testing.T, fx *singleNodeRunFixture, topologyGen int64, serviceMetadata string) {
	t.Helper()
	ctx := context.Background()
	session := fx.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)
	_, err := session.Run(ctx, `
		MERGE (root:TopologyRoot {id: 'singleton'})
		SET root.topology_generation = $gen,
		    root.service_metadata    = $meta
	`, map[string]interface{}{"gen": topologyGen, "meta": serviceMetadata})
	require.NoError(t, err)
	t.Cleanup(func() {
		s := fx.client.NewSession(context.Background(), neo4j.AccessModeWrite)
		defer s.Close(context.Background())
		r, _ := s.Run(context.Background(), `
			MATCH (root:TopologyRoot {id: 'singleton'})
			REMOVE root.topology_generation, root.service_metadata
		`, nil)
		if r != nil {
			_, _ = r.Consume(context.Background())
		}
	})
}

// seedTableNode upserts a :Table node with the given identity + metadata props.
func seedTableNode(t *testing.T, fx *singleNodeRunFixture, serviceName, schemaName, tableName, nodeType, imageTag, manifestVersion string) {
	t.Helper()
	ctx := context.Background()
	session := fx.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)
	_, err := session.Run(ctx, `
		MERGE (t:Table {service_name: $svc, schema_name: $schema, table_name: $table})
		SET t.node_type        = $node_type,
		    t.image_tag        = $image_tag,
		    t.manifest_version = $manifest_version,
		    t.active           = true
	`, map[string]interface{}{
		"svc":              serviceName,
		"schema":           schemaName,
		"table":            tableName,
		"node_type":        nodeType,
		"image_tag":        imageTag,
		"manifest_version": manifestVersion,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		s := fx.client.NewSession(context.Background(), neo4j.AccessModeWrite)
		defer s.Close(context.Background())
		r, _ := s.Run(context.Background(), `
			MATCH (t:Table {service_name: $svc, schema_name: $schema, table_name: $table})
			DETACH DELETE t
		`, map[string]interface{}{"svc": serviceName, "schema": schemaName, "table": tableName})
		if r != nil {
			_, _ = r.Consume(context.Background())
		}
	})
}

// readRunProps returns a map of :Run node properties for the given runID and
// registers a cleanup to delete the :Run node.
func readRunProps(t *testing.T, fx *singleNodeRunFixture, runID string) map[string]interface{} {
	t.Helper()
	ctx := context.Background()
	session := fx.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)
	result, err := session.Run(ctx, `
		MATCH (r:Run {run_id: $run_id})
		RETURN r.kind               AS kind,
		       r.topology_generation AS topology_generation,
		       r.source_run_id      AS source_run_id,
		       r.schedule_name      AS schedule_name,
		       r.service_metadata   AS service_metadata
	`, map[string]interface{}{"run_id": runID})
	require.NoError(t, err)
	require.True(t, result.Next(ctx), "expected :Run node for run_id=%s", runID)
	rec := result.Record()
	props := map[string]interface{}{}
	for _, key := range []string{"kind", "topology_generation", "source_run_id", "schedule_name", "service_metadata"} {
		v, _ := rec.Get(key)
		props[key] = v
	}
	t.Cleanup(func() {
		s := fx.client.NewSession(context.Background(), neo4j.AccessModeWrite)
		defer s.Close(context.Background())
		r, _ := s.Run(context.Background(), `
			MATCH (r:Run {run_id: $run_id}) DETACH DELETE r
		`, map[string]interface{}{"run_id": runID})
		if r != nil {
			_, _ = r.Consume(context.Background())
		}
	})
	return props
}

// readExecutesEdges returns all :EXECUTES edge property maps for a given runID.
func readExecutesEdges(t *testing.T, fx *singleNodeRunFixture, runID string) []map[string]interface{} {
	t.Helper()
	ctx := context.Background()
	session := fx.client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)
	result, err := session.Run(ctx, `
		MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(:Table)
		RETURN e.task_id          AS task_id,
		       e.image_tag        AS image_tag,
		       e.manifest_version AS manifest_version,
		       e.status           AS status
	`, map[string]interface{}{"run_id": runID})
	require.NoError(t, err)
	var edges []map[string]interface{}
	for result.Next(ctx) {
		rec := result.Record()
		edge := map[string]interface{}{}
		for _, key := range []string{"task_id", "image_tag", "manifest_version", "status"} {
			v, _ := rec.Get(key)
			edge[key] = v
		}
		edges = append(edges, edge)
	}
	require.NoError(t, result.Err())
	return edges
}

func TestRunRepository_SnapshotGraph_StampsKindAndSourceRunID(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	repo := neo4jinfra.NewRunRepository(client, slog.New(slog.NewTextHandler(os.Stdout, nil)))
	runID := uuid.New().String()
	scheduleName := "snap-kind-" + uuid.New().String()[:8]
	sourceRunID := uuid.New()

	cleanup := seedScheduleNodes(t, ctx, client, scheduleName)
	t.Cleanup(cleanup)

	require.NoError(t, repo.SnapshotGraph(
		ctx, runID, scheduleName, "rerun", &sourceRunID,
	))

	session := client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)
	result, err := session.Run(ctx,
		`MATCH (r:Run {run_id: $run_id}) RETURN r.kind AS kind, r.source_run_id AS source_run_id`,
		map[string]interface{}{"run_id": runID})
	require.NoError(t, err)
	require.True(t, result.Next(ctx))
	rec := result.Record()
	kindRaw, _ := rec.Get("kind")
	sourceRaw, _ := rec.Get("source_run_id")
	assert.Equal(t, "rerun", kindRaw)
	assert.Equal(t, sourceRunID.String(), sourceRaw)
}

func TestRunRepository_SnapshotGraph_StampsKindButOmitsNilSourceRunID(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	repo := neo4jinfra.NewRunRepository(client, slog.New(slog.NewTextHandler(os.Stdout, nil)))
	runID := uuid.New().String()
	scheduleName := "snap-nilsrc-" + uuid.New().String()[:8]

	cleanup := seedScheduleNodes(t, ctx, client, scheduleName)
	t.Cleanup(cleanup)

	require.NoError(t, repo.SnapshotGraph(
		ctx, runID, scheduleName, "cron", nil,
	))

	session := client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)
	result, err := session.Run(ctx,
		`MATCH (r:Run {run_id: $run_id}) RETURN r.kind AS kind, r.source_run_id AS source_run_id`,
		map[string]interface{}{"run_id": runID})
	require.NoError(t, err)
	require.True(t, result.Next(ctx))
	rec := result.Record()
	kindRaw, _ := rec.Get("kind")
	sourceRaw, _ := rec.Get("source_run_id")
	assert.Equal(t, "cron", kindRaw)
	assert.Nil(t, sourceRaw, "source_run_id property absent on the :Run node")
}

func TestSnapshotGraph_StampsGenerationAndServiceMetadataAndEdgeImageTag(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)

	scheduleName := "test_gen_sched_" + uuid.New().String()[:8]
	tableNameA := "test_gen_table_" + uuid.New().String()[:8]

	// Seed a Table node with image_tag and manifest_version, plus the :TopologyRoot.
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	_, err := session.Run(ctx, `
		MERGE (root:TopologyRoot {id: 'singleton'})
		SET root.topology_generation = 7,
		    root.service_metadata = '{"svc-a":{"manifest_version":"v3","image_tag":"abc123"}}'

		WITH root

		MERGE (t:Table {schema_name: 'public', table_name: $table_name, service_name: 'svc-a', schedule_name: $schedule_name})
		SET t.active = true,
		    t.manifest_version = 'v3',
		    t.image_tag = 'abc123',
		    t.topology_generation = 7,
		    t.node_type = 'dbt-model',
		    t.owner = 'team-a',
		    t.criticality = 'CORE'
	`, map[string]interface{}{
		"table_name":    tableNameA,
		"schedule_name": scheduleName,
	})
	require.NoError(t, err)
	session.Close(ctx)

	t.Cleanup(func() {
		s := client.NewSession(context.Background(), neo4j.AccessModeWrite)
		defer s.Close(context.Background())
		_, _ = s.Run(context.Background(), `
			MATCH (t:Table {table_name: $table_name}) DETACH DELETE t
		`, map[string]interface{}{"table_name": tableNameA})
		_, _ = s.Run(context.Background(), `
			MATCH (r:Run {schedule_name: $sched}) DETACH DELETE r
		`, map[string]interface{}{"sched": scheduleName})
		_, _ = s.Run(context.Background(), `
			MATCH (root:TopologyRoot {id: 'singleton'})
			REMOVE root.topology_generation, root.service_metadata
		`, nil)
	})

	repo := neo4jinfra.NewRunRepository(client, slog.Default())
	runID := uuid.New().String()
	require.NoError(t, repo.SnapshotGraph(ctx, runID, scheduleName, "cron", nil))

	readSession := client.NewSession(ctx, neo4j.AccessModeRead)
	defer readSession.Close(ctx)

	// Assert Run node has topology_generation + service_metadata.
	res, err := readSession.Run(ctx, `
		MATCH (r:Run {run_id: $run_id})
		RETURN r.topology_generation AS gen, r.service_metadata AS sm
	`, map[string]interface{}{"run_id": runID})
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	gen, _ := res.Record().Get("gen")
	assert.Equal(t, int64(7), gen.(int64))
	sm, _ := res.Record().Get("sm")
	assert.Contains(t, sm.(string), `"image_tag":"abc123"`)

	// Assert EXECUTES edge has image_tag stamped.
	res2, err := readSession.Run(ctx, `
		MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(:Table {table_name: $table_name})
		RETURN e.image_tag AS image_tag, e.manifest_version AS mv
	`, map[string]interface{}{
		"run_id":     runID,
		"table_name": tableNameA,
	})
	require.NoError(t, err)
	require.True(t, res2.Next(ctx))
	edgeTag, _ := res2.Record().Get("image_tag")
	edgeMV, _ := res2.Record().Get("mv")
	assert.Equal(t, "abc123", edgeTag)
	assert.Equal(t, "v3", edgeMV)
}

// TestRunRepository_SnapshotGraph_AssignsTaskUUIDs verifies that:
//  1. After SnapshotGraph, every node returned by GetScheduleInitNodes has a
//     non-empty, valid UUID in TaskID.
//  2. Re-calling SnapshotGraph (idempotent replay) does NOT change existing
//     task_id values on the EXECUTES edges.
func TestRunRepository_SnapshotGraph_AssignsTaskUUIDs(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)

	scheduleName := "test_uuid_" + uuid.New().String()[:8]
	cleanup := seedScheduleNodes(t, ctx, client, scheduleName)
	t.Cleanup(cleanup)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	repo := neo4jinfra.NewRunRepository(client, logger)

	runID := uuid.New().String()

	// ── First snapshot ────────────────────────────────────────────────────────
	require.NoError(t, repo.SnapshotGraph(ctx, runID, scheduleName, "cron", nil))

	initNodes, err := repo.GetScheduleInitNodes(ctx, scheduleName, runID)
	require.NoError(t, err)

	// We seeded 3 nodes: node_a (model), node_b (model), node_c (seed).
	// AllNodes must include all three.
	require.NotEmpty(t, initNodes.AllNodes, "AllNodes must not be empty after snapshot")

	// Collect task_ids from first snapshot.
	taskIDsByTable := make(map[string]string)
	for _, n := range initNodes.AllNodes {
		assert.NotEmpty(t, n.TaskID,
			"node %s.%s must have a non-empty TaskID after snapshot", n.SchemaName, n.TableName)
		assert.True(t, isValidUUID(n.TaskID),
			"TaskID for %s.%s must be a valid UUID, got %q", n.SchemaName, n.TableName, n.TaskID)
		taskIDsByTable[n.TableName] = n.TaskID
	}

	// RootNodes must also carry TaskIDs.
	for _, n := range initNodes.RootNodes {
		assert.NotEmpty(t, n.TaskID,
			"root node %s.%s must have a non-empty TaskID", n.SchemaName, n.TableName)
		assert.True(t, isValidUUID(n.TaskID),
			"TaskID for root node %s.%s must be a valid UUID, got %q", n.SchemaName, n.TableName, n.TaskID)
	}

	// SeedNodes must also carry TaskIDs.
	for _, n := range initNodes.SeedNodes {
		assert.NotEmpty(t, n.TaskID,
			"seed node %s.%s must have a non-empty TaskID", n.SchemaName, n.TableName)
		assert.True(t, isValidUUID(n.TaskID),
			"TaskID for seed node %s.%s must be a valid UUID, got %q", n.SchemaName, n.TableName, n.TaskID)
	}

	// ── Second snapshot (idempotent) ──────────────────────────────────────────
	require.NoError(t, repo.SnapshotGraph(ctx, runID, scheduleName, "cron", nil),
		"second SnapshotGraph call (same runID) must succeed")

	initNodes2, err := repo.GetScheduleInitNodes(ctx, scheduleName, runID)
	require.NoError(t, err)
	require.Equal(t, len(initNodes.AllNodes), len(initNodes2.AllNodes),
		"node count must not change after re-snapshot")

	for _, n := range initNodes2.AllNodes {
		original, ok := taskIDsByTable[n.TableName]
		require.True(t, ok, "unexpected node %s in second snapshot", n.TableName)
		assert.Equal(t, original, n.TaskID,
			"TaskID for %s.%s must be stable across re-snapshots", n.SchemaName, n.TableName)
	}
}

// TestSnapshotSingleNodeRun_Latest verifies the latest-mode Cypher:
//   - Reads :TopologyRoot for topology_generation + service_metadata
//   - Creates :Run with kind="single_node_run", no source_run_id
//   - Creates :EXECUTES edge with image_tag/manifest_version from :Table
//   - Returns task_id, image_tag, manifest_version, node_type
func TestSnapshotSingleNodeRun_Latest(t *testing.T) {
	ctx := context.Background()
	fx, cleanup := newRunRepoWithNeo4j(t)
	defer cleanup()

	// Seed :TopologyRoot + :Table for "svcA.public.users".
	seedTopologyRoot(t, fx, /*topologyGen=*/ 7, /*serviceMetadata=*/ `{"svcA":{"image_tag":"v3","manifest_version":"m3"}}`)
	seedTableNode(t, fx, "svcA", "public", "users", "dbt-model", "v3", "m3")

	runID := uuid.New().String()
	scheduleName := "single-node-run-" + runID[:8]

	taskID, imageTag, manifestVersion, nodeType, err := fx.repo.SnapshotSingleNodeRun(
		ctx, runID, scheduleName, nil,
		"svcA", "public", "users",
		"latest",
	)
	require.NoError(t, err)
	require.NotEmpty(t, taskID, "task_id must be non-empty")
	assert.True(t, isValidUUID(taskID), "task_id must be a valid UUID, got %q", taskID)
	assert.Equal(t, "v3", imageTag)
	assert.Equal(t, "m3", manifestVersion)
	assert.Equal(t, "dbt-model", nodeType)

	// :Run exists with topology_generation=7, kind=single_node_run, no source_run_id.
	props := readRunProps(t, fx, runID)
	assert.Equal(t, "single_node_run", props["kind"])
	assert.Equal(t, int64(7), props["topology_generation"])
	assert.Nil(t, props["source_run_id"], "source_run_id must be absent in latest mode")

	// One :EXECUTES edge with image_tag=v3, manifest_version=m3, task_id matching.
	edges := readExecutesEdges(t, fx, runID)
	require.Len(t, edges, 1)
	assert.Equal(t, "v3", edges[0]["image_tag"])
	assert.Equal(t, "m3", edges[0]["manifest_version"])
	assert.Equal(t, taskID, edges[0]["task_id"])
	assert.Equal(t, "PENDING", edges[0]["status"])
}
