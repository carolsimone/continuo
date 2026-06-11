package neo4jinfra_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	domainRun "github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Integration tests for RunAggregateRepository against the docker-compose
// Neo4j. Mirrors the env-var conventions in test_helpers_test.go: tests skip
// when Neo4j is unreachable. Each fixture is marked with t.Name() so cleanup
// is per-test and parallel-safe.

type seededNode struct {
	schema  string
	table   string
	service string
	status  string
}

type seededEdge struct {
	childSchema  string
	childTable   string
	parentSchema string
	parentTable  string
}

// newTestAggRepo builds a RunAggregateRepository against the live Neo4j and
// returns it together with the raw client (for fixtures) and a cleanup func.
func newTestAggRepo(t *testing.T) (*neo4jinfra.RunAggregateRepository, neo4jinfra.Neo4jClient, func()) {
	t.Helper()
	client := newTestClient(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	repo := neo4jinfra.NewRunAggregateRepository(client, logger)
	marker := t.Name()
	cleanup := func() {
		ctx := context.Background()
		session := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer session.Close(ctx)
		_, _ = session.Run(ctx,
			"MATCH (n) WHERE n.test_marker = $m DETACH DELETE n",
			map[string]any{"m": marker},
		)
	}
	cleanup() // start clean in case a prior run aborted mid-test
	return repo, client, cleanup
}

// seedRun creates a :Run with aggregate counters, the table nodes, the
// EXECUTES edges, and the DEPENDS_ON edges between tables. Every node is
// tagged with test_marker = t.Name() for cleanup.
func seedRun(
	t *testing.T,
	ctx context.Context,
	client neo4jinfra.Neo4jClient,
	runID string,
	totalNodes int,
	version int,
	terminalCount int,
	nodes []seededNode,
	edges []seededEdge,
) {
	t.Helper()
	marker := t.Name()
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	_, err := session.Run(ctx, `
		CREATE (run:Run {
			run_id:         $run_id,
			schedule_name:  'sched',
			total_nodes:    $total_nodes,
			terminal_count: $terminal_count,
			version:        $version,
			test_marker:    $marker
		})
	`, map[string]any{
		"run_id":         runID,
		"total_nodes":    totalNodes,
		"terminal_count": terminalCount,
		"version":        version,
		"marker":         marker,
	})
	require.NoError(t, err)

	for _, n := range nodes {
		taskID := uuid.New().String()
		_, err := session.Run(ctx, `
			MATCH (run:Run {run_id: $run_id})
			CREATE (t:Table {
				schema_name:   $schema_name,
				table_name:    $table_name,
				service_name:  $service_name,
				node_type:     'model',
				schedule_name: 'sched',
				test_marker:   $marker
			})
			CREATE (run)-[:EXECUTES {
				task_id:          $task_id,
				status:           $status,
				manifest_version: 'mv1',
				image_tag:        'it1'
			}]->(t)
		`, map[string]any{
			"run_id":       runID,
			"schema_name":  n.schema,
			"table_name":   n.table,
			"service_name": n.service,
			"status":       n.status,
			"task_id":      taskID,
			"marker":       marker,
		})
		require.NoError(t, err)
	}

	for _, e := range edges {
		_, err := session.Run(ctx, `
			MATCH (child:Table {schema_name: $cs, table_name: $ct, test_marker: $marker})
			MATCH (parent:Table {schema_name: $ps, table_name: $pt, test_marker: $marker})
			MERGE (child)-[:DEPENDS_ON]->(parent)
		`, map[string]any{
			"cs":     e.childSchema,
			"ct":     e.childTable,
			"ps":     e.parentSchema,
			"pt":     e.parentTable,
			"marker": marker,
		})
		require.NoError(t, err)
	}
}

func nodeByKey(agg *domainRun.Run, k domainRun.NodeKey) *domainRun.RunNode {
	for _, n := range agg.Nodes() {
		if n.Key == k {
			return n
		}
	}
	return nil
}

func TestRunAggregateRepository_RehydrateScopeFull_ReturnsAllNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestAggRepo(t)
	defer cleanup()

	ctx := context.Background()
	runID := fmt.Sprintf("run-%s", t.Name())
	seedRun(t, ctx, client, runID, 2, 0, 0,
		[]seededNode{
			{"public", "a", "svc-1", "PENDING"},
			{"public", "b", "svc-1", "PENDING"},
		},
		[]seededEdge{
			{childSchema: "public", childTable: "b", parentSchema: "public", parentTable: "a"},
		},
	)

	agg, err := repo.Rehydrate(ctx, runID, domainRun.ScopeFull{})
	require.NoError(t, err)

	assert.Equal(t, 2, agg.TotalNodes)
	assert.Equal(t, 0, agg.TerminalCount)
	assert.Equal(t, 0, agg.Version)
	require.Len(t, agg.Nodes(), 2)

	kA := domainRun.NodeKey{ServiceName: "svc-1", SchemaName: "public", TableName: "a"}
	kB := domainRun.NodeKey{ServiceName: "svc-1", SchemaName: "public", TableName: "b"}
	nA := nodeByKey(agg, kA)
	nB := nodeByKey(agg, kB)
	require.NotNil(t, nA, "node A must be loaded")
	require.NotNil(t, nB, "node B must be loaded")
	assert.Contains(t, nA.Downstreams, kB, "A.Downstreams must contain B")
	assert.Contains(t, nB.Upstreams, kA, "B.Upstreams must contain A")
}

func TestRunAggregateRepository_RehydrateScopeNodeCompletion_Failed_IncludesTransitiveDownstream(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestAggRepo(t)
	defer cleanup()

	ctx := context.Background()
	runID := fmt.Sprintf("run-%s", t.Name())
	seedRun(t, ctx, client, runID, 3, 0, 0,
		[]seededNode{
			{"public", "a", "svc-1", "PENDING"},
			{"public", "b", "svc-1", "PENDING"},
			{"public", "c", "svc-1", "PENDING"},
		},
		[]seededEdge{
			{childSchema: "public", childTable: "b", parentSchema: "public", parentTable: "a"},
			{childSchema: "public", childTable: "c", parentSchema: "public", parentTable: "b"},
		},
	)

	kA := domainRun.NodeKey{ServiceName: "svc-1", SchemaName: "public", TableName: "a"}
	agg, err := repo.Rehydrate(ctx, runID,
		domainRun.ScopeNodeCompletion{Key: kA, Status: "FAILED"})
	require.NoError(t, err)

	keys := make(map[domainRun.NodeKey]bool)
	for _, n := range agg.Nodes() {
		keys[n.Key] = true
	}
	assert.True(t, keys[kA], "target must be in scope")
	assert.True(t, keys[domainRun.NodeKey{ServiceName: "svc-1", SchemaName: "public", TableName: "b"}],
		"immediate downstream must be in scope")
	assert.True(t, keys[domainRun.NodeKey{ServiceName: "svc-1", SchemaName: "public", TableName: "c"}],
		"transitive downstream must be in scope")
}

func TestRunAggregateRepository_RehydrateScopeNodeCompletion_Succeeded_IncludesNeighbourhood(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestAggRepo(t)
	defer cleanup()

	ctx := context.Background()
	runID := fmt.Sprintf("run-%s", t.Name())
	// A->C, B->C: completing A SUCCEEDED requires loading C (downstream) AND
	// B (C's other upstream) so the aggregate can evaluate unblocking.
	seedRun(t, ctx, client, runID, 3, 0, 0,
		[]seededNode{
			{"public", "a", "svc-1", "PENDING"},
			{"public", "b", "svc-1", "PENDING"},
			{"public", "c", "svc-1", "PENDING"},
		},
		[]seededEdge{
			{childSchema: "public", childTable: "c", parentSchema: "public", parentTable: "a"},
			{childSchema: "public", childTable: "c", parentSchema: "public", parentTable: "b"},
		},
	)

	kA := domainRun.NodeKey{ServiceName: "svc-1", SchemaName: "public", TableName: "a"}
	agg, err := repo.Rehydrate(ctx, runID,
		domainRun.ScopeNodeCompletion{Key: kA, Status: "SUCCEEDED"})
	require.NoError(t, err)

	keys := make(map[domainRun.NodeKey]bool)
	for _, n := range agg.Nodes() {
		keys[n.Key] = true
	}
	kB := domainRun.NodeKey{ServiceName: "svc-1", SchemaName: "public", TableName: "b"}
	kC := domainRun.NodeKey{ServiceName: "svc-1", SchemaName: "public", TableName: "c"}
	assert.True(t, keys[kA], "target A must be in scope")
	assert.True(t, keys[kC], "immediate downstream C must be in scope")
	assert.True(t, keys[kB], "C's other upstream B must be in scope for unblocking evaluation")
}

func TestRunAggregateRepository_Save_PersistsNodeStatusesAndCounters(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestAggRepo(t)
	defer cleanup()

	ctx := context.Background()
	runID := fmt.Sprintf("run-%s", t.Name())
	seedRun(t, ctx, client, runID, 2, 0, 0,
		[]seededNode{
			{"public", "a", "svc-1", "PENDING"},
			{"public", "b", "svc-1", "PENDING"},
		},
		[]seededEdge{
			{childSchema: "public", childTable: "b", parentSchema: "public", parentTable: "a"},
		},
	)

	kA := domainRun.NodeKey{ServiceName: "svc-1", SchemaName: "public", TableName: "a"}
	agg, err := repo.Rehydrate(ctx, runID,
		domainRun.ScopeNodeCompletion{Key: kA, Status: "SUCCEEDED"})
	require.NoError(t, err)

	_, err = agg.CompleteNode(kA, "SUCCEEDED")
	require.NoError(t, err)
	require.NoError(t, repo.Save(ctx, agg))

	reloaded, err := repo.Rehydrate(ctx, runID, domainRun.ScopeFull{})
	require.NoError(t, err)
	assert.Equal(t, 1, reloaded.TerminalCount, "terminal_count must be persisted")
	assert.Equal(t, 1, reloaded.Version, "version must be bumped on save")

	kB := domainRun.NodeKey{ServiceName: "svc-1", SchemaName: "public", TableName: "b"}
	nA := nodeByKey(reloaded, kA)
	nB := nodeByKey(reloaded, kB)
	require.NotNil(t, nA)
	require.NotNil(t, nB)
	assert.Equal(t, "SUCCEEDED", nA.Status, "A's EXECUTES.status must be persisted")
	assert.Equal(t, "PENDING", nB.Status, "B must remain PENDING")
}

// TestRunAggregateRepository_Save_DiscriminatesByServiceName guards the
// EXECUTES-identity fix: two :Table nodes that share schema.table but belong to
// different services must each carry their own :EXECUTES.status. The batched
// Save keys on (service_name, schema_name, table_name), so completing the
// svc-1 node must not leak its status onto the svc-2 node's edge.
func TestRunAggregateRepository_Save_DiscriminatesByServiceName(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestAggRepo(t)
	defer cleanup()

	ctx := context.Background()
	runID := fmt.Sprintf("run-%s", t.Name())
	seedRun(t, ctx, client, runID, 2, 0, 0,
		[]seededNode{
			{"public", "shared", "svc-1", "PENDING"},
			{"public", "shared", "svc-2", "PENDING"},
		},
		nil,
	)

	k1 := domainRun.NodeKey{ServiceName: "svc-1", SchemaName: "public", TableName: "shared"}
	agg, err := repo.Rehydrate(ctx, runID, domainRun.ScopeFull{})
	require.NoError(t, err)
	_, err = agg.CompleteNode(k1, "SUCCEEDED")
	require.NoError(t, err)
	require.NoError(t, repo.Save(ctx, agg))

	reloaded, err := repo.Rehydrate(ctx, runID, domainRun.ScopeFull{})
	require.NoError(t, err)
	n1 := nodeByKey(reloaded, k1)
	n2 := nodeByKey(reloaded, domainRun.NodeKey{ServiceName: "svc-2", SchemaName: "public", TableName: "shared"})
	require.NotNil(t, n1)
	require.NotNil(t, n2)
	assert.Equal(t, "SUCCEEDED", n1.Status, "svc-1's edge must be updated")
	assert.Equal(t, "PENDING", n2.Status, "svc-2's edge must NOT be touched (different service)")
}

// readTerminalStatus reads the raw :Run.terminal_status property so casing
// assertions can inspect exactly what a writer stamped, independent of the
// aggregate reconstruction in collectRunFromFlatRows.
func readTerminalStatus(t *testing.T, ctx context.Context, client neo4jinfra.Neo4jClient, runID string) string {
	t.Helper()
	session := client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)
	res, err := session.Run(ctx,
		"MATCH (r:Run {run_id: $run_id}) RETURN r.terminal_status AS ts",
		map[string]any{"run_id": runID})
	require.NoError(t, err)
	require.True(t, res.Next(ctx), "run %s must exist", runID)
	v, _ := res.Record().Get("ts")
	if v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}

// TestRunAggregateRepository_Save_StampsCanonicalLowercaseTerminalStatus guards
// the casing fix: the aggregate's in-memory status is uppercase ("SUCCEEDED"),
// but Save must normalize to the canonical lowercase form ("succeeded") so the
// projection the UI reads agrees with FinalizeRun and the snapshot writer.
func TestRunAggregateRepository_Save_StampsCanonicalLowercaseTerminalStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestAggRepo(t)
	defer cleanup()

	ctx := context.Background()
	runID := fmt.Sprintf("run-%s", t.Name())
	seedRun(t, ctx, client, runID, 1, 0, 0,
		[]seededNode{{"public", "a", "svc-1", "PENDING"}}, nil)

	kA := domainRun.NodeKey{ServiceName: "svc-1", SchemaName: "public", TableName: "a"}
	agg, err := repo.Rehydrate(ctx, runID, domainRun.ScopeFull{})
	require.NoError(t, err)
	_, err = agg.CompleteNode(kA, "SUCCEEDED")
	require.NoError(t, err)
	require.Equal(t, domainRun.RunStatusSucceeded, agg.Status,
		"single-node completion must finalize the aggregate")
	require.NoError(t, repo.Save(ctx, agg))

	assert.Equal(t, "succeeded", readTerminalStatus(t, ctx, client, runID),
		"Save must stamp canonical lowercase terminal_status, not the uppercase enum")
}

// TestRunAggregateRepository_FinalizeRun_NormalizesCasing guards that the
// run.finalized:v1 projection path also stores the canonical lowercase form even
// if an upstream value arrives in another casing.
func TestRunAggregateRepository_FinalizeRun_NormalizesCasing(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestAggRepo(t)
	defer cleanup()

	ctx := context.Background()
	runID := fmt.Sprintf("run-%s", t.Name())
	seedRun(t, ctx, client, runID, 1, 0, 0,
		[]seededNode{{"public", "a", "svc-1", "PENDING"}}, nil)

	require.NoError(t, repo.FinalizeRun(ctx, runID, "SUCCEEDED"))
	assert.Equal(t, "succeeded", readTerminalStatus(t, ctx, client, runID),
		"FinalizeRun must store the canonical lowercase form")
}

// TestRunAggregateRepository_RehydrateScopeNodeCompletion_DiscriminatesByServiceName
// guards the rehydrate target-match identity fix: two :Table nodes that share
// schema.table but belong to different services, each wired to its own
// downstream. Rehydrating the svc-1 target must load only svc-1's downstream,
// never svc-2's same-named node or downstream.
func TestRunAggregateRepository_RehydrateScopeNodeCompletion_DiscriminatesByServiceName(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestAggRepo(t)
	defer cleanup()

	ctx := context.Background()
	runID := fmt.Sprintf("run-%s", t.Name())
	// svc-1: public.shared -> public.down1 ; svc-2: public.shared -> public.down2.
	// The two "shared" tables share schema.table but differ by service.
	seedRun(t, ctx, client, runID, 4, 0, 0,
		[]seededNode{
			{"public", "shared", "svc-1", "PENDING"},
			{"public", "down1", "svc-1", "PENDING"},
			{"public", "shared", "svc-2", "PENDING"},
			{"public", "down2", "svc-2", "PENDING"},
		},
		nil,
	)
	// DEPENDS_ON edges keyed on (service, schema, table) so the two "shared"
	// nodes don't collapse. seededEdge matches on schema/table only, so wire the
	// service-scoped edges directly.
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	_, err := session.Run(ctx, `
		MATCH (c:Table {service_name: 'svc-1', schema_name: 'public', table_name: 'down1', test_marker: $m})
		MATCH (p:Table {service_name: 'svc-1', schema_name: 'public', table_name: 'shared', test_marker: $m})
		MERGE (c)-[:DEPENDS_ON]->(p)
	`, map[string]any{"m": t.Name()})
	require.NoError(t, err)
	_, err = session.Run(ctx, `
		MATCH (c:Table {service_name: 'svc-2', schema_name: 'public', table_name: 'down2', test_marker: $m})
		MATCH (p:Table {service_name: 'svc-2', schema_name: 'public', table_name: 'shared', test_marker: $m})
		MERGE (c)-[:DEPENDS_ON]->(p)
	`, map[string]any{"m": t.Name()})
	require.NoError(t, err)
	session.Close(ctx)

	target := domainRun.NodeKey{ServiceName: "svc-1", SchemaName: "public", TableName: "shared"}
	agg, err := repo.Rehydrate(ctx, runID,
		domainRun.ScopeNodeCompletion{Key: target, Status: "FAILED"})
	require.NoError(t, err)

	keys := make(map[domainRun.NodeKey]bool)
	for _, n := range agg.Nodes() {
		keys[n.Key] = true
	}
	assert.True(t, keys[target], "svc-1 target must be in scope")
	assert.True(t, keys[domainRun.NodeKey{ServiceName: "svc-1", SchemaName: "public", TableName: "down1"}],
		"svc-1's downstream must be in scope")
	assert.False(t, keys[domainRun.NodeKey{ServiceName: "svc-2", SchemaName: "public", TableName: "shared"}],
		"svc-2's same-named node must NOT be matched as the target")
	assert.False(t, keys[domainRun.NodeKey{ServiceName: "svc-2", SchemaName: "public", TableName: "down2"}],
		"svc-2's downstream must NOT be loaded")
}

func TestRunAggregateRepository_Save_StaleVersion_ReturnsErrVersionConflict(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestAggRepo(t)
	defer cleanup()

	ctx := context.Background()
	runID := fmt.Sprintf("run-%s", t.Name())
	seedRun(t, ctx, client, runID, 2, 0, 0,
		[]seededNode{
			{"public", "a", "svc-1", "PENDING"},
			{"public", "b", "svc-1", "PENDING"},
		},
		[]seededEdge{
			{childSchema: "public", childTable: "b", parentSchema: "public", parentTable: "a"},
		},
	)

	kA := domainRun.NodeKey{ServiceName: "svc-1", SchemaName: "public", TableName: "a"}
	hint := domainRun.ScopeNodeCompletion{Key: kA, Status: "SUCCEEDED"}

	agg1, err := repo.Rehydrate(ctx, runID, hint)
	require.NoError(t, err)
	agg2, err := repo.Rehydrate(ctx, runID, hint)
	require.NoError(t, err)

	_, err = agg1.CompleteNode(kA, "SUCCEEDED")
	require.NoError(t, err)
	require.NoError(t, repo.Save(ctx, agg1), "first save must succeed")

	_, err = agg2.CompleteNode(kA, "SUCCEEDED")
	require.NoError(t, err)
	err = repo.Save(ctx, agg2)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domainRun.ErrVersionConflict),
		"stale save must return ErrVersionConflict, got: %v", err)
}
