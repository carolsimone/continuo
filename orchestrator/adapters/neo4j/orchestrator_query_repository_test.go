package neo4jinfra_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestQueryRepo builds an OrchestratorQueryRepository against the live Neo4j and returns
// it together with a cleanup function that wipes any nodes created by the test.
// It intentionally re-uses the env conventions already in use by run_repository_test.go
// (NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD with the docker-compose defaults).
func newTestQueryRepo(t *testing.T) (*neo4jinfra.OrchestratorQueryRepository, neo4jinfra.Neo4jClient, func()) {
	t.Helper()
	client := newTestClient(t) // already defined in run_repository_test.go
	repo := neo4jinfra.NewOrchestratorQueryRepository(client, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	cleanup := func() {
		ctx := context.Background()
		session := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer session.Close(ctx)
		_, _ = session.Run(ctx, "MATCH (r:Run {test_marker: $m}) DETACH DELETE r", map[string]any{"m": t.Name()})
	}
	return repo, client, cleanup
}

func seedRunWithGeneration(t *testing.T, ctx context.Context, client neo4jinfra.Neo4jClient, runID string, gen *int64) {
	t.Helper()
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)
	var query string
	params := map[string]any{"run_id": runID, "marker": t.Name()}
	if gen == nil {
		query = `CREATE (r:Run {run_id: $run_id, schedule_name: 'sched', test_marker: $marker})`
	} else {
		query = `CREATE (r:Run {run_id: $run_id, schedule_name: 'sched', topology_generation: $gen, test_marker: $marker})`
		params["gen"] = *gen
	}
	_, err := session.Run(ctx, query, params)
	require.NoError(t, err)
}

func TestOrchestratorQueryRepository_GetRunTopologyGeneration_ReturnsValue(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()

	ctx := context.Background()
	runID := fmt.Sprintf("run-%s-1", t.Name())
	gen := int64(7)
	seedRunWithGeneration(t, ctx, client, runID, &gen)

	got, err := repo.GetRunTopologyGeneration(ctx, runID)
	require.NoError(t, err)
	assert.Equal(t, int64(7), got)
}

func TestOrchestratorQueryRepository_GetRunTopologyGeneration_ReturnsZeroWhenUnset(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()

	ctx := context.Background()
	runID := fmt.Sprintf("run-%s-2", t.Name())
	seedRunWithGeneration(t, ctx, client, runID, nil) // property absent

	got, err := repo.GetRunTopologyGeneration(ctx, runID)
	require.NoError(t, err)
	assert.Equal(t, int64(0), got)
}

func TestOrchestratorQueryRepository_GetRunTopologyGeneration_ReturnsZeroWhenRunMissing(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, _, cleanup := newTestQueryRepo(t)
	defer cleanup()

	ctx := context.Background()
	got, err := repo.GetRunTopologyGeneration(ctx, "nonexistent-run")
	require.NoError(t, err)
	assert.Equal(t, int64(0), got)
}

func TestOrchestratorQueryRepository_ListActiveRuns_OnlyUnfinalized(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()

	ctx := context.Background()

	// Seed: one unfinalized run + one finalized run.
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	_, err := session.Run(ctx, `
        CREATE (r1:Run {run_id: 'active-1', schedule_name: 'sched-a', topology_generation: 5, test_marker: $m})
        CREATE (r2:Run {run_id: 'finalized-1', schedule_name: 'sched-b', topology_generation: 4,
                       completed_at: datetime(), terminal_status: 'SUCCEEDED', test_marker: $m})
    `, map[string]any{"m": t.Name()})
	require.NoError(t, err)
	session.Close(ctx)

	runs, err := repo.ListActiveRuns(ctx)
	require.NoError(t, err)
	// Filter to runs created by this test (cleanup window).
	filtered := make([]*domain.ActiveRun, 0)
	for _, r := range runs {
		if r.RunID == "active-1" || r.RunID == "finalized-1" {
			filtered = append(filtered, r)
		}
	}
	require.Len(t, filtered, 1)
	assert.Equal(t, "active-1", filtered[0].RunID)
	assert.Equal(t, "sched-a", filtered[0].ScheduleName)
	assert.Equal(t, int64(5), filtered[0].TopologyGeneration)
}

func TestOrchestratorQueryRepository_ListActiveRuns_ZeroForUnsetGeneration(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()

	ctx := context.Background()
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	_, err := session.Run(ctx, `
        CREATE (r:Run {run_id: 'active-no-gen', schedule_name: 'sched-c', test_marker: $m})
    `, map[string]any{"m": t.Name()})
	require.NoError(t, err)
	session.Close(ctx)

	runs, err := repo.ListActiveRuns(ctx)
	require.NoError(t, err)
	var found *domain.ActiveRun
	for _, r := range runs {
		if r.RunID == "active-no-gen" {
			found = r
			break
		}
	}
	require.NotNil(t, found)
	assert.Equal(t, int64(0), found.TopologyGeneration)
}

// TestOrchestratorQueryRepository_ListActiveRuns_NewestFirst verifies that
// ListActiveRuns returns in-flight runs for the same schedule ordered
// newest-first (created_at DESC), and excludes finalized runs regardless of
// created_at. The ordering contract is required by RunQueryService.ListActiveRunDrifts
// so that keeping the head row per schedule always yields the newest run.
func TestOrchestratorQueryRepository_ListActiveRuns_NewestFirst(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()

	ctx := context.Background()
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	_, err := session.Run(ctx, `
        CREATE (r1:Run {
            run_id: 'order-older', schedule_name: 'sched-order',
            topology_generation: 1,
            created_at: datetime('2026-05-01T00:00:00Z'),
            test_marker: $m
        })
        CREATE (r2:Run {
            run_id: 'order-newer', schedule_name: 'sched-order',
            topology_generation: 2,
            created_at: datetime('2026-05-02T00:00:00Z'),
            test_marker: $m
        })
        CREATE (r3:Run {
            run_id: 'order-finalized', schedule_name: 'sched-order',
            topology_generation: 3,
            created_at: datetime('2026-05-03T00:00:00Z'),
            completed_at: datetime(), terminal_status: 'SUCCEEDED',
            test_marker: $m
        })
    `, map[string]any{"m": t.Name()})
	require.NoError(t, err)
	session.Close(ctx)

	runs, err := repo.ListActiveRuns(ctx)
	require.NoError(t, err)

	// Filter to runs created by this test.
	var filtered []*domain.ActiveRun
	for _, r := range runs {
		if r.RunID == "order-older" || r.RunID == "order-newer" || r.RunID == "order-finalized" {
			filtered = append(filtered, r)
		}
	}

	// Finalized run must be excluded; only the two in-flight runs appear.
	require.Len(t, filtered, 2, "expected 2 in-flight runs, finalized excluded")

	// Newest (2026-05-02) must precede older (2026-05-01) within the schedule.
	assert.Equal(t, "order-newer", filtered[0].RunID, "head row must be the newest in-flight run")
	assert.Equal(t, "order-older", filtered[1].RunID, "second row must be the older in-flight run")
}

func TestListScheduleTopologies(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()
	marker := t.Name()

	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	_, err := session.Run(ctx, `
        CREATE (:Table {service_name:'svc', schema_name:'s', table_name:'a',
                        schedule_name:'sched-x', active:true,
                        last_updated_at: datetime('2026-05-26T10:00:00Z'),
                        test_marker:$m})
        CREATE (:Table {service_name:'svc', schema_name:'s', table_name:'b',
                        schedule_name:'sched-x', active:true,
                        last_updated_at: datetime('2026-05-26T11:00:00Z'),
                        test_marker:$m})
        CREATE (:Table {service_name:'svc', schema_name:'s', table_name:'c',
                        schedule_name:'sched-y', active:true,
                        last_updated_at: datetime('2026-05-26T09:00:00Z'),
                        test_marker:$m})
        CREATE (:Table {service_name:'svc', schema_name:'s', table_name:'d',
                        schedule_name:'sched-x', active:false,
                        last_updated_at: datetime('2026-05-26T12:00:00Z'),
                        test_marker:$m})
        CREATE (:Table {service_name:'svc', schema_name:'s', table_name:'e',
                        schedule_name:null, active:true,
                        last_updated_at: datetime('2026-05-26T08:00:00Z'),
                        test_marker:$m})
    `, map[string]any{"m": marker})
	require.NoError(t, err)
	session.Close(ctx)

	t.Cleanup(func() {
		s := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer s.Close(ctx)
		_, _ = s.Run(ctx, "MATCH (t:Table {test_marker:$m}) DETACH DELETE t",
			map[string]any{"m": marker})
	})

	got, err := repo.ListScheduleTopologies(ctx)
	require.NoError(t, err)

	bySchedule := map[string]*domain.ScheduleTopologySummary{}
	for _, s := range got {
		if s.ScheduleName == "sched-x" || s.ScheduleName == "sched-y" {
			bySchedule[s.ScheduleName] = s
		}
	}
	require.Contains(t, bySchedule, "sched-x")
	require.Contains(t, bySchedule, "sched-y")
	assert.Equal(t, 2, bySchedule["sched-x"].NodeCount, "active rows only")
	assert.Equal(t, 1, bySchedule["sched-y"].NodeCount)
	assert.Equal(t, "2026-05-26T11:00:00",
		bySchedule["sched-x"].LastUpdatedAt.Format("2006-01-02T15:04:05"),
		"max(last_updated_at) of active rows")
}

func TestListScheduleTopologies_EmptyReturnsEmptySlice(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, _, cleanup := newTestQueryRepo(t)
	defer cleanup()
	got, err := repo.ListScheduleTopologies(context.Background())
	require.NoError(t, err)
	require.NotNil(t, got, "must return [] not nil")
}

func TestGetScheduleGraph_PopulatesTopologyGeneration(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()
	marker := t.Name()

	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	_, err := s.Run(ctx, `
        MERGE (root:TopologyRoot {id:'singleton'})
        SET   root.topology_generation = 42
        CREATE (:Table {service_name:'svc', schema_name:'s', table_name:'a',
                        schedule_name:'gen-test', active:true,
                        last_updated_at: datetime('2026-05-26T10:00:00Z'),
                        test_marker:$m})
    `, map[string]any{"m": marker})
	require.NoError(t, err)
	s.Close(ctx)
	t.Cleanup(func() {
		s := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer s.Close(ctx)
		_, _ = s.Run(ctx, "MATCH (t:Table {test_marker:$m}) DETACH DELETE t",
			map[string]any{"m": marker})
		_, _ = s.Run(ctx, "MATCH (r:TopologyRoot {id:'singleton'}) REMOVE r.topology_generation",
			nil)
	})

	g, err := repo.GetScheduleGraph(ctx, "gen-test")
	require.NoError(t, err)
	assert.Equal(t, int64(42), g.TopologyGeneration)
	assert.Len(t, g.Nodes, 1, "schedule graph must return the seeded node")
}
