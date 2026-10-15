package neo4jinfra_test

import (
	"context"
	"errors"
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

func TestGetScheduleGraph_PopulatesPromotionSeq(t *testing.T) {
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
        SET   root.promotion_seq = 42
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
		_, _ = s.Run(ctx, "MATCH (r:TopologyRoot {id:'singleton'}) REMOVE r.promotion_seq",
			nil)
	})

	g, err := repo.GetScheduleGraph(ctx, "gen-test")
	require.NoError(t, err)
	assert.Equal(t, int64(42), g.PromotionSeq)
	assert.Len(t, g.Nodes, 1, "schedule graph must return the seeded node")
}

func TestGetNodeLocation_ReturnsPathAndService(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()
	marker := t.Name()

	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	_, err := s.Run(ctx, `
        CREATE (:Table {unique_id:'analytics.loc_target', service_name:'service-1',
                        schema_name:'analytics', table_name:'loc_target',
                        original_file_path:'models/loc_target.sql', active:true,
                        test_marker:$m})
    `, map[string]any{"m": marker})
	require.NoError(t, err)
	s.Close(ctx)
	t.Cleanup(func() {
		ws := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer ws.Close(ctx)
		_, _ = ws.Run(ctx, "MATCH (t:Table {test_marker:$m}) DETACH DELETE t",
			map[string]any{"m": marker})
	})

	loc, err := repo.GetNodeLocation(ctx, "analytics.loc_target")
	require.NoError(t, err)
	assert.Equal(t, "models/loc_target.sql", loc.FilePath)
	assert.Equal(t, "service-1", loc.ServiceName)
}

func TestGetNodeLocation_UnknownNode_ErrNodeNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Neo4j")
	}
	repo, _, cleanup := newTestQueryRepo(t)
	defer cleanup()

	_, err := repo.GetNodeLocation(context.Background(), "analytics.absent")
	require.True(t, errors.Is(err, domain.ErrNodeNotFound))
}

// The drift views compare runs with the live topology's promotion seq, which the
// swap writes on :TopologyRoot in its own transaction.
func TestOrchestratorQueryRepository_GetGeneration_ReadsTheLivePromotionSeq(t *testing.T) {
	repo, client, cleanup := newTestQueryRepo(t)
	defer cleanup()
	ctx := context.Background()
	t.Cleanup(func() {
		runWrite(t, client, `MATCH (root:TopologyRoot {id:'singleton'}) REMOVE root.promotion_seq`, nil)
	})

	// Consume each write before GetGeneration reads through a separate session.
	runWrite(t, client, `MATCH (root:TopologyRoot {id:'singleton'}) REMOVE root.promotion_seq`, nil)
	got, err := repo.GetGeneration(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), got, "0 before the first promotion")

	runWrite(t, client, `MERGE (root:TopologyRoot {id:'singleton'}) SET root.promotion_seq = 9`, nil)
	got, err = repo.GetGeneration(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(9), got)
}
