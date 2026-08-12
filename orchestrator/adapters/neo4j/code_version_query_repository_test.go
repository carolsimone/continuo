package neo4jinfra_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/codeversion"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newCodeVersionQueryRepo builds a CodeVersionQueryRepository against the
// given client.
func newCodeVersionQueryRepo(client neo4jinfra.Neo4jClient) *neo4jinfra.CodeVersionQueryRepository {
	return neo4jinfra.NewCodeVersionQueryRepository(client,
		slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
}

// seedDependsOn creates a :DEPENDS_ON edge between two already-seeded :Table
// nodes, addressed by unique_id.
func seedDependsOn(t *testing.T, client neo4jinfra.Neo4jClient, from, to string) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx,
		`MATCH (a:Table {unique_id:$from}) MATCH (b:Table {unique_id:$to}) MERGE (a)-[:DEPENDS_ON]->(b)`,
		map[string]any{"from": from, "to": to})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// wipeRunFixtures deletes :Run nodes (and their :EXECUTES edges) tagged with
// the given test marker. wipeVersionFixtures does not touch :Run nodes, so
// RunExecutions tests clean up separately.
func wipeRunFixtures(t *testing.T, client neo4jinfra.Neo4jClient, marker string) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `MATCH (r:Run {test_marker: $m}) DETACH DELETE r`, map[string]any{"m": marker})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// ---- NodeVersions ----

func TestCodeVersionQueryRepository_NodeVersions_NewestFirstIsCurrentOnce(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	writer := newVersionRepo(client)
	base := time.Now().UTC()
	for i, h := range []string{"sha256:v1", "sha256:v2", "sha256:v3"} {
		_, err := writer.WriteVersions(ctx, versionWriteInput("rel-"+strconv.Itoa(i+1),
			base.Add(time.Duration(i)*time.Minute),
			[]codeversion.NodeVersion{nodeInput("analytics.revenue", h)}, nil))
		require.NoError(t, err)
	}

	repo := newCodeVersionQueryRepo(client)
	versions, err := repo.NodeVersions(ctx, "analytics.revenue", 10)
	require.NoError(t, err)
	require.Len(t, versions, 3)

	assert.Equal(t, "sha256:v3", versions[0].ContentHash, "newest promoted_at first")
	assert.Equal(t, "sha256:v2", versions[1].ContentHash)
	assert.Equal(t, "sha256:v1", versions[2].ContentHash)

	currentCount := 0
	for i, v := range versions {
		if v.IsCurrent {
			currentCount++
			assert.Equal(t, 0, i, "the current version is the newest one")
		}
	}
	assert.Equal(t, 1, currentCount, "exactly one version is current")
}

func TestCodeVersionQueryRepository_NodeVersions_RespectsLimit(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	writer := newVersionRepo(client)
	base := time.Now().UTC()
	for i, h := range []string{"sha256:v1", "sha256:v2", "sha256:v3"} {
		_, err := writer.WriteVersions(ctx, versionWriteInput("rel-"+strconv.Itoa(i+1),
			base.Add(time.Duration(i)*time.Minute),
			[]codeversion.NodeVersion{nodeInput("analytics.revenue", h)}, nil))
		require.NoError(t, err)
	}

	repo := newCodeVersionQueryRepo(client)
	versions, err := repo.NodeVersions(ctx, "analytics.revenue", 2)
	require.NoError(t, err)
	require.Len(t, versions, 2)
	assert.Equal(t, "sha256:v3", versions[0].ContentHash)
	assert.Equal(t, "sha256:v2", versions[1].ContentHash)
}

func TestCodeVersionQueryRepository_NodeVersions_UnknownNodeReturnsErrNodeNotFound(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })

	repo := newCodeVersionQueryRepo(client)
	versions, err := repo.NodeVersions(ctx, "analytics.ghost", 10)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrNodeNotFound))
	assert.Nil(t, versions)
}

func TestCodeVersionQueryRepository_NodeVersions_KnownNodeNoVersionsReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	repo := newCodeVersionQueryRepo(client)
	versions, err := repo.NodeVersions(ctx, "analytics.revenue", 10)
	require.NoError(t, err)
	assert.Empty(t, versions)
}

// A retired node's :Table is gone but its versions survive as free-standing
// :NodeVersion nodes; the chain walk must still find them by unique_id, with
// is_current false everywhere since there is no :CURRENT pointer left.
func TestCodeVersionQueryRepository_NodeVersions_RetiredNodeReturnsHistoryIsCurrentFalse(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	writer := newVersionRepo(client)
	base := time.Now().UTC()
	_, err := writer.WriteVersions(ctx, versionWriteInput("rel-1", base,
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1")}, nil))
	require.NoError(t, err)
	_, err = writer.WriteVersions(ctx, versionWriteInput("rel-2", base.Add(time.Minute),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v2")}, nil))
	require.NoError(t, err)

	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	res, err := s.Run(ctx, `MATCH (t:Table {unique_id:'analytics.revenue'}) DETACH DELETE t`, nil)
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))

	repo := newCodeVersionQueryRepo(client)
	versions, err := repo.NodeVersions(ctx, "analytics.revenue", 10)
	require.NoError(t, err)
	require.Len(t, versions, 2)
	assert.Equal(t, "sha256:v2", versions[0].ContentHash, "still newest first by promoted_at")
	for _, v := range versions {
		assert.False(t, v.IsCurrent, "no :Table means no :CURRENT pointer")
	}
}

// ---- VersionsBySeq ----

func TestCodeVersionQueryRepository_VersionsBySeq_ReturnsNamedPair(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	writer := newVersionRepo(client)
	base := time.Now().UTC()
	_, err := writer.WriteVersions(ctx, versionWriteInput("rel-1", base,
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1")}, nil))
	require.NoError(t, err)
	_, err = writer.WriteVersions(ctx, versionWriteInput("rel-2", base.Add(time.Minute),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v2")}, nil))
	require.NoError(t, err)

	repo := newCodeVersionQueryRepo(client)
	from, to, err := repo.VersionsBySeq(ctx, "analytics.revenue", 1, 2)
	require.NoError(t, err)
	assert.Equal(t, int64(1), from.VersionSeq)
	assert.Equal(t, "sha256:v1", from.ContentHash)
	assert.False(t, from.IsCurrent)
	assert.Equal(t, int64(2), to.VersionSeq)
	assert.Equal(t, "sha256:v2", to.ContentHash)
	assert.True(t, to.IsCurrent)
}

func TestCodeVersionQueryRepository_VersionsBySeq_MissingSeqReturnsErrNodeNotFound(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	writer := newVersionRepo(client)
	_, err := writer.WriteVersions(ctx, versionWriteInput("rel-1", time.Now().UTC(),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1")}, nil))
	require.NoError(t, err)

	repo := newCodeVersionQueryRepo(client)
	_, _, err = repo.VersionsBySeq(ctx, "analytics.revenue", 1, 99)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrNodeNotFound))
	assert.Contains(t, err.Error(), "99", "the error names the missing seq")
}

// ---- Ancestors ----

func TestCodeVersionQueryRepository_Ancestors_OrderedMostRecentlyChangedFirstBoundedByDepth(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })

	for _, uid := range []string{"n", "a", "b", "c"} {
		seedVersionTable(t, client, uid, "sha256:seed")
	}
	seedDependsOn(t, client, "n", "a")
	seedDependsOn(t, client, "a", "b")
	seedDependsOn(t, client, "b", "c")

	writer := newVersionRepo(client)
	base := time.Now().UTC()
	_, err := writer.WriteVersions(ctx, versionWriteInput("rel-a", base,
		[]codeversion.NodeVersion{nodeInput("a", "sha256:a1")}, nil))
	require.NoError(t, err)
	_, err = writer.WriteVersions(ctx, versionWriteInput("rel-b", base.Add(time.Hour),
		[]codeversion.NodeVersion{nodeInput("b", "sha256:b1")}, nil))
	require.NoError(t, err)
	_, err = writer.WriteVersions(ctx, versionWriteInput("rel-c", base.Add(2*time.Hour),
		[]codeversion.NodeVersion{nodeInput("c", "sha256:c1")}, nil))
	require.NoError(t, err)

	repo := newCodeVersionQueryRepo(client)
	ancestors, err := repo.Ancestors(ctx, "n", 2, time.Time{})
	require.NoError(t, err)
	require.Len(t, ancestors, 2, "c sits at depth 3, beyond the depth-2 cap")
	assert.Equal(t, "b", ancestors[0].UniqueID, "b changed most recently")
	assert.Equal(t, int32(2), ancestors[0].Depth)
	require.Len(t, ancestors[0].Versions, 1)
	assert.Equal(t, "sha256:b1", ancestors[0].Versions[0].ContentHash)
	assert.Equal(t, "a", ancestors[1].UniqueID)
	assert.Equal(t, int32(1), ancestors[1].Depth)
}

func TestCodeVersionQueryRepository_Ancestors_RespectsSinceFilter(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })

	for _, uid := range []string{"n", "a", "b"} {
		seedVersionTable(t, client, uid, "sha256:seed")
	}
	seedDependsOn(t, client, "n", "a")
	seedDependsOn(t, client, "n", "b")

	writer := newVersionRepo(client)
	base := time.Now().UTC()
	_, err := writer.WriteVersions(ctx, versionWriteInput("rel-a", base,
		[]codeversion.NodeVersion{nodeInput("a", "sha256:a1")}, nil))
	require.NoError(t, err)
	_, err = writer.WriteVersions(ctx, versionWriteInput("rel-b", base.Add(2*time.Hour),
		[]codeversion.NodeVersion{nodeInput("b", "sha256:b1")}, nil))
	require.NoError(t, err)

	repo := newCodeVersionQueryRepo(client)
	since := base.Add(time.Hour)
	ancestors, err := repo.Ancestors(ctx, "n", 1, since)
	require.NoError(t, err)
	require.Len(t, ancestors, 1, "a's only version predates since")
	assert.Equal(t, "b", ancestors[0].UniqueID)
}

// ---- UnitsForNode ----

func TestCodeVersionQueryRepository_UnitsForNode_ReturnsCurrentVersionUnits(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	writer := newVersionRepo(client)
	_, err := writer.WriteVersions(ctx, versionWriteInput("rel-1", time.Now().UTC(),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1",
			codeversion.UnitRef{UnitID: "svc:m1", Checksum: "u1"},
			codeversion.UnitRef{UnitID: "svc:m2", Checksum: "u2"})},
		[]codeversion.CodeUnitVersion{
			{UnitID: "svc:m1", Checksum: "u1", Source: "macro one"},
			{UnitID: "svc:m2", Checksum: "u2", Source: "macro two"},
		}))
	require.NoError(t, err)

	repo := newCodeVersionQueryRepo(client)
	units, err := repo.UnitsForNode(ctx, "analytics.revenue")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"svc:m1", "svc:m2"}, units)
}

func TestCodeVersionQueryRepository_UnitsForNode_NoCurrentVersionReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	repo := newCodeVersionQueryRepo(client)
	units, err := repo.UnitsForNode(ctx, "analytics.revenue")
	require.NoError(t, err)
	assert.Empty(t, units)
}

// ---- UnitVersions ----

func TestCodeVersionQueryRepository_UnitVersions_NewestFirstIsCurrent(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	writer := newVersionRepo(client)
	base := time.Now().UTC()
	_, err := writer.WriteVersions(ctx, versionWriteInput("rel-1", base,
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1",
			codeversion.UnitRef{UnitID: "svc:m1", Checksum: "u1"})},
		[]codeversion.CodeUnitVersion{{UnitID: "svc:m1", Checksum: "u1", Source: "one"}}))
	require.NoError(t, err)
	_, err = writer.WriteVersions(ctx, versionWriteInput("rel-2", base.Add(time.Minute),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v2",
			codeversion.UnitRef{UnitID: "svc:m1", Checksum: "u2"})},
		[]codeversion.CodeUnitVersion{{UnitID: "svc:m1", Checksum: "u2", Source: "two"}}))
	require.NoError(t, err)

	repo := newCodeVersionQueryRepo(client)
	versions, err := repo.UnitVersions(ctx, "svc:m1", 10)
	require.NoError(t, err)
	require.Len(t, versions, 2)
	assert.Equal(t, "u2", versions[0].Checksum, "newest first")
	assert.True(t, versions[0].IsCurrent)
	assert.Equal(t, "u1", versions[1].Checksum)
	assert.False(t, versions[1].IsCurrent)
}

// ---- RunExecutions ----

func TestCodeVersionQueryRepository_RunExecutions_NewestFirst(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	marker := t.Name()
	t.Cleanup(func() {
		wipeVersionFixtures(t, client)
		wipeRunFixtures(t, client, marker)
	})
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	base := time.Now().UTC()
	res, err := s.Run(ctx, `
		MATCH (t:Table {unique_id:'analytics.revenue'})
		CREATE (r1:Run {run_id:'run-1', schedule_name:'sched', operation:'run',
		                created_at: $t1, completed_at: $t1, test_marker: $m})
		CREATE (r1)-[:EXECUTES {task_id:'task-1', status:'succeeded', image_tag:'img1', content_hash:'sha256:v1'}]->(t)
		CREATE (r2:Run {run_id:'run-2', schedule_name:'sched', operation:'run',
		                created_at: $t2, completed_at: $t2, test_marker: $m})
		CREATE (r2)-[:EXECUTES {task_id:'task-2', status:'failed', image_tag:'img2', content_hash:'sha256:v1'}]->(t)
	`, map[string]any{"t1": base, "t2": base.Add(time.Minute), "m": marker})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))

	repo := newCodeVersionQueryRepo(client)
	runs, err := repo.RunExecutions(ctx, "analytics.revenue", 10)
	require.NoError(t, err)
	require.Len(t, runs, 2)
	assert.Equal(t, "run-2", runs[0].RunID, "newest first")
	assert.Equal(t, "failed", runs[0].Status)
	assert.Equal(t, "img2", runs[0].ImageTag)
	assert.Equal(t, "sha256:v1", runs[0].ContentHash)
	assert.Equal(t, "task-2", runs[0].TaskID)
	assert.Equal(t, "run-1", runs[1].RunID)
}
