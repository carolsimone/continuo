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
	"github.com/carolsimone/continuo/orchestrator/service/queries"
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
	versions, err := repo.NodeVersions(ctx, "analytics.revenue", 10, true)
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
	versions, err := repo.NodeVersions(ctx, "analytics.revenue", 2, true)
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
	versions, err := repo.NodeVersions(ctx, "analytics.ghost", 10, true)
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
	versions, err := repo.NodeVersions(ctx, "analytics.revenue", 10, true)
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
	versions, err := repo.NodeVersions(ctx, "analytics.revenue", 10, true)
	require.NoError(t, err)
	require.Len(t, versions, 2)
	assert.Equal(t, "sha256:v2", versions[0].ContentHash, "still newest first by promoted_at")
	for _, v := range versions {
		assert.False(t, v.IsCurrent, "no :Table means no :CURRENT pointer")
	}
}

// ---- CurrentNodeVersion ----

// The revert case is the point of CurrentNodeVersion: after re-promoting an
// older, already-recorded version, NodeVersions still lists the superseded
// version first (it orders by promoted_at, and a version node is immutable),
// but CurrentNodeVersion must follow :CURRENT back to the reverted-to
// version instead.
func TestCodeVersionQueryRepository_CurrentNodeVersion_AfterRevert_ReturnsCurrentNotNewest(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.rev_node", "sha256:h1")

	writer := newVersionRepo(client)
	base := time.Now().UTC()
	_, err := writer.WriteVersions(ctx, versionWriteInput("rel-1", base,
		[]codeversion.NodeVersion{nodeInput("analytics.rev_node", "sha256:h1")}, nil))
	require.NoError(t, err)
	_, err = writer.WriteVersions(ctx, versionWriteInput("rel-2", base.Add(time.Minute),
		[]codeversion.NodeVersion{nodeInput("analytics.rev_node", "sha256:h2")}, nil))
	require.NoError(t, err)
	// The revert: re-promote content_hash h1. h1's :NodeVersion already
	// exists, so its own promoted_at (set ON CREATE only) stays at its
	// original, older value — only the :CURRENT pointer moves.
	_, err = writer.WriteVersions(ctx, versionWriteInput("rel-3", base.Add(2*time.Minute),
		[]codeversion.NodeVersion{nodeInput("analytics.rev_node", "sha256:h1")}, nil))
	require.NoError(t, err)

	repo := newCodeVersionQueryRepo(client)
	got, err := repo.CurrentNodeVersion(ctx, "analytics.rev_node", true)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "sha256:h1", got[0].ContentHash, "the reverted-to version is current")
	assert.Equal(t, "select 1", got[0].RawCode)
	assert.True(t, got[0].IsCurrent)

	// Contrast: newest-first would have led the fixer astray.
	newest, err := repo.NodeVersions(ctx, "analytics.rev_node", 20, false)
	require.NoError(t, err)
	require.NotEmpty(t, newest)
	assert.Equal(t, "sha256:h2", newest[0].ContentHash, "newest-first still surfaces the superseded version")
}

func TestCodeVersionQueryRepository_CurrentNodeVersion_KnownNodeNoCurrentReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.no_cur", "sha256:v1")

	repo := newCodeVersionQueryRepo(client)
	got, err := repo.CurrentNodeVersion(ctx, "analytics.no_cur", true)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestCodeVersionQueryRepository_CurrentNodeVersion_UnknownNodeReturnsErrNodeNotFound(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })

	repo := newCodeVersionQueryRepo(client)
	got, err := repo.CurrentNodeVersion(ctx, "analytics.ghost", true)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrNodeNotFound))
	assert.Nil(t, got)
}

// TestCodeVersionQueryRepository_CurrentNodeVersion_RetiredTableReturnsEmpty
// proves a soft-retired node is excluded from CurrentNodeVersion rather than
// reported as running its obsolete code. release_promotion_repository.go's
// Step B retires a table a newer release dropped by setting active=false
// without removing its :CURRENT edge — it is kept, inactive, solely so a
// :Run-[:EXECUTES] edge from run history is not destroyed — so the :CURRENT
// match here still finds a version unless the read filters on t.active.
func TestCodeVersionQueryRepository_CurrentNodeVersion_RetiredTableReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.retired_node", "sha256:v1")

	writer := newVersionRepo(client)
	_, err := writer.WriteVersions(ctx, versionWriteInput("rel-1", time.Now().UTC(),
		[]codeversion.NodeVersion{nodeInput("analytics.retired_node", "sha256:v1")}, nil))
	require.NoError(t, err)

	// Soft-retire: active=false, :CURRENT edge left untouched — exactly what
	// Step B does, and distinct from the hard-orphan case above (DETACH DELETE
	// of the :Table itself), which NodeVersions' retired-node test already
	// covers.
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	res, err := s.Run(ctx,
		`MATCH (t:Table {unique_id:'analytics.retired_node'}) SET t.active = false, t.retired_at = datetime()`, nil)
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))

	repo := newCodeVersionQueryRepo(client)
	got, err := repo.CurrentNodeVersion(ctx, "analytics.retired_node", true)
	require.NoError(t, err)
	assert.Empty(t, got, "a retired table's obsolete :CURRENT version must not be reported as running now")
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
	ancestors, err := repo.Ancestors(ctx, "n", 2, time.Time{}, 5)
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
	ancestors, err := repo.Ancestors(ctx, "n", 1, since, 5)
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
	runs, err := repo.RunExecutions(ctx, "analytics.revenue", 10, "")
	require.NoError(t, err)
	require.Len(t, runs, 2)
	assert.Equal(t, "run-2", runs[0].RunID, "newest first")
	assert.Equal(t, "failed", runs[0].Status)
	assert.Equal(t, "img2", runs[0].ImageTag)
	assert.Equal(t, "sha256:v1", runs[0].ContentHash)
	assert.Equal(t, "task-2", runs[0].TaskID)
	assert.Equal(t, "run-1", runs[1].RunID)
}

func TestCodeVersionQueryRepository_RunExecutions_FiltersByOperation(t *testing.T) {
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
		CREATE (r1)-[:EXECUTES {task_id:'task-1', status:'succeeded'}]->(t)
		CREATE (r2:Run {run_id:'run-2', schedule_name:'sched', operation:'test',
		                created_at: $t2, completed_at: $t2, test_marker: $m})
		CREATE (r2)-[:EXECUTES {task_id:'task-2', status:'succeeded'}]->(t)
	`, map[string]any{"t1": base, "t2": base.Add(time.Minute), "m": marker})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))

	repo := newCodeVersionQueryRepo(client)
	runs, err := repo.RunExecutions(ctx, "analytics.revenue", 10, "test")
	require.NoError(t, err)
	require.Len(t, runs, 1, "the 'run' row must be filtered out server-side")
	assert.Equal(t, "run-2", runs[0].RunID)
	assert.Equal(t, "test", runs[0].Operation)
}

func TestCodeVersionQueryRepository_RunExecutions_UnknownNodeReturnsErrNodeNotFound(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })

	repo := newCodeVersionQueryRepo(client)
	runs, err := repo.RunExecutions(ctx, "analytics.ghost", 10, "")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrNodeNotFound))
	assert.Nil(t, runs)
}

func TestCodeVersionQueryRepository_RunExecutions_KnownNodeNoRunsReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	repo := newCodeVersionQueryRepo(client)
	runs, err := repo.RunExecutions(ctx, "analytics.revenue", 10, "")
	require.NoError(t, err)
	assert.Empty(t, runs)
}

// A retired node (:Table deleted, its history preserved as free-standing
// :NodeVersion nodes — see the RetiredNode NodeVersions test above) has no
// runs of its own to fall back on, but nodeKnown must still recognise it via
// its surviving :NodeVersion rows, not report ErrNodeNotFound.
func TestCodeVersionQueryRepository_RunExecutions_RetiredNodeKnownViaNodeVersionReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	writer := newVersionRepo(client)
	_, err := writer.WriteVersions(ctx, versionWriteInput("rel-1", time.Now().UTC(),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1")}, nil))
	require.NoError(t, err)

	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	res, err := s.Run(ctx, `MATCH (t:Table {unique_id:'analytics.revenue'}) DETACH DELETE t`, nil)
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))

	repo := newCodeVersionQueryRepo(client)
	runs, err := repo.RunExecutions(ctx, "analytics.revenue", 10, "")
	require.NoError(t, err, "the node is known via its surviving :NodeVersion rows, not unknown")
	assert.Empty(t, runs)
}

// ---- NodeVersions include_code ----

func TestCodeVersionQueryRepository_NodeVersions_IncludeCodeFalseOmitsCodeBodies(t *testing.T) {
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
	light, err := repo.NodeVersions(ctx, "analytics.revenue", 10, false)
	require.NoError(t, err)
	require.Len(t, light, 1)
	assert.Empty(t, light[0].RawCode)
	assert.Empty(t, light[0].CompiledCode)
	assert.Equal(t, "sha256:v1", light[0].ContentHash, "hashes are unaffected")
	assert.NotEmpty(t, light[0].ConfigJSON, "config_json is unaffected")

	full, err := repo.NodeVersions(ctx, "analytics.revenue", 10, true)
	require.NoError(t, err)
	require.Len(t, full, 1)
	assert.Equal(t, "select 1", full[0].RawCode)
	assert.Equal(t, "select 1", full[0].CompiledCode)
}

// ---- UnitVersions not-found ----

func TestCodeVersionQueryRepository_UnitVersions_UnknownUnitReturnsErrUnitNotFound(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })

	repo := newCodeVersionQueryRepo(client)
	versions, err := repo.UnitVersions(ctx, "svc:ghost", 10)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnitNotFound))
	assert.Nil(t, versions)
}

func TestCodeVersionQueryRepository_UnitVersions_KnownUnitNoVersionsReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })

	// A :CodeUnit with no :CodeUnitVersion is not produced by the write path
	// today, but the port contract still promises OK+empty for a known
	// anchor with no history, so seed the edge case directly.
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	res, err := s.Run(ctx, `MERGE (cu:CodeUnit {unit_id: 'svc:known-empty'})`, nil)
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))

	repo := newCodeVersionQueryRepo(client)
	versions, err := repo.UnitVersions(ctx, "svc:known-empty", 10)
	require.NoError(t, err)
	assert.Empty(t, versions)
}

// ---- UnitVersionsBatch ----

func TestCodeVersionQueryRepository_UnitVersionsBatch_ReturnsAllChainsCappedPerUnit(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	writer := newVersionRepo(client)
	base := time.Now().UTC()
	_, err := writer.WriteVersions(ctx, versionWriteInput("rel-1", base,
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1",
			codeversion.UnitRef{UnitID: "svc:m1", Checksum: "u1a"},
			codeversion.UnitRef{UnitID: "svc:m2", Checksum: "u2a"},
			codeversion.UnitRef{UnitID: "svc:m3", Checksum: "u3a"})},
		[]codeversion.CodeUnitVersion{
			{UnitID: "svc:m1", Checksum: "u1a", Source: "one-a"},
			{UnitID: "svc:m2", Checksum: "u2a", Source: "two-a"},
			{UnitID: "svc:m3", Checksum: "u3a", Source: "three-a"},
		}))
	require.NoError(t, err)
	// A second version of m1 only: exercises the per-unit cap and the
	// newest-first ordering within a unit's own chain.
	_, err = writer.WriteVersions(ctx, versionWriteInput("rel-2", base.Add(time.Minute),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v2",
			codeversion.UnitRef{UnitID: "svc:m1", Checksum: "u1b"},
			codeversion.UnitRef{UnitID: "svc:m2", Checksum: "u2a"},
			codeversion.UnitRef{UnitID: "svc:m3", Checksum: "u3a"})},
		[]codeversion.CodeUnitVersion{{UnitID: "svc:m1", Checksum: "u1b", Source: "one-b"}}))
	require.NoError(t, err)

	repo := newCodeVersionQueryRepo(client)
	byUnit, err := repo.UnitVersionsBatch(ctx, []string{"svc:m1", "svc:m2", "svc:m3"}, 1)
	require.NoError(t, err)
	require.Len(t, byUnit, 3, "one round trip returns all three units' chains")
	require.Len(t, byUnit["svc:m1"], 1, "capped per-unit at limit=1")
	assert.Equal(t, "u1b", byUnit["svc:m1"][0].Checksum, "newest first")
	assert.True(t, byUnit["svc:m1"][0].IsCurrent)
	require.Len(t, byUnit["svc:m2"], 1)
	assert.Equal(t, "u2a", byUnit["svc:m2"][0].Checksum)
	require.Len(t, byUnit["svc:m3"], 1)
	assert.Equal(t, "u3a", byUnit["svc:m3"][0].Checksum)
}

func TestCodeVersionQueryRepository_UnitVersionsBatch_UnitWithNoVersionsIsAbsentFromResult(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })

	repo := newCodeVersionQueryRepo(client)
	byUnit, err := repo.UnitVersionsBatch(ctx, []string{"svc:ghost"}, 10)
	require.NoError(t, err)
	_, ok := byUnit["svc:ghost"]
	assert.False(t, ok, "an id with no recorded version is simply absent, not an error")
}

// ---- Revert-aware ancestors (comment 1: promotion transitions) ----

// seedRevertScenario writes A at base, B at base+1h, then reverts to A (a
// second promotion of A's already-recorded content) at base+2h, on ancestor
// uid "a" reachable from "n" one hop away. A's own :NodeVersion.promoted_at
// never changes (version nodes are immutable): only the :CURRENT edge's
// promoted_at moves to the revert time.
func seedRevertScenario(t *testing.T, ctx context.Context, client neo4jinfra.Neo4jClient, base time.Time) {
	t.Helper()
	seedVersionTable(t, client, "n", "sha256:seed")
	seedVersionTable(t, client, "a", "sha256:seed")
	seedDependsOn(t, client, "n", "a")

	writer := newVersionRepo(client)
	_, err := writer.WriteVersions(ctx, versionWriteInput("rel-a1", base,
		[]codeversion.NodeVersion{nodeInput("a", "sha256:A")}, nil))
	require.NoError(t, err)
	_, err = writer.WriteVersions(ctx, versionWriteInput("rel-a2", base.Add(time.Hour),
		[]codeversion.NodeVersion{nodeInput("a", "sha256:B")}, nil))
	require.NoError(t, err)
	_, err = writer.WriteVersions(ctx, versionWriteInput("rel-a3", base.Add(2*time.Hour),
		[]codeversion.NodeVersion{nodeInput("a", "sha256:A")}, nil))
	require.NoError(t, err)
}

func TestCodeVersionQueryRepository_Ancestors_RevertReportsCurrentThenNewestOther(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedRevertScenario(t, ctx, client, time.Now().UTC())

	repo := newCodeVersionQueryRepo(client)
	ancestors, err := repo.Ancestors(ctx, "n", 1, time.Time{}, 5)
	require.NoError(t, err)
	require.Len(t, ancestors, 1)
	require.Len(t, ancestors[0].Versions, 2)

	assert.Equal(t, "sha256:A", ancestors[0].Versions[0].ContentHash,
		"Versions[0] is the version :CURRENT points to, not the newest-by-promoted_at one")
	assert.True(t, ancestors[0].Versions[0].IsCurrent)
	assert.Equal(t, "sha256:B", ancestors[0].Versions[1].ContentHash,
		"Versions[1] is the newest OTHER version — B, still the actual From side of the revert")
	assert.False(t, ancestors[0].Versions[1].IsCurrent)
}

// since must key off the effective last-change time (the :CURRENT edge's own
// promoted_at when a revert moved it), not a version node's own immutable
// promoted_at — otherwise a since window that only covers the revert itself
// would wrongly exclude this ancestor.
func TestCodeVersionQueryRepository_Ancestors_SinceFilterUsesEffectiveRecencyAcrossRevert(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	base := time.Now().UTC()
	seedRevertScenario(t, ctx, client, base)

	repo := newCodeVersionQueryRepo(client)

	// Window covers only the revert (base+2h), strictly after B's own
	// creation (base+1h) and A's own creation (base). A's or B's own
	// promoted_at would both wrongly predate this window.
	since := base.Add(90 * time.Minute)
	ancestors, err := repo.Ancestors(ctx, "n", 1, since, 5)
	require.NoError(t, err)
	require.Len(t, ancestors, 1, "the revert happened after `since`, so effective recency must include it")
	assert.Equal(t, "a", ancestors[0].UniqueID)

	// A window strictly after the revert excludes it.
	sinceAfter := base.Add(3 * time.Hour)
	ancestors, err = repo.Ancestors(ctx, "n", 1, sinceAfter, 5)
	require.NoError(t, err)
	assert.Empty(t, ancestors)
}

func TestCodeVersionQueryService_GetUpstreamChanges_RevertReportsActualTransition_Integration(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedRevertScenario(t, ctx, client, time.Now().UTC())

	repo := newCodeVersionQueryRepo(client)
	svc := queries.NewCodeVersionQueryService(repo)

	changes, err := svc.GetUpstreamChanges(ctx, "n", 1, time.Time{})
	require.NoError(t, err)
	require.Len(t, changes, 1)
	assert.Equal(t, "a", changes[0].UniqueID)
	assert.Equal(t, "sha256:B", changes[0].Diff.From.ContentHash, "the actual From of the latest change is B")
	assert.Equal(t, "sha256:A", changes[0].Diff.To.ContentHash, "the actual To of the latest change is A")
	assert.True(t, changes[0].Diff.To.IsCurrent)
}

// ---- Ancestor cap applied before version-body fetch (comment 3) ----

func TestCodeVersionQueryRepository_Ancestors_CapAppliesToTheMostRecentlyChanged(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })

	seedVersionTable(t, client, "n", "sha256:seed")
	uids := []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7"}
	for _, uid := range uids {
		seedVersionTable(t, client, uid, "sha256:seed")
		seedDependsOn(t, client, "n", uid)
	}
	writer := newVersionRepo(client)
	base := time.Now().UTC()
	for i, uid := range uids {
		_, err := writer.WriteVersions(ctx, versionWriteInput("rel-"+uid, base.Add(time.Duration(i)*time.Minute),
			[]codeversion.NodeVersion{nodeInput(uid, "sha256:"+uid)}, nil))
		require.NoError(t, err)
	}

	repo := newCodeVersionQueryRepo(client)
	ancestors, err := repo.Ancestors(ctx, "n", 1, time.Time{}, 5)
	require.NoError(t, err)
	require.Len(t, ancestors, 5, "cap must apply even though 7 ancestors changed")

	got := make([]string, len(ancestors))
	for i, a := range ancestors {
		got[i] = a.UniqueID
	}
	assert.Equal(t, []string{"a7", "a6", "a5", "a4", "a3"}, got,
		"the 5 retained are the 5 most recently changed, newest first")
}

// ---- GetCodeUnitVersions node-selector path (comment 9: batched) ----

func TestCodeVersionQueryService_GetCodeUnitVersions_ByUniqueID_BatchesAllUnits_Integration(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	writer := newVersionRepo(client)
	_, err := writer.WriteVersions(ctx, versionWriteInput("rel-1", time.Now().UTC(),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1",
			codeversion.UnitRef{UnitID: "svc:m1", Checksum: "u1"},
			codeversion.UnitRef{UnitID: "svc:m2", Checksum: "u2"},
			codeversion.UnitRef{UnitID: "svc:m3", Checksum: "u3"})},
		[]codeversion.CodeUnitVersion{
			{UnitID: "svc:m1", Checksum: "u1", Source: "one"},
			{UnitID: "svc:m2", Checksum: "u2", Source: "two"},
			{UnitID: "svc:m3", Checksum: "u3", Source: "three"},
		}))
	require.NoError(t, err)

	repo := newCodeVersionQueryRepo(client)
	svc := queries.NewCodeVersionQueryService(repo)

	got, err := svc.GetCodeUnitVersions(ctx, "", "analytics.revenue", 10)
	require.NoError(t, err)
	require.Len(t, got, 3, "all three units' chains come back from the node-selector path")
	ids := []string{got[0].UnitID, got[1].UnitID, got[2].UnitID}
	assert.ElementsMatch(t, []string{"svc:m1", "svc:m2", "svc:m3"}, ids)
}

// ---- GetCodeUnitVersions not-found (comment 4) ----

func TestCodeVersionQueryService_GetCodeUnitVersions_UnknownUnitID_Integration(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })

	repo := newCodeVersionQueryRepo(client)
	svc := queries.NewCodeVersionQueryService(repo)

	_, err := svc.GetCodeUnitVersions(ctx, "svc:ghost", "", 10)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnitNotFound))
}

func TestCodeVersionQueryService_GetCodeUnitVersions_UnknownNodeUniqueID_Integration(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })

	repo := newCodeVersionQueryRepo(client)
	svc := queries.NewCodeVersionQueryService(repo)

	_, err := svc.GetCodeUnitVersions(ctx, "", "analytics.ghost", 10)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrNodeNotFound))
}

func TestCodeVersionQueryService_GetCodeUnitVersions_KnownNodeNoUnits_Integration(t *testing.T) {
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
	svc := queries.NewCodeVersionQueryService(repo)

	got, err := svc.GetCodeUnitVersions(ctx, "", "analytics.revenue", 10)
	require.NoError(t, err, "a known node whose current version uses no shared code is OK+empty, not NOT_FOUND")
	assert.Empty(t, got)
}
