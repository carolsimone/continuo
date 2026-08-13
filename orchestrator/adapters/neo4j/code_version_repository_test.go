package neo4jinfra_test

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/domain/codeversion"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wipeVersionFixtures clears the version graph and the topology it hangs off, so
// each test starts from a known-empty slate.
func wipeVersionFixtures(t *testing.T, client neo4jinfra.Neo4jClient) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	for _, q := range []string{
		`MATCH (n:NodeVersion) DETACH DELETE n`,
		`MATCH (n:CodeUnitVersion) DETACH DELETE n`,
		`MATCH (n:CodeUnit) DETACH DELETE n`,
		`MATCH (n:Table) DETACH DELETE n`,
		`MATCH (n:Rejection) DETACH DELETE n`,
		`MATCH (m:Meta {key:'current_release'}) DELETE m`,
	} {
		res, err := s.Run(ctx, q, nil)
		require.NoError(t, err)
		_, err = res.Consume(ctx)
		require.NoError(t, err)
	}
}

// seedVersionTable creates the :Table a version write attaches to, carrying the
// content_hash a topology swap would have stamped on it.
func seedVersionTable(t *testing.T, client neo4jinfra.Neo4jClient, uniqueID, contentHash string) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx,
		`MERGE (t:Table {unique_id: $uid}) SET t.content_hash = $ch, t.active = true`,
		map[string]any{"uid": uniqueID, "ch": contentHash})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// seedRejection creates a minimal open :Rejection fixture — no [:RESOLVED_BY]
// edge — so it is eligible for a version write's rejection-linking query.
func seedRejection(t *testing.T, client neo4jinfra.Neo4jClient, releaseID, nodeID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx,
		`CREATE (:Rejection {release_id: $release_id, node_id: $node_id, at: $at})`,
		map[string]any{"release_id": releaseID, "node_id": nodeID, "at": at})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

func newVersionRepo(client neo4jinfra.Neo4jClient) *neo4jinfra.CodeVersionRepository {
	return neo4jinfra.NewCodeVersionRepository(client,
		slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
}

func nodeInput(uniqueID, hash string, refs ...codeversion.UnitRef) codeversion.NodeVersion {
	return codeversion.NodeVersion{
		UniqueID: uniqueID, ContentHash: hash, SourceHash: "s", SharedCodeHash: "m",
		ConfigHash: "c", Runtime: "dbt", RawCode: "select 1", CompiledCode: "select 1",
		ConfigJSON: `{"materialized":"table"}`, UnitRefs: refs,
	}
}

func versionWriteInput(releaseID string, at time.Time, nodes []codeversion.NodeVersion,
	units []codeversion.CodeUnitVersion) codeversion.WriteInput {
	return codeversion.WriteInput{
		ReleaseID: releaseID, Repo: "org/svc", CommitSHA: "sha-" + releaseID,
		PromotedAt: at, Nodes: nodes, Units: units,
	}
}

// versionScalar runs a single-value read query and returns the value under `v`,
// or nil when the query matched nothing.
func versionScalar(t *testing.T, client neo4jinfra.Neo4jClient, cypher string, params map[string]any) any {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)
	res, err := s.Run(ctx, cypher, params)
	require.NoError(t, err)
	if !res.Next(ctx) {
		require.NoError(t, res.Err())
		return nil
	}
	v, _ := res.Record().Get("v")
	require.NoError(t, res.Err())
	return v
}

func TestCodeVersionRepository_FirstWriteCreatesCurrentVersion(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	repo := newVersionRepo(client)
	now := time.Now().UTC()
	res, err := repo.WriteVersions(ctx, versionWriteInput("rel-1", now,
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1")}, nil))
	require.NoError(t, err)
	assert.Equal(t, 1, res.NodeVersionsCreated)
	assert.Equal(t, 1, res.CurrentPointersMoved)
	assert.Empty(t, res.UnmatchedNodeIDs)

	assert.Equal(t, "sha256:v1", versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->(v) RETURN v.content_hash AS v`, nil))
	assert.Equal(t, int64(1), versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->(v) RETURN v.version_seq AS v`, nil))
	assert.Equal(t, "rel-1", versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->(v) RETURN v.release_id AS v`, nil))
	assert.Equal(t, "sha-rel-1", versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->(v) RETURN v.commit_sha AS v`, nil))
	assert.Equal(t, `{"materialized":"table"}`, versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->(v) RETURN v.config_json AS v`, nil))
	assert.Equal(t, false, versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->(v) RETURN v.backfilled AS v`, nil))
}

// Versions grow with edits, not with releases.
func TestCodeVersionRepository_UnchangedHashWritesNothing(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	repo := newVersionRepo(client)
	base := time.Now().UTC()
	_, err := repo.WriteVersions(ctx, versionWriteInput("rel-1", base,
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1")}, nil))
	require.NoError(t, err)

	res, err := repo.WriteVersions(ctx, versionWriteInput("rel-2", base.Add(time.Minute),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1")}, nil))
	require.NoError(t, err)
	assert.Equal(t, 0, res.NodeVersionsCreated)
	assert.Equal(t, 0, res.CurrentPointersMoved)
	assert.Equal(t, int64(1), versionScalar(t, client,
		`MATCH (v:NodeVersion {unique_id:'analytics.revenue'}) RETURN count(v) AS v`, nil))
}

func TestCodeVersionRepository_ChangedHashWritesNoPreviousEdge(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	repo := newVersionRepo(client)
	base := time.Now().UTC()
	_, err := repo.WriteVersions(ctx, versionWriteInput("rel-1", base,
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1")}, nil))
	require.NoError(t, err)
	_, err = repo.WriteVersions(ctx, versionWriteInput("rel-2", base.Add(time.Minute),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v2")}, nil))
	require.NoError(t, err)

	assert.Equal(t, "sha256:v2", versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->(v) RETURN v.content_hash AS v`, nil))
	assert.Equal(t, int64(2), versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->(v) RETURN v.version_seq AS v`, nil))
	assert.Equal(t, int64(0), versionScalar(t, client,
		`MATCH (:NodeVersion {unique_id:'analytics.revenue'})-[p:PREVIOUS]->()
		 RETURN count(p) AS v`, nil), "the retired :PREVIOUS chain must not be written")
}

// A revert must reuse the version that already exists rather than duplicate it,
// and must not write a :PREVIOUS edge for it.
func TestCodeVersionRepository_RevertMovesCurrentWithoutWritingAPreviousEdge(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	repo := newVersionRepo(client)
	base := time.Now().UTC()
	for i, h := range []string{"sha256:v1", "sha256:v2", "sha256:v1"} {
		_, err := repo.WriteVersions(ctx, versionWriteInput("rel-"+strconv.Itoa(i+1),
			base.Add(time.Duration(i)*time.Minute),
			[]codeversion.NodeVersion{nodeInput("analytics.revenue", h)}, nil))
		require.NoError(t, err)
	}

	assert.Equal(t, int64(2), versionScalar(t, client,
		`MATCH (v:NodeVersion {unique_id:'analytics.revenue'}) RETURN count(v) AS v`, nil),
		"the reverted-to version is reused, not duplicated")
	assert.Equal(t, "sha256:v1", versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->(v) RETURN v.content_hash AS v`, nil))
	assert.Equal(t, int64(0), versionScalar(t, client,
		`MATCH (:NodeVersion {unique_id:'analytics.revenue'})-[p:PREVIOUS]->()
		 RETURN count(p) AS v`, nil), "the retired :PREVIOUS chain must not be written")
	assert.Equal(t, int64(1), versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->(v) RETURN count(v) AS v`, nil),
		"exactly one :CURRENT edge")
}

// A stale redelivery may add history but must never yank the pointer backwards.
func TestCodeVersionRepository_StalePromotedAtDoesNotMoveCurrent(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:new")

	repo := newVersionRepo(client)
	now := time.Now().UTC()
	_, err := repo.WriteVersions(ctx, versionWriteInput("rel-2", now,
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:new")}, nil))
	require.NoError(t, err)

	res, err := repo.WriteVersions(ctx, versionWriteInput("rel-1", now.Add(-time.Hour),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:old")}, nil))
	require.NoError(t, err)
	assert.Equal(t, 1, res.NodeVersionsCreated, "the historical version is still recorded")
	assert.Equal(t, 0, res.CurrentPointersMoved)
	assert.Equal(t, "sha256:new", versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->(v) RETURN v.content_hash AS v`, nil))
}

func TestCodeVersionRepository_WritesSharedCodeVersionsAndUsesCodeEdges(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	repo := newVersionRepo(client)
	now := time.Now().UTC()
	res, err := repo.WriteVersions(ctx, versionWriteInput("rel-1", now,
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1",
			codeversion.UnitRef{UnitID: "svc:m1", Checksum: "u1"},
			codeversion.UnitRef{UnitID: "svc:m2", Checksum: "u2"})},
		[]codeversion.CodeUnitVersion{
			{UnitID: "svc:m1", Checksum: "u1", Source: "macro one"},
			{UnitID: "svc:m2", Checksum: "u2", Source: "macro two"},
		}))
	require.NoError(t, err)
	assert.Equal(t, 2, res.UnitVersionsCreated)

	assert.Equal(t, int64(2), versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->()-[:USES_CODE]->(u)
		 RETURN count(u) AS v`, nil))
	assert.Equal(t, "u1", versionScalar(t, client,
		`MATCH (:CodeUnit {unit_id:'svc:m1'})-[:CURRENT]->(v) RETURN v.checksum AS v`, nil))
	assert.Equal(t, "macro one", versionScalar(t, client,
		`MATCH (:CodeUnit {unit_id:'svc:m1'})-[:CURRENT]->(v) RETURN v.source AS v`, nil))
}

func TestCodeVersionRepository_SharedCodeUnitChangeWritesNoPreviousEdge(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	repo := newVersionRepo(client)
	base := time.Now().UTC()
	_, err := repo.WriteVersions(ctx, versionWriteInput("rel-1", base,
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1",
			codeversion.UnitRef{UnitID: "svc:m1", Checksum: "u1"})},
		[]codeversion.CodeUnitVersion{{UnitID: "svc:m1", Checksum: "u1", Source: "one"}}))
	require.NoError(t, err)
	_, err = repo.WriteVersions(ctx, versionWriteInput("rel-2", base.Add(time.Minute),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v2",
			codeversion.UnitRef{UnitID: "svc:m1", Checksum: "u2"})},
		[]codeversion.CodeUnitVersion{{UnitID: "svc:m1", Checksum: "u2", Source: "two"}}))
	require.NoError(t, err)

	assert.Equal(t, "u2", versionScalar(t, client,
		`MATCH (:CodeUnit {unit_id:'svc:m1'})-[:CURRENT]->(v) RETURN v.checksum AS v`, nil))
	assert.Equal(t, int64(0), versionScalar(t, client,
		`MATCH (:CodeUnitVersion {unit_id:'svc:m1'})-[p:PREVIOUS]->()
		 RETURN count(p) AS v`, nil), "the retired :PREVIOUS chain must not be written")
	assert.Equal(t, int64(1), versionScalar(t, client,
		`MATCH (c:CodeUnit {unit_id:'svc:m1'}) RETURN count(c) AS v`, nil),
		"one :CodeUnit per unit id")
}

// The topology-swap group and the version group read the same message; when the
// version group wins the race the node has no :Table yet. That must be reported,
// not silently lost.
func TestCodeVersionRepository_ReportsNodesWithNoTable(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	repo := newVersionRepo(client)
	res, err := repo.WriteVersions(ctx, versionWriteInput("rel-1", time.Now().UTC(),
		[]codeversion.NodeVersion{
			nodeInput("analytics.revenue", "sha256:v1"),
			nodeInput("analytics.missing", "sha256:v9"),
		}, nil))
	require.NoError(t, err)
	assert.Equal(t, []string{"analytics.missing"}, res.UnmatchedNodeIDs)
	assert.Equal(t, 1, res.NodeVersionsCreated)
}

// A retired node's :Table is deleted by the topology swap's orphan cleanup. Its
// versions survive as free-standing nodes, and when the node returns the pointer
// reattaches to the version that already exists.
func TestCodeVersionRepository_RetiredNodeReattachesWithoutDuplicating(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")

	repo := newVersionRepo(client)
	base := time.Now().UTC()
	_, err := repo.WriteVersions(ctx, versionWriteInput("rel-1", base,
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1")}, nil))
	require.NoError(t, err)

	// The node leaves the topology: the swap deletes its :Table along with the
	// pointer edge, leaving the version node behind.
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	res0, err := s.Run(ctx, `MATCH (t:Table {unique_id:'analytics.revenue'}) DETACH DELETE t`, nil)
	require.NoError(t, err)
	_, err = res0.Consume(ctx)
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))

	seedVersionTable(t, client, "analytics.revenue", "sha256:v1")
	res, err := repo.WriteVersions(ctx, versionWriteInput("rel-2", base.Add(time.Minute),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1")}, nil))
	require.NoError(t, err)
	assert.Equal(t, 0, res.NodeVersionsCreated, "the existing version is reused")
	assert.Equal(t, 1, res.CurrentPointersMoved)
	assert.Equal(t, int64(1), versionScalar(t, client,
		`MATCH (v:NodeVersion {unique_id:'analytics.revenue'}) RETURN count(v) AS v`, nil))
	assert.Equal(t, int64(1), versionScalar(t, client,
		`MATCH (v:NodeVersion {unique_id:'analytics.revenue'}) RETURN v.version_seq AS v`, nil),
		"the version keeps the sequence number it was created with")
}

func TestCodeVersionRepository_ReturnsGraphReleaseID(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })

	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	r, err := s.Run(ctx, `MERGE (m:Meta {key:'current_release'}) SET m.release_id = 'rel-7'`, nil)
	require.NoError(t, err)
	_, err = r.Consume(ctx)
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))

	res, err := newVersionRepo(client).WriteVersions(ctx,
		versionWriteInput("rel-7", time.Now().UTC(), nil, nil))
	require.NoError(t, err)
	assert.Equal(t, "rel-7", res.GraphReleaseID)
}

// More than one batch must behave exactly like one.
func TestCodeVersionRepository_WritesAcrossBatches(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })

	var nodes []codeversion.NodeVersion
	for i := 0; i < 250; i++ {
		uid := "analytics.n" + strconv.Itoa(i)
		seedVersionTable(t, client, uid, "sha256:h")
		nodes = append(nodes, nodeInput(uid, "sha256:h"))
	}
	res, err := newVersionRepo(client).WriteVersions(ctx,
		versionWriteInput("rel-1", time.Now().UTC(), nodes, nil))
	require.NoError(t, err)
	assert.Equal(t, 250, res.NodeVersionsCreated)
	assert.Equal(t, int64(250), versionScalar(t, client,
		`MATCH (:Table)-[:CURRENT]->(v:NodeVersion) RETURN count(v) AS v`, nil))
}

// A same-hash promotion writes no version, but it must still advance the node's
// watermark. Otherwise a transiently-delayed intermediate release, reclaimed
// later, still looks newer than the stale watermark and drags :CURRENT back onto
// code that a newer release already superseded.
func TestCodeVersionRepository_SameHashPromotionAdvancesTheStaleGuard(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:a")

	repo := newVersionRepo(client)
	base := time.Now().UTC()

	// R1 records hash A.
	_, err := repo.WriteVersions(ctx, versionWriteInput("rel-1", base,
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:a")}, nil))
	require.NoError(t, err)

	// R3 carries the same hash A and so writes no version — but must move the
	// watermark forward to its own promotion time.
	_, err = repo.WriteVersions(ctx, versionWriteInput("rel-3", base.Add(2*time.Hour),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:a")}, nil))
	require.NoError(t, err)

	// R2 was stuck in between and is reclaimed now. It is older than R3, so it
	// may record its version but must not take the pointer.
	res, err := repo.WriteVersions(ctx, versionWriteInput("rel-2", base.Add(1*time.Hour),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:b")}, nil))
	require.NoError(t, err)
	assert.Equal(t, 1, res.NodeVersionsCreated, "the intermediate version is still recorded")
	assert.Equal(t, 0, res.CurrentPointersMoved, "but it must not become current")

	assert.Equal(t, "sha256:a", versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->(v) RETURN v.content_hash AS v`, nil))
}

// A late delivery of an older release must still record its version without
// disturbing :CURRENT, and must not write a :PREVIOUS edge for it.
func TestCodeVersionRepository_OlderReleaseWritesNoPreviousEdge(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:new")

	repo := newVersionRepo(client)
	base := time.Now().UTC()

	_, err := repo.WriteVersions(ctx, versionWriteInput("rel-2", base,
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:new")}, nil))
	require.NoError(t, err)

	_, err = repo.WriteVersions(ctx, versionWriteInput("rel-1", base.Add(-time.Hour),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:old")}, nil))
	require.NoError(t, err)

	assert.Equal(t, int64(0), versionScalar(t, client,
		`MATCH (:NodeVersion {unique_id:'analytics.revenue'})-[p:PREVIOUS]->()
		 RETURN count(p) AS v`, nil),
		"the retired :PREVIOUS chain must not be written")
	assert.Equal(t, "sha256:new", versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->(v) RETURN v.content_hash AS v`, nil))
}

// The pointer guard lives in the write statement, not in a pre-write read, so a
// second writer that decided from the same earlier state cannot install a stale
// pointer over a newer one.
func TestCodeVersionRepository_PointerGuardIsEnforcedInTheWrite(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v2")

	repo := newVersionRepo(client)
	base := time.Now().UTC()

	_, err := repo.WriteVersions(ctx, versionWriteInput("rel-2", base.Add(time.Hour),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v2")}, nil))
	require.NoError(t, err)

	// Replay an older release repeatedly: each attempt re-reads the graph and
	// each must lose to the newer watermark.
	for i := 0; i < 3; i++ {
		res, err := repo.WriteVersions(ctx, versionWriteInput("rel-1", base,
			[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1")}, nil))
		require.NoError(t, err)
		assert.Equal(t, 0, res.CurrentPointersMoved, "attempt %d must not move the pointer", i)
	}
	assert.Equal(t, int64(1), versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->() RETURN count(*) AS v`, nil),
		"exactly one :CURRENT edge survives")
	assert.Equal(t, "sha256:v2", versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->(v) RETURN v.content_hash AS v`, nil))
}

// When the topology has already applied a newer promotion, a node from this
// older release is gone for good. Its history must still be recorded — just with
// nothing to attach it to — rather than retried until the message is dropped.
func TestCodeVersionRepository_GraphAheadRecordsUnattachedHistory(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })

	base := time.Now().UTC()

	// The graph reflects a NEWER release than the one being ingested.
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	r, err := s.Run(ctx,
		`MERGE (m:Meta {key:'current_release'}) SET m.release_id = 'rel-9', m.updated_at = $at`,
		map[string]any{"at": base.Add(time.Hour)})
	require.NoError(t, err)
	_, err = r.Consume(ctx)
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))

	res, err := newVersionRepo(client).WriteVersions(ctx, versionWriteInput("rel-1", base,
		[]codeversion.NodeVersion{nodeInput("analytics.retired", "sha256:gone")}, nil))
	require.NoError(t, err)
	assert.True(t, res.GraphAhead)
	assert.Equal(t, []string{"analytics.retired"}, res.UnmatchedNodeIDs)
	assert.Equal(t, 1, res.NodeVersionsCreated, "the history is recorded, not lost")

	assert.Equal(t, "sha256:gone", versionScalar(t, client,
		`MATCH (v:NodeVersion {unique_id:'analytics.retired'}) RETURN v.content_hash AS v`, nil))
	assert.Equal(t, true, versionScalar(t, client,
		`MATCH (v:NodeVersion {unique_id:'analytics.retired'}) RETURN v.backfilled AS v`, nil))
	assert.Equal(t, int64(1), versionScalar(t, client,
		`MATCH (v:NodeVersion {unique_id:'analytics.retired'}) RETURN v.version_seq AS v`, nil))
	assert.Nil(t, versionScalar(t, client,
		`MATCH (:Table)-[:CURRENT]->(v:NodeVersion {unique_id:'analytics.retired'}) RETURN v.content_hash AS v`, nil),
		"nothing points at it: there is no :Table to attach")
}

// A merely-late delivery — the graph has not yet applied this release — must
// still be reported as unmatched so the handler retries and trails the swap.
func TestCodeVersionRepository_SwapNotLandedIsNotReportedAsGraphAhead(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })

	base := time.Now().UTC()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	r, err := s.Run(ctx,
		`MERGE (m:Meta {key:'current_release'}) SET m.release_id = 'rel-0', m.updated_at = $at`,
		map[string]any{"at": base.Add(-time.Hour)})
	require.NoError(t, err)
	_, err = r.Consume(ctx)
	require.NoError(t, err)
	require.NoError(t, s.Close(ctx))

	res, err := newVersionRepo(client).WriteVersions(ctx, versionWriteInput("rel-1", base,
		[]codeversion.NodeVersion{nodeInput("analytics.pending", "sha256:x")}, nil))
	require.NoError(t, err)
	assert.False(t, res.GraphAhead, "an older topology means the swap has not landed yet")
	assert.Equal(t, []string{"analytics.pending"}, res.UnmatchedNodeIDs)
	assert.Equal(t, 0, res.NodeVersionsCreated, "nothing is written; the handler retries instead")
}

// A release's version write is the fix for every rejection filed against the
// node before that release was promoted: once the version becomes current,
// each such rejection still lacking a resolution is linked to it, a rejection
// filed after the promotion is left alone, and an already-resolved rejection
// keeps pointing at its original fix rather than being moved onto whatever
// version a later release introduces.
func TestWriteVersions_LinksOpenRejectionsToTheNewVersion(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.revenue", "sha256:v0")

	t0 := time.Now().UTC()
	seedRejection(t, client, "rel-early", "analytics.revenue", t0)
	seedRejection(t, client, "rel-late", "analytics.revenue", t0.Add(2*time.Hour))

	resolvedByQuery := `MATCH (:Rejection {release_id: $release_id, node_id: 'analytics.revenue'})-[:RESOLVED_BY]->(v)
		RETURN v.content_hash AS v`

	repo := newVersionRepo(client)
	res, err := repo.WriteVersions(ctx, versionWriteInput("rel-1", t0.Add(time.Hour),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1")}, nil))
	require.NoError(t, err)
	assert.Equal(t, 1, res.RejectionsResolved)

	assert.Equal(t, "sha256:v1", versionScalar(t, client, resolvedByQuery, map[string]any{"release_id": "rel-early"}),
		"the rejection filed before the promotion is linked to the version that just became current")
	assert.Nil(t, versionScalar(t, client, resolvedByQuery, map[string]any{"release_id": "rel-late"}),
		"a rejection dated after the promotion must not be linked")

	// A second, identical write reports no new links: the node's code is
	// unchanged, so nothing is written or linked.
	res2, err := repo.WriteVersions(ctx, versionWriteInput("rel-1", t0.Add(time.Hour),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v1")}, nil))
	require.NoError(t, err)
	assert.Equal(t, 0, res2.RejectionsResolved, "a replay of the same release must not report the link again")

	// A later release changes the node's code again. The already-resolved
	// rejection must keep pointing at its original fix rather than move onto
	// the newer version, while the still-open one becomes eligible now that
	// its own promotion has landed.
	res3, err := repo.WriteVersions(ctx, versionWriteInput("rel-2", t0.Add(3*time.Hour),
		[]codeversion.NodeVersion{nodeInput("analytics.revenue", "sha256:v2")}, nil))
	require.NoError(t, err)
	assert.Equal(t, 1, res3.RejectionsResolved, "only the still-open rejection is newly linked")

	assert.Equal(t, "sha256:v1", versionScalar(t, client, resolvedByQuery, map[string]any{"release_id": "rel-early"}),
		"an already-resolved rejection must not be moved onto a later version")
	assert.Equal(t, int64(1), versionScalar(t, client,
		`MATCH (:Rejection {release_id:'rel-early', node_id:'analytics.revenue'})-[r:RESOLVED_BY]->() RETURN count(r) AS v`, nil),
		"still exactly one [:RESOLVED_BY] edge on the already-resolved rejection")
	assert.Equal(t, "sha256:v2", versionScalar(t, client, resolvedByQuery, map[string]any{"release_id": "rel-late"}),
		"the previously-open rejection is now linked to the version that resolved it")
}

// A release whose promoted_at trails the node's watermark must not be able to
// claim it resolved anything, even when that promoted_at is still later than
// the rejection's — its code is not what the node is actually running. The
// watermark is advanced past it here by an intervening same-hash promotion,
// which writes no version and so never runs the linking query itself,
// leaving the rejection open for the late release to wrongly claim if the
// watermark guard is missing.
func TestWriteVersions_StaleRedeliveryCannotLinkAnOpenRejection(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)
	wipeVersionFixtures(t, client)
	t.Cleanup(func() { wipeVersionFixtures(t, client) })
	seedVersionTable(t, client, "analytics.watermark", "sha256:base")

	repo := newVersionRepo(client)
	t0 := time.Now().UTC()

	_, err := repo.WriteVersions(ctx, versionWriteInput("rel-base", t0,
		[]codeversion.NodeVersion{nodeInput("analytics.watermark", "sha256:base")}, nil))
	require.NoError(t, err)

	seedRejection(t, client, "rel-target", "analytics.watermark", t0.Add(30*time.Minute))

	// A same-hash promotion two hours later advances the node's watermark
	// without writing a new version, so its batch never adds this node to
	// currents and its link query never runs for it — the rejection stays
	// open.
	_, err = repo.WriteVersions(ctx, versionWriteInput("rel-samehash", t0.Add(2*time.Hour),
		[]codeversion.NodeVersion{nodeInput("analytics.watermark", "sha256:base")}, nil))
	require.NoError(t, err)

	// A late-arriving release carries an older promoted_at than the watermark
	// the same-hash promotion just set, and a different content hash, so it
	// still writes a version and includes this node in currents — even though
	// its own promoted_at (t0+1h) is after the rejection's at (t0+30m).
	res, err := repo.WriteVersions(ctx, versionWriteInput("rel-stale", t0.Add(time.Hour),
		[]codeversion.NodeVersion{nodeInput("analytics.watermark", "sha256:reverted")}, nil))
	require.NoError(t, err)
	assert.Equal(t, 0, res.CurrentPointersMoved, "the stale release must not move the pointer")
	assert.Equal(t, 0, res.RejectionsResolved,
		"the stale release's code is not what is running, so it must not resolve anything")

	assert.Nil(t, versionScalar(t, client,
		`MATCH (:Rejection {release_id:'rel-target', node_id:'analytics.watermark'})-[:RESOLVED_BY]->(v)
		 RETURN v.content_hash AS v`, nil),
		"the rejection must remain unresolved")
}
