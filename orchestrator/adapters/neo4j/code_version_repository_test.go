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

func TestCodeVersionRepository_ChangedHashChainsPrevious(t *testing.T) {
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
	assert.Equal(t, "sha256:v1", versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->()-[:PREVIOUS]->(p)
		 RETURN p.content_hash AS v`, nil))
	assert.Equal(t, int64(2), versionScalar(t, client,
		`MATCH (:Table {unique_id:'analytics.revenue'})-[:CURRENT]->(v) RETURN v.version_seq AS v`, nil))
}

// A revert must not close the chain into a cycle: the version already exists, so
// only the pointer moves.
func TestCodeVersionRepository_RevertMovesCurrentWithoutCreatingACycle(t *testing.T) {
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
		`MATCH (a:NodeVersion {unique_id:'analytics.revenue'})-[:PREVIOUS]->(b)-[:PREVIOUS]->(a)
		 RETURN count(a) AS v`, nil), "no :PREVIOUS cycle")
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

func TestCodeVersionRepository_SharedCodeUnitChainsOnChecksumChange(t *testing.T) {
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
	assert.Equal(t, "u1", versionScalar(t, client,
		`MATCH (:CodeUnit {unit_id:'svc:m1'})-[:CURRENT]->()-[:PREVIOUS]->(p) RETURN p.checksum AS v`, nil))
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

// A late delivery of an older release must not claim to supersede the newer code
// that is already current: that would reverse the chain's supersession order.
func TestCodeVersionRepository_OlderReleaseDoesNotChainOverNewerCode(t *testing.T) {
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
		`MATCH (:NodeVersion {unique_id:'analytics.revenue', content_hash:'sha256:old'})
		       -[:PREVIOUS]->(:NodeVersion {content_hash:'sha256:new'})
		 RETURN count(*) AS v`, nil),
		"the older version must not point at the newer one as its predecessor")
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
