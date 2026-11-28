package neo4jinfra_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/catalog/adapters/neo4j"
	"github.com/carolsimone/continuo/catalog/domain/casebase"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newPrecedentReader builds a PrecedentQueryRepository against the given
// client for use in these integration tests.
func newPrecedentReader(client neo4jinfra.Neo4jClient) *neo4jinfra.PrecedentQueryRepository {
	return neo4jinfra.NewPrecedentQueryRepository(client,
		slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
}

// seedFailurePrecedent creates one :FailurePrecedent fixture of
// precedentFixtureTenant and MERGEs its :ErrorSignature hub, matching the
// shape CaseBaseRepository.RecordFailurePrecedent writes.
func seedFailurePrecedent(
	t *testing.T, client neo4jinfra.Neo4jClient,
	releaseID, nodeID, signature, category, reason string,
	at time.Time, rawCode, contentHash string,
) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `
		CREATE (rej:FailurePrecedent {
			tenant_id: $tenant_id,
			release_id: $release_id, node_id: $node_id, stage: 'validation',
			category: $category, reason: $reason, error_excerpt: 'excerpt',
			dbt_log_uri: 'uri', at: $at, raw_code: $raw_code,
			content_hash: $content_hash, stub: false
		})
		MERGE (sig:ErrorSignature {tenant_id: $tenant_id, signature: $signature})
		ON CREATE SET sig.category = $category, sig.reason = $reason
		MERGE (rej)-[:HAS_SIGNATURE]->(sig)
	`, map[string]any{
		"tenant_id":  precedentFixtureTenant,
		"release_id": releaseID, "node_id": nodeID, "signature": signature,
		"category": category, "reason": reason, "at": at.UTC(),
		"raw_code": rawCode, "content_hash": contentHash,
	})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// seedPrecedentVersion creates one :NodeVersion fixture with a full property
// set, so versionViewFromProps has real values to map. contentHash must be
// distinct per uniqueID across a test (node_version_unique is on the pair).
func seedPrecedentVersion(
	t *testing.T, client neo4jinfra.Neo4jClient,
	uniqueID, marker, contentHash, rawCode string, promotedAt time.Time,
) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `
		CREATE (:NodeVersion {
			tenant_id: $tenant_id, unique_id: $uid, content_hash: $hash, promoted_at: $at, test_marker: $m,
			source_hash: $hash, shared_code_hash: '', config_hash: '',
			runtime: 'sql', raw_code: $raw_code, compiled_code: '',
			compiled_truncated: false, config_json: '{}', repo: 'org/repo',
			commit_sha: 'deadbeef', release_id: $m, version_seq: 1,
			healed: false, backfilled: false
		})
	`, map[string]any{
		"tenant_id": precedentFixtureTenant,
		"uid":       uniqueID, "hash": contentHash, "at": promotedAt.UTC(),
		"m": marker, "raw_code": rawCode,
	})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// linkPrecedentResolvedBy links [:RESOLVED_BY] from one failure precedent to one
// version, addressed by its (unique_id, content_hash) identity.
func linkPrecedentResolvedBy(
	t *testing.T, client neo4jinfra.Neo4jClient,
	releaseID, nodeID, versionUniqueID, versionContentHash string,
) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `
		MATCH (rej:FailurePrecedent {tenant_id: $tenant_id, release_id: $release_id, node_id: $node_id})
		MATCH (v:NodeVersion {tenant_id: $tenant_id, unique_id: $uid, content_hash: $hash})
		MERGE (rej)-[:RESOLVED_BY]->(v)
	`, map[string]any{
		"tenant_id":  precedentFixtureTenant,
		"release_id": releaseID, "node_id": nodeID,
		"uid": versionUniqueID, "hash": versionContentHash,
	})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// setPrecedentResolvedByPromotedAt stamps promoted_at on an existing
// [:RESOLVED_BY] edge, matching what the forward-link (code_version_repository.go)
// writes at resolution time.
func setPrecedentResolvedByPromotedAt(
	t *testing.T, client neo4jinfra.Neo4jClient,
	releaseID, nodeID, versionUniqueID, versionContentHash string, at time.Time,
) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `
		MATCH (:FailurePrecedent {tenant_id: $tenant_id, release_id: $release_id, node_id: $node_id})
		      -[rb:RESOLVED_BY]->(:NodeVersion {tenant_id: $tenant_id, unique_id: $uid, content_hash: $hash})
		SET rb.promoted_at = $at
	`, map[string]any{
		"tenant_id":  precedentFixtureTenant,
		"release_id": releaseID, "node_id": nodeID,
		"uid": versionUniqueID, "hash": versionContentHash, "at": at.UTC(),
	})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// seedPrecedentCurrentPointer MERGEs a live :Node and points its [:CURRENT]
// edge at the version addressed by (unique_id, content_hash) — the same shape
// the detail query's res_is_current comparison reads.
func seedPrecedentCurrentPointer(
	t *testing.T, client neo4jinfra.Neo4jClient,
	nodeUniqueID, versionUniqueID, versionContentHash string,
) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `
		MERGE (n:Node {tenant_id: $tenant_id, unique_id: $node_uid})
		SET n.active = true
		WITH n
		MATCH (v:NodeVersion {tenant_id: $tenant_id, unique_id: $version_uid, content_hash: $hash})
		MERGE (n)-[:CURRENT]->(v)
	`, map[string]any{
		"tenant_id": precedentFixtureTenant,
		"node_uid":  nodeUniqueID, "version_uid": versionUniqueID, "hash": versionContentHash,
	})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// seedPrecedentFix creates one :Fix fixture linked [:PROPOSED] from the
// given failure precedent, stamped with project, and its PR facts on the fix's
// :PullRequest of that project, the shape RecordFix writes.
func seedPrecedentFix(
	t *testing.T, client neo4jinfra.Neo4jClient,
	releaseID, nodeID, fixID, project, prURL string, prNumber int, prState, openedBy string,
	openedAt time.Time,
) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `
		MATCH (rej:FailurePrecedent {tenant_id: $tenant_id, release_id: $release_id, node_id: $node_id})
		CREATE (f:Fix {tenant_id: $tenant_id, fix_id: $fix_id})
		MERGE (rej)-[:PROPOSED {project: $project}]->(f)
		CREATE (f)-[:HAS_PR]->(:PullRequest {
			tenant_id: $tenant_id, fix_id: $fix_id, project: $project,
			pr_url: $pr_url, pr_number: $pr_number, pr_state: $pr_state,
			opened_by: $opened_by, opened_at: $opened_at
		})
	`, map[string]any{
		"tenant_id":  precedentFixtureTenant,
		"release_id": releaseID, "node_id": nodeID, "fix_id": fixID, "project": project,
		"pr_url": prURL, "pr_number": prNumber, "pr_state": prState,
		"opened_by": openedBy, "opened_at": openedAt.UTC(),
	})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// TestPrecedentReader_MatchesBySignature verifies the signature lookup path
// and the resolved-first ordering: two failure precedents share signature S (one
// resolved, one open — the open one deliberately timestamped LATER than the
// resolved one, so resolved-first sorting ahead of it can only be explained
// by the "resolved" rank, not recency), and a third failure precedent on a different
// signature must not appear at all. The resolved row must carry its
// ResolvingVersion, the PriorVersion it superseded (next-older by
// promoted_at), and its Fixes.
func TestPrecedentReader_MatchesBySignature(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	sigS := marker + "-sig-S"
	sigT := marker + "-sig-T"
	t0 := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)

	relOpen, nodeOpen := marker+"-rel-open", marker+"-node-open"
	relResolved, nodeResolved := marker+"-rel-resolved", marker+"-node-resolved"
	relOther, nodeOther := marker+"-rel-other", marker+"-node-other"

	seedFailurePrecedent(t, client, relOpen, nodeOpen, sigS, "logic", "logic:missing_object",
		t0.Add(10*time.Minute), "select open", "hash-open")
	seedFailurePrecedent(t, client, relResolved, nodeResolved, sigS, "logic", "logic:missing_object",
		t0, "select resolved-fail", "hash-resolved-fail")
	seedFailurePrecedent(t, client, relOther, nodeOther, sigT, "syntax", "syntax:typo",
		t0, "select other", "hash-other")

	seedPrecedentVersion(t, client, nodeResolved, marker, "hash-prior", "select prior", t0.Add(time.Hour))
	seedPrecedentVersion(t, client, nodeResolved, marker, "hash-resolving", "select resolving", t0.Add(2*time.Hour))
	linkPrecedentResolvedBy(t, client, relResolved, nodeResolved, nodeResolved, "hash-resolving")

	seedPrecedentFix(t, client, relResolved, nodeResolved, marker+"-fix", "core",
		"https://github.com/org/repo/pull/7", 7, "open", "agent-remediation", t0.Add(30*time.Minute))

	repo := newPrecedentReader(client)
	precedents, err := repo.Precedents(ctx, precedentFixtureTenant, sigS, "", "", 10, true)
	require.NoError(t, err)
	require.Len(t, precedents, 2, "only signature S's two failure precedents match")

	resolved, open := precedents[0], precedents[1]
	assert.Equal(t, nodeResolved, resolved.Failure.NodeID,
		"the resolved row must sort first despite being OLDER by `at`")
	assert.Equal(t, nodeOpen, open.Failure.NodeID)

	require.NotNil(t, resolved.ResolvingVersion, "the resolved row must carry its resolving version")
	assert.Equal(t, "hash-resolving", resolved.ResolvingVersion.ContentHash)
	assert.Equal(t, "select resolving", resolved.ResolvingVersion.RawCode)
	require.NotNil(t, resolved.PriorVersion, "the resolved row must carry the version its fix superseded")
	assert.Equal(t, "hash-prior", resolved.PriorVersion.ContentHash)
	assert.Equal(t, "select prior", resolved.PriorVersion.RawCode)
	require.Len(t, resolved.Fixes, 1)
	assert.Equal(t, marker+"-fix", resolved.Fixes[0].FixID)
	assert.Equal(t, "https://github.com/org/repo/pull/7", resolved.Fixes[0].PrURL)
	assert.Equal(t, 7, resolved.Fixes[0].PrNumber)
	assert.Equal(t, "open", resolved.Fixes[0].PrState)

	assert.Nil(t, open.ResolvingVersion, "the still-open row has no resolving version")
	assert.Nil(t, open.PriorVersion)
	assert.Empty(t, open.Fixes)
}

// TestPrecedentReader_MatchesByCategoryReason verifies the fallback lookup
// used when the caller has no signature: an empty signature with a
// (category, reason) pair matches every :ErrorSignature hub sharing that
// pair, regardless of the hub's own signature string, and excludes a
// failure precedent filed under a different category/reason.
func TestPrecedentReader_MatchesByCategoryReason(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	t0 := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	sigA1, sigA2, sigB := marker+"-sig-A1", marker+"-sig-A2", marker+"-sig-B"
	relA1, nodeA1 := marker+"-rel-a1", marker+"-node-a1"
	relA2, nodeA2 := marker+"-rel-a2", marker+"-node-a2"
	relB, nodeB := marker+"-rel-b", marker+"-node-b"

	seedFailurePrecedent(t, client, relA1, nodeA1, sigA1, "logic", "logic:missing_object",
		t0, "select a1", "hash-a1")
	seedFailurePrecedent(t, client, relA2, nodeA2, sigA2, "logic", "logic:missing_object",
		t0.Add(time.Minute), "select a2", "hash-a2")
	seedFailurePrecedent(t, client, relB, nodeB, sigB, "syntax", "syntax:typo",
		t0, "select b", "hash-b")

	repo := newPrecedentReader(client)
	precedents, err := repo.Precedents(ctx, precedentFixtureTenant, "", "logic", "logic:missing_object", 10, true)
	require.NoError(t, err)
	require.Len(t, precedents, 2, "both signatures sharing the (category, reason) pair must match")

	got := []string{precedents[0].Failure.NodeID, precedents[1].Failure.NodeID}
	assert.ElementsMatch(t, []string{nodeA1, nodeA2}, got)
	assert.NotContains(t, got, nodeB, "a different category/reason must not match")

	// Each row must carry its OWN signature from the graph, not the empty
	// selector the caller passed in: sigA1 and sigA2 differ even though both
	// share the (category, reason) pair that matched them.
	bySignature := map[string]string{}
	for _, p := range precedents {
		bySignature[p.Failure.NodeID] = p.Failure.Signature
	}
	assert.Equal(t, sigA1, bySignature[nodeA1])
	assert.Equal(t, sigA2, bySignature[nodeA2])
}

// TestPrecedentReader_LimitAppliedBeforeBodies verifies limit caps the row
// count exactly, and that resolved-first / newest-within-group ordering
// holds across the surviving rows: 4 failure precedents on one signature (two open,
// two resolved, each pair newest-last-seeded), limit 2, must return exactly
// the two resolved rows, newest resolved first.
func TestPrecedentReader_LimitAppliedBeforeBodies(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	sig := marker + "-sig"
	t0 := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)

	rel1, node1 := marker+"-rel-1", marker+"-node-1" // open, oldest
	rel2, node2 := marker+"-rel-2", marker+"-node-2" // open, newer
	rel3, node3 := marker+"-rel-3", marker+"-node-3" // resolved, older
	rel4, node4 := marker+"-rel-4", marker+"-node-4" // resolved, newest

	seedFailurePrecedent(t, client, rel1, node1, sig, "logic", "logic:missing_object", t0, "s1", "h1")
	seedFailurePrecedent(t, client, rel2, node2, sig, "logic", "logic:missing_object", t0.Add(time.Minute), "s2", "h2")
	seedFailurePrecedent(t, client, rel3, node3, sig, "logic", "logic:missing_object", t0.Add(2*time.Minute), "s3", "h3")
	seedFailurePrecedent(t, client, rel4, node4, sig, "logic", "logic:missing_object", t0.Add(3*time.Minute), "s4", "h4")

	seedPrecedentVersion(t, client, node3, marker, "v3-hash", "fix3", t0.Add(time.Hour))
	linkPrecedentResolvedBy(t, client, rel3, node3, node3, "v3-hash")
	seedPrecedentVersion(t, client, node4, marker, "v4-hash", "fix4", t0.Add(time.Hour))
	linkPrecedentResolvedBy(t, client, rel4, node4, node4, "v4-hash")

	// node3's fix (v3-hash) is later superseded by a newer deployed version;
	// node4's fix (v4-hash) is still what :CURRENT points at. This exercises
	// both IsCurrent outcomes in one test.
	seedPrecedentVersion(t, client, node3, marker, "v3-superseding-hash", "fix3-newer", t0.Add(2*time.Hour))
	seedPrecedentCurrentPointer(t, client, node3, node3, "v3-superseding-hash")
	seedPrecedentCurrentPointer(t, client, node4, node4, "v4-hash")

	repo := newPrecedentReader(client)
	precedents, err := repo.Precedents(ctx, precedentFixtureTenant, sig, "", "", 2, true)
	require.NoError(t, err)
	require.Len(t, precedents, 2, "limit caps the identity query before any body is fetched")

	assert.Equal(t, node4, precedents[0].Failure.NodeID, "resolved + newest ranks first")
	assert.Equal(t, node3, precedents[1].Failure.NodeID, "resolved + older ranks second, ahead of every open row")

	require.NotNil(t, precedents[0].ResolvingVersion)
	assert.True(t, precedents[0].ResolvingVersion.IsCurrent,
		"node4's resolving version v4-hash is still what :CURRENT points at")
	require.NotNil(t, precedents[1].ResolvingVersion)
	assert.False(t, precedents[1].ResolvingVersion.IsCurrent,
		"node3's resolving version v3-hash was superseded by v3-superseding-hash")
}

// TestPrecedentReader_DuplicateResolvedByEdgeIsOneRow verifies the identity
// query dedups a failure precedent whose two writers each linked a [:RESOLVED_BY]
// edge in a race (both writers' "no existing edge" guards passed before
// either committed, so the failure precedent ends up with two edges to two different
// versions). Without dedup, the failure precedent consumes two slots in the
// resolved-first/newest LIMIT window and can appear twice in the response,
// evicting a genuine precedent. It also verifies the detail query resolves
// the race deterministically — oldest resolving version by promoted_at wins,
// matching the back-link's own "oldest version that could have fixed it"
// rule — rather than depending on Neo4j's unspecified row order for a bare
// OPTIONAL MATCH.
func TestPrecedentReader_DuplicateResolvedByEdgeIsOneRow(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	sig := marker + "-sig"
	t0 := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	rel, node := marker+"-rel", marker+"-node"

	seedFailurePrecedent(t, client, rel, node, sig, "logic", "logic:missing_object",
		t0, "select failing", "hash-failing")
	seedPrecedentVersion(t, client, node, marker, "hash-race-a", "select race a", t0.Add(time.Hour))
	seedPrecedentVersion(t, client, node, marker, "hash-race-b", "select race b", t0.Add(2*time.Hour))
	// Linked in reverse promoted_at order, so a query that merely returns
	// "whichever row Neo4j visits last" cannot coincidentally match the
	// oldest-wins rule by matching insertion order.
	linkPrecedentResolvedBy(t, client, rel, node, node, "hash-race-b")
	linkPrecedentResolvedBy(t, client, rel, node, node, "hash-race-a")

	repo := newPrecedentReader(client)
	precedents, err := repo.Precedents(ctx, precedentFixtureTenant, sig, "", "", 10, true)
	require.NoError(t, err)
	require.Len(t, precedents, 1, "two RESOLVED_BY edges on one failure precedent must still yield exactly one row")
	assert.Equal(t, node, precedents[0].Failure.NodeID)

	require.NotNil(t, precedents[0].ResolvingVersion)
	assert.Equal(t, "hash-race-a", precedents[0].ResolvingVersion.ContentHash,
		"the OLDEST resolving version by promoted_at must win deterministically, not whichever edge Neo4j visits last")
}

// TestPrecedentReader_CategoryReasonFallbackUsesFailurePrecedentsOwnReason verifies
// the (category, reason) fallback filters on each :FailurePrecedent's own
// properties, not the shared :ErrorSignature hub's. RecordFailurePrecedent sets the
// hub's category/reason under ON CREATE only (first writer wins), so a
// second failure precedent sharing the signature but classified under a different
// reason must still be found under ITS OWN reason.
func TestPrecedentReader_CategoryReasonFallbackUsesFailurePrecedentsOwnReason(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	sig := marker + "-sig" // one shared signature, two different reasons
	t0 := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	relA, nodeA := marker+"-rel-a", marker+"-node-a"
	relB, nodeB := marker+"-rel-b", marker+"-node-b"

	caseBase := newCaseBaseRepo(client)
	// relA lands first, so the :ErrorSignature hub's category/reason freeze
	// on its values (ON CREATE only).
	require.NoError(t, caseBase.RecordFailurePrecedent(ctx, casebase.FailurePrecedent{
		ReleaseID: relA, NodeID: nodeA, Stage: "validation",
		Category: "logic", Reason: "reason-a", Signature: sig,
		ErrorExcerpt: "excerpt", DBTLogURI: "uri", At: t0,
	}))
	require.NoError(t, caseBase.RecordFailurePrecedent(ctx, casebase.FailurePrecedent{
		ReleaseID: relB, NodeID: nodeB, Stage: "validation",
		Category: "logic", Reason: "reason-b", Signature: sig,
		ErrorExcerpt: "excerpt", DBTLogURI: "uri", At: t0.Add(time.Minute),
	}))

	repo := newPrecedentReader(client)
	precedents, err := repo.Precedents(ctx, precedentFixtureTenant, "", "logic", "reason-b", 10, true)
	require.NoError(t, err)
	require.Len(t, precedents, 1, "must find the failure precedent under its OWN reason, not the hub's first-seen one")
	assert.Equal(t, nodeB, precedents[0].Failure.NodeID)
}

// TestPrecedentReader_DiffBaselineUsesEdgePromotedAt verifies the detail
// query derives the diff baseline from the [:RESOLVED_BY] edge's own
// promoted_at (set by the forward-link at resolution time) rather than the
// resolving version node's own promoted_at, which for a reverted-to version
// stays at its original, much earlier value forever (version nodes are
// immutable). Using the version's own promoted_at as the baseline would find
// no qualifying prior version at all here.
func TestPrecedentReader_DiffBaselineUsesEdgePromotedAt(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	sig := marker + "-sig"
	t0 := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	rel, node := marker+"-rel", marker+"-node"

	seedFailurePrecedent(t, client, rel, node, sig, "logic", "logic:missing_object",
		t0, "select failing", "hash-failing")

	// v0 is the reverted-to version: its own promoted_at predates even the
	// failure precedent and never changes once created.
	seedPrecedentVersion(t, client, node, marker, "hash-v0", "select v0", t0.Add(-time.Hour))
	// v2 is the version v0's revert actually replaced — the one active right
	// before the resolving (re-)promotion.
	seedPrecedentVersion(t, client, node, marker, "hash-v2", "select v2", t0.Add(2*time.Hour))

	linkPrecedentResolvedBy(t, client, rel, node, node, "hash-v0")
	// The resolution's real promotion time, long after v0's own promoted_at.
	setPrecedentResolvedByPromotedAt(t, client, rel, node, node, "hash-v0", t0.Add(3*time.Hour))

	repo := newPrecedentReader(client)
	precedents, err := repo.Precedents(ctx, precedentFixtureTenant, sig, "", "", 10, true)
	require.NoError(t, err)
	require.Len(t, precedents, 1)

	require.NotNil(t, precedents[0].PriorVersion,
		"the baseline must be the edge's promoted_at — using the reverted version's own would find no prior at all")
	assert.Equal(t, "hash-v2", precedents[0].PriorVersion.ContentHash)
}

// TestPrecedentReader_IncludeCodeFalseOmitsFailingCode verifies includeCode
// governs the FAILING code only: with includeCode=false the failure precedent's
// RawCode comes back "" even though the stored node carries raw_code, while
// the resolving and prior version bodies are still populated — the service
// renders the resolution diff from them regardless of includeCode.
func TestPrecedentReader_IncludeCodeFalseOmitsFailingCode(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	sig := marker + "-sig"
	t0 := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	rel, node := marker+"-rel", marker+"-node"

	seedFailurePrecedent(t, client, rel, node, sig, "logic", "logic:missing_object",
		t0, "select failing", "hash-failing")
	seedPrecedentVersion(t, client, node, marker, "hash-prior", "select prior", t0.Add(time.Hour))
	seedPrecedentVersion(t, client, node, marker, "hash-resolving", "select resolving", t0.Add(2*time.Hour))
	linkPrecedentResolvedBy(t, client, rel, node, node, "hash-resolving")

	repo := newPrecedentReader(client)
	precedents, err := repo.Precedents(ctx, precedentFixtureTenant, sig, "", "", 10, false)
	require.NoError(t, err)
	require.Len(t, precedents, 1)

	p := precedents[0]
	assert.Equal(t, "", p.Failure.RawCode, "includeCode=false must blank the failing code")
	require.NotNil(t, p.ResolvingVersion)
	assert.Equal(t, "select resolving", p.ResolvingVersion.RawCode,
		"resolving-version code is always fetched — the caller renders the diff from it")
	require.NotNil(t, p.PriorVersion)
	assert.Equal(t, "select prior", p.PriorVersion.RawCode,
		"prior-version code is always fetched — the caller renders the diff from it")
}

// TestPrecedentReader_FixResolvedFailurePrecedentCarriesEditedAndLivePrState
// verifies the read path widened to recognize fix-level provenance: a
// failure precedent resolved by a MERGED PR (a [:RESOLVED_BY] edge to a :Fix,
// not a :NodeVersion)
// counts as resolved by the identity query — it must sort ahead of a still-open
// failure precedent filed LATER on the same signature — and the detail path surfaces
// the merged PR's [:EDITED] provenance and the :PullRequest's live pr_state.
// The edited node (nu) is an UPSTREAM node distinct from the rejected node, so
// the test also proves the edited target is carried faithfully rather than
// assumed equal to the failure precedent node.
func TestPrecedentReader_FixResolvedFailurePrecedentCarriesEditedAndLivePrState(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	sig := marker + "-sig"
	t0 := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	relA, nodeA := marker+"-rel-a", marker+"-node-a"
	relOpen, nodeOpen := marker+"-rel-open", marker+"-node-open"
	nu := marker + "-nu" // the edited upstream :Node, distinct from nodeA
	fixID := marker + "-fix"
	closedAt := t0.Add(2 * time.Hour)

	// The fix-resolved failure precedent, filed EARLIER than the open one.
	seedFailurePrecedent(t, client, relA, nodeA, sig, "logic", "logic:missing_object",
		t0, "select failing", "hash-failing")
	// A still-open failure precedent on the same signature, filed LATER — resolved-first
	// ordering must still rank the fix-resolved failure precedent ahead of it.
	seedFailurePrecedent(t, client, relOpen, nodeOpen, sig, "logic", "logic:missing_object",
		t0.Add(30*time.Minute), "select open", "hash-open")

	// The edit's target :Node must already exist; the writer never mints one.
	seedEditTargetNode(t, client, nu, marker)

	repo := newCaseBaseRepo(client)
	require.NoError(t, repo.RecordFix(ctx,
		casebase.Fix{FixID: fixID, ReleaseID: relA, NodeID: nodeA},
		casebase.PullRequest{
			FixID: fixID, Project: "core",
			PrURL: "https://github.com/org/repo/pull/9", PrNumber: 9,
			State: "open", OpenedBy: "agent-remediation", OpenedAt: t0.Add(10 * time.Minute),
		}))
	require.NoError(t, repo.RecordPullRequestOutcome(ctx, casebase.PullRequestOutcome{
		FixID: fixID, ReleaseID: relA, Project: "core",
		Outcome: "merged", ClosedAt: closedAt,
		ResolvedNodeIDs: []string{nodeA},
		Edits: []casebase.EditOutcome{
			{Path: "models/upstream.sql", TargetNodeID: nu, Amended: false, Diff: "D1"},
		},
	}))

	reader := newPrecedentReader(client)
	precedents, err := reader.Precedents(ctx, precedentFixtureTenant, sig, "", "", 10, true)
	require.NoError(t, err)
	require.Len(t, precedents, 2, "both failure precedents on the signature match")

	// The identity query counts the [:RESOLVED_BY]->(:Fix) edge as
	// resolved: nodeA sorts first despite being OLDER by `at`.
	resolved, open := precedents[0], precedents[1]
	assert.Equal(t, nodeA, resolved.Failure.NodeID,
		"a fix-resolved failure precedent must rank resolved-first, ahead of a newer open one")
	assert.Equal(t, nodeOpen, open.Failure.NodeID)

	require.Len(t, resolved.Edited, 1, "the merged PR's EDITED provenance is surfaced")
	e := resolved.Edited[0]
	assert.Equal(t, nu, e.NodeID, "the edited (upstream) node is carried, distinct from the rejected node")
	assert.NotEqual(t, nodeA, e.NodeID)
	assert.Equal(t, "models/upstream.sql", e.Path)
	assert.False(t, e.Amended)
	assert.Equal(t, "D1", e.Diff, "a non-amended edit keeps the edge's stored fix diff")
	assert.Nil(t, e.MergedVersion, "no straddling merged version selected for a non-amended edit")
	assert.Nil(t, e.MergedPrior)

	require.Len(t, resolved.Fixes, 1)
	assert.Equal(t, fixID, resolved.Fixes[0].FixID)
	assert.Equal(t, "merged", resolved.Fixes[0].PrState,
		"pr_state comes live from the :PullRequest node")

	assert.Empty(t, open.Edited, "the still-open failure precedent carries no edited provenance")
}

// TestPrecedentReader_AmendedEditSelectsStraddlingVersions verifies the new
// per-key edited query's straddling-version selection: for an AMENDED edit
// whose target :Node has :NodeVersions promoted both before and after the PR
// closed, MergedVersion is the FIRST version promoted after closed_at
// (ascending) and MergedPrior is the NEWEST version promoted before it — the
// two versions that bracket the human's merged truth.
func TestPrecedentReader_AmendedEditSelectsStraddlingVersions(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	sig := marker + "-sig"
	t0 := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	relC, nodeC := marker+"-rel-c", marker+"-node-c"
	nu := marker + "-nu"
	fixID := marker + "-fix"
	closedAt := t0.Add(2 * time.Hour)

	seedFailurePrecedent(t, client, relC, nodeC, sig, "logic", "logic:missing_object",
		t0, "select failing", "hash-failing")
	seedEditTargetNode(t, client, nu, marker)

	// Four versions on the edited node straddling closed_at. The selection must
	// pick "hash-after" (first after closed_at) as merged and "hash-before"
	// (newest before merged) as prior — never the way-before/way-after decoys.
	seedPrecedentVersion(t, client, nu, marker, "hash-way-before", "select way before", closedAt.Add(-3*time.Hour))
	seedPrecedentVersion(t, client, nu, marker, "hash-before", "select before", closedAt.Add(-1*time.Hour))
	seedPrecedentVersion(t, client, nu, marker, "hash-after", "select after", closedAt.Add(1*time.Hour))
	seedPrecedentVersion(t, client, nu, marker, "hash-way-after", "select way after", closedAt.Add(3*time.Hour))

	repo := newCaseBaseRepo(client)
	require.NoError(t, repo.RecordFix(ctx,
		casebase.Fix{FixID: fixID, ReleaseID: relC, NodeID: nodeC},
		casebase.PullRequest{
			FixID: fixID, Project: "core",
			PrURL: "https://github.com/org/repo/pull/11", PrNumber: 11,
			State: "open", OpenedBy: "agent-remediation", OpenedAt: t0.Add(10 * time.Minute),
		}))
	require.NoError(t, repo.RecordPullRequestOutcome(ctx, casebase.PullRequestOutcome{
		FixID: fixID, ReleaseID: relC, Project: "core",
		Outcome: "merged", ClosedAt: closedAt,
		ResolvedNodeIDs: []string{nodeC},
		Edits: []casebase.EditOutcome{
			{Path: "models/amended.sql", TargetNodeID: nu, Amended: true, Diff: "Dc"},
		},
	}))

	reader := newPrecedentReader(client)
	precedents, err := reader.Precedents(ctx, precedentFixtureTenant, sig, "", "", 10, true)
	require.NoError(t, err)
	require.Len(t, precedents, 1)

	require.Len(t, precedents[0].Edited, 1)
	e := precedents[0].Edited[0]
	assert.True(t, e.Amended)
	require.NotNil(t, e.MergedVersion, "an amended edit with a straddling version selects the merged truth")
	assert.Equal(t, "hash-after", e.MergedVersion.ContentHash,
		"the FIRST version promoted after closed_at (ascending) is the merged version")
	assert.Equal(t, "select after", e.MergedVersion.RawCode)
	require.NotNil(t, e.MergedPrior, "the version the merge superseded is the newest one before it")
	assert.Equal(t, "hash-before", e.MergedPrior.ContentHash)
	assert.Equal(t, "select before", e.MergedPrior.RawCode)
}

// TestPrecedentReader_FixResolvedWithAllEditsAbsentSurfacesResolvedByFix
// verifies the detail query surfaces the fix-resolved signal even when the
// merged PR drew NO [:EDITED] edges — every edit target was absent from the
// graph, so RecordPullRequestOutcome skipped them (it never mints a :Node).
// The failure precedent is genuinely fixed and the identity query already counts it
// resolved-first; ResolvedByFix must be true so the service's Resolved
// boolean agrees, with no Edited rows and no own-timeline version.
func TestPrecedentReader_FixResolvedWithAllEditsAbsentSurfacesResolvedByFix(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	sig := marker + "-sig"
	t0 := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	relA, nodeA := marker+"-rel-a", marker+"-node-a"
	fixID := marker + "-fix"
	missing := marker + "-missing" // edit target that is NOT in the graph
	closedAt := t0.Add(2 * time.Hour)

	seedFailurePrecedent(t, client, relA, nodeA, sig, "logic", "logic:missing_object",
		t0, "select failing", "hash-failing")

	repo := newCaseBaseRepo(client)
	require.NoError(t, repo.RecordFix(ctx,
		casebase.Fix{FixID: fixID, ReleaseID: relA, NodeID: nodeA},
		casebase.PullRequest{
			FixID: fixID, Project: "core",
			PrURL: "https://github.com/org/repo/pull/13", PrNumber: 13,
			State: "open", OpenedBy: "agent-remediation", OpenedAt: t0.Add(10 * time.Minute),
		}))
	// Merged: resolves nodeA, but the sole edit's target :Node is absent, so no
	// [:EDITED] edge is drawn.
	require.NoError(t, repo.RecordPullRequestOutcome(ctx, casebase.PullRequestOutcome{
		FixID: fixID, ReleaseID: relA, Project: "core",
		Outcome: "merged", ClosedAt: closedAt,
		ResolvedNodeIDs: []string{nodeA},
		Edits: []casebase.EditOutcome{
			{Path: "models/absent.sql", TargetNodeID: missing, Amended: false, Diff: "D-absent"},
		},
	}))

	reader := newPrecedentReader(client)
	precedents, err := reader.Precedents(ctx, precedentFixtureTenant, sig, "", "", 10, true)
	require.NoError(t, err)
	require.Len(t, precedents, 1)

	p := precedents[0]
	assert.True(t, p.ResolvedByFix,
		"a RESOLVED_BY->(:Fix) edge must surface even when no EDITED edge was drawn")
	assert.Empty(t, p.Edited, "no EDITED edge was drawn for an absent target")
	assert.Nil(t, p.ResolvingVersion, "there is no own-timeline resolving version")
}

// TestPrecedentReader_UnresolvedFailurePrecedentIsNotResolvedByFix pins the
// negative: a failure precedent with no [:RESOLVED_BY] edge at all reports
// ResolvedByFix=false.
func TestPrecedentReader_UnresolvedFailurePrecedentIsNotResolvedByFix(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	sig := marker + "-sig"
	t0 := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	rel, node := marker+"-rel", marker+"-node"

	seedFailurePrecedent(t, client, rel, node, sig, "logic", "logic:missing_object",
		t0, "select failing", "hash-failing")

	reader := newPrecedentReader(client)
	precedents, err := reader.Precedents(ctx, precedentFixtureTenant, sig, "", "", 10, true)
	require.NoError(t, err)
	require.Len(t, precedents, 1)

	assert.False(t, precedents[0].ResolvedByFix, "an unresolved failure precedent has no fix edge")
	assert.Nil(t, precedents[0].ResolvingVersion)
	assert.Empty(t, precedents[0].Edited)
}

// TestPrecedentReader_MultiProjectFixScopesEditedProvenancePerProject
// verifies the per-resolving-project scoping of edited-node provenance. A single
// :Fix spans two projects: a core failure precedent resolved by core's merged PR
// (a non-amended edit to a core node) and a finance failure precedent resolved by
// finance's merged PR (an amended edit to a finance node). Each failure precedent links
// to the SHARED :Fix, so without scoping the read would walk EVERY [:EDITED]
// edge and EVERY merged :PullRequest on that fix. The RESOLVED_BY edge's
// project stamp scopes the walk: the core failure precedent surfaces only the core edit,
// the finance failure precedent only the finance edit, and the finance amended straddle
// is selected against finance's own :PullRequest closed_at — never core's
// earlier one.
func TestPrecedentReader_MultiProjectFixScopesEditedProvenancePerProject(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	sig := marker + "-sig"
	t0 := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	fixID := marker + "-fix"

	coreRel, coreNode := marker+"-core-rel", marker+"-core-node"
	finRel, finNode := marker+"-fin-rel", marker+"-fin-node"
	coreEdited := marker + "-core-edited"
	finEdited := marker + "-fin-edited"

	coreClosed := t0.Add(2 * time.Hour) // Tc — earlier close
	finClosed := t0.Add(5 * time.Hour)  // Tf — later close

	// Two failure precedents on the SAME signature, resolved by the SAME fix but by
	// different projects' PRs.
	seedFailurePrecedent(t, client, coreRel, coreNode, sig, "logic", "logic:missing_object",
		t0, "select core", "hash-core")
	seedFailurePrecedent(t, client, finRel, finNode, sig, "logic", "logic:missing_object",
		t0.Add(time.Minute), "select fin", "hash-fin")

	seedEditTargetNode(t, client, coreEdited, marker)
	seedEditTargetNode(t, client, finEdited, marker)

	// Versions on the finance-edited node straddling the two closes. Scoped to
	// finance's closed_at (Tf), only "hash-after-tf" qualifies as the merged
	// version; were the read to leak core's earlier closed_at (Tc), the
	// "hash-between" version (promoted after Tc but before Tf) would wrongly win.
	seedPrecedentVersion(t, client, finEdited, marker, "hash-between", "select between", t0.Add(3*time.Hour))
	seedPrecedentVersion(t, client, finEdited, marker, "hash-after-tf", "select after tf", t0.Add(6*time.Hour))

	repo := newCaseBaseRepo(client)

	// pr_opened per project, sharing the one fix.
	require.NoError(t, repo.RecordFix(ctx,
		casebase.Fix{FixID: fixID, ReleaseID: coreRel, NodeID: coreNode},
		casebase.PullRequest{
			FixID: fixID, Project: "core",
			PrURL: "https://github.com/org/core/pull/1", PrNumber: 1,
			State: "open", OpenedBy: "agent-remediation", OpenedAt: t0.Add(30 * time.Minute),
		}))
	require.NoError(t, repo.RecordFix(ctx,
		casebase.Fix{FixID: fixID, ReleaseID: finRel, NodeID: finNode},
		casebase.PullRequest{
			FixID: fixID, Project: "finance",
			PrURL: "https://github.com/org/finance/pull/1", PrNumber: 1,
			State: "open", OpenedBy: "agent-remediation", OpenedAt: t0.Add(30 * time.Minute),
		}))

	// core PR merges: edits the core node, not amended.
	require.NoError(t, repo.RecordPullRequestOutcome(ctx, casebase.PullRequestOutcome{
		FixID: fixID, ReleaseID: coreRel, Project: "core",
		Outcome: "merged", ClosedAt: coreClosed,
		PrURL: "https://github.com/org/core/pull/1", PrNumber: 1,
		ResolvedNodeIDs: []string{coreNode},
		Edits: []casebase.EditOutcome{
			{Path: "models/core.sql", TargetNodeID: coreEdited, Amended: false, Diff: "D-core"},
		},
	}))
	// finance PR merges: edits the finance node, amended.
	require.NoError(t, repo.RecordPullRequestOutcome(ctx, casebase.PullRequestOutcome{
		FixID: fixID, ReleaseID: finRel, Project: "finance",
		Outcome: "merged", ClosedAt: finClosed,
		PrURL: "https://github.com/org/finance/pull/1", PrNumber: 1,
		ResolvedNodeIDs: []string{finNode},
		Edits: []casebase.EditOutcome{
			{Path: "models/finance.sql", TargetNodeID: finEdited, Amended: true, Diff: "D-fin"},
		},
	}))

	reader := newPrecedentReader(client)
	precedents, err := reader.Precedents(ctx, precedentFixtureTenant, sig, "", "", 10, true)
	require.NoError(t, err)
	require.Len(t, precedents, 2, "both failure precedents on the signature match")

	byNode := map[string]casebase.PrecedentView{}
	for _, p := range precedents {
		byNode[p.Failure.NodeID] = p
	}
	coreP, ok := byNode[coreNode]
	require.True(t, ok, "the core failure precedent is present")
	finP, ok := byNode[finNode]
	require.True(t, ok, "the finance failure precedent is present")

	// The core failure precedent surfaces ONLY the core edit — never finance's.
	require.Len(t, coreP.Edited, 1, "core failure precedent surfaces exactly its own project's edit")
	assert.Equal(t, coreEdited, coreP.Edited[0].NodeID, "core must not surface finance's edited node")
	assert.Equal(t, "D-core", coreP.Edited[0].Diff)
	assert.False(t, coreP.Edited[0].Amended)
	assert.Nil(t, coreP.Edited[0].MergedVersion, "a non-amended edit selects no straddling version")

	// The finance failure precedent surfaces ONLY the finance edit, and selects the
	// straddling merged version against finance's own PR closed_at.
	require.Len(t, finP.Edited, 1, "finance failure precedent surfaces exactly its own project's edit")
	fe := finP.Edited[0]
	assert.Equal(t, finEdited, fe.NodeID, "finance must not surface core's edited node")
	assert.Equal(t, "D-fin", fe.Diff)
	assert.True(t, fe.Amended)
	require.NotNil(t, fe.MergedVersion, "an amended edit selects the merged-truth version")
	assert.Equal(t, "hash-after-tf", fe.MergedVersion.ContentHash,
		"the merged version is selected against finance's own closed_at, not core's earlier one")
	require.NotNil(t, fe.MergedPrior, "the version the merge superseded is the newest before it")
	assert.Equal(t, "hash-between", fe.MergedPrior.ContentHash)
}

// TestPrecedentReader_MultiProjectFixScopesFixPRsPerProject verifies
// the per-proposing-project scoping of a precedent's PR facts. A single
// :Fix spans two projects: a core failure precedent whose fix PR is core's, and a
// finance failure precedent whose fix PR is finance's. Both failure precedents link to the
// SHARED :Fix, which carries both projects' :PullRequest nodes. Without
// scoping the read would fan EVERY :PullRequest into EVERY failure precedent's Fixes
// list — a core failure precedent would surface finance's PR url/number/state (and a
// duplicate fix entry). The [:PROPOSED] edge's project stamp scopes the PR
// join: the core failure precedent surfaces only core's PR, the finance failure precedent only
// finance's.
func TestPrecedentReader_MultiProjectFixScopesFixPRsPerProject(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	sig := marker + "-sig"
	t0 := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	fixID := marker + "-fix"

	coreRel, coreNode := marker+"-core-rel", marker+"-core-node"
	finRel, finNode := marker+"-fin-rel", marker+"-fin-node"

	// Two failure precedents on the SAME signature, resolved by the SAME fix but by
	// different projects' PRs.
	seedFailurePrecedent(t, client, coreRel, coreNode, sig, "logic", "logic:missing_object",
		t0, "select core", "hash-core")
	seedFailurePrecedent(t, client, finRel, finNode, sig, "logic", "logic:missing_object",
		t0.Add(time.Minute), "select fin", "hash-fin")

	repo := newCaseBaseRepo(client)

	// pr_opened per project, sharing the one fix. Each RecordFix stamps
	// its project on the [:PROPOSED] edge and lands its PR on its own
	// :PullRequest node.
	require.NoError(t, repo.RecordFix(ctx,
		casebase.Fix{FixID: fixID, ReleaseID: coreRel, NodeID: coreNode},
		casebase.PullRequest{
			FixID: fixID, Project: "core",
			PrURL: "https://github.com/org/core/pull/1", PrNumber: 1,
			State: "open", OpenedBy: "agent-remediation", OpenedAt: t0.Add(30 * time.Minute),
		}))
	require.NoError(t, repo.RecordFix(ctx,
		casebase.Fix{FixID: fixID, ReleaseID: finRel, NodeID: finNode},
		casebase.PullRequest{
			FixID: fixID, Project: "finance",
			PrURL: "https://github.com/org/finance/pull/7", PrNumber: 7,
			State: "open", OpenedBy: "agent-remediation", OpenedAt: t0.Add(30 * time.Minute),
		}))

	reader := newPrecedentReader(client)
	precedents, err := reader.Precedents(ctx, precedentFixtureTenant, sig, "", "", 10, true)
	require.NoError(t, err)
	require.Len(t, precedents, 2, "both failure precedents on the signature match")

	byNode := map[string]casebase.PrecedentView{}
	for _, p := range precedents {
		byNode[p.Failure.NodeID] = p
	}
	coreP, ok := byNode[coreNode]
	require.True(t, ok, "the core failure precedent is present")
	finP, ok := byNode[finNode]
	require.True(t, ok, "the finance failure precedent is present")

	// The core failure precedent surfaces ONLY core's PR — never finance's, and never a
	// duplicate.
	require.Len(t, coreP.Fixes, 1, "core failure precedent surfaces exactly its own project's PR")
	assert.Equal(t, fixID, coreP.Fixes[0].FixID)
	assert.Equal(t, "https://github.com/org/core/pull/1", coreP.Fixes[0].PrURL,
		"core must not surface finance's PR url")
	assert.Equal(t, 1, coreP.Fixes[0].PrNumber)
	assert.Equal(t, "open", coreP.Fixes[0].PrState)

	// The finance failure precedent surfaces ONLY finance's PR.
	require.Len(t, finP.Fixes, 1, "finance failure precedent surfaces exactly its own project's PR")
	assert.Equal(t, "https://github.com/org/finance/pull/7", finP.Fixes[0].PrURL,
		"finance must not surface core's PR url")
	assert.Equal(t, 7, finP.Fixes[0].PrNumber)
}

// TestPrecedentReader_NoMatchIsEmptyNotError verifies that a signature with no
// recorded failure precedents is a valid, non-error answer: an empty slice, not an
// error.
func TestPrecedentReader_NoMatchIsEmptyNotError(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	repo := newPrecedentReader(client)
	precedents, err := repo.Precedents(ctx, precedentFixtureTenant, marker+"-unknown-signature", "", "", 10, true)
	require.NoError(t, err)
	assert.Empty(t, precedents)
}
