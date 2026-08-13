package neo4jinfra_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
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

// seedPrecedentRejection creates one :Rejection fixture and MERGEs its
// :ErrorSignature hub, matching the shape CaseBaseRepository.RecordRejection
// writes.
func seedPrecedentRejection(
	t *testing.T, client neo4jinfra.Neo4jClient,
	releaseID, nodeID, signature, category, reason string,
	at time.Time, rawCode, contentHash string,
) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `
		CREATE (rej:Rejection {
			release_id: $release_id, node_id: $node_id, stage: 'validation',
			category: $category, reason: $reason, error_excerpt: 'excerpt',
			dbt_log_uri: 'uri', at: $at, raw_code: $raw_code,
			content_hash: $content_hash, stub: false
		})
		MERGE (sig:ErrorSignature {signature: $signature})
		ON CREATE SET sig.category = $category, sig.reason = $reason
		MERGE (rej)-[:HAS_SIGNATURE]->(sig)
	`, map[string]any{
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
			unique_id: $uid, content_hash: $hash, promoted_at: $at, test_marker: $m,
			source_hash: $hash, shared_code_hash: '', config_hash: '',
			runtime: 'sql', raw_code: $raw_code, compiled_code: '',
			compiled_truncated: false, config_json: '{}', repo: 'org/repo',
			commit_sha: 'deadbeef', release_id: $m, version_seq: 1,
			healed: false, backfilled: false
		})
	`, map[string]any{
		"uid": uniqueID, "hash": contentHash, "at": promotedAt.UTC(),
		"m": marker, "raw_code": rawCode,
	})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// linkPrecedentResolvedBy links [:RESOLVED_BY] from one rejection to one
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
		MATCH (rej:Rejection {release_id: $release_id, node_id: $node_id})
		MATCH (v:NodeVersion {unique_id: $uid, content_hash: $hash})
		MERGE (rej)-[:RESOLVED_BY]->(v)
	`, map[string]any{
		"release_id": releaseID, "node_id": nodeID,
		"uid": versionUniqueID, "hash": versionContentHash,
	})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// seedPrecedentCurrentPointer MERGEs a :Table and points its [:CURRENT] edge
// at the version addressed by (unique_id, content_hash) — the same shape the
// detail query's res_is_current comparison reads.
func seedPrecedentCurrentPointer(
	t *testing.T, client neo4jinfra.Neo4jClient,
	tableUniqueID, versionUniqueID, versionContentHash string,
) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `
		MERGE (t:Table {unique_id: $table_uid})
		WITH t
		MATCH (v:NodeVersion {unique_id: $version_uid, content_hash: $hash})
		MERGE (t)-[:CURRENT]->(v)
	`, map[string]any{
		"table_uid": tableUniqueID, "version_uid": versionUniqueID, "hash": versionContentHash,
	})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// seedPrecedentProposal creates one :Proposal fixture linked [:PROPOSED] from
// the given rejection.
func seedPrecedentProposal(
	t *testing.T, client neo4jinfra.Neo4jClient,
	releaseID, nodeID, proposalID, prURL string, prNumber int, prState, openedBy string,
	openedAt time.Time,
) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `
		MATCH (rej:Rejection {release_id: $release_id, node_id: $node_id})
		CREATE (p:Proposal {
			proposal_id: $proposal_id, pr_url: $pr_url, pr_number: $pr_number,
			pr_state: $pr_state, opened_by: $opened_by, opened_at: $opened_at
		})
		MERGE (rej)-[:PROPOSED]->(p)
	`, map[string]any{
		"release_id": releaseID, "node_id": nodeID, "proposal_id": proposalID,
		"pr_url": prURL, "pr_number": prNumber, "pr_state": prState,
		"opened_by": openedBy, "opened_at": openedAt.UTC(),
	})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// TestPrecedentReader_MatchesBySignature verifies the signature lookup path
// and the resolved-first ordering: two rejections share signature S (one
// resolved, one open — the open one deliberately timestamped LATER than the
// resolved one, so resolved-first sorting ahead of it can only be explained
// by the "resolved" rank, not recency), and a third rejection on a different
// signature must not appear at all. The resolved row must carry its
// ResolvingVersion, the PriorVersion it superseded (next-older by
// promoted_at), and its Proposals.
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

	seedPrecedentRejection(t, client, relOpen, nodeOpen, sigS, "logic", "logic:missing_object",
		t0.Add(10*time.Minute), "select open", "hash-open")
	seedPrecedentRejection(t, client, relResolved, nodeResolved, sigS, "logic", "logic:missing_object",
		t0, "select resolved-fail", "hash-resolved-fail")
	seedPrecedentRejection(t, client, relOther, nodeOther, sigT, "syntax", "syntax:typo",
		t0, "select other", "hash-other")

	seedPrecedentVersion(t, client, nodeResolved, marker, "hash-prior", "select prior", t0.Add(time.Hour))
	seedPrecedentVersion(t, client, nodeResolved, marker, "hash-resolving", "select resolving", t0.Add(2*time.Hour))
	linkPrecedentResolvedBy(t, client, relResolved, nodeResolved, nodeResolved, "hash-resolving")

	seedPrecedentProposal(t, client, relResolved, nodeResolved, marker+"-proposal",
		"https://github.com/org/repo/pull/7", 7, "open", "remediation-agent", t0.Add(30*time.Minute))

	repo := newPrecedentReader(client)
	precedents, err := repo.Precedents(ctx, sigS, "", "", 10, true)
	require.NoError(t, err)
	require.Len(t, precedents, 2, "only signature S's two rejections match")

	resolved, open := precedents[0], precedents[1]
	assert.Equal(t, nodeResolved, resolved.Rejection.NodeID,
		"the resolved row must sort first despite being OLDER by `at`")
	assert.Equal(t, nodeOpen, open.Rejection.NodeID)

	require.NotNil(t, resolved.ResolvingVersion, "the resolved row must carry its resolving version")
	assert.Equal(t, "hash-resolving", resolved.ResolvingVersion.ContentHash)
	assert.Equal(t, "select resolving", resolved.ResolvingVersion.RawCode)
	require.NotNil(t, resolved.PriorVersion, "the resolved row must carry the version its fix superseded")
	assert.Equal(t, "hash-prior", resolved.PriorVersion.ContentHash)
	assert.Equal(t, "select prior", resolved.PriorVersion.RawCode)
	require.Len(t, resolved.Proposals, 1)
	assert.Equal(t, marker+"-proposal", resolved.Proposals[0].ProposalID)
	assert.Equal(t, "https://github.com/org/repo/pull/7", resolved.Proposals[0].PrURL)
	assert.Equal(t, 7, resolved.Proposals[0].PrNumber)
	assert.Equal(t, "open", resolved.Proposals[0].PrState)

	assert.Nil(t, open.ResolvingVersion, "the still-open row has no resolving version")
	assert.Nil(t, open.PriorVersion)
	assert.Empty(t, open.Proposals)
}

// TestPrecedentReader_MatchesByCategoryReason verifies the fallback lookup
// used when the caller has no signature: an empty signature with a
// (category, reason) pair matches every :ErrorSignature hub sharing that
// pair, regardless of the hub's own signature string, and excludes a
// rejection filed under a different category/reason.
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

	seedPrecedentRejection(t, client, relA1, nodeA1, sigA1, "logic", "logic:missing_object",
		t0, "select a1", "hash-a1")
	seedPrecedentRejection(t, client, relA2, nodeA2, sigA2, "logic", "logic:missing_object",
		t0.Add(time.Minute), "select a2", "hash-a2")
	seedPrecedentRejection(t, client, relB, nodeB, sigB, "syntax", "syntax:typo",
		t0, "select b", "hash-b")

	repo := newPrecedentReader(client)
	precedents, err := repo.Precedents(ctx, "", "logic", "logic:missing_object", 10, true)
	require.NoError(t, err)
	require.Len(t, precedents, 2, "both signatures sharing the (category, reason) pair must match")

	got := []string{precedents[0].Rejection.NodeID, precedents[1].Rejection.NodeID}
	assert.ElementsMatch(t, []string{nodeA1, nodeA2}, got)
	assert.NotContains(t, got, nodeB, "a different category/reason must not match")

	// Each row must carry its OWN signature from the graph, not the empty
	// selector the caller passed in: sigA1 and sigA2 differ even though both
	// share the (category, reason) pair that matched them.
	bySignature := map[string]string{}
	for _, p := range precedents {
		bySignature[p.Rejection.NodeID] = p.Rejection.Signature
	}
	assert.Equal(t, sigA1, bySignature[nodeA1])
	assert.Equal(t, sigA2, bySignature[nodeA2])
}

// TestPrecedentReader_LimitAppliedBeforeBodies verifies limit caps the row
// count exactly, and that resolved-first / newest-within-group ordering
// holds across the surviving rows: 4 rejections on one signature (two open,
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

	seedPrecedentRejection(t, client, rel1, node1, sig, "logic", "logic:missing_object", t0, "s1", "h1")
	seedPrecedentRejection(t, client, rel2, node2, sig, "logic", "logic:missing_object", t0.Add(time.Minute), "s2", "h2")
	seedPrecedentRejection(t, client, rel3, node3, sig, "logic", "logic:missing_object", t0.Add(2*time.Minute), "s3", "h3")
	seedPrecedentRejection(t, client, rel4, node4, sig, "logic", "logic:missing_object", t0.Add(3*time.Minute), "s4", "h4")

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
	precedents, err := repo.Precedents(ctx, sig, "", "", 2, true)
	require.NoError(t, err)
	require.Len(t, precedents, 2, "limit caps the identity query before any body is fetched")

	assert.Equal(t, node4, precedents[0].Rejection.NodeID, "resolved + newest ranks first")
	assert.Equal(t, node3, precedents[1].Rejection.NodeID, "resolved + older ranks second, ahead of every open row")

	require.NotNil(t, precedents[0].ResolvingVersion)
	assert.True(t, precedents[0].ResolvingVersion.IsCurrent,
		"node4's resolving version v4-hash is still what :CURRENT points at")
	require.NotNil(t, precedents[1].ResolvingVersion)
	assert.False(t, precedents[1].ResolvingVersion.IsCurrent,
		"node3's resolving version v3-hash was superseded by v3-superseding-hash")
}

// TestPrecedentReader_DuplicateResolvedByEdgeIsOneRow verifies the identity
// query dedups a rejection whose two writers each linked a [:RESOLVED_BY]
// edge in a race (both writers' "no existing edge" guards passed before
// either committed, so the rejection ends up with two edges to two different
// versions). Without dedup, the rejection consumes two slots in the
// resolved-first/newest LIMIT window and can appear twice in the response,
// evicting a genuine precedent.
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

	seedPrecedentRejection(t, client, rel, node, sig, "logic", "logic:missing_object",
		t0, "select failing", "hash-failing")
	seedPrecedentVersion(t, client, node, marker, "hash-race-a", "select race a", t0.Add(time.Hour))
	seedPrecedentVersion(t, client, node, marker, "hash-race-b", "select race b", t0.Add(2*time.Hour))
	linkPrecedentResolvedBy(t, client, rel, node, node, "hash-race-a")
	linkPrecedentResolvedBy(t, client, rel, node, node, "hash-race-b")

	repo := newPrecedentReader(client)
	precedents, err := repo.Precedents(ctx, sig, "", "", 10, true)
	require.NoError(t, err)
	require.Len(t, precedents, 1, "two RESOLVED_BY edges on one rejection must still yield exactly one row")
	assert.Equal(t, node, precedents[0].Rejection.NodeID)
}

// TestPrecedentReader_IncludeCodeFalseOmitsFailingCode verifies includeCode
// governs the FAILING code only: with includeCode=false the rejection's
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

	seedPrecedentRejection(t, client, rel, node, sig, "logic", "logic:missing_object",
		t0, "select failing", "hash-failing")
	seedPrecedentVersion(t, client, node, marker, "hash-prior", "select prior", t0.Add(time.Hour))
	seedPrecedentVersion(t, client, node, marker, "hash-resolving", "select resolving", t0.Add(2*time.Hour))
	linkPrecedentResolvedBy(t, client, rel, node, node, "hash-resolving")

	repo := newPrecedentReader(client)
	precedents, err := repo.Precedents(ctx, sig, "", "", 10, false)
	require.NoError(t, err)
	require.Len(t, precedents, 1)

	p := precedents[0]
	assert.Equal(t, "", p.Rejection.RawCode, "includeCode=false must blank the failing code")
	require.NotNil(t, p.ResolvingVersion)
	assert.Equal(t, "select resolving", p.ResolvingVersion.RawCode,
		"resolving-version code is always fetched — the caller renders the diff from it")
	require.NotNil(t, p.PriorVersion)
	assert.Equal(t, "select prior", p.PriorVersion.RawCode,
		"prior-version code is always fetched — the caller renders the diff from it")
}

// TestPrecedentReader_NoMatchIsEmptyNotError verifies that a signature with no
// recorded rejections is a valid, non-error answer: an empty slice, not an
// error.
func TestPrecedentReader_NoMatchIsEmptyNotError(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	repo := newPrecedentReader(client)
	precedents, err := repo.Precedents(ctx, marker+"-unknown-signature", "", "", 10, true)
	require.NoError(t, err)
	assert.Empty(t, precedents)
}
