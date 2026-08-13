package neo4jinfra_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/domain/casebase"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newCaseBaseRepo constructs a CaseBaseRepository against the given client for
// use in these integration tests.
func newCaseBaseRepo(client neo4jinfra.Neo4jClient) *neo4jinfra.CaseBaseRepository {
	return neo4jinfra.NewCaseBaseRepository(client,
		slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
}

// caseBaseCleanup returns a closure that removes every node a case-base test
// could have touched. Fixture nodes created directly by the test carry
// test_marker; nodes the repository itself writes (:Rejection, :ErrorSignature,
// :Proposal) carry no such property, so they are caught instead by the marker
// embedded in their identity values (release_id, node_id, unique_id, signature,
// proposal_id).
func caseBaseCleanup(t *testing.T, client neo4jinfra.Neo4jClient, marker string) func() {
	t.Helper()
	ctx := context.Background()
	return func() {
		s := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer s.Close(ctx)
		_, _ = s.Run(ctx, `
			MATCH (n)
			WHERE n.test_marker = $m
			   OR n.release_id CONTAINS $m
			   OR n.node_id CONTAINS $m
			   OR n.unique_id CONTAINS $m
			   OR n.signature CONTAINS $m
			   OR n.proposal_id CONTAINS $m
			DETACH DELETE n
		`, map[string]any{"m": marker})
	}
}

// runScalar runs a single aggregate-only read query (e.g. `RETURN count(x) AS
// c`) and returns the value bound to alias. Cypher aggregation with no
// grouping keys always yields exactly one row, so this never needs to handle a
// missing result.
func runScalar(t *testing.T, client neo4jinfra.Neo4jClient, cypher string, params map[string]any, alias string) any {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeRead)
	defer s.Close(ctx)
	res, err := s.Run(ctx, cypher, params)
	require.NoError(t, err)
	require.True(t, res.Next(ctx))
	v, _ := res.Record().Get(alias)
	require.NoError(t, res.Err())
	return v
}

// seedNodeVersion creates one :NodeVersion fixture row. contentHash must be
// distinct per uniqueID across a test (node_version_unique is keyed on the
// pair), which is why every caller passes its own literal.
func seedNodeVersion(t *testing.T, client neo4jinfra.Neo4jClient, uniqueID, marker, contentHash string, promotedAt time.Time) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx,
		`CREATE (:NodeVersion {unique_id: $uid, content_hash: $hash, promoted_at: $at, test_marker: $m})`,
		map[string]any{"uid": uniqueID, "hash": contentHash, "at": promotedAt, "m": marker})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// TestCaseBaseRepository_RecordRejectionWritesRejectionAndSignature verifies
// that RecordRejection writes one :Rejection with all its properties, MERGEs
// one globally-shared :ErrorSignature hub node keyed on signature, and links
// them with one [:HAS_SIGNATURE] edge — and that calling it twice with
// identical input converges rather than duplicating any of that.
func TestCaseBaseRepository_RecordRejectionWritesRejectionAndSignature(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	repo := newCaseBaseRepo(client)

	rej := casebase.Rejection{
		ReleaseID:    marker + "-rel-1",
		NodeID:       marker + "-node-1",
		Stage:        "validation",
		Category:     "null_constraint",
		Reason:       "not-null violation on column x",
		Signature:    marker + "-sig-1",
		ErrorExcerpt: "ERROR: null value in column \"x\" violates not-null constraint",
		DBTLogURI:    "s3://bucket/logs/" + marker + ".log",
		At:           time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC),
		RawCode:      "select 1 as x",
		ContentHash:  "hash-1",
	}

	require.NoError(t, repo.RecordRejection(ctx, rej))
	require.NoError(t, repo.RecordRejection(ctx, rej), "second call with identical input must converge")

	session := client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	res, err := session.Run(ctx, `
		MATCH (rej:Rejection {release_id: $release_id, node_id: $node_id})
		RETURN rej.stage AS stage, rej.category AS category, rej.reason AS reason,
		       rej.error_excerpt AS error_excerpt, rej.dbt_log_uri AS dbt_log_uri,
		       rej.at AS at, rej.raw_code AS raw_code, rej.content_hash AS content_hash
	`, map[string]any{"release_id": rej.ReleaseID, "node_id": rej.NodeID})
	require.NoError(t, err)
	records, err := res.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, records, 1, "exactly one :Rejection node")

	rec := records[0]
	stage, _ := rec.Get("stage")
	category, _ := rec.Get("category")
	reason, _ := rec.Get("reason")
	errorExcerpt, _ := rec.Get("error_excerpt")
	dbtLogURI, _ := rec.Get("dbt_log_uri")
	at, _ := rec.Get("at")
	rawCode, _ := rec.Get("raw_code")
	contentHash, _ := rec.Get("content_hash")

	assert.Equal(t, rej.Stage, stage)
	assert.Equal(t, rej.Category, category)
	assert.Equal(t, rej.Reason, reason)
	assert.Equal(t, rej.ErrorExcerpt, errorExcerpt)
	assert.Equal(t, rej.DBTLogURI, dbtLogURI)
	atGot, ok := at.(time.Time)
	require.True(t, ok, "at must round-trip as a time.Time, got %T", at)
	assert.True(t, rej.At.Equal(atGot), "at round-trips; want %v got %v", rej.At, atGot)
	assert.Equal(t, rej.RawCode, rawCode)
	assert.Equal(t, rej.ContentHash, contentHash)

	sigRes, err := session.Run(ctx, `
		MATCH (:Rejection {release_id: $release_id, node_id: $node_id})-[:HAS_SIGNATURE]->(sig:ErrorSignature {signature: $signature})
		RETURN sig.category AS category, sig.reason AS reason
	`, map[string]any{"release_id": rej.ReleaseID, "node_id": rej.NodeID, "signature": rej.Signature})
	require.NoError(t, err)
	sigRecords, err := sigRes.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, sigRecords, 1, "exactly one [:HAS_SIGNATURE] edge to the signature hub")
	sigCategory, _ := sigRecords[0].Get("category")
	sigReason, _ := sigRecords[0].Get("reason")
	assert.Equal(t, rej.Category, sigCategory)
	assert.Equal(t, rej.Reason, sigReason)

	// The signature hub is MERGEd on `signature` alone — confirm the two
	// RecordRejection calls did not mint a second :ErrorSignature.
	sigCount := runScalar(t, client,
		`MATCH (s:ErrorSignature {signature: $signature}) RETURN count(s) AS c`,
		map[string]any{"signature": rej.Signature}, "c")
	assert.EqualValues(t, 1, sigCount)
}

// TestCaseBaseRepository_FailedEdgeOnlyWhenTableExists verifies the
// FOREACH-guarded [:FAILED] anchor: when the node's :Table already exists in
// the topology, RecordRejection links it; when it does not, RecordRejection
// still records the rejection but must neither create the edge nor mint a
// :Table — that lifecycle belongs solely to the topology handler.
func TestCaseBaseRepository_FailedEdgeOnlyWhenTableExists(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	repo := newCaseBaseRepo(client)

	// Case A: the node's :Table already exists in the topology.
	releaseA := marker + "-rel-a"
	nodeA := marker + "-node-a"
	seed := client.NewSession(ctx, neo4j.AccessModeWrite)
	seedRes, err := seed.Run(ctx,
		`CREATE (:Table {unique_id: $uid, test_marker: $m})`,
		map[string]any{"uid": nodeA, "m": marker})
	require.NoError(t, err)
	_, err = seedRes.Consume(ctx)
	require.NoError(t, err)
	seed.Close(ctx)

	require.NoError(t, repo.RecordRejection(ctx, casebase.Rejection{
		ReleaseID: releaseA, NodeID: nodeA, Stage: "validation",
		Category: "cat", Reason: "reason", Signature: marker + "-sig-a",
		ErrorExcerpt: "excerpt", DBTLogURI: "uri", At: time.Now().UTC(),
	}))

	edgesA := runScalar(t, client, `
		MATCH (t:Table {unique_id: $uid})-[f:FAILED {release_id: $release_id}]->(rej:Rejection {release_id: $release_id, node_id: $uid})
		RETURN count(f) AS c
	`, map[string]any{"uid": nodeA, "release_id": releaseA}, "c")
	assert.EqualValues(t, 1, edgesA, "Case A: [:FAILED] edge must exist when the :Table exists")

	// Case B: no :Table exists for this node.
	releaseB := marker + "-rel-b"
	nodeB := marker + "-node-b"
	require.NoError(t, repo.RecordRejection(ctx, casebase.Rejection{
		ReleaseID: releaseB, NodeID: nodeB, Stage: "validation",
		Category: "cat", Reason: "reason", Signature: marker + "-sig-b",
		ErrorExcerpt: "excerpt", DBTLogURI: "uri", At: time.Now().UTC(),
	}))

	rejCountB := runScalar(t, client,
		`MATCH (rej:Rejection {release_id: $release_id, node_id: $node_id}) RETURN count(rej) AS c`,
		map[string]any{"release_id": releaseB, "node_id": nodeB}, "c")
	assert.EqualValues(t, 1, rejCountB, "Case B: :Rejection must still be recorded with no :Table present")

	edgesB := runScalar(t, client,
		`MATCH (:Rejection {release_id: $release_id, node_id: $node_id})<-[f:FAILED]-() RETURN count(f) AS c`,
		map[string]any{"release_id": releaseB, "node_id": nodeB}, "c")
	assert.EqualValues(t, 0, edgesB, "Case B: no [:FAILED] edge without a :Table")

	tableCountB := runScalar(t, client,
		`MATCH (t:Table {unique_id: $uid}) RETURN count(t) AS c`,
		map[string]any{"uid": nodeB}, "c")
	assert.EqualValues(t, 0, tableCountB, "Case B: RecordRejection must never create a :Table")
}

// TestCaseBaseRepository_BackLinksResolvedByWhenNewerVersionExists verifies
// the late-arrival back-link: when the fix has already been promoted by the
// time RecordRejection runs, it links [:RESOLVED_BY] to the OLDEST
// :NodeVersion promoted after the rejection's At — not the newest — and a
// later call must not move an already-established link, even when a
// still-later version arrives that would itself rank first by promoted_at.
func TestCaseBaseRepository_BackLinksResolvedByWhenNewerVersionExists(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	repo := newCaseBaseRepo(client)

	nodeID := marker + "-node"
	releaseID := marker + "-rel"
	t0 := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)

	rejection := casebase.Rejection{
		ReleaseID: releaseID, NodeID: nodeID, Stage: "validation",
		Category: "cat", Reason: "reason", Signature: marker + "-sig",
		ErrorExcerpt: "excerpt", DBTLogURI: "uri", At: t0,
	}

	// Seed seq1 (excluded — older than At) together with seq2 AND seq3 (both
	// newer than At) *before* the first RecordRejection call, so two
	// candidates qualify simultaneously at MERGE time. With only one
	// qualifying candidate present, any LIMIT 1 picks it regardless of sort
	// direction; with two present, only `ORDER BY promoted_at ASC` — not
	// DESC — can pick the older one.
	seedNodeVersion(t, client, nodeID, marker, "hash-seq1", t0.Add(-time.Hour))
	seedNodeVersion(t, client, nodeID, marker, "hash-seq2", t0.Add(time.Hour))
	seedNodeVersion(t, client, nodeID, marker, "hash-seq3", t0.Add(2*time.Hour))

	require.NoError(t, repo.RecordRejection(ctx, rejection))

	resolvedByQuery := `
		MATCH (:Rejection {release_id: $release_id, node_id: $node_id})-[:RESOLVED_BY]->(v:NodeVersion)
		RETURN v.content_hash AS c
	`
	resolvedByParams := map[string]any{"release_id": releaseID, "node_id": nodeID}
	edgeCountQuery := `MATCH (:Rejection {release_id: $release_id, node_id: $node_id})-[r:RESOLVED_BY]->() RETURN count(r) AS c`

	resolvedHash := runScalar(t, client, resolvedByQuery, resolvedByParams, "c")
	assert.Equal(t, "hash-seq2", resolvedHash,
		"with seq2 and seq3 both qualifying at once, must back-link to the OLDEST of the two")
	edgeCount := runScalar(t, client, edgeCountQuery, resolvedByParams, "c")
	assert.EqualValues(t, 1, edgeCount, "exactly one [:RESOLVED_BY] edge")

	// A redelivery of the identical rejection, with no new version, must
	// leave the established link untouched.
	require.NoError(t, repo.RecordRejection(ctx, rejection))

	resolvedHashAfter := runScalar(t, client, resolvedByQuery, resolvedByParams, "c")
	assert.Equal(t, "hash-seq2", resolvedHashAfter, "a redelivery must not move the link once resolved")
	edgeCountAfter := runScalar(t, client, edgeCountQuery, resolvedByParams, "c")
	assert.EqualValues(t, 1, edgeCountAfter, "still exactly one [:RESOLVED_BY] edge after redelivery")

	// A version older than the linked seq2 — the kind an orphan/backfill
	// write in code_version_repository.go can mint with an arbitrary
	// promoted_at — arrives after the link is already established. It would
	// rank first under `ORDER BY promoted_at ASC` if the query re-evaluated
	// candidates from scratch; the `existing IS NULL` guard must keep the
	// already-set link from moving to it, and must not add a second edge.
	seedNodeVersion(t, client, nodeID, marker, "hash-seq4", t0.Add(30*time.Minute))

	require.NoError(t, repo.RecordRejection(ctx, rejection))

	resolvedHashFinal := runScalar(t, client, resolvedByQuery, resolvedByParams, "c")
	assert.Equal(t, "hash-seq2", resolvedHashFinal,
		"the existing link must not move to a later-arriving version, even one that would rank first under ASC")
	edgeCountFinal := runScalar(t, client, edgeCountQuery, resolvedByParams, "c")
	assert.EqualValues(t, 1, edgeCountFinal,
		"still exactly one [:RESOLVED_BY] edge — the existing IS NULL guard must block a second link")
}

// TestCaseBaseRepository_RecordProposalCreatesStubRejection verifies the
// out-of-order half of the case base: a proposal arriving before its
// rejection creates a stub :Rejection so the [:PROPOSED] edge has an anchor,
// and the rejection landing later completes that same stub in place rather
// than creating a second :Rejection. A replayed RecordProposal converges to
// one :Proposal.
func TestCaseBaseRepository_RecordProposalCreatesStubRejection(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	repo := newCaseBaseRepo(client)

	releaseID := marker + "-rel"
	nodeID := marker + "-node"
	proposalID := marker + "-proposal"
	openedAt := time.Date(2026, 8, 13, 9, 0, 0, 0, time.UTC)

	proposal := casebase.Proposal{
		ProposalID: proposalID, ReleaseID: releaseID, NodeID: nodeID,
		PrURL: "https://github.com/org/repo/pull/42", PrNumber: 42,
		PrState: "open", OpenedBy: "remediation-agent", OpenedAt: openedAt,
	}
	require.NoError(t, repo.RecordProposal(ctx, proposal))

	session := client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)

	stubRes, err := session.Run(ctx, `
		MATCH (rej:Rejection {release_id: $release_id, node_id: $node_id})-[:PROPOSED]->(p:Proposal {proposal_id: $proposal_id})
		RETURN rej.stub AS stub, p.pr_url AS pr_url, p.pr_number AS pr_number,
		       p.pr_state AS pr_state, p.opened_by AS opened_by, p.opened_at AS opened_at
	`, map[string]any{"release_id": releaseID, "node_id": nodeID, "proposal_id": proposalID})
	require.NoError(t, err)
	stubRecords, err := stubRes.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, stubRecords, 1, "a stub :Rejection with a [:PROPOSED] edge must exist")

	stub, _ := stubRecords[0].Get("stub")
	assert.Equal(t, true, stub, "a rejection created purely from a proposal must be a stub")
	prURL, _ := stubRecords[0].Get("pr_url")
	prNumber, _ := stubRecords[0].Get("pr_number")
	prState, _ := stubRecords[0].Get("pr_state")
	openedBy, _ := stubRecords[0].Get("opened_by")
	openedAtGot, _ := stubRecords[0].Get("opened_at")
	assert.Equal(t, proposal.PrURL, prURL)
	assert.EqualValues(t, proposal.PrNumber, prNumber)
	assert.Equal(t, "open", prState)
	assert.Equal(t, proposal.OpenedBy, openedBy)
	openedAtTime, ok := openedAtGot.(time.Time)
	require.True(t, ok, "opened_at must round-trip as a time.Time, got %T", openedAtGot)
	assert.True(t, proposal.OpenedAt.Equal(openedAtTime))

	// The rejection arrives later and must complete the stub in place.
	rej := casebase.Rejection{
		ReleaseID: releaseID, NodeID: nodeID, Stage: "validation",
		Category: "cat", Reason: "reason", Signature: marker + "-sig",
		ErrorExcerpt: "excerpt", DBTLogURI: "uri", At: time.Now().UTC(),
		RawCode: "select 1", ContentHash: "hash-1",
	}
	require.NoError(t, repo.RecordRejection(ctx, rej))

	filledRes, err := session.Run(ctx, `
		MATCH (rej:Rejection {release_id: $release_id, node_id: $node_id})
		RETURN rej.stub AS stub, rej.stage AS stage, rej.category AS category, rej.reason AS reason,
		       rej.error_excerpt AS error_excerpt, rej.dbt_log_uri AS dbt_log_uri,
		       rej.raw_code AS raw_code, rej.content_hash AS content_hash
	`, map[string]any{"release_id": releaseID, "node_id": nodeID})
	require.NoError(t, err)
	filledRecords, err := filledRes.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, filledRecords, 1, "still exactly one :Rejection after the rejection completes the stub")

	stub2, _ := filledRecords[0].Get("stub")
	stage, _ := filledRecords[0].Get("stage")
	category, _ := filledRecords[0].Get("category")
	reason, _ := filledRecords[0].Get("reason")
	errorExcerpt, _ := filledRecords[0].Get("error_excerpt")
	dbtLogURI, _ := filledRecords[0].Get("dbt_log_uri")
	rawCode, _ := filledRecords[0].Get("raw_code")
	contentHash, _ := filledRecords[0].Get("content_hash")
	assert.Equal(t, false, stub2, "RecordRejection must flip stub to false")
	assert.Equal(t, rej.Stage, stage)
	assert.Equal(t, rej.Category, category)
	assert.Equal(t, rej.Reason, reason)
	assert.Equal(t, rej.ErrorExcerpt, errorExcerpt)
	assert.Equal(t, rej.DBTLogURI, dbtLogURI)
	assert.Equal(t, rej.RawCode, rawCode)
	assert.Equal(t, rej.ContentHash, contentHash)

	proposedEdges := runScalar(t, client, `
		MATCH (:Rejection {release_id: $release_id, node_id: $node_id})-[:PROPOSED]->(:Proposal {proposal_id: $proposal_id})
		RETURN count(*) AS c
	`, map[string]any{"release_id": releaseID, "node_id": nodeID, "proposal_id": proposalID}, "c")
	assert.EqualValues(t, 1, proposedEdges, "[:PROPOSED] edge must survive the stub being completed")

	// A replayed RecordProposal must converge onto the same :Proposal node and
	// must not disturb the :Rejection it points at. The stub fields are only
	// ever written with ON CREATE SET, so a redelivered pr_opened event — a
	// routine Redis consumer-group occurrence — must not flip a completed
	// rejection back to a stub or overwrite its classified-at with the
	// proposal's opened_at.
	require.NoError(t, repo.RecordProposal(ctx, proposal))
	proposalCount := runScalar(t, client,
		`MATCH (p:Proposal {proposal_id: $proposal_id}) RETURN count(p) AS c`,
		map[string]any{"proposal_id": proposalID}, "c")
	assert.EqualValues(t, 1, proposalCount, "RecordProposal replay converges to one :Proposal")

	afterReplayRes, err := session.Run(ctx, `
		MATCH (rej:Rejection {release_id: $release_id, node_id: $node_id})
		RETURN rej.stub AS stub, rej.at AS at
	`, map[string]any{"release_id": releaseID, "node_id": nodeID})
	require.NoError(t, err)
	afterReplayRecords, err := afterReplayRes.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, afterReplayRecords, 1)

	stubAfterReplay, _ := afterReplayRecords[0].Get("stub")
	assert.Equal(t, false, stubAfterReplay,
		"a RecordProposal replay must not flip a completed rejection back to a stub")
	atAfterReplay, _ := afterReplayRecords[0].Get("at")
	atAfterReplayTime, ok := atAfterReplay.(time.Time)
	require.True(t, ok, "at must round-trip as a time.Time, got %T", atAfterReplay)
	assert.True(t, rej.At.Equal(atAfterReplayTime),
		"a RecordProposal replay must not overwrite the rejection's classified-at with the proposal's opened_at")
}
