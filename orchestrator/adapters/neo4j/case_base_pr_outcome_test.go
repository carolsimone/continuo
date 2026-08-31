package neo4jinfra_test

import (
	"context"
	"testing"
	"time"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/domain/casebase"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedEditTargetTable creates one :Table fixture row so [:EDITED] has a target
// to point at. RecordPullRequestOutcome must never mint a :Table itself.
func seedEditTargetTable(t *testing.T, client neo4jinfra.Neo4jClient, uniqueID, marker string) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `CREATE (:Table {unique_id: $uid, test_marker: $m})`,
		map[string]any{"uid": uniqueID, "m": marker})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// TestCaseBaseRepository_RecordPullRequestOutcomeMerged verifies the core of a
// merged PR outcome: it stamps the same :PullRequest RecordProposal opened
// (matched through [:HAS_PR], not a second node), draws one [:RESOLVED_BY] per
// resolved node to the SHARED :Proposal with the any-edit-amended flag, and
// draws one [:EDITED] per edit to the edit's :Table carrying path/amended/diff/
// service. Redelivery converges to the same single edges.
func TestCaseBaseRepository_RecordPullRequestOutcomeMerged(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	repo := newCaseBaseRepo(client)

	releaseID := marker + "-rel"
	proposalID := marker + "-proposal"
	na := marker + "-na"
	nb := marker + "-nb"
	nu := marker + "-nu"
	openedAt := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)
	closedAt := time.Date(2026, 8, 14, 11, 30, 0, 0, time.UTC)

	// The edit's target :Table must already exist in the topology.
	seedEditTargetTable(t, client, nu, marker)

	// pr_opened landed first, opening the :Proposal and its :PullRequest.
	require.NoError(t, repo.RecordProposal(ctx,
		casebase.Proposal{ProposalID: proposalID, ReleaseID: releaseID, NodeID: na},
		casebase.PullRequest{
			ProposalID: proposalID, Service: "core",
			PrURL: "https://github.com/org/repo/pull/42", PrNumber: 42,
			State: "open", OpenedBy: "agent-remediation", OpenedAt: openedAt,
		}))

	outcome := casebase.PullRequestOutcome{
		ProposalID:      proposalID,
		ReleaseID:       releaseID,
		Service:         "core",
		Outcome:         "merged",
		ClosedAt:        closedAt,
		ResolvedNodeIDs: []string{na, nb},
		Edits: []casebase.EditOutcome{
			{Path: "models/revenue.sql", TargetNodeID: nu, Amended: true, Diff: "d"},
		},
	}
	require.NoError(t, repo.RecordPullRequestOutcome(ctx, outcome))
	// Redelivery must converge, not duplicate.
	require.NoError(t, repo.RecordPullRequestOutcome(ctx, outcome))

	// Exactly one :PullRequest for (proposal, service) — the outcome updated the
	// one RecordProposal opened rather than minting a second.
	prCount := runScalar(t, client, `
		MATCH (:Proposal {proposal_id: $proposal_id})-[:HAS_PR]->(pl:PullRequest {service: $service})
		RETURN count(pl) AS c
	`, map[string]any{"proposal_id": proposalID, "service": "core"}, "c")
	assert.EqualValues(t, 1, prCount, "the merged outcome updates the one :PullRequest, never a second")

	// Exactly one :Proposal for the proposal_id — every resolved node's
	// [:RESOLVED_BY] converges on one shared node, never a duplicate.
	proposalCount := runScalar(t, client,
		`MATCH (p:Proposal {proposal_id: $proposal_id}) RETURN count(p) AS c`,
		map[string]any{"proposal_id": proposalID}, "c")
	assert.EqualValues(t, 1, proposalCount, "the merged outcome converges on one shared :Proposal, never a duplicate")

	// :PullRequest terminal state.
	session := client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)
	prRes, err := session.Run(ctx, `
		MATCH (:Proposal {proposal_id: $proposal_id})-[:HAS_PR]->(pl:PullRequest {service: $service})
		RETURN pl.pr_state AS pr_state, pl.closed_at AS closed_at
	`, map[string]any{"proposal_id": proposalID, "service": "core"})
	require.NoError(t, err)
	prRecords, err := prRes.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, prRecords, 1)
	prState, _ := prRecords[0].Get("pr_state")
	assert.Equal(t, "merged", prState)
	closedAtGot, _ := prRecords[0].Get("closed_at")
	closedAtTime, ok := closedAtGot.(time.Time)
	require.True(t, ok, "closed_at must round-trip as a time.Time, got %T", closedAtGot)
	assert.True(t, closedAt.Equal(closedAtTime))

	// One [:RESOLVED_BY] per resolved node to the SAME :Proposal, amended=true.
	for _, node := range []string{na, nb} {
		rbRes, err := session.Run(ctx, `
			MATCH (rej:Rejection {release_id: $release_id, node_id: $node_id})-[rb:RESOLVED_BY]->(p:Proposal {proposal_id: $proposal_id})
			RETURN rb.amended AS amended, rej.stub AS stub
		`, map[string]any{"release_id": releaseID, "node_id": node, "proposal_id": proposalID})
		require.NoError(t, err)
		rbRecords, err := rbRes.Collect(ctx)
		require.NoError(t, err)
		require.Len(t, rbRecords, 1, "exactly one RESOLVED_BY edge for node %s after redelivery", node)
		amended, _ := rbRecords[0].Get("amended")
		assert.Equal(t, true, amended, "amended flag OR-reduces the PR's edits")
		stub, _ := rbRecords[0].Get("stub")
		assert.Equal(t, true, stub, "a rejection first seen at outcome time is a stub")
	}

	// One [:EDITED] to nu with all edge properties.
	edRes, err := session.Run(ctx, `
		MATCH (:Proposal {proposal_id: $proposal_id})-[ed:EDITED]->(t:Table {unique_id: $nu})
		RETURN ed.path AS path, ed.amended AS amended, ed.diff AS diff, ed.service AS service
	`, map[string]any{"proposal_id": proposalID, "nu": nu})
	require.NoError(t, err)
	edRecords, err := edRes.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, edRecords, 1, "exactly one EDITED edge after redelivery")
	path, _ := edRecords[0].Get("path")
	amended, _ := edRecords[0].Get("amended")
	diff, _ := edRecords[0].Get("diff")
	service, _ := edRecords[0].Get("service")
	assert.Equal(t, "models/revenue.sql", path)
	assert.Equal(t, true, amended)
	assert.Equal(t, "d", diff)
	assert.Equal(t, "core", service)
}

// TestCaseBaseRepository_RecordPullRequestOutcomeMergedSkipsAbsentTable verifies
// that an edit whose target :Table is absent draws no [:EDITED] edge and does
// not error — this writer must never mint a :Table.
func TestCaseBaseRepository_RecordPullRequestOutcomeMergedSkipsAbsentTable(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	repo := newCaseBaseRepo(client)

	releaseID := marker + "-rel"
	proposalID := marker + "-proposal"
	na := marker + "-na"
	missing := marker + "-missing"
	closedAt := time.Date(2026, 8, 14, 11, 30, 0, 0, time.UTC)

	require.NoError(t, repo.RecordPullRequestOutcome(ctx, casebase.PullRequestOutcome{
		ProposalID: proposalID, ReleaseID: releaseID, Service: "core",
		Outcome: "merged", ClosedAt: closedAt,
		ResolvedNodeIDs: []string{na},
		Edits:           []casebase.EditOutcome{{Path: "models/x.sql", TargetNodeID: missing, Amended: false}},
	}))

	editedCount := runScalar(t, client, `
		MATCH (:Proposal {proposal_id: $proposal_id})-[ed:EDITED]->() RETURN count(ed) AS c
	`, map[string]any{"proposal_id": proposalID}, "c")
	assert.EqualValues(t, 0, editedCount, "no EDITED edge when the target :Table is absent")

	tableCount := runScalar(t, client,
		`MATCH (t:Table {unique_id: $uid}) RETURN count(t) AS c`,
		map[string]any{"uid": missing}, "c")
	assert.EqualValues(t, 0, tableCount, "RecordPullRequestOutcome must never create a :Table")

	// The RESOLVED_BY edge and PullRequest state still land.
	rbCount := runScalar(t, client, `
		MATCH (:Rejection {release_id: $release_id, node_id: $node_id})-[rb:RESOLVED_BY]->(:Proposal {proposal_id: $proposal_id})
		RETURN count(rb) AS c
	`, map[string]any{"release_id": releaseID, "node_id": na, "proposal_id": proposalID}, "c")
	assert.EqualValues(t, 1, rbCount, "the resolved-node edge still lands when an edit target is absent")
}

// TestCaseBaseRepository_RecordPullRequestOutcomeRejected verifies a rejected
// outcome updates only the :PullRequest terminal state and draws zero
// RESOLVED_BY/EDITED edges — even when (defensively) resolved nodes and edits
// are passed.
func TestCaseBaseRepository_RecordPullRequestOutcomeRejected(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	repo := newCaseBaseRepo(client)

	releaseID := marker + "-rel"
	proposalID := marker + "-proposal"
	na := marker + "-na"
	nu := marker + "-nu"
	openedAt := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)
	closedAt := time.Date(2026, 8, 14, 11, 30, 0, 0, time.UTC)

	seedEditTargetTable(t, client, nu, marker)
	require.NoError(t, repo.RecordProposal(ctx,
		casebase.Proposal{ProposalID: proposalID, ReleaseID: releaseID, NodeID: na},
		casebase.PullRequest{
			ProposalID: proposalID, Service: "core",
			PrURL: "https://github.com/org/repo/pull/42", PrNumber: 42,
			State: "open", OpenedBy: "agent-remediation", OpenedAt: openedAt,
		}))

	// A rejected outcome that (defensively) still carries resolved nodes and an
	// edit — the repository must ignore them and draw no edges.
	require.NoError(t, repo.RecordPullRequestOutcome(ctx, casebase.PullRequestOutcome{
		ProposalID: proposalID, ReleaseID: releaseID, Service: "core",
		Outcome: "rejected", ClosedAt: closedAt,
		ResolvedNodeIDs: []string{na},
		Edits:           []casebase.EditOutcome{{Path: "models/x.sql", TargetNodeID: nu, Amended: true, Diff: "d"}},
	}))

	prState := runScalar(t, client, `
		MATCH (:Proposal {proposal_id: $proposal_id})-[:HAS_PR]->(pl:PullRequest {service: $service})
		RETURN pl.pr_state AS s
	`, map[string]any{"proposal_id": proposalID, "service": "core"}, "s")
	assert.Equal(t, "rejected", prState, "a rejected outcome stamps the terminal state")

	rbCount := runScalar(t, client, `
		MATCH (:Rejection)-[rb:RESOLVED_BY]->(:Proposal {proposal_id: $proposal_id}) RETURN count(rb) AS c
	`, map[string]any{"proposal_id": proposalID}, "c")
	assert.EqualValues(t, 0, rbCount, "a rejected outcome draws no RESOLVED_BY edge")

	editedCount := runScalar(t, client, `
		MATCH (:Proposal {proposal_id: $proposal_id})-[ed:EDITED]->() RETURN count(ed) AS c
	`, map[string]any{"proposal_id": proposalID}, "c")
	assert.EqualValues(t, 0, editedCount, "a rejected outcome draws no EDITED edge")
}

// TestCaseBaseRepository_RecordPullRequestOutcomeAmendedOrReduction proves the
// `amended` flag on every [:RESOLVED_BY] edge is the OR-reduction across ALL of
// the PR's edits, not a pass-through of a single value, while each [:EDITED]
// edge keeps its own per-edit `amended`. Two present :Table targets receive two
// edits: one amended, one not. Every RESOLVED_BY must read amended=true (some
// edit was amended); the two EDITED edges keep true and false respectively.
// The all-false case then shows RESOLVED_BY collapses to false only when NO
// edit was amended.
func TestCaseBaseRepository_RecordPullRequestOutcomeAmendedOrReduction(t *testing.T) {
	runAmendedOrReductionCase := func(t *testing.T, editAmended []bool, wantResolvedAmended bool) {
		t.Helper()
		client := newTestClient(t)
		ctx := context.Background()
		marker := t.Name()
		cleanup := caseBaseCleanup(t, client, marker)
		cleanup()
		defer cleanup()

		repo := newCaseBaseRepo(client)

		releaseID := marker + "-rel"
		proposalID := marker + "-proposal"
		na := marker + "-na"
		nb := marker + "-nb"
		nx := marker + "-nx" // first edit's target :Table
		ny := marker + "-ny" // second edit's target :Table
		closedAt := time.Date(2026, 8, 14, 11, 30, 0, 0, time.UTC)

		// Both edits' target :Table nodes exist in the topology.
		seedEditTargetTable(t, client, nx, marker)
		seedEditTargetTable(t, client, ny, marker)

		require.NoError(t, repo.RecordPullRequestOutcome(ctx, casebase.PullRequestOutcome{
			ProposalID: proposalID, ReleaseID: releaseID, Service: "core",
			Outcome: "merged", ClosedAt: closedAt,
			ResolvedNodeIDs: []string{na, nb},
			Edits: []casebase.EditOutcome{
				{Path: "models/x.sql", TargetNodeID: nx, Amended: editAmended[0], Diff: "dx"},
				{Path: "models/y.sql", TargetNodeID: ny, Amended: editAmended[1], Diff: "dy"},
			},
		}))

		// Every RESOLVED_BY edge carries the OR-reduced flag.
		for _, node := range []string{na, nb} {
			rbAmended := runScalar(t, client, `
				MATCH (:Rejection {release_id: $release_id, node_id: $node_id})-[rb:RESOLVED_BY]->(:Proposal {proposal_id: $proposal_id})
				RETURN rb.amended AS amended
			`, map[string]any{"release_id": releaseID, "node_id": node, "proposal_id": proposalID}, "amended")
			assert.Equal(t, wantResolvedAmended, rbAmended,
				"RESOLVED_BY.amended for %s must be the OR across all edits", node)
		}

		// Each EDITED edge keeps its own per-edit amended, independent of the OR.
		for target, want := range map[string]bool{nx: editAmended[0], ny: editAmended[1]} {
			edAmended := runScalar(t, client, `
				MATCH (:Proposal {proposal_id: $proposal_id})-[ed:EDITED]->(:Table {unique_id: $uid})
				RETURN ed.amended AS amended
			`, map[string]any{"proposal_id": proposalID, "uid": target}, "amended")
			assert.Equal(t, want, edAmended,
				"EDITED.amended for %s is per-edit, not the OR-reduced value", target)
		}
	}

	t.Run("mixed edits OR to true", func(t *testing.T) {
		runAmendedOrReductionCase(t, []bool{true, false}, true)
	})
	t.Run("all-false edits OR to false", func(t *testing.T) {
		runAmendedOrReductionCase(t, []bool{false, false}, false)
	})
}

// TestCaseBaseRepository_RecordPullRequestOutcomeLegacyMergedEmptyLists proves
// the merged-path Cypher is safe when the resolved set and edits are both empty
// — a legacy payload emitted before those fields existed. The FOREACH/UNWIND
// over empty lists must still stamp the :PullRequest terminal state (the write
// before them persists) and draw zero provenance edges, without error.
func TestCaseBaseRepository_RecordPullRequestOutcomeLegacyMergedEmptyLists(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	marker := t.Name()
	cleanup := caseBaseCleanup(t, client, marker)
	cleanup()
	defer cleanup()

	repo := newCaseBaseRepo(client)

	releaseID := marker + "-rel"
	proposalID := marker + "-proposal"
	na := marker + "-na"
	openedAt := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)
	closedAt := time.Date(2026, 8, 14, 11, 30, 0, 0, time.UTC)

	// The :Proposal + :PullRequest exist via [:HAS_PR] from a prior pr_opened.
	require.NoError(t, repo.RecordProposal(ctx,
		casebase.Proposal{ProposalID: proposalID, ReleaseID: releaseID, NodeID: na},
		casebase.PullRequest{
			ProposalID: proposalID, Service: "core",
			PrURL: "https://github.com/org/repo/pull/42", PrNumber: 42,
			State: "open", OpenedBy: "agent-remediation", OpenedAt: openedAt,
		}))

	// A merged outcome with no resolved nodes and no edits — must not error.
	require.NoError(t, repo.RecordPullRequestOutcome(ctx, casebase.PullRequestOutcome{
		ProposalID: proposalID, ReleaseID: releaseID, Service: "core",
		Outcome: "merged", ClosedAt: closedAt,
		ResolvedNodeIDs: nil,
		Edits:           nil,
	}))

	// The :PullRequest terminal state still landed.
	session := client.NewSession(ctx, neo4j.AccessModeRead)
	defer session.Close(ctx)
	prRes, err := session.Run(ctx, `
		MATCH (:Proposal {proposal_id: $proposal_id})-[:HAS_PR]->(pl:PullRequest {service: $service})
		RETURN pl.pr_state AS pr_state, pl.closed_at AS closed_at
	`, map[string]any{"proposal_id": proposalID, "service": "core"})
	require.NoError(t, err)
	prRecords, err := prRes.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, prRecords, 1)
	prState, _ := prRecords[0].Get("pr_state")
	assert.Equal(t, "merged", prState, "a legacy merged outcome still advances pr_state")
	closedAtGot, _ := prRecords[0].Get("closed_at")
	closedAtTime, ok := closedAtGot.(time.Time)
	require.True(t, ok, "closed_at must round-trip as a time.Time, got %T", closedAtGot)
	assert.True(t, closedAt.Equal(closedAtTime), "a legacy merged outcome stamps closed_at")

	// Zero provenance edges drawn.
	rbCount := runScalar(t, client, `
		MATCH (:Rejection)-[rb:RESOLVED_BY]->(:Proposal {proposal_id: $proposal_id}) RETURN count(rb) AS c
	`, map[string]any{"proposal_id": proposalID}, "c")
	assert.EqualValues(t, 0, rbCount, "a legacy merged outcome draws no RESOLVED_BY edge")

	editedCount := runScalar(t, client, `
		MATCH (:Proposal {proposal_id: $proposal_id})-[ed:EDITED]->() RETURN count(ed) AS c
	`, map[string]any{"proposal_id": proposalID}, "c")
	assert.EqualValues(t, 0, editedCount, "a legacy merged outcome draws no EDITED edge")
}
