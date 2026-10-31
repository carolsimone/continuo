//go:build integration

package neo4jinfra

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/casebase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pullRequest returns the properties of the tenant's :PullRequest of
// (proposalID, service), reached from its :Proposal through [:HAS_PR], and
// checks there is exactly one.
func (f *swapFixture) pullRequest(proposalID, service string) map[string]any {
	f.t.Helper()
	recs := f.rows(`
		MATCH (:Proposal {tenant_id: $tenant_id, proposal_id: $proposal_id})-[:HAS_PR]->
		      (pl:PullRequest {tenant_id: $tenant_id, proposal_id: $proposal_id, service: $service})
		RETURN properties(pl) AS p`, map[string]any{"proposal_id": proposalID, "service": service})
	require.Len(f.t, recs, 1, "exactly one :PullRequest for (proposal, service)")
	v, _ := recs[0].Get("p")
	return v.(map[string]any)
}

// resolvedByProposal counts the [:RESOLVED_BY] edges from the tenant's
// rejections to its :Proposal proposalID.
func (f *swapFixture) resolvedByProposal(proposalID string) int64 {
	f.t.Helper()
	return f.count(`
		MATCH (:Rejection {tenant_id: $tenant_id})-[r:RESOLVED_BY]->(:Proposal {tenant_id: $tenant_id, proposal_id: $proposal_id})
		RETURN count(r) AS n`, map[string]any{"proposal_id": proposalID})
}

// editedEdges counts the [:EDITED] edges from the tenant's :Proposal
// proposalID.
func (f *swapFixture) editedEdges(proposalID string) int64 {
	f.t.Helper()
	return f.count(`
		MATCH (:Proposal {tenant_id: $tenant_id, proposal_id: $proposal_id})-[r:EDITED]->()
		RETURN count(r) AS n`, map[string]any{"proposal_id": proposalID})
}

// A merged outcome stamps the :PullRequest RecordProposal opened, draws one
// [:RESOLVED_BY] per resolved node to the one shared :Proposal with the
// any-edit-amended flag, and one [:EDITED] per edit to the edit's :Node with
// its path, amended flag, diff and service. A redelivery converges.
func TestRecordPullRequestOutcome_Merged(t *testing.T) {
	f := newSwapFixture(t)
	openedAt := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)
	closedAt := time.Date(2026, 8, 14, 11, 30, 0, 0, time.UTC)
	f.seedLiveNode("nu")

	f.recordProposal(casebase.Proposal{ProposalID: "prop", ReleaseID: "rel", NodeID: "na"}, openPullRequest("prop", "core", 42, openedAt))
	outcome := casebase.PullRequestOutcome{
		ProposalID: "prop", ReleaseID: "rel", Service: "core", Outcome: "merged", ClosedAt: closedAt,
		ResolvedNodeIDs: []string{"na", "nb"},
		Edits:           []casebase.EditOutcome{{Path: "models/revenue.sql", TargetNodeID: "nu", Amended: true, Diff: "d"}},
	}
	f.recordOutcome(outcome)
	f.recordOutcome(outcome)

	pl := f.pullRequest("prop", "core")
	assert.Equal(t, "merged", pl["pr_state"])
	closed, ok := pl["closed_at"].(time.Time)
	require.True(t, ok, "closed_at must round-trip as a time.Time, got %T", pl["closed_at"])
	assert.True(t, closedAt.Equal(closed))
	assert.Equal(t, int64(1), f.count(`MATCH (p:Proposal {tenant_id: $tenant_id, proposal_id: 'prop'}) RETURN count(p) AS n`, nil),
		"every resolved node converges on one shared :Proposal")

	for _, node := range []string{"na", "nb"} {
		recs := f.rows(`
			MATCH (rej:Rejection {tenant_id: $tenant_id, release_id: 'rel', node_id: $node_id})-[rb:RESOLVED_BY]->
			      (:Proposal {tenant_id: $tenant_id, proposal_id: 'prop'})
			RETURN rb.amended AS amended, rej.stub AS stub`, map[string]any{"node_id": node})
		require.Len(t, recs, 1, "exactly one RESOLVED_BY for %s after redelivery", node)
		amended, _ := recs[0].Get("amended")
		assert.Equal(t, true, amended, "the amended flag OR-reduces the PR's edits")
		stub, _ := recs[0].Get("stub")
		assert.Equal(t, true, stub, "a rejection first seen at outcome time is a stub")
	}

	edited := f.rows(`
		MATCH (:Proposal {tenant_id: $tenant_id, proposal_id: 'prop'})-[ed:EDITED]->(:Node {tenant_id: $tenant_id, unique_id: 'nu'})
		RETURN properties(ed) AS p`, nil)
	require.Len(t, edited, 1, "exactly one EDITED after redelivery")
	v, _ := edited[0].Get("p")
	ed := v.(map[string]any)
	assert.Equal(t, "models/revenue.sql", ed["path"])
	assert.Equal(t, true, ed["amended"])
	assert.Equal(t, "d", ed["diff"])
	assert.Equal(t, "core", ed["service"])
}

// An edit whose node the tenant does not hold draws no [:EDITED] and creates
// no :Node; the resolved node's edge and the PR's state still land.
func TestRecordPullRequestOutcome_MergedSkipsAbsentNode(t *testing.T) {
	f := newSwapFixture(t)
	f.recordOutcome(casebase.PullRequestOutcome{
		ProposalID: "prop", ReleaseID: "rel", Service: "core", Outcome: "merged",
		ClosedAt:        time.Date(2026, 8, 14, 11, 30, 0, 0, time.UTC),
		ResolvedNodeIDs: []string{"na"},
		Edits:           []casebase.EditOutcome{{Path: "models/x.sql", TargetNodeID: "missing"}},
	})

	assert.Zero(t, f.editedEdges("prop"), "no EDITED when the target :Node is absent")
	assert.Nil(t, f.node("missing"), "the case base never creates a :Node")
	assert.Equal(t, int64(1), f.resolvedByProposal("prop"), "the resolved node's edge still lands")
	assert.Equal(t, "merged", f.pullRequest("prop", "core")["pr_state"])
}

// A merged PR's [:EDITED] link to a node stays when a later release drops the
// node: the swap retires the node and keeps it with its fix PRs' edits.
func TestRecordPullRequestOutcome_EditedLinkSurvivesNodeRetirement(t *testing.T) {
	f := newSwapFixture(t)
	base := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	f.promoteAt("r1", 1, releaseOf(modelNode("analytics.table_a"), modelNode("analytics.table_e")), base)

	f.recordOutcome(casebase.PullRequestOutcome{
		ProposalID: "prop", ReleaseID: "r1", Service: "core", Outcome: "merged", ClosedAt: base.Add(time.Hour),
		ResolvedNodeIDs: []string{"analytics.table_a"},
		Edits:           []casebase.EditOutcome{{Path: "models/table_e.sql", TargetNodeID: "analytics.table_e", Diff: "d"}},
	})
	editedOfE := `
		MATCH (:Proposal {tenant_id: $tenant_id, proposal_id: 'prop'})-[ed:EDITED {path: 'models/table_e.sql'}]->
		      (:Node {tenant_id: $tenant_id, unique_id: 'analytics.table_e'})
		RETURN count(ed) AS n`
	require.Equal(t, int64(1), f.count(editedOfE, nil))

	f.promoteAt("r2", 2, releaseOf(modelNode("analytics.table_a")), base.Add(2*time.Hour))
	retired := f.node("analytics.table_e")
	require.NotNil(t, retired, "the dropped node stays")
	assert.Equal(t, false, retired["active"])
	assert.Equal(t, int64(1), f.count(editedOfE, nil), "the [:EDITED] link survives the node's retirement")

	// The edit of a node that is already retired still attaches to it.
	f.recordOutcome(casebase.PullRequestOutcome{
		ProposalID: "prop-2", ReleaseID: "r2", Service: "core", Outcome: "merged", ClosedAt: base.Add(3 * time.Hour),
		Edits: []casebase.EditOutcome{{Path: "models/table_e.sql", TargetNodeID: "analytics.table_e"}},
	})
	assert.Equal(t, int64(1), f.editedEdges("prop-2"), "a retired node still takes a later fix PR's [:EDITED]")
}

// A rejected outcome stamps only the :PullRequest's terminal state and draws
// no edge, even when its payload carries resolved nodes and edits.
func TestRecordPullRequestOutcome_Rejected(t *testing.T) {
	f := newSwapFixture(t)
	openedAt := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)
	f.seedLiveNode("nu")
	f.recordProposal(casebase.Proposal{ProposalID: "prop", ReleaseID: "rel", NodeID: "na"}, openPullRequest("prop", "core", 42, openedAt))

	f.recordOutcome(casebase.PullRequestOutcome{
		ProposalID: "prop", ReleaseID: "rel", Service: "core", Outcome: "rejected",
		ClosedAt:        time.Date(2026, 8, 14, 11, 30, 0, 0, time.UTC),
		ResolvedNodeIDs: []string{"na"},
		Edits:           []casebase.EditOutcome{{Path: "models/x.sql", TargetNodeID: "nu", Amended: true, Diff: "d"}},
	})

	assert.Equal(t, "rejected", f.pullRequest("prop", "core")["pr_state"])
	assert.Zero(t, f.resolvedByProposal("prop"), "a rejected outcome draws no RESOLVED_BY")
	assert.Zero(t, f.editedEdges("prop"), "a rejected outcome draws no EDITED")
}

// Every [:RESOLVED_BY] of a merged PR carries the OR of its edits' amended
// flags, while each [:EDITED] keeps its own edit's flag.
func TestRecordPullRequestOutcome_AmendedOrReduction(t *testing.T) {
	for name, tc := range map[string]struct {
		editAmended  [2]bool
		wantResolved bool
	}{
		"mixed edits OR to true":      {[2]bool{true, false}, true},
		"all-false edits OR to false": {[2]bool{false, false}, false},
	} {
		t.Run(name, func(t *testing.T) {
			f := newSwapFixture(t)
			f.seedLiveNode("nx")
			f.seedLiveNode("ny")

			f.recordOutcome(casebase.PullRequestOutcome{
				ProposalID: "prop", ReleaseID: "rel", Service: "core", Outcome: "merged",
				ClosedAt:        time.Date(2026, 8, 14, 11, 30, 0, 0, time.UTC),
				ResolvedNodeIDs: []string{"na", "nb"},
				Edits: []casebase.EditOutcome{
					{Path: "models/x.sql", TargetNodeID: "nx", Amended: tc.editAmended[0], Diff: "dx"},
					{Path: "models/y.sql", TargetNodeID: "ny", Amended: tc.editAmended[1], Diff: "dy"},
				},
			})

			for _, node := range []string{"na", "nb"} {
				assert.Equal(t, tc.wantResolved, f.scalar(`
					MATCH (:Rejection {tenant_id: $tenant_id, release_id: 'rel', node_id: $node_id})-[rb:RESOLVED_BY]->
					      (:Proposal {tenant_id: $tenant_id, proposal_id: 'prop'})
					RETURN rb.amended AS v`, map[string]any{"node_id": node}),
					"RESOLVED_BY.amended of %s is the OR across all edits", node)
			}
			for target, want := range map[string]bool{"nx": tc.editAmended[0], "ny": tc.editAmended[1]} {
				assert.Equal(t, want, f.scalar(`
					MATCH (:Proposal {tenant_id: $tenant_id, proposal_id: 'prop'})-[ed:EDITED]->(:Node {tenant_id: $tenant_id, unique_id: $uid})
					RETURN ed.amended AS v`, map[string]any{"uid": target}),
					"EDITED.amended of %s is its own edit's", target)
			}
		})
	}
}

// A merged outcome with no resolved nodes and no edits stamps the terminal
// state and draws no edge.
func TestRecordPullRequestOutcome_MergedWithEmptyLists(t *testing.T) {
	f := newSwapFixture(t)
	openedAt := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)
	closedAt := time.Date(2026, 8, 14, 11, 30, 0, 0, time.UTC)
	f.recordProposal(casebase.Proposal{ProposalID: "prop", ReleaseID: "rel", NodeID: "na"}, openPullRequest("prop", "core", 42, openedAt))

	f.recordOutcome(casebase.PullRequestOutcome{
		ProposalID: "prop", ReleaseID: "rel", Service: "core", Outcome: "merged", ClosedAt: closedAt,
	})

	pl := f.pullRequest("prop", "core")
	assert.Equal(t, "merged", pl["pr_state"])
	closed, ok := pl["closed_at"].(time.Time)
	require.True(t, ok, "closed_at must round-trip as a time.Time, got %T", pl["closed_at"])
	assert.True(t, closedAt.Equal(closed))
	assert.Zero(t, f.resolvedByProposal("prop"))
	assert.Zero(t, f.editedEdges("prop"))
}

// The open and close events are consumed by independent groups, so the close
// can land first: it creates the :PullRequest with the pr_url and pr_number it
// carries, and the later open fills opened_by and opened_at without resetting
// the terminal state or dropping closed_at.
func TestRecordPullRequestOutcome_CloseBeforeOpenFillsPRFactsWithoutResettingState(t *testing.T) {
	f := newSwapFixture(t)
	openedAt := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)
	closedAt := time.Date(2026, 8, 20, 11, 30, 0, 0, time.UTC)
	pr := openPullRequest("prop", "core", 77, openedAt)

	f.recordOutcome(casebase.PullRequestOutcome{
		ProposalID: "prop", ReleaseID: "rel", Service: "core", Outcome: "merged", ClosedAt: closedAt,
		PrURL: pr.PrURL, PrNumber: pr.PrNumber, ResolvedNodeIDs: []string{"na"},
	})
	afterClose := f.pullRequest("prop", "core")
	assert.Equal(t, pr.PrURL, afterClose["pr_url"], "a close-first :PullRequest takes pr_url from the close")
	assert.EqualValues(t, pr.PrNumber, afterClose["pr_number"])
	assert.Equal(t, "merged", afterClose["pr_state"])
	assert.NotContains(t, afterClose, "opened_by", "the open-only facts wait for the open")

	f.recordProposal(casebase.Proposal{ProposalID: "prop", ReleaseID: "rel", NodeID: "na"}, pr)
	afterOpen := f.pullRequest("prop", "core")
	assert.Equal(t, "merged", afterOpen["pr_state"], "the open does not reset the terminal state")
	closed, ok := afterOpen["closed_at"].(time.Time)
	require.True(t, ok, "closed_at survives the open, got %T", afterOpen["closed_at"])
	assert.True(t, closedAt.Equal(closed))
	assert.Equal(t, "agent-remediation", afterOpen["opened_by"])
	opened, ok := afterOpen["opened_at"].(time.Time)
	require.True(t, ok, "opened_at must round-trip as a time.Time, got %T", afterOpen["opened_at"])
	assert.True(t, openedAt.Equal(opened))
	assert.Equal(t, pr.PrURL, afterOpen["pr_url"])
	assert.EqualValues(t, pr.PrNumber, afterOpen["pr_number"])
}

// In the ordinary order the open lands first; the close applies the terminal
// state and keeps every open fact.
func TestRecordPullRequestOutcome_OpenBeforeCloseYieldsTerminalStateAndAllFacts(t *testing.T) {
	f := newSwapFixture(t)
	openedAt := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)
	closedAt := time.Date(2026, 8, 20, 11, 30, 0, 0, time.UTC)
	pr := openPullRequest("prop", "core", 88, openedAt)

	f.recordProposal(casebase.Proposal{ProposalID: "prop", ReleaseID: "rel", NodeID: "na"}, pr)
	f.recordOutcome(casebase.PullRequestOutcome{
		ProposalID: "prop", ReleaseID: "rel", Service: "core", Outcome: "merged", ClosedAt: closedAt,
		PrURL: pr.PrURL, PrNumber: pr.PrNumber, ResolvedNodeIDs: []string{"na"},
	})

	pl := f.pullRequest("prop", "core")
	assert.Equal(t, "merged", pl["pr_state"])
	closed, ok := pl["closed_at"].(time.Time)
	require.True(t, ok, "closed_at must round-trip as a time.Time, got %T", pl["closed_at"])
	assert.True(t, closedAt.Equal(closed))
	assert.Equal(t, "agent-remediation", pl["opened_by"], "the open facts survive the close")
	opened, ok := pl["opened_at"].(time.Time)
	require.True(t, ok, "opened_at must round-trip as a time.Time, got %T", pl["opened_at"])
	assert.True(t, openedAt.Equal(opened))
	assert.Equal(t, pr.PrURL, pl["pr_url"])
	assert.EqualValues(t, pr.PrNumber, pl["pr_number"])
}
