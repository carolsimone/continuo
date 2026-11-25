//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

// FailLegacyInFlight fails the attempt rows that nothing drives, since the
// runner stamps a remediation on every row it writes: a row in flight
// (generating or verifying) with no remediation. A row that was generating takes
// the reason as its rationale; a row that was verifying takes it as its verify
// error and keeps its rationale, edits and verifications. On both, each node
// outcome still in flight becomes failed with the reason and every other outcome
// stays as it was, and a row that recorded no outcome at all is failed too and
// still reads back with none. A row of a remediation, whatever its status, and a
// row that is not in flight are left alone, and a second call finds nothing to
// move.
//
// The remediation_id of a proposal row is cleared (ON DELETE SET NULL) when the
// retention sweeper deletes its remediation, so a runner row of a deleted
// remediation is selected by "no remediation id" like any other. That never
// moves a row the runner drives: the sweeper deletes only done and system_failure
// remediations, whose rows are terminal and never generating or verifying.
func TestFailLegacyInFlight_FailsRowsWithNoRemediation(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		const reason = "continuo changed its remediation engine while this attempt was in flight; use Try again"
		r := mustCreate(t, env, newRemediation("rel-legacy-inflight-runner", 1, contractBase))
		handedOver := proposal.NodeOutcome{Status: proposal.StatusSkipped, Reason: "no remediation type matches this failure", Code: model.HandoverReasonNoType}
		settledFailure := proposal.NodeOutcome{Status: proposal.StatusFailed, Reason: "the model declined"}
		get := func(release string, attempt int) proposal.View {
			t.Helper()
			return findAttempt(t, env, release, attempt)
		}

		edits := []proposal.FileEdit{{Path: "models/a.sql", ContentURI: "s3://c", DiffURI: "s3://d", TargetNodeID: "model.a", MemberNodeIDs: []string{"model.a"}}}
		runs := []proposal.Verification{{Project: "svc", Kind: "dbt", RunID: "run-1", Phase: proposal.PhaseQueued}}

		// A row with no remediation in verification: a fix under check.
		require.NoError(t, env.proposals.Upsert(ctx, proposal.Proposal{
			Source: "validation", ReleaseID: "rel-legacy-inflight-verifying", NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a", "model.b", "model.c"}, Attempt: 1, Status: proposal.StatusVerifying,
			NodeOutcomes: map[string]proposal.NodeOutcome{
				"model.a": {Status: proposal.StatusVerifying},
				"model.b": {Status: proposal.StatusVerifying},
				"model.c": handedOver,
			},
			Edits: edits, Verifications: runs, Rationale: "the fix", SourceResolved: true, CreatedAt: contractBase,
		}))
		// A row with no remediation still generating.
		require.NoError(t, env.proposals.InsertGenerating(ctx, proposal.Proposal{
			Source: "validation", ReleaseID: "rel-legacy-inflight-generating", NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a", "model.b"}, Attempt: 1,
			NodeOutcomes: map[string]proposal.NodeOutcome{
				"model.a": {Status: proposal.StatusGenerating},
				"model.b": settledFailure,
			},
			CreatedAt: contractBase,
		}))
		// An attempt that recorded no per-node outcome at all: its node_outcomes
		// holds no entries, which aggregates to SQL NULL, not an empty object.
		require.NoError(t, env.proposals.InsertGenerating(ctx, proposal.Proposal{
			Source: "validation", ReleaseID: "rel-legacy-inflight-no-outcomes", NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a"}, Attempt: 1, NodeOutcomes: nil, CreatedAt: contractBase,
		}))
		// The runner's rows in flight, which carry their remediation.
		require.NoError(t, env.proposals.InsertGenerating(ctx, proposal.Proposal{
			Source: "validation", ReleaseID: "rel-legacy-inflight-runner", RemediationID: r.ID.String(), NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a"}, Attempt: 1,
			NodeOutcomes: map[string]proposal.NodeOutcome{"model.a": {Status: proposal.StatusGenerating}},
			CreatedAt:    contractBase,
		}))
		runnerVerifying := map[string]proposal.NodeOutcome{"model.a": {Status: proposal.StatusVerifying}}
		require.NoError(t, env.proposals.Upsert(ctx, proposal.Proposal{
			Source: "validation", ReleaseID: "rel-legacy-inflight-runner", RemediationID: r.ID.String(), NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a"}, Attempt: 2, Status: proposal.StatusVerifying,
			NodeOutcomes: runnerVerifying, Edits: edits, Verifications: runs, Rationale: "the runner's fix", SourceResolved: true,
			CreatedAt: contractBase,
		}))
		// Rows with no remediation that ended: nothing to fail.
		require.NoError(t, env.proposals.Upsert(ctx, proposal.Proposal{
			Source: "validation", ReleaseID: "rel-legacy-inflight-ended", NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a"}, Attempt: 1, Status: proposal.StatusFailed,
			NodeOutcomes: map[string]proposal.NodeOutcome{"model.a": settledFailure},
			Rationale:    "the model declined", CreatedAt: contractBase,
		}))
		require.NoError(t, env.proposals.Upsert(ctx, proposal.Proposal{
			Source: "validation", ReleaseID: "rel-legacy-inflight-ended", NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a"}, Attempt: 2, Status: proposal.StatusProposed,
			NodeOutcomes: map[string]proposal.NodeOutcome{"model.a": {Status: proposal.StatusProposed}},
			Edits:        edits, Verifications: runs, Rationale: "a verified fix", SourceResolved: true, CreatedAt: contractBase,
		}))

		beforeVerifying := get("rel-legacy-inflight-verifying", 1)
		beforeGenerating := get("rel-legacy-inflight-generating", 1)
		require.Equal(t, proposal.StatusVerifying, beforeVerifying.Status)
		require.Equal(t, proposal.StatusGenerating, beforeGenerating.Status)
		untouched := []proposal.View{
			get("rel-legacy-inflight-runner", 1),
			get("rel-legacy-inflight-runner", 2),
			get("rel-legacy-inflight-ended", 1),
			get("rel-legacy-inflight-ended", 2),
		}

		moved, err := env.proposals.FailLegacyInFlight(ctx, reason)
		require.NoError(t, err)
		require.Equal(t, 3, moved, "the three rows in flight with no remediation, and no other")

		verifying := get("rel-legacy-inflight-verifying", 1)
		require.Equal(t, proposal.StatusFailed, verifying.Status)
		require.Equal(t, reason, verifying.VerifyError, "a row that was verifying records the reason as its verify error")
		require.Equal(t, "the fix", verifying.Rationale, "and keeps its rationale")
		require.Equal(t, edits, verifying.Edits, "its edits")
		require.Equal(t, runs, verifying.Verifications, "and its verifications")
		require.Equal(t, map[string]proposal.NodeOutcome{
			"model.a": {Status: proposal.StatusFailed, Reason: reason},
			"model.b": {Status: proposal.StatusFailed, Reason: reason},
			"model.c": handedOver,
		}, verifying.NodeOutcomes, "each outcome in flight fails with the reason, the hand-over stays")

		generating := get("rel-legacy-inflight-generating", 1)
		require.Equal(t, proposal.StatusFailed, generating.Status)
		require.Equal(t, reason, generating.Rationale, "a row that was generating records the reason as its rationale")
		require.Empty(t, generating.VerifyError)
		require.Equal(t, map[string]proposal.NodeOutcome{
			"model.a": {Status: proposal.StatusFailed, Reason: reason},
			"model.b": settledFailure,
		}, generating.NodeOutcomes, "the outcome in flight fails with the reason, the one that ended stays")

		noOutcomes := get("rel-legacy-inflight-no-outcomes", 1)
		require.Equal(t, proposal.StatusFailed, noOutcomes.Status)
		require.Equal(t, reason, noOutcomes.Rationale)
		require.Empty(t, noOutcomes.NodeOutcomes, "a row with no per-node outcome still reads back with none")

		require.Equal(t, untouched[0], get("rel-legacy-inflight-runner", 1), "a runner row generating is the runner's to finish")
		require.Equal(t, untouched[1], get("rel-legacy-inflight-runner", 2), "so is a runner row verifying")
		require.Equal(t, untouched[2], get("rel-legacy-inflight-ended", 1), "a row that failed already is left as it is")
		require.Equal(t, untouched[3], get("rel-legacy-inflight-ended", 2), "so is a row that was proposed")

		moved, err = env.proposals.FailLegacyInFlight(ctx, reason)
		require.NoError(t, err)
		require.Zero(t, moved, "a second call finds nothing in flight")
		require.Equal(t, verifying, get("rel-legacy-inflight-verifying", 1))
		require.Equal(t, generating, get("rel-legacy-inflight-generating", 1))
		require.Equal(t, noOutcomes, get("rel-legacy-inflight-no-outcomes", 1))
		for i, want := range untouched {
			require.Equal(t, want, get(want.ReleaseID, want.Attempt), "row %d after the second call", i)
		}
	})
}
