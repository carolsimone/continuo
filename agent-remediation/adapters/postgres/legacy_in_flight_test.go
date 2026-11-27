//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

// FailLegacyInFlight fails the attempt rows that nothing drives, since the
// runner stamps a remediation on every row it writes: a row in flight
// (generating or verifying) with no remediation. A row that was generating takes
// the reason as its rationale; a row that was verifying takes it as its verify
// error and keeps its rationale, edits and release checks. On both, each node
// outcome still in flight becomes failed with the reason and every other outcome
// stays as it was, and a row that recorded no outcome at all is failed too and
// still reads back with none. A row of a remediation, whatever its status, and a
// row that is not in flight are left alone, and a second call finds nothing to
// move.
//
// The remediation_id of a fix row is cleared (ON DELETE SET NULL) when the
// retention sweeper deletes its remediation, so a runner row of a deleted
// remediation is selected by "no remediation id" like any other. That never
// moves a row the runner drives: the sweeper deletes only done and system_failure
// remediations, whose rows are terminal and never generating or verifying.
func TestFailLegacyInFlight_FailsRowsWithNoRemediation(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		const reason = "continuo changed its remediation engine while this attempt was in flight; use Try again"
		r := mustCreate(t, env, newRemediation("rel-legacy-inflight-runner", 1, contractBase))
		handedOver := fix.NodeOutcome{Status: fix.StatusSkipped, Reason: "no remediation type matches this failure", Code: model.HandoverReasonNoType}
		settledFailure := fix.NodeOutcome{Status: fix.StatusFailed, Reason: "the model declined"}
		get := func(release string, attempt int) fix.View {
			t.Helper()
			return findAttempt(t, env, release, attempt)
		}

		edits := []fix.FileEdit{{Path: "models/a.sql", ContentURI: "s3://c", DiffURI: "s3://d", TargetNodeID: "model.a", MemberNodeIDs: []string{"model.a"}}}
		runs := []fix.ReleaseCheck{{Project: "svc", Kind: "dbt", CheckID: "run-1", Phase: fix.PhaseQueued}}

		// A row with no remediation in its release check: a fix under check.
		require.NoError(t, env.fixes.Upsert(ctx, fix.Fix{
			Source: "validation", ReleaseID: "rel-legacy-inflight-verifying", NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a", "model.b", "model.c"}, Attempt: 1, Status: fix.StatusVerifying,
			NodeOutcomes: map[string]fix.NodeOutcome{
				"model.a": {Status: fix.StatusVerifying},
				"model.b": {Status: fix.StatusVerifying},
				"model.c": handedOver,
			},
			Edits: edits, ReleaseChecks: runs, Rationale: "the fix", SourceResolved: true, CreatedAt: contractBase,
		}))
		// A row with no remediation still generating.
		require.NoError(t, env.fixes.InsertGenerating(ctx, fix.Fix{
			Source: "validation", ReleaseID: "rel-legacy-inflight-generating", NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a", "model.b"}, Attempt: 1,
			NodeOutcomes: map[string]fix.NodeOutcome{
				"model.a": {Status: fix.StatusGenerating},
				"model.b": settledFailure,
			},
			CreatedAt: contractBase,
		}))
		// An attempt that recorded no per-node outcome at all: its node_outcomes
		// holds no entries, which aggregates to SQL NULL, not an empty object.
		require.NoError(t, env.fixes.InsertGenerating(ctx, fix.Fix{
			Source: "validation", ReleaseID: "rel-legacy-inflight-no-outcomes", NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a"}, Attempt: 1, NodeOutcomes: nil, CreatedAt: contractBase,
		}))
		// The runner's rows in flight, which carry their remediation.
		require.NoError(t, env.fixes.InsertGenerating(ctx, fix.Fix{
			Source: "validation", ReleaseID: "rel-legacy-inflight-runner", RemediationID: r.ID.String(), NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a"}, Attempt: 1,
			NodeOutcomes: map[string]fix.NodeOutcome{"model.a": {Status: fix.StatusGenerating}},
			CreatedAt:    contractBase,
		}))
		runnerVerifying := map[string]fix.NodeOutcome{"model.a": {Status: fix.StatusVerifying}}
		require.NoError(t, env.fixes.Upsert(ctx, fix.Fix{
			Source: "validation", ReleaseID: "rel-legacy-inflight-runner", RemediationID: r.ID.String(), NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a"}, Attempt: 2, Status: fix.StatusVerifying,
			NodeOutcomes: runnerVerifying, Edits: edits, ReleaseChecks: runs, Rationale: "the runner's fix", SourceResolved: true,
			CreatedAt: contractBase,
		}))
		// Rows with no remediation that ended: nothing to fail.
		require.NoError(t, env.fixes.Upsert(ctx, fix.Fix{
			Source: "validation", ReleaseID: "rel-legacy-inflight-ended", NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a"}, Attempt: 1, Status: fix.StatusFailed,
			NodeOutcomes: map[string]fix.NodeOutcome{"model.a": settledFailure},
			Rationale:    "the model declined", CreatedAt: contractBase,
		}))
		require.NoError(t, env.fixes.Upsert(ctx, fix.Fix{
			Source: "validation", ReleaseID: "rel-legacy-inflight-ended", NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a"}, Attempt: 2, Status: fix.StatusProposed,
			NodeOutcomes: map[string]fix.NodeOutcome{"model.a": {Status: fix.StatusProposed}},
			Edits:        edits, ReleaseChecks: runs, Rationale: "a verified fix", SourceResolved: true, CreatedAt: contractBase,
		}))

		beforeVerifying := get("rel-legacy-inflight-verifying", 1)
		beforeGenerating := get("rel-legacy-inflight-generating", 1)
		require.Equal(t, fix.StatusVerifying, beforeVerifying.Status)
		require.Equal(t, fix.StatusGenerating, beforeGenerating.Status)
		untouched := []fix.View{
			get("rel-legacy-inflight-runner", 1),
			get("rel-legacy-inflight-runner", 2),
			get("rel-legacy-inflight-ended", 1),
			get("rel-legacy-inflight-ended", 2),
		}

		moved, err := env.fixes.FailLegacyInFlight(ctx, reason)
		require.NoError(t, err)
		require.Equal(t, 3, moved, "the three rows in flight with no remediation, and no other")

		verifying := get("rel-legacy-inflight-verifying", 1)
		require.Equal(t, fix.StatusFailed, verifying.Status)
		require.Equal(t, reason, verifying.VerifyError, "a row that was verifying records the reason as its verify error")
		require.Equal(t, "the fix", verifying.Rationale, "and keeps its rationale")
		require.Equal(t, edits, verifying.Edits, "its edits")
		require.Equal(t, runs, verifying.ReleaseChecks, "and its release checks")
		require.Equal(t, map[string]fix.NodeOutcome{
			"model.a": {Status: fix.StatusFailed, Reason: reason},
			"model.b": {Status: fix.StatusFailed, Reason: reason},
			"model.c": handedOver,
		}, verifying.NodeOutcomes, "each outcome in flight fails with the reason, the hand-over stays")

		generating := get("rel-legacy-inflight-generating", 1)
		require.Equal(t, fix.StatusFailed, generating.Status)
		require.Equal(t, reason, generating.Rationale, "a row that was generating records the reason as its rationale")
		require.Empty(t, generating.VerifyError)
		require.Equal(t, map[string]fix.NodeOutcome{
			"model.a": {Status: fix.StatusFailed, Reason: reason},
			"model.b": settledFailure,
		}, generating.NodeOutcomes, "the outcome in flight fails with the reason, the one that ended stays")

		noOutcomes := get("rel-legacy-inflight-no-outcomes", 1)
		require.Equal(t, fix.StatusFailed, noOutcomes.Status)
		require.Equal(t, reason, noOutcomes.Rationale)
		require.Empty(t, noOutcomes.NodeOutcomes, "a row with no per-node outcome still reads back with none")

		require.Equal(t, untouched[0], get("rel-legacy-inflight-runner", 1), "a runner row generating is the runner's to finish")
		require.Equal(t, untouched[1], get("rel-legacy-inflight-runner", 2), "so is a runner row verifying")
		require.Equal(t, untouched[2], get("rel-legacy-inflight-ended", 1), "a row that failed already is left as it is")
		require.Equal(t, untouched[3], get("rel-legacy-inflight-ended", 2), "so is a row that was proposed")

		moved, err = env.fixes.FailLegacyInFlight(ctx, reason)
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
