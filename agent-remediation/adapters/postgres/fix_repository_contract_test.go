//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

// ReopenFixRow moves the failed fix row of one try of a
// remediation back in flight. Reopened generating, the row loses the
// rationale its failure recorded and takes back the per-node outcomes
// FailGenerating rewrote, keeping every outcome FailGenerating left alone.
// Reopened verifying, it loses its verify error and keeps its edits, runs and
// rationale, with the outcomes given. It changes no row of another
// remediation, no row that belongs to no remediation and no row that is not failed.
func TestFixRepository_ReopenFixRowMovesOnlyItsFailedRowBack(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		r := mustCreate(t, env, newRemediation("rel-reopen", 1, contractBase))
		other := mustCreate(t, env, newRemediation("rel-reopen-other", 1, contractBase))
		get := func(id string) fix.View {
			t.Helper()
			v, err := env.fixes.Get(ctx, id)
			require.NoError(t, err)
			return v
		}
		handedOver := fix.NodeOutcome{Status: fix.StatusFailed, Reason: "no remediation type matches this failure", Code: model.HandoverReasonNoType}
		generating := map[string]fix.NodeOutcome{
			"model.a": {Status: fix.StatusGenerating},
			"model.b": {Status: fix.StatusGenerating},
			"model.c": handedOver,
		}

		require.NoError(t, env.fixes.InsertGenerating(ctx, fix.Fix{
			Source: "validation", ReleaseID: "rel-reopen", RemediationID: r.ID.String(), NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a", "model.b", "model.c"}, NodeOutcomes: generating, Try: 1, CreatedAt: contractBase,
		}))
		_, err := env.fixes.FailGenerating(ctx, "rel-reopen", "stuck; dead-lettered")
		require.NoError(t, err)
		first := findTry(t, env, "rel-reopen", 1)
		require.Equal(t, fix.StatusFailed, first.Status)
		require.Equal(t, fix.NodeOutcome{Status: fix.StatusFailed, Reason: "stuck; dead-lettered"}, first.NodeOutcomes["model.a"])

		_, err = env.fixes.ReopenFixRow(ctx, r.ID, 1, fix.StatusGenerating, generating)
		require.Error(t, err, "a row reopened generating takes back its own outcomes")
		require.Equal(t, fix.StatusFailed, get(first.ID).Status, "a refused reopen writes nothing")

		hit, err := env.fixes.ReopenFixRow(ctx, r.ID, 1, fix.StatusGenerating, nil)
		require.NoError(t, err)
		require.True(t, hit)
		got := get(first.ID)
		require.Equal(t, fix.StatusGenerating, got.Status)
		require.Empty(t, got.Rationale, "the reason of the failure is gone")
		require.Equal(t, generating, got.NodeOutcomes, "the outcomes FailGenerating rewrote are generating again, the hand-over kept")

		hit, err = env.fixes.ReopenFixRow(ctx, r.ID, 1, fix.StatusGenerating, nil)
		require.NoError(t, err)
		require.False(t, hit, "a row in flight is not reopened")

		edits := []fix.FileEdit{{Path: "models/a.sql", ContentURI: "s3://c", DiffURI: "s3://d", TargetNodeID: "model.a", MemberNodeIDs: []string{"model.a"}}}
		runs := []fix.ReleaseCheck{{Project: "svc", Kind: "dbt", CheckID: "run-1", Phase: fix.PhaseQueued}}
		verifyingOutcomes := map[string]fix.NodeOutcome{"model.a": {Status: fix.StatusVerifying}, "model.c": handedOver}
		require.NoError(t, env.fixes.Upsert(ctx, fix.Fix{
			Source: "validation", ReleaseID: "rel-reopen", RemediationID: r.ID.String(), NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a", "model.c"}, NodeOutcomes: verifyingOutcomes, Try: 2, Status: fix.StatusVerifying,
			Edits: edits, ReleaseChecks: runs, Rationale: "the fix", SourceResolved: true, CreatedAt: contractBase,
		}))
		second := findTry(t, env, "rel-reopen", 2)
		_, err = env.fixes.MarkVerifyFailed(ctx, second.ID, "stuck; dead-lettered", runs)
		require.NoError(t, err)
		require.Equal(t, fix.StatusFailed, get(second.ID).Status)

		hit, err = env.fixes.ReopenFixRow(ctx, r.ID, 2, fix.StatusVerifying, verifyingOutcomes)
		require.NoError(t, err)
		require.True(t, hit)
		got = get(second.ID)
		require.Equal(t, fix.StatusVerifying, got.Status)
		require.Empty(t, got.VerifyError)
		require.Equal(t, "the fix", got.Rationale)
		require.Equal(t, edits, got.Edits)
		require.Equal(t, runs, got.ReleaseChecks)
		require.Equal(t, verifyingOutcomes, got.NodeOutcomes)

		require.NoError(t, env.fixes.Upsert(ctx, fix.Fix{
			Source: "validation", ReleaseID: "rel-reopen", NodeID: "model.a", Try: 3, Status: fix.StatusFailed,
			Rationale: "no remediation's", CreatedAt: contractBase,
		}))
		require.NoError(t, env.fixes.Upsert(ctx, fix.Fix{
			Source: "validation", ReleaseID: "rel-reopen-other", RemediationID: other.ID.String(), NodeID: "model.a",
			Try: 4, Status: fix.StatusFailed, Rationale: "the other's", CreatedAt: contractBase,
		}))
		for _, try := range []int{3, 4} {
			hit, err = env.fixes.ReopenFixRow(ctx, r.ID, try, fix.StatusGenerating, nil)
			require.NoError(t, err)
			require.False(t, hit, "try %d belongs to no try of the remediation", try)
		}
		require.Equal(t, fix.StatusFailed, findTry(t, env, "rel-reopen", 3).Status)
		require.Equal(t, fix.StatusFailed, findTry(t, env, "rel-reopen-other", 4).Status)

		_, err = env.fixes.ReopenFixRow(ctx, r.ID, 3, fix.StatusProposed, nil)
		require.Error(t, err, "a row is reopened generating or verifying")
	})
}

// A row keeps the approvers it is recorded with, and RecordApprovers adds to
// them: one entry per node, an approver for a node the row already holds
// replacing it, the whole sorted by node id, so the fixed nodes and the
// handed-over nodes of one try end up on the same row. A try of the
// release that has no row is ErrNotFound with nothing written, and a row
// recorded without approvers reads back with none. An upsert of a row that
// carries approvers replaces the row's, and one that carries none keeps them.
func TestFixRepository_KeepsApprovers(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		r := mustCreate(t, env, newRemediation("rel-approvers", 1, contractBase))
		get := func(try int) fix.View {
			t.Helper()
			return findTry(t, env, "rel-approvers", try)
		}
		row := func(try int, approvers []fix.Approver) fix.Fix {
			return fix.Fix{
				Source: "validation", ReleaseID: "rel-approvers", RemediationID: r.ID.String(), NodeID: "model.a",
				ResolvedNodeIDs: []string{"model.a", "model.b", "model.c"}, Try: try, Status: fix.StatusVerifying,
				NodeOutcomes: map[string]fix.NodeOutcome{
					"model.a": {Status: fix.StatusVerifying}, "model.b": {Status: fix.StatusVerifying},
					"model.c": {Status: fix.StatusSkipped, Reason: "no type", Code: model.HandoverReasonNoType},
				},
				Approvers: approvers, CreatedAt: contractBase,
			}
		}
		fixed := []fix.Approver{
			{NodeID: "model.a", Owner: "finance", Source: model.ApproverSourceBundle},
			{NodeID: "model.b", Source: model.ApproverSourceNone},
		}

		require.NoError(t, env.fixes.Upsert(ctx, row(1, nil)))
		require.Empty(t, get(1).Approvers, "a row recorded without approvers reads back with none")

		require.NoError(t, env.fixes.InsertGenerating(ctx, fix.Fix{
			Source: "validation", ReleaseID: "rel-approvers", RemediationID: r.ID.String(), NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a"}, Try: 3, CreatedAt: contractBase,
		}))
		require.Empty(t, get(3).Approvers, "a row in flight holds no approver")

		require.NoError(t, env.fixes.Upsert(ctx, row(2, fixed)))
		require.Equal(t, fixed, get(2).Approvers, "an upserted row keeps its approvers")
		view, err := env.fixes.Get(ctx, get(2).ID)
		require.NoError(t, err)
		require.Equal(t, fixed, view.Approvers)

		handedOver := []fix.Approver{{NodeID: "model.c", Owner: "growth", Source: model.ApproverSourceContractFile}}
		require.NoError(t, env.fixes.RecordApprovers(ctx, "rel-approvers", 1, 1, append(append([]fix.Approver(nil), handedOver...), fixed...)))
		require.Equal(t, []fix.Approver{fixed[0], fixed[1], handedOver[0]}, get(1).Approvers,
			"the approvers are stored sorted by node id")

		require.NoError(t, env.fixes.RecordApprovers(ctx, "rel-approvers", 1, 2, handedOver))
		require.Equal(t, []fix.Approver{fixed[0], fixed[1], handedOver[0]}, get(2).Approvers,
			"the fixed and the handed-over nodes of one try add up on its row")

		renamed := []fix.Approver{{NodeID: "model.b", Owner: "ops", Source: model.ApproverSourceBundle}}
		require.NoError(t, env.fixes.RecordApprovers(ctx, "rel-approvers", 1, 2, renamed))
		require.Equal(t, []fix.Approver{fixed[0], renamed[0], handedOver[0]}, get(2).Approvers,
			"an approver for a node the row holds replaces its entry")
		require.NoError(t, env.fixes.RecordApprovers(ctx, "rel-approvers", 1, 2, nil))
		require.Equal(t, []fix.Approver{fixed[0], renamed[0], handedOver[0]}, get(2).Approvers, "recording nothing changes nothing")

		require.ErrorIs(t, env.fixes.RecordApprovers(ctx, "rel-approvers", 1, 9, handedOver), repository.ErrNotFound)
		require.ErrorIs(t, env.fixes.RecordApprovers(ctx, "rel-approvers", 2, 1, handedOver), repository.ErrNotFound, "try 1 of another round is another row")
		require.ErrorIs(t, env.fixes.RecordApprovers(ctx, "rel-other", 1, 1, handedOver), repository.ErrNotFound)
		require.Equal(t, []fix.Approver{fixed[0], fixed[1], handedOver[0]}, get(1).Approvers, "a refused call writes nothing")

		require.NoError(t, env.fixes.Upsert(ctx, row(2, nil)))
		require.Equal(t, []fix.Approver{fixed[0], renamed[0], handedOver[0]}, get(2).Approvers,
			"an upsert that carries no approvers keeps the row's")
		require.NoError(t, env.fixes.Upsert(ctx, row(2, handedOver)))
		require.Equal(t, handedOver, get(2).Approvers, "an upsert that carries approvers replaces the row's")
	})
}

// CountTries counts the terminal tries of one round: a row in flight is no
// finished try yet, and the round's hand-over row (fix.HandOverTry), written
// once its tries are spent, is no try of its own.
func TestFixRepository_CountTriesLeavesOutTheHandOverRow(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		for try, status := range map[int]fix.Status{1: fix.StatusFailed, 2: fix.StatusFailed, 3: fix.StatusVerifying, fix.HandOverTry: fix.StatusEscalated} {
			require.NoError(t, env.fixes.Upsert(ctx, fix.Fix{
				Source: "validation", ReleaseID: "rel-count", Round: 1, NodeID: "model.a",
				Try: try, Status: status, CreatedAt: contractBase,
			}))
		}
		n, err := env.fixes.CountTries(ctx, "rel-count", 1)
		require.NoError(t, err)
		require.Equal(t, 2, n, "the two failed tries count; the verifying try and the hand-over row do not")
	})
}

// findTry returns the fix row of one try of a release.
func findTry(t *testing.T, env contractEnv, release string, try int) fix.View {
	t.Helper()
	views, err := env.fixes.List(context.Background(), repository.FixFilter{ReleaseID: release})
	require.NoError(t, err)
	for _, v := range views {
		if v.Try == try {
			return v
		}
	}
	require.Failf(t, "no fix row", "release %s try %d", release, try)
	return fix.View{}
}
