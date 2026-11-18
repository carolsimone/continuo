//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

// ReopenTryAttempt moves the failed proposal row of one attempt of a
// remediation back in flight. Reopened generating, the row loses the
// rationale its failure recorded and takes back the per-node outcomes
// FailGenerating rewrote, keeping every outcome FailGenerating left alone.
// Reopened verifying, it loses its verify error and keeps its edits, runs and
// rationale, with the outcomes given. It changes no row of another
// remediation, no row of the driver and no row that is not failed.
func TestProposalRepository_ReopenTryAttemptMovesOnlyItsFailedRowBack(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		r := mustCreate(t, env, newRemediation("rel-reopen", 1, contractBase))
		other := mustCreate(t, env, newRemediation("rel-reopen-other", 1, contractBase))
		get := func(id string) proposal.View {
			t.Helper()
			v, err := env.proposals.Get(ctx, id)
			require.NoError(t, err)
			return v
		}
		handedOver := proposal.NodeOutcome{Status: proposal.StatusFailed, Reason: "no remediation type matches this failure", Code: model.HandoverReasonNoType}
		generating := map[string]proposal.NodeOutcome{
			"model.a": {Status: proposal.StatusGenerating},
			"model.b": {Status: proposal.StatusGenerating},
			"model.c": handedOver,
		}

		require.NoError(t, env.proposals.InsertGenerating(ctx, proposal.Proposal{
			Source: "validation", ReleaseID: "rel-reopen", RemediationID: r.ID.String(), NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a", "model.b", "model.c"}, NodeOutcomes: generating, Attempt: 1, CreatedAt: contractBase,
		}))
		_, err := env.proposals.FailGenerating(ctx, "rel-reopen", "stuck; dead-lettered")
		require.NoError(t, err)
		first := findAttempt(t, env, "rel-reopen", 1)
		require.Equal(t, proposal.StatusFailed, first.Status)
		require.Equal(t, proposal.NodeOutcome{Status: proposal.StatusFailed, Reason: "stuck; dead-lettered"}, first.NodeOutcomes["model.a"])

		_, err = env.proposals.ReopenTryAttempt(ctx, r.ID, 1, proposal.StatusGenerating, generating)
		require.Error(t, err, "a row reopened generating takes back its own outcomes")
		require.Equal(t, proposal.StatusFailed, get(first.ID).Status, "a refused reopen writes nothing")

		hit, err := env.proposals.ReopenTryAttempt(ctx, r.ID, 1, proposal.StatusGenerating, nil)
		require.NoError(t, err)
		require.True(t, hit)
		got := get(first.ID)
		require.Equal(t, proposal.StatusGenerating, got.Status)
		require.Empty(t, got.Rationale, "the reason of the failure is gone")
		require.Equal(t, generating, got.NodeOutcomes, "the outcomes FailGenerating rewrote are generating again, the hand-over kept")

		hit, err = env.proposals.ReopenTryAttempt(ctx, r.ID, 1, proposal.StatusGenerating, nil)
		require.NoError(t, err)
		require.False(t, hit, "a row in flight is not reopened")

		edits := []proposal.FileEdit{{Path: "models/a.sql", ContentURI: "s3://c", DiffURI: "s3://d", TargetNodeID: "model.a", MemberNodeIDs: []string{"model.a"}}}
		runs := []proposal.Verification{{Service: "svc", Kind: "dbt", RunID: "run-1", Phase: proposal.PhaseQueued}}
		verifyingOutcomes := map[string]proposal.NodeOutcome{"model.a": {Status: proposal.StatusVerifying}, "model.c": handedOver}
		require.NoError(t, env.proposals.Upsert(ctx, proposal.Proposal{
			Source: "validation", ReleaseID: "rel-reopen", RemediationID: r.ID.String(), NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a", "model.c"}, NodeOutcomes: verifyingOutcomes, Attempt: 2, Status: proposal.StatusVerifying,
			Edits: edits, Verifications: runs, Rationale: "the fix", SourceResolved: true, CreatedAt: contractBase,
		}))
		second := findAttempt(t, env, "rel-reopen", 2)
		_, err = env.proposals.MarkVerifyFailed(ctx, second.ID, "stuck; dead-lettered", runs)
		require.NoError(t, err)
		require.Equal(t, proposal.StatusFailed, get(second.ID).Status)

		hit, err = env.proposals.ReopenTryAttempt(ctx, r.ID, 2, proposal.StatusVerifying, verifyingOutcomes)
		require.NoError(t, err)
		require.True(t, hit)
		got = get(second.ID)
		require.Equal(t, proposal.StatusVerifying, got.Status)
		require.Empty(t, got.VerifyError)
		require.Equal(t, "the fix", got.Rationale)
		require.Equal(t, edits, got.Edits)
		require.Equal(t, runs, got.Verifications)
		require.Equal(t, verifyingOutcomes, got.NodeOutcomes)

		require.NoError(t, env.proposals.Upsert(ctx, proposal.Proposal{
			Source: "validation", ReleaseID: "rel-reopen", NodeID: "model.a", Attempt: 3, Status: proposal.StatusFailed,
			Rationale: "the driver's", CreatedAt: contractBase,
		}))
		require.NoError(t, env.proposals.Upsert(ctx, proposal.Proposal{
			Source: "validation", ReleaseID: "rel-reopen-other", RemediationID: other.ID.String(), NodeID: "model.a",
			Attempt: 4, Status: proposal.StatusFailed, Rationale: "the other's", CreatedAt: contractBase,
		}))
		for _, attempt := range []int{3, 4} {
			hit, err = env.proposals.ReopenTryAttempt(ctx, r.ID, attempt, proposal.StatusGenerating, nil)
			require.NoError(t, err)
			require.False(t, hit, "attempt %d belongs to no try of the remediation", attempt)
		}
		require.Equal(t, proposal.StatusFailed, findAttempt(t, env, "rel-reopen", 3).Status)
		require.Equal(t, proposal.StatusFailed, findAttempt(t, env, "rel-reopen-other", 4).Status)

		_, err = env.proposals.ReopenTryAttempt(ctx, r.ID, 3, proposal.StatusProposed, nil)
		require.Error(t, err, "a row is reopened generating or verifying")
	})
}

// A row keeps the approvers it is recorded with, and RecordApprovers adds to
// them: one entry per node, an approver for a node the row already holds
// replacing it, the whole sorted by node id, so the fixed nodes and the
// handed-over nodes of one attempt end up on the same row. An attempt of the
// release that has no row is ErrNotFound with nothing written, and a row
// recorded without approvers reads back with none.
func TestProposalRepository_KeepsApprovers(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		r := mustCreate(t, env, newRemediation("rel-approvers", 1, contractBase))
		get := func(attempt int) proposal.View {
			t.Helper()
			return findAttempt(t, env, "rel-approvers", attempt)
		}
		row := func(attempt int, approvers []proposal.Approver) proposal.Proposal {
			return proposal.Proposal{
				Source: "validation", ReleaseID: "rel-approvers", RemediationID: r.ID.String(), NodeID: "model.a",
				ResolvedNodeIDs: []string{"model.a", "model.b", "model.c"}, Attempt: attempt, Status: proposal.StatusVerifying,
				NodeOutcomes: map[string]proposal.NodeOutcome{
					"model.a": {Status: proposal.StatusVerifying}, "model.b": {Status: proposal.StatusVerifying},
					"model.c": {Status: proposal.StatusSkipped, Reason: "no type", Code: model.HandoverReasonNoType},
				},
				Approvers: approvers, CreatedAt: contractBase,
			}
		}
		fixed := []proposal.Approver{
			{NodeID: "model.a", Owner: "finance", Source: proposal.ApproverSourceBundle},
			{NodeID: "model.b", Source: proposal.ApproverSourceNone},
		}

		require.NoError(t, env.proposals.Upsert(ctx, row(1, nil)))
		require.Empty(t, get(1).Approvers, "a row recorded without approvers reads back with none")

		require.NoError(t, env.proposals.InsertGenerating(ctx, proposal.Proposal{
			Source: "validation", ReleaseID: "rel-approvers", RemediationID: r.ID.String(), NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a"}, Attempt: 3, CreatedAt: contractBase,
		}))
		require.Empty(t, get(3).Approvers, "a row in flight holds no approver")

		require.NoError(t, env.proposals.Upsert(ctx, row(2, fixed)))
		require.Equal(t, fixed, get(2).Approvers, "an upserted row keeps its approvers")
		view, err := env.proposals.Get(ctx, get(2).ID)
		require.NoError(t, err)
		require.Equal(t, fixed, view.Approvers)

		handedOver := []proposal.Approver{{NodeID: "model.c", Owner: "growth", Source: proposal.ApproverSourceContractFile}}
		require.NoError(t, env.proposals.RecordApprovers(ctx, "rel-approvers", 1, append(append([]proposal.Approver(nil), handedOver...), fixed...)))
		require.Equal(t, []proposal.Approver{fixed[0], fixed[1], handedOver[0]}, get(1).Approvers,
			"the approvers are stored sorted by node id")

		require.NoError(t, env.proposals.RecordApprovers(ctx, "rel-approvers", 2, handedOver))
		require.Equal(t, []proposal.Approver{fixed[0], fixed[1], handedOver[0]}, get(2).Approvers,
			"the fixed and the handed-over nodes of one attempt add up on its row")

		renamed := []proposal.Approver{{NodeID: "model.b", Owner: "ops", Source: proposal.ApproverSourceBundle}}
		require.NoError(t, env.proposals.RecordApprovers(ctx, "rel-approvers", 2, renamed))
		require.Equal(t, []proposal.Approver{fixed[0], renamed[0], handedOver[0]}, get(2).Approvers,
			"an approver for a node the row holds replaces its entry")
		require.NoError(t, env.proposals.RecordApprovers(ctx, "rel-approvers", 2, nil))
		require.Equal(t, []proposal.Approver{fixed[0], renamed[0], handedOver[0]}, get(2).Approvers, "recording nothing changes nothing")

		require.ErrorIs(t, env.proposals.RecordApprovers(ctx, "rel-approvers", 9, handedOver), repository.ErrNotFound)
		require.ErrorIs(t, env.proposals.RecordApprovers(ctx, "rel-other", 1, handedOver), repository.ErrNotFound)
		require.Equal(t, []proposal.Approver{fixed[0], fixed[1], handedOver[0]}, get(1).Approvers, "a refused call writes nothing")
	})
}

// findAttempt returns the proposal row of one attempt of a release.
func findAttempt(t *testing.T, env contractEnv, release string, attempt int) proposal.View {
	t.Helper()
	views, err := env.proposals.List(context.Background(), repository.ProposalFilter{ReleaseID: release})
	require.NoError(t, err)
	for _, v := range views {
		if v.Attempt == attempt {
			return v
		}
	}
	require.Failf(t, "no proposal row", "release %s attempt %d", release, attempt)
	return proposal.View{}
}
