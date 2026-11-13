package outputs_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/memory"
	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/outputs"
	"github.com/carolsimone/continuo/pkg/streams"
)

var (
	ctx = context.Background()
	now = time.Date(2026, 11, 13, 9, 30, 0, 0, time.UTC)
)

type fixedClock struct{}

func (fixedClock) Now() time.Time { return now }

func trigger() remediation.Trigger {
	return remediation.Trigger{
		TenantID: "default", Source: "validation", ReleaseID: "rel-1", Round: 1,
		Repo: "acme/dbt", CommitSHA: "abc123",
		Nodes: []remediation.TriggerNode{
			{NodeID: "model.b", ErrorSignature: "sig-b", Service: "sales"},
			{NodeID: "model.a", ErrorSignature: "sig-a", Service: "finance"},
		},
	}
}

func newRemediation(t *testing.T, store *memory.Store) remediation.Remediation {
	t.Helper()
	rem := remediation.Remediation{
		ID: uuid.New(), TenantID: "default", ReleaseID: "rel-1", Round: 1,
		Flow: "default", Status: remediation.StatusRunning,
	}
	created, err := store.RemediationRepository().Create(ctx, rem)
	require.NoError(t, err)
	require.True(t, created)
	return rem
}

func verifying(rem remediation.Remediation) proposal.Proposal {
	return proposal.Proposal{
		Source: "validation", ReleaseID: "rel-1", RemediationID: rem.ID.String(), RemediationRound: 1,
		NodeID: "model.a", ResolvedNodeIDs: []string{"model.a", "model.b"}, ErrorSignature: "sig-a",
		Attempt: 1, Status: proposal.StatusVerifying,
		NodeOutcomes:  map[string]proposal.NodeOutcome{"model.a": {Status: proposal.StatusVerifying}, "model.b": {Status: proposal.StatusVerifying}},
		Verifications: []proposal.Verification{{Service: "sales", Kind: "dbt", RunID: "verify-rel-1-sales-a1", Phase: proposal.PhaseQueued}},
		Confidence:    proposal.ConfidenceHigh, Rationale: "the column was renamed", Model: "model-x",
		ProposedSQLURI: "s3://b/a.sql", DiffURI: "s3://b/a.diff", SourceResolved: true,
		Edits: []proposal.FileEdit{
			{Path: "dbt/services/sales/models/a.sql", ContentURI: "s3://b/a.sql", DiffURI: "s3://b/a.diff", TargetNodeID: "model.a"},
			{Path: "dbt/services/finance/models/b.sql", ContentURI: "s3://b/b.sql", DiffURI: "s3://b/b.diff", TargetNodeID: "model.b"},
		},
		TriggerPayload: []byte(`{}`), CreatedAt: now,
	}
}

func seed(t *testing.T, store *memory.Store, p proposal.Proposal) string {
	t.Helper()
	require.NoError(t, store.ProposalRepository().Upsert(ctx, p))
	rows := store.Proposals()
	return rows[len(rows)-1].ID
}

func verified(t *testing.T, proposalID string) remediation.Verified {
	t.Helper()
	checked, err := remediation.NewChecked(remediation.Draft{}, nil)
	require.NoError(t, err)
	v, err := remediation.NewVerified(proposalID, []remediation.Checked{checked}, nil,
		[]proposal.Verification{{Service: "sales", Kind: "dbt", RunID: "verify-rel-1-sales-a1", Phase: proposal.PhasePassed}})
	require.NoError(t, err)
	return v
}

func TestPullRequest_Meta(t *testing.T) {
	m := outputs.NewPullRequest(fixedClock{}).Meta()
	assert.Equal(t, "pull_request", string(m.Name))
	assert.Equal(t, step.KindOutput, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Empty(t, m.Needs)
}

func TestHandOver_Meta(t *testing.T) {
	m := outputs.NewHandOver(fixedClock{}).Meta()
	assert.Equal(t, "hand_over", string(m.Name))
	assert.Equal(t, step.KindOutput, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Empty(t, m.Needs)
}

func TestPullRequest_ZeroVerifiedWritesNothing(t *testing.T) {
	store := memory.NewStore()
	rem := newRemediation(t, store)
	seed(t, store, verifying(rem))

	u := memory.NewUnitOfWork(store)
	require.NoError(t, u.Begin(ctx))
	err := outputs.NewPullRequest(fixedClock{}).Pass(ctx, u, step.OutputInput{Remediation: rem, Trigger: trigger()}, remediation.Verified{})
	require.Error(t, err)
	require.NoError(t, u.Commit())

	assert.Equal(t, proposal.StatusVerifying, store.Proposals()[0].Proposal.Status)
	assert.Empty(t, store.OutboxEntries())
}

func TestPullRequest_MarksTheFixVerifiedAndEnqueuesTheProposedEvent(t *testing.T) {
	store := memory.NewStore()
	rem := newRemediation(t, store)
	p := verifying(rem)
	id := seed(t, store, p)

	u := memory.NewUnitOfWork(store)
	require.NoError(t, u.Begin(ctx))
	require.NoError(t, outputs.NewPullRequest(fixedClock{}).Pass(ctx, u, step.OutputInput{Remediation: rem, Trigger: trigger()}, verified(t, id)))
	require.NoError(t, u.Commit())

	row := store.Proposals()[0].Proposal
	assert.Equal(t, proposal.StatusProposed, row.Status)
	assert.Equal(t, proposal.StatusProposed, row.NodeOutcomes["model.a"].Status)
	assert.Equal(t, proposal.PhasePassed, row.Verifications[0].Phase)

	entries := store.OutboxEntries()
	require.Len(t, entries, 1)
	e := entries[0]
	eventID := event.RemediationEventID("rel-1", 1)
	assert.Equal(t, uuid.NewSHA1(uuid.NameSpaceOID, []byte(eventID.String())), e.ID)
	assert.Equal(t, event.AggregateType, e.AggregateType)
	assert.Equal(t, event.AggregateIDForRelease("rel-1"), e.AggregateID)
	assert.Equal(t, event.EventType, e.EventType)
	assert.Equal(t, streams.RemediationProposedV1, e.StreamName)
	assert.Equal(t, "pending", e.Status)
	assert.Nil(t, e.MessageProcessingID)

	// The payload is built from the row's fields, as handlers.Enqueue builds it.
	var got map[string]any
	require.NoError(t, json.Unmarshal(e.Payload, &got))
	assert.Equal(t, map[string]any{
		"event_id":          eventID.String(),
		"source":            "validation",
		"release_id":        "rel-1",
		"remediation_round": float64(1),
		"node_id":           "model.a",
		"resolved_node_ids": []any{"model.a", "model.b"},
		"error_signature":   "sig-a",
		"proposed_sql_uri":  "s3://b/a.sql",
		"diff_uri":          "s3://b/a.diff",
		"edits": []any{
			map[string]any{"path": "dbt/services/sales/models/a.sql", "content_uri": "s3://b/a.sql", "diff_uri": "s3://b/a.diff", "target_node_id": "model.a"},
			map[string]any{"path": "dbt/services/finance/models/b.sql", "content_uri": "s3://b/b.sql", "diff_uri": "s3://b/b.diff", "target_node_id": "model.b"},
		},
		"rationale":       "the column was renamed",
		"confidence":      "high",
		"model":           "model-x",
		"attempt":         float64(1),
		"source_resolved": true,
		"proposed_at":     now.Format(time.RFC3339),
	}, got)
}

func TestPullRequest_ASecondPassOnAFinalizedRowWritesNothing(t *testing.T) {
	store := memory.NewStore()
	rem := newRemediation(t, store)
	id := seed(t, store, verifying(rem))
	pr := outputs.NewPullRequest(fixedClock{})
	in := step.OutputInput{Remediation: rem, Trigger: trigger()}

	for range 2 {
		u := memory.NewUnitOfWork(store)
		require.NoError(t, u.Begin(ctx))
		require.NoError(t, pr.Pass(ctx, u, in, verified(t, id)))
		require.NoError(t, u.Commit())
	}
	assert.Len(t, store.OutboxEntries(), 1, "the repeated pass emits nothing")
	assert.Equal(t, proposal.StatusProposed, store.Proposals()[0].Proposal.Status)
}

func TestHandOver_WritesTheEscalatedRowForTheWholeFailingSet(t *testing.T) {
	store := memory.NewStore()
	rem := newRemediation(t, store)
	// Two terminal attempts of round 1: the next attempt is 3.
	for i, st := range []proposal.Status{proposal.StatusFailed, proposal.StatusFailed} {
		p := verifying(rem)
		p.Attempt, p.Status = i+1, st
		seed(t, store, p)
	}

	u := memory.NewUnitOfWork(store)
	require.NoError(t, u.Begin(ctx))
	require.NoError(t, outputs.NewHandOver(fixedClock{}).GiveUp(ctx, u, step.OutputInput{Remediation: rem, Trigger: trigger()}))
	require.NoError(t, u.Commit())

	rows := store.Proposals()
	require.Len(t, rows, 3)
	got := rows[2].Proposal
	assert.Equal(t, 3, got.Attempt)
	assert.Equal(t, proposal.StatusEscalated, got.Status)
	assert.Equal(t, rem.ID.String(), got.RemediationID)
	assert.Equal(t, "validation", got.Source)
	assert.Equal(t, "rel-1", got.ReleaseID)
	assert.Equal(t, 1, got.RemediationRound)
	assert.Equal(t, []string{"model.a", "model.b"}, got.ResolvedNodeIDs)
	assert.Equal(t, "model.a", got.NodeID)
	assert.Equal(t, "sig-a", got.ErrorSignature)
	assert.Equal(t, []string{"finance", "sales"}, got.Services)
	assert.Equal(t, map[string]proposal.NodeOutcome{
		"model.a": {Status: proposal.StatusEscalated},
		"model.b": {Status: proposal.StatusEscalated},
	}, got.NodeOutcomes)
	assert.Empty(t, got.Rationale)
	assert.Equal(t, now, got.CreatedAt)
	assert.Empty(t, store.OutboxEntries())
}

func TestHandOver_CountsTheTerminalAttemptsOfEveryRoundUpToTheTriggersRound(t *testing.T) {
	store := memory.NewStore()
	rem := newRemediation(t, store)
	p := verifying(rem)
	p.Attempt, p.Status, p.RemediationRound = 1, proposal.StatusFailed, 1
	seed(t, store, p)
	p.Attempt, p.Status = 2, proposal.StatusEscalated
	seed(t, store, p)

	tr := trigger()
	tr.Round = 2
	u := memory.NewUnitOfWork(store)
	require.NoError(t, u.Begin(ctx))
	require.NoError(t, outputs.NewHandOver(fixedClock{}).GiveUp(ctx, u, step.OutputInput{Remediation: rem, Trigger: tr}))
	require.NoError(t, u.Commit())

	rows := store.Proposals()
	require.Len(t, rows, 3)
	assert.Equal(t, 3, rows[2].Proposal.Attempt)
	assert.Equal(t, 2, rows[2].Proposal.RemediationRound)
}

func TestHandOver_FinalizesTheInFlightRowOfTheAttemptItChooses(t *testing.T) {
	store := memory.NewStore()
	rem := newRemediation(t, store)
	p := verifying(rem)
	p.Attempt, p.Status = 1, proposal.StatusGenerating
	seed(t, store, p)

	u := memory.NewUnitOfWork(store)
	require.NoError(t, u.Begin(ctx))
	require.NoError(t, outputs.NewHandOver(fixedClock{}).GiveUp(ctx, u, step.OutputInput{Remediation: rem, Trigger: trigger()}))
	require.NoError(t, u.Commit())

	rows := store.Proposals()
	require.Len(t, rows, 1, "an in-flight row is not counted, so the escalated row finalizes it")
	assert.Equal(t, proposal.StatusEscalated, rows[0].Proposal.Status)
	assert.Equal(t, rem.ID.String(), rows[0].Proposal.RemediationID)
}
