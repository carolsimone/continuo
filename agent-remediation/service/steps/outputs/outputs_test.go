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
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
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
		CreatedAt: now,
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
	m := outputs.NewHandOver().Meta()
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

	// The payload is stored under the remediation's tenant and built from the
	// row's fields.
	tenantID, body, err := outbox.UnwrapTenant(e.Payload)
	require.NoError(t, err)
	assert.Equal(t, rem.TenantID, tenantID)
	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
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

func TestPullRequest_StoresTheApproversOfTheFixedNodesWithTheVerifiedRow(t *testing.T) {
	store := memory.NewStore()
	rem := newRemediation(t, store)
	id := seed(t, store, verifying(rem))
	approvers := []proposal.Approver{
		{NodeID: "model.a", Owner: "finance", Source: model.ApproverSourceBundle},
		{NodeID: "model.b", Source: model.ApproverSourceNone},
	}
	pr := outputs.NewPullRequest(fixedClock{})

	u := memory.NewUnitOfWork(store)
	require.NoError(t, u.Begin(ctx))
	require.NoError(t, pr.Pass(ctx, u, step.OutputInput{Remediation: rem, Trigger: trigger(), Approvers: approvers}, verified(t, id)))
	require.NoError(t, u.Commit())

	row := store.Proposals()[0].Proposal
	assert.Equal(t, proposal.StatusProposed, row.Status)
	assert.Equal(t, approvers, row.Approvers, "the row that is marked verified carries its fixed nodes' approvers")
	assert.Len(t, store.OutboxEntries(), 1)

	// A pass on a row that is already final changes nothing, the approvers
	// included.
	other := []proposal.Approver{{NodeID: "model.c", Owner: "ops", Source: model.ApproverSourceBundle}}
	u = memory.NewUnitOfWork(store)
	require.NoError(t, u.Begin(ctx))
	require.NoError(t, pr.Pass(ctx, u, step.OutputInput{Remediation: rem, Trigger: trigger(), Approvers: other}, verified(t, id)))
	require.NoError(t, u.Commit())
	assert.Equal(t, approvers, store.Proposals()[0].Proposal.Approvers)
}

func TestPullRequest_ARefusedPassStoresNoApprovers(t *testing.T) {
	store := memory.NewStore()
	rem := newRemediation(t, store)
	seed(t, store, verifying(rem))

	u := memory.NewUnitOfWork(store)
	require.NoError(t, u.Begin(ctx))
	err := outputs.NewPullRequest(fixedClock{}).Pass(ctx, u,
		step.OutputInput{Remediation: rem, Trigger: trigger(), Approvers: []proposal.Approver{{NodeID: "model.a", Owner: "finance"}}},
		remediation.Verified{})
	require.Error(t, err)
	require.NoError(t, u.Commit())

	assert.Empty(t, store.Proposals()[0].Proposal.Approvers)
}

// handedOverEntryID is the id of the outbox entry that carries the
// remediation.handed_over:v1 event of remediation id.
func handedOverEntryID(id uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("handed_over|"+id.String()))
}

// endedRemediation is a remediation of tenant acme that made three tries.
func endedRemediation(t *testing.T, store *memory.Store) remediation.Remediation {
	t.Helper()
	rem := newRemediation(t, store)
	rem.TenantID = "acme"
	rem.Flow = "rejected-release"
	rem.CurrentTry = 3
	return rem
}

// handedOver is two nodes left to a human, sorted by node id.
func handedOver() []remediation.HandOver {
	return []remediation.HandOver{
		{NodeID: "model.a", Code: model.HandoverReasonTriesSpent, Message: "tried 3 times; the last try failed: model.a: still broken"},
		{NodeID: "model.b", Code: model.HandoverReasonDeclined, Message: "the compile error names no source file, so there is no file to fix"},
	}
}

func giveUp(t *testing.T, store *memory.Store, in step.OutputInput) error {
	t.Helper()
	u := memory.NewUnitOfWork(store)
	require.NoError(t, u.Begin(ctx))
	defer func() { _ = u.Rollback() }()
	if err := outputs.NewHandOver().GiveUp(ctx, u, in); err != nil {
		return err
	}
	return u.Commit()
}

func TestHandOver_EmitsOneEventWithEveryHandedOverNode(t *testing.T) {
	store := memory.NewStore()
	rem := endedRemediation(t, store)

	require.NoError(t, giveUp(t, store, step.OutputInput{Remediation: rem, Trigger: trigger(), HandedOver: handedOver()}))

	assert.Empty(t, store.Proposals(), "the output writes no proposal row")
	entries := store.OutboxEntries()
	require.Len(t, entries, 1)
	e := entries[0]
	assert.Equal(t, handedOverEntryID(rem.ID), e.ID)
	assert.Equal(t, event.AggregateType, e.AggregateType)
	assert.Equal(t, event.AggregateIDForRelease("rel-1"), e.AggregateID)
	assert.Equal(t, event.HandedOverEventType, e.EventType)
	assert.Equal(t, streams.RemediationHandedOverV1, e.StreamName)
	assert.Equal(t, "pending", e.Status)
	assert.Nil(t, e.MessageProcessingID)

	tenantID, body, err := outbox.UnwrapTenant(e.Payload)
	require.NoError(t, err)
	assert.Equal(t, "acme", tenantID, "the event is stored under the remediation's tenant")
	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	assert.Equal(t, map[string]any{
		"remediation_id":    rem.ID.String(),
		"release_id":        "rel-1",
		"remediation_round": float64(1),
		"flow":              "rejected-release",
		"tries":             float64(3),
		"nodes": []any{
			map[string]any{"node_id": "model.a", "reason": string(model.HandoverReasonTriesSpent), "message": "tried 3 times; the last try failed: model.a: still broken"},
			map[string]any{"node_id": "model.b", "reason": string(model.HandoverReasonDeclined), "message": "the compile error names no source file, so there is no file to fix"},
		},
	}, got)
	var p events.RemediationHandedOver
	require.NoError(t, json.Unmarshal(body, &p))
	assert.Len(t, p.Nodes, 2)
}

// The entry id is derived from the remediation: two runs of one remediation's
// end name the same entry, and the second collides with the first, so the
// outbox keeps one row. Another remediation's event has another id.
func TestHandOver_EventIdIsStableSoARerunEmitsOnce(t *testing.T) {
	store := memory.NewStore()
	rem := endedRemediation(t, store)
	in := step.OutputInput{Remediation: rem, Trigger: trigger(), HandedOver: handedOver()}

	require.NoError(t, giveUp(t, store, in))
	require.Error(t, giveUp(t, store, in), "a rerun of the same end collides with the event already enqueued")

	entries := store.OutboxEntries()
	require.Len(t, entries, 1, "the outbox keeps one row")
	assert.Equal(t, handedOverEntryID(rem.ID), entries[0].ID)

	again := memory.NewStore()
	require.NoError(t, giveUp(t, again, in))
	require.Len(t, again.OutboxEntries(), 1)
	assert.Equal(t, entries[0].ID, again.OutboxEntries()[0].ID, "another run of the same remediation's end names the same entry")

	other := rem
	other.ID = uuid.New()
	require.NoError(t, giveUp(t, store, step.OutputInput{Remediation: other, Trigger: trigger(), HandedOver: handedOver()}))
	entries = store.OutboxEntries()
	require.Len(t, entries, 2)
	assert.Equal(t, handedOverEntryID(other.ID), entries[1].ID)
	assert.NotEqual(t, entries[0].ID, entries[1].ID, "another remediation's event has another id")
}

func TestHandOver_NoNodeToHandOverWritesNothing(t *testing.T) {
	store := memory.NewStore()
	rem := endedRemediation(t, store)

	require.NoError(t, giveUp(t, store, step.OutputInput{Remediation: rem, Trigger: trigger()}))

	assert.Empty(t, store.OutboxEntries())
	assert.Empty(t, store.Proposals())
}

func TestHandOver_NamesEachNodesOwner(t *testing.T) {
	store := memory.NewStore()
	rem := endedRemediation(t, store)
	approvers := []proposal.Approver{
		{NodeID: "model.a", Owner: "finance", Source: model.ApproverSourceBundle},
		{NodeID: "model.b", Source: model.ApproverSourceNone},
	}

	require.NoError(t, giveUp(t, store, step.OutputInput{Remediation: rem, Trigger: trigger(), HandedOver: handedOver(), Approvers: approvers}))

	entries := store.OutboxEntries()
	require.Len(t, entries, 1)
	_, body, err := outbox.UnwrapTenant(entries[0].Payload)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	nodes := got["nodes"].([]any)
	require.Len(t, nodes, 2)
	assert.Equal(t, map[string]any{
		"node_id": "model.a", "reason": string(model.HandoverReasonTriesSpent),
		"message": "tried 3 times; the last try failed: model.a: still broken", "owner": "finance",
	}, nodes[0], "a node with an owner names it")
	assert.Equal(t, map[string]any{
		"node_id": "model.b", "reason": string(model.HandoverReasonDeclined),
		"message": "the compile error names no source file, so there is no file to fix",
	}, nodes[1], "a node with no owner carries no owner key")
}

func TestHandOver_AnApproverOfAnotherNodeIsNotNamed(t *testing.T) {
	store := memory.NewStore()
	rem := endedRemediation(t, store)
	approvers := []proposal.Approver{
		{NodeID: "model.a", Owner: "finance", Source: model.ApproverSourceBundle},
		{NodeID: "model.fixed", Owner: "growth", Source: model.ApproverSourceBundle},
	}

	require.NoError(t, giveUp(t, store, step.OutputInput{Remediation: rem, Trigger: trigger(), HandedOver: handedOver()[:1], Approvers: approvers}))

	_, body, err := outbox.UnwrapTenant(store.OutboxEntries()[0].Payload)
	require.NoError(t, err)
	var p events.RemediationHandedOver
	require.NoError(t, json.Unmarshal(body, &p))
	assert.Equal(t, []events.HandedOverNode{{
		NodeID: "model.a", Reason: string(model.HandoverReasonTriesSpent),
		Message: "tried 3 times; the last try failed: model.a: still broken", Owner: "finance",
	}}, p.Nodes)
}
