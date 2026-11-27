package memory_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/memory"
	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/outbox"
)

var ctx = context.Background()

func TestMemoryUnitOfWork_CommitPublishesAndRollbackDiscards(t *testing.T) {
	store := memory.NewStore()
	p := fix.Fix{ReleaseID: "rel", Try: 1, NodeID: "n", Status: fix.StatusFailed}

	u := memory.NewUnitOfWork(store)
	require.NoError(t, u.Begin(ctx))
	require.NoError(t, u.FixRepo().Upsert(ctx, p))
	require.NoError(t, u.Rollback())
	require.Empty(t, store.Fixes(), "a rolled back write leaves no trace")

	u = memory.NewUnitOfWork(store)
	require.NoError(t, u.Begin(ctx))
	require.NoError(t, u.FixRepo().Upsert(ctx, p))
	require.NoError(t, u.Commit())
	require.Len(t, store.Fixes(), 1)
	require.Equal(t, 1, store.Fixes()[0].Fix.Round, "an unset round defaults to 1 as in Postgres")
}

func TestMemoryUnitOfWork_FailNextCommitFailsOnceAndDiscardsTheWrites(t *testing.T) {
	store := memory.NewStore()
	write := func() error {
		u := memory.NewUnitOfWork(store)
		if err := u.Begin(ctx); err != nil {
			return err
		}
		defer func() { _ = u.Rollback() }()
		if err := u.FixRepo().Upsert(ctx, fix.Fix{ReleaseID: "rel", Try: 1, Status: fix.StatusFailed}); err != nil {
			return err
		}
		return u.Commit()
	}
	store.FailNextCommit()
	require.Error(t, write())
	require.Empty(t, store.Fixes())
	require.NoError(t, write(), "only the next commit fails")
	require.Len(t, store.Fixes(), 1)
}

func TestMemoryFixRepository_UniqueOnReleaseAndTryAndCompareAndSet(t *testing.T) {
	store := memory.NewStore()
	repo := store.FixRepository()
	gen := fix.Fix{
		ReleaseID: "rel", Try: 1, NodeID: "n", ResolvedNodeIDs: []string{"n"}, Source: "validation",
		NodeOutcomes: map[string]fix.NodeOutcome{"n": {Status: fix.StatusGenerating}},
	}
	require.NoError(t, repo.InsertGenerating(ctx, gen))
	require.NoError(t, repo.InsertGenerating(ctx, gen))
	require.Len(t, store.Fixes(), 1, "a redelivered try collides on (release, try)")

	n, err := repo.CountTries(ctx, "rel", 1)
	require.NoError(t, err)
	require.Zero(t, n, "an in-flight try is not counted")

	verifying := gen
	verifying.Status = fix.StatusVerifying
	verifying.Source = "changed"
	verifying.NodeOutcomes = map[string]fix.NodeOutcome{"n": {Status: fix.StatusVerifying}}
	require.NoError(t, repo.Upsert(ctx, verifying))
	stored := store.Fixes()
	require.Len(t, stored, 1)
	require.Equal(t, "validation", stored[0].Fix.Source, "the conflict update keeps the identity columns")
	id := stored[0].ID

	hit, err := repo.MarkVerifyFailed(ctx, id, "rejected", nil)
	require.NoError(t, err)
	require.True(t, hit)
	checked, err := remediation.NewChecked(remediation.Draft{}, nil)
	require.NoError(t, err)
	verified, err := remediation.NewVerified(id, []remediation.Checked{checked}, nil, nil)
	require.NoError(t, err)
	hit, err = repo.MarkFixVerified(ctx, verified)
	require.NoError(t, err)
	require.False(t, hit, "the row is no longer verifying")
	view, err := repo.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, fix.StatusFailed, view.Status)
	require.Equal(t, "rejected", view.VerifyError)
	require.Equal(t, fix.StatusFailed, view.NodeOutcomes["n"].Status)

	n, err = repo.CountTries(ctx, "rel", 1)
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestMemoryFixRepository_FailGeneratingFailsOnlyInFlightRows(t *testing.T) {
	store := memory.NewStore()
	repo := store.FixRepository()
	require.NoError(t, repo.InsertGenerating(ctx, fix.Fix{
		ReleaseID: "rel", Try: 1, NodeOutcomes: map[string]fix.NodeOutcome{"a": {Status: fix.StatusGenerating}},
	}))
	require.NoError(t, repo.Upsert(ctx, fix.Fix{ReleaseID: "rel", Try: 2, Status: fix.StatusProposed}))
	n, err := repo.FailGenerating(ctx, "rel", "the worker crashed")
	require.NoError(t, err)
	require.Equal(t, 1, n)
	got := store.Fixes()
	require.Equal(t, fix.StatusFailed, got[0].Fix.Status)
	require.Equal(t, fix.NodeOutcome{Status: fix.StatusFailed, Reason: "the worker crashed"}, got[0].Fix.NodeOutcomes["a"])
	require.Equal(t, fix.StatusProposed, got[1].Fix.Status)
}

func TestMemoryMessageProcessing_DedupsOnBothAxes(t *testing.T) {
	store := memory.NewStore()
	u := memory.NewUnitOfWork(store)
	require.NoError(t, u.Begin(ctx))
	repo := u.MessageProcessingRepo()
	log := slog.New(slog.DiscardHandler)
	entry := uuid.New()

	id, dup, err := messageprocessing.DedupWithOutboxEntryID(ctx, repo, log, "1-0", "stream-a", []byte(`{}`), &entry)
	require.NoError(t, err)
	require.False(t, dup)

	same, dup, err := messageprocessing.DedupWithOutboxEntryID(ctx, repo, log, "1-0", "stream-a", []byte(`{}`), nil)
	require.NoError(t, err)
	require.True(t, dup, "same message id and stream")
	require.Equal(t, id, same)

	same, dup, err = messageprocessing.DedupWithOutboxEntryID(ctx, repo, log, "2-0", "stream-a", []byte(`{}`), &entry)
	require.NoError(t, err)
	require.True(t, dup, "same outbox entry id, new stream message id")
	require.Equal(t, id, same)

	_, dup, err = messageprocessing.DedupWithOutboxEntryID(ctx, repo, log, "3-0", "stream-b", []byte(`{}`), &entry)
	require.NoError(t, err)
	require.False(t, dup, "another consumer group's stream is its own dedup scope")

	done, err := repo.AlreadyProcessed(ctx, "9-9", "stream-a", &entry)
	require.NoError(t, err)
	require.True(t, done)
	done, err = repo.AlreadyProcessed(ctx, "9-9", "stream-c", &entry)
	require.NoError(t, err)
	require.False(t, done)

	require.NoError(t, repo.UpdateState(ctx, id, messageprocessing.StateCompleted))
	require.Error(t, repo.UpdateState(ctx, uuid.New(), messageprocessing.StateCompleted))
	require.NoError(t, u.Commit())
	require.Len(t, store.ProcessedMessages(), 2)
}

func TestMemoryOutbox_CreateStampsTheEntryAndRequiresItsMessage(t *testing.T) {
	store := memory.NewStore()
	u := memory.NewUnitOfWork(store)
	require.NoError(t, u.Begin(ctx))
	defer func() { _ = u.Rollback() }()

	entry := &outbox.Entry{AggregateType: "remediation", AggregateID: uuid.New(), EventType: "e", StreamName: "s", Payload: []byte(`{}`)}
	require.NoError(t, u.OutboxRepo().Create(ctx, entry))
	require.NotEqual(t, uuid.Nil, entry.ID)
	require.Equal(t, "pending", entry.Status)
	require.False(t, entry.CreatedAt.IsZero())

	missing := uuid.New()
	err := u.OutboxRepo().Create(ctx, &outbox.Entry{MessageProcessingID: &missing})
	require.Error(t, err, "the foreign key to message_processing holds")

	require.ErrorIs(t, u.OutboxRepo().MarkProcessed(ctx, entry.ID), errors.ErrUnsupported)
}

func TestMemoryStore_ARepositoryUsedBeforeBeginIsAnError(t *testing.T) {
	u := memory.NewUnitOfWork(memory.NewStore())
	_, err := u.FixRepo().CountTries(ctx, "rel", 1)
	require.Error(t, err)
}

func TestMemoryRemediationRepository_AFixNeedsItsRemediationAndMarkFixVerifiedRefusesAZero(t *testing.T) {
	store := memory.NewStore()
	repo := store.FixRepository()
	err := repo.InsertGenerating(ctx, fix.Fix{ReleaseID: "rel", Try: 1, RemediationID: uuid.NewString()})
	require.Error(t, err)

	hit, err := repo.MarkFixVerified(ctx, remediation.Verified{})
	require.Error(t, err)
	require.False(t, hit)
}

func TestMemoryRemediationRepository_TriesNeedTheirRemediation(t *testing.T) {
	store := memory.NewStore()
	repo := store.RemediationRepository()
	err := repo.SaveTry(ctx, remediation.Try{RemediationID: uuid.New(), Try: 1, Status: remediation.TryDrafting, DeadlineAt: time.Now()})
	require.Error(t, err)
}

func TestMemoryFixRepository_RecordApproversAddsToTheRowsApprovers(t *testing.T) {
	store := memory.NewStore()
	repo := store.FixRepository()
	require.NoError(t, repo.Upsert(ctx, fix.Fix{
		Source: "validation", ReleaseID: "rel", NodeID: "model.a", Try: 1, Status: fix.StatusVerifying,
		Approvers: []fix.Approver{{NodeID: "model.b", Owner: "finance", Source: model.ApproverSourceBundle}},
	}))

	require.NoError(t, repo.RecordApprovers(ctx, "rel", 1, 1, []fix.Approver{
		{NodeID: "model.c", Source: model.ApproverSourceNone},
		{NodeID: "model.a", Owner: "sales", Source: model.ApproverSourceBundle},
		{NodeID: "model.b", Owner: "ops", Source: model.ApproverSourceContractFile},
	}))

	require.Equal(t, []fix.Approver{
		{NodeID: "model.a", Owner: "sales", Source: model.ApproverSourceBundle},
		{NodeID: "model.b", Owner: "ops", Source: model.ApproverSourceContractFile},
		{NodeID: "model.c", Source: model.ApproverSourceNone},
	}, store.Fixes()[0].Fix.Approvers)

	view, err := repo.Get(ctx, store.Fixes()[0].ID)
	require.NoError(t, err)
	require.Equal(t, store.Fixes()[0].Fix.Approvers, view.Approvers)

	require.ErrorIs(t, repo.RecordApprovers(ctx, "rel", 1, 2, []fix.Approver{{NodeID: "model.a"}}), repository.ErrNotFound)
}
