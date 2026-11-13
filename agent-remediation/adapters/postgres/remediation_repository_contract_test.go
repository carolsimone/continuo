//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/memory"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

// contractEnv is one implementation of the repositories under test: the
// Postgres adapters on the integration database, or the in-memory store. Every
// contract case runs against both.
type contractEnv struct {
	// remediations is the remediation repository outside a unit of work.
	remediations repository.RemediationRepository
	// proposals is the proposal repository outside a unit of work.
	proposals repository.ProposalRepository
	// newUoW opens a fresh, not yet begun unit of work.
	newUoW func() uow.UnitOfWork
	// childRows counts the try, cluster and step rows of a remediation.
	childRows func(t *testing.T, id uuid.UUID) int
	// proposalRemediation returns the remediation id of a proposal ("" when none).
	proposalRemediation func(t *testing.T, proposalID string) string
	// proposalIDs lists the ids of every stored proposal.
	proposalIDs func(t *testing.T) []string
}

type contractImpl struct {
	name string
	env  func(t *testing.T) contractEnv
}

func contractImpls() []contractImpl {
	return []contractImpl{
		{name: "postgres", env: postgresEnv},
		{name: "memory", env: memoryEnv},
	}
}

func postgresEnv(t *testing.T) contractEnv {
	db := newTestDB(t)
	ctx := context.Background()
	var existing int
	require.NoError(t, db.GetContext(ctx, &existing, `SELECT count(*) FROM remediation`),
		"run the agent_remediation Flyway migrations before integration tests")
	require.Zero(t, existing, "refusing non-empty test database: remediation contains pre-existing rows")
	// The empty-table guard makes this serial test the owner of every remediation
	// row. Cleanup runs before newTestDB's, so the proposals' foreign key clears
	// first; the try, cluster and step rows go with their remediation.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var ids []string
		if !assert.NoError(t, db.SelectContext(cleanupCtx, &ids, `SELECT id FROM remediation`)) {
			return
		}
		if len(ids) > 0 {
			_, err := db.ExecContext(cleanupCtx, `DELETE FROM remediation WHERE id = ANY($1::uuid[])`, pq.Array(ids))
			assert.NoError(t, err, "delete test-owned remediations")
		}
	})
	return contractEnv{
		remediations: NewRemediationRepository(db),
		proposals:    NewProposalRepository(db, nil),
		newUoW: func() uow.UnitOfWork {
			return NewUnitOfWork(db, slog.New(slog.DiscardHandler), nil)
		},
		childRows: func(t *testing.T, id uuid.UUID) int {
			var n int
			require.NoError(t, db.GetContext(ctx, &n, `SELECT
				(SELECT count(*) FROM remediation_try WHERE remediation_id = $1) +
				(SELECT count(*) FROM remediation_cluster WHERE remediation_id = $1) +
				(SELECT count(*) FROM remediation_step WHERE remediation_id = $1)`, id))
			return n
		},
		proposalRemediation: func(t *testing.T, proposalID string) string {
			return proposalRemediationOf(t, db, proposalID)
		},
		proposalIDs: func(t *testing.T) []string {
			var ids []string
			require.NoError(t, db.SelectContext(ctx, &ids, `SELECT id FROM proposal`))
			return ids
		},
	}
}

func proposalRemediationOf(t *testing.T, db *sqlx.DB, proposalID string) string {
	var id *string
	require.NoError(t, db.GetContext(context.Background(), &id, `SELECT remediation_id FROM proposal WHERE id = $1`, proposalID))
	if id == nil {
		return ""
	}
	return *id
}

func memoryEnv(t *testing.T) contractEnv {
	store := memory.NewStore()
	return contractEnv{
		remediations: store.RemediationRepository(),
		proposals:    store.ProposalRepository(),
		newUoW:       func() uow.UnitOfWork { return memory.NewUnitOfWork(store) },
		childRows: func(t *testing.T, id uuid.UUID) int {
			return len(store.Tries(id)) + len(store.Clusters(id)) + len(store.Steps(id))
		},
		proposalRemediation: func(t *testing.T, proposalID string) string {
			for _, p := range store.Proposals() {
				if p.ID == proposalID {
					return p.Proposal.RemediationID
				}
			}
			require.Failf(t, "proposal not found", "id %s", proposalID)
			return ""
		},
		proposalIDs: func(t *testing.T) []string {
			var ids []string
			for _, p := range store.Proposals() {
				ids = append(ids, p.ID)
			}
			return ids
		},
	}
}

// forEachImpl runs a contract case against every implementation.
func forEachImpl(t *testing.T, run func(t *testing.T, env contractEnv)) {
	for _, impl := range contractImpls() {
		t.Run(impl.name, func(t *testing.T) { run(t, impl.env(t)) })
	}
}

// contractBase is the clock of the cases: a whole microsecond, which a
// timestamptz column keeps exactly.
var contractBase = time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)

func newRemediation(release string, round int, createdAt time.Time) remediation.Remediation {
	return remediation.Remediation{
		ID:               uuid.New(),
		TenantID:         "default",
		ReleaseID:        release,
		Round:            round,
		Flow:             "standard",
		Status:           remediation.StatusRunning,
		TriggerPayload:   []byte(`{"release_id":"` + release + `"}`),
		TriggerMessageID: "1700000000000-0",
		TriggerFields:    map[string]string{"outbox_entry_id": "e1"},
		CreatedAt:        createdAt,
		UpdatedAt:        createdAt,
	}
}

func mustCreate(t *testing.T, env contractEnv, r remediation.Remediation) remediation.Remediation {
	t.Helper()
	created, err := env.remediations.Create(context.Background(), r)
	require.NoError(t, err)
	require.True(t, created)
	return r
}

func newTry(id uuid.UUID, n int, status remediation.TryStatus, deadline time.Time) remediation.Try {
	return remediation.Try{
		RemediationID: id,
		Try:           n,
		Attempt:       n,
		Status:        status,
		FileRevisions: map[string]string{"models/a.sql": "rev1"},
		DeadlineAt:    deadline,
		CreatedAt:     contractBase,
		UpdatedAt:     contractBase,
	}
}

// startTry saves a try and makes it the remediation's current one.
func startTry(t *testing.T, env contractEnv, r remediation.Remediation, status remediation.TryStatus, deadline time.Time) remediation.Try {
	t.Helper()
	ctx := context.Background()
	try := newTry(r.ID, 1, status, deadline)
	require.NoError(t, env.remediations.SaveTry(ctx, try))
	r.CurrentTry = 1
	require.NoError(t, env.remediations.SaveRemediation(ctx, r))
	return try
}

func sameInstant(t *testing.T, want, got time.Time, msg string) {
	t.Helper()
	assert.True(t, want.Equal(got), "%s: want %s, got %s", msg, want, got)
}

func TestRemediationRepository_CreateTwiceForOneKeyReportsNotCreated(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		first := newRemediation("rel-1", 1, contractBase)
		created, err := env.remediations.Create(ctx, first)
		require.NoError(t, err)
		require.True(t, created)

		second := newRemediation("rel-1", 1, contractBase.Add(time.Hour))
		second.Flow = "other"
		created, err = env.remediations.Create(ctx, second)
		require.NoError(t, err)
		require.False(t, created, "the same (tenant, release, round) is not created again")

		got, found, err := env.remediations.FindByRelease(ctx, "default", "rel-1", 1)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, first.ID, got.ID, "the first row is the one that stays")
		require.Equal(t, "standard", got.Flow)
		require.Equal(t, remediation.StatusRunning, got.Status)
		require.JSONEq(t, string(first.TriggerPayload), string(got.TriggerPayload))
		require.Equal(t, first.TriggerFields, got.TriggerFields)
		require.Equal(t, "1700000000000-0", got.TriggerMessageID)
		require.Empty(t, got.LeaseOwner)
		require.Nil(t, got.LeaseUntil)
		sameInstant(t, contractBase, got.CreatedAt, "created_at")

		created, err = env.remediations.Create(ctx, newRemediation("rel-1", 2, contractBase))
		require.NoError(t, err)
		require.True(t, created, "another round is another remediation")

		_, found, err = env.remediations.FindByRelease(ctx, "default", "rel-1", 3)
		require.NoError(t, err)
		require.False(t, found)
		_, found, err = env.remediations.FindByRelease(ctx, "other-tenant", "rel-1", 1)
		require.NoError(t, err)
		require.False(t, found)
	})
}

func TestRemediationRepository_ClaimNextFromTwoGoroutinesClaimsDifferentRows(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		a := mustCreate(t, env, newRemediation("rel-a", 1, contractBase))
		b := mustCreate(t, env, newRemediation("rel-b", 1, contractBase.Add(time.Second)))

		type claim struct {
			id    uuid.UUID
			found bool
			err   error
		}
		results := make([]claim, 2)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i, owner := range []string{"worker-1", "worker-2"} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				id, found, err := env.remediations.ClaimNext(ctx, owner, contractBase, time.Minute)
				results[i] = claim{id, found, err}
			}()
		}
		close(start)
		wg.Wait()

		for _, c := range results {
			require.NoError(t, c.err)
			require.True(t, c.found)
		}
		require.NotEqual(t, results[0].id, results[1].id, "two workers never hold one remediation")
		require.ElementsMatch(t, []uuid.UUID{a.ID, b.ID}, []uuid.UUID{results[0].id, results[1].id})
	})
}

func TestRemediationRepository_ClaimNextWithOneRowClaimsItOnce(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		only := mustCreate(t, env, newRemediation("rel-only", 1, contractBase))

		var found [2]bool
		var ids [2]uuid.UUID
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i, owner := range []string{"worker-1", "worker-2"} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				id, ok, err := env.remediations.ClaimNext(ctx, owner, contractBase, time.Minute)
				assert.NoError(t, err)
				ids[i], found[i] = id, ok
			}()
		}
		close(start)
		wg.Wait()

		require.NotEqual(t, found[0], found[1], "exactly one of two concurrent claims takes the only row")
		for i := range ids {
			if found[i] {
				require.Equal(t, only.ID, ids[i])
			}
		}
	})
}

func TestRemediationRepository_ClaimNextTakesTheOldestFirstAndSkipsOtherStatuses(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		newer := mustCreate(t, env, newRemediation("rel-new", 1, contractBase.Add(time.Minute)))
		older := mustCreate(t, env, newRemediation("rel-old", 1, contractBase))
		done := newRemediation("rel-done", 1, contractBase.Add(-time.Hour))
		done.Status = remediation.StatusDone
		mustCreate(t, env, done)

		id, found, err := env.remediations.ClaimNext(ctx, "w", contractBase, time.Minute)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, older.ID, id)

		id, found, err = env.remediations.ClaimNext(ctx, "w", contractBase, time.Minute)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, newer.ID, id)

		_, found, err = env.remediations.ClaimNext(ctx, "w", contractBase, time.Minute)
		require.NoError(t, err)
		require.False(t, found, "a done remediation is never claimed, and the others are leased")
	})
}

func TestRemediationRepository_LeaseRules(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		r := mustCreate(t, env, newRemediation("rel-lease", 1, contractBase))

		id, found, err := env.remediations.ClaimNext(ctx, "owner", contractBase, time.Minute)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, r.ID, id)

		snap, err := env.remediations.Load(ctx, r.ID)
		require.NoError(t, err)
		require.Equal(t, "owner", snap.Remediation.LeaseOwner)
		require.NotNil(t, snap.Remediation.LeaseUntil)
		sameInstant(t, contractBase.Add(time.Minute), *snap.Remediation.LeaseUntil, "lease_until")

		claimed, err := env.remediations.Claim(ctx, r.ID, "intruder", contractBase.Add(30*time.Second), time.Minute)
		require.NoError(t, err)
		require.False(t, claimed, "a remediation under a live lease cannot be claimed")

		err = env.remediations.Hold(ctx, r.ID, "intruder", contractBase.Add(30*time.Second), time.Minute)
		require.ErrorIs(t, err, repository.ErrLeaseLost, "a non-owner cannot hold the lease")

		require.NoError(t, env.remediations.Hold(ctx, r.ID, "owner", contractBase.Add(30*time.Second), time.Minute))
		snap, err = env.remediations.Load(ctx, r.ID)
		require.NoError(t, err)
		sameInstant(t, contractBase.Add(90*time.Second), *snap.Remediation.LeaseUntil, "extended lease_until")

		// Release by someone else changes nothing; release by the owner frees it.
		require.NoError(t, env.remediations.ReleaseLease(ctx, r.ID, "intruder"))
		snap, err = env.remediations.Load(ctx, r.ID)
		require.NoError(t, err)
		require.Equal(t, "owner", snap.Remediation.LeaseOwner)
		require.NoError(t, env.remediations.ReleaseLease(ctx, r.ID, "owner"))
		snap, err = env.remediations.Load(ctx, r.ID)
		require.NoError(t, err)
		require.Empty(t, snap.Remediation.LeaseOwner)
		require.Nil(t, snap.Remediation.LeaseUntil)

		claimed, err = env.remediations.Claim(ctx, r.ID, "second", contractBase.Add(time.Second), time.Minute)
		require.NoError(t, err)
		require.True(t, claimed, "a free lease can be claimed")
	})
}

func TestRemediationRepository_AnExpiredLeaseCanBeClaimedAgain(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		r := mustCreate(t, env, newRemediation("rel-expired", 1, contractBase))
		_, found, err := env.remediations.ClaimNext(ctx, "first", contractBase, time.Minute)
		require.NoError(t, err)
		require.True(t, found)

		_, found, err = env.remediations.ClaimNext(ctx, "second", contractBase.Add(time.Minute), time.Minute)
		require.NoError(t, err)
		require.False(t, found, "the lease is still live at the instant it ends")

		later := contractBase.Add(time.Minute + time.Second)
		err = env.remediations.Hold(ctx, r.ID, "first", later, time.Minute)
		require.ErrorIs(t, err, repository.ErrLeaseLost, "an owner whose lease ended cannot hold it")

		id, found, err := env.remediations.ClaimNext(ctx, "second", later, time.Minute)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, r.ID, id)

		snap, err := env.remediations.Load(ctx, r.ID)
		require.NoError(t, err)
		require.Equal(t, "second", snap.Remediation.LeaseOwner)

		claimed, err := env.remediations.Claim(ctx, r.ID, "third", later.Add(2*time.Minute), time.Minute)
		require.NoError(t, err)
		require.True(t, claimed, "Claim also takes an expired lease")
	})
}

func TestRemediationRepository_ClaimNextSkipsAVerifyingTryAndListVerifyingReturnsIt(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		verifying := mustCreate(t, env, newRemediation("rel-verifying", 1, contractBase))
		startTry(t, env, verifying, remediation.TryVerifying, contractBase.Add(time.Hour))
		cluster := remediation.Cluster{
			RemediationID: verifying.ID, Try: 1, Cluster: 1, Kind: workflow.ClusterSingle,
			TargetNodeID: "model.a", Members: []string{"model.a"}, Status: remediation.ClusterChecked, Round: 1,
			UpdatedAt: contractBase,
		}
		require.NoError(t, env.remediations.SaveCluster(ctx, cluster))
		require.NoError(t, env.remediations.SaveStep(ctx, remediation.StepRun{
			RemediationID: verifying.ID, Try: 1, Cluster: 0, Step: "verification_run", Kind: "check", Round: 1,
			Status: remediation.StepWaiting, WaitingOn: []string{"run-1"}, StartedAt: contractBase,
		}))
		drafting := mustCreate(t, env, newRemediation("rel-drafting", 1, contractBase.Add(time.Second)))
		startTry(t, env, drafting, remediation.TryDrafting, contractBase.Add(time.Hour))

		id, found, err := env.remediations.ClaimNext(ctx, "w", contractBase, time.Minute)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, drafting.ID, id, "the older remediation is verifying, so the next one is taken")

		_, found, err = env.remediations.ClaimNext(ctx, "w", contractBase, time.Minute)
		require.NoError(t, err)
		require.False(t, found)

		snaps, err := env.remediations.ListVerifying(ctx, 10)
		require.NoError(t, err)
		require.Len(t, snaps, 1)
		require.Equal(t, verifying.ID, snaps[0].Remediation.ID)
		require.NotNil(t, snaps[0].Try)
		require.Equal(t, remediation.TryVerifying, snaps[0].Try.Status)
		require.Len(t, snaps[0].Clusters, 1)
		require.Len(t, snaps[0].Steps, 1)
		require.Equal(t, []string{"run-1"}, snaps[0].Steps[0].WaitingOn)

		claimed, err := env.remediations.Claim(ctx, verifying.ID, "poller", contractBase, time.Minute)
		require.NoError(t, err)
		require.True(t, claimed, "the poller claims a verifying remediation by id")

		none, err := env.remediations.ListVerifying(ctx, 0)
		require.NoError(t, err)
		require.Len(t, none, 1, "a leased remediation is still listed; a non-positive limit means no limit")
	})
}

func TestRemediationRepository_ListVerifyingIsOldestFirstAndLimited(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		var ids []uuid.UUID
		for i, release := range []string{"rel-1", "rel-2", "rel-3"} {
			r := mustCreate(t, env, newRemediation(release, 1, contractBase.Add(time.Duration(i)*time.Second)))
			startTry(t, env, r, remediation.TryVerifying, contractBase.Add(time.Hour))
			ids = append(ids, r.ID)
		}
		snaps, err := env.remediations.ListVerifying(ctx, 2)
		require.NoError(t, err)
		require.Len(t, snaps, 2)
		require.Equal(t, ids[0], snaps[0].Remediation.ID)
		require.Equal(t, ids[1], snaps[1].Remediation.ID)
	})
}

func TestRemediationRepository_SaveStepTwiceOnOneKeyUpdatesTheRow(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		r := mustCreate(t, env, newRemediation("rel-step", 1, contractBase))
		startTry(t, env, r, remediation.TryDrafting, contractBase.Add(time.Hour))

		step := remediation.StepRun{
			RemediationID: r.ID, Try: 1, Cluster: 1, Step: "draft", Kind: "prompt", Round: 1,
			Status: remediation.StepRunning, InputDigest: "d1", StartedAt: contractBase,
		}
		require.NoError(t, env.remediations.SaveStep(ctx, step))
		other := step
		other.Step, other.Kind = "gather", "context"
		require.NoError(t, env.remediations.SaveStep(ctx, other))

		finished := contractBase.Add(time.Minute)
		step.Status = remediation.StepDone
		step.Output = []byte(`{"edits":1}`)
		step.Findings = []remediation.Finding{{Code: "c1", Message: "m1", File: "a.sql", Line: 3}}
		step.Model, step.PromptVersion, step.TokensIn, step.TokensOut, step.CacheHit = "model-x", "v2", 10, 20, true
		step.FileRevision, step.Failures, step.LastError = "rev9", 2, "boom"
		step.FinishedAt = &finished
		require.NoError(t, env.remediations.SaveStep(ctx, step))

		snap, err := env.remediations.Load(ctx, r.ID)
		require.NoError(t, err)
		require.Len(t, snap.Steps, 2, "the second save of one key is an update, not a new row")
		require.Equal(t, "draft", snap.Steps[0].Step, "steps are ordered by id, and an update keeps the id")
		require.Equal(t, "gather", snap.Steps[1].Step)
		require.Less(t, snap.Steps[0].ID, snap.Steps[1].ID)

		got := snap.Steps[0]
		require.Equal(t, remediation.StepDone, got.Status)
		require.JSONEq(t, `{"edits":1}`, string(got.Output))
		require.Equal(t, step.Findings, got.Findings)
		require.Equal(t, "model-x", got.Model)
		require.Equal(t, "v2", got.PromptVersion)
		require.Equal(t, 10, got.TokensIn)
		require.Equal(t, 20, got.TokensOut)
		require.True(t, got.CacheHit)
		require.Equal(t, "rev9", got.FileRevision)
		require.Equal(t, 2, got.Failures)
		require.Equal(t, "boom", got.LastError)
		require.NotNil(t, got.FinishedAt)
		sameInstant(t, finished, *got.FinishedAt, "finished_at")
		require.Nil(t, snap.Steps[1].FinishedAt)

		step.Round = 2
		require.NoError(t, env.remediations.SaveStep(ctx, step))
		snap, err = env.remediations.Load(ctx, r.ID)
		require.NoError(t, err)
		require.Len(t, snap.Steps, 3, "another round is another row")
	})
}

func TestRemediationRepository_LoadReturnsTheCurrentTryWithOrderedChildren(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		r := mustCreate(t, env, newRemediation("rel-load", 1, contractBase))

		snap, err := env.remediations.Load(ctx, r.ID)
		require.NoError(t, err)
		require.Nil(t, snap.Try, "a remediation with no try has no current try")
		require.Empty(t, snap.Clusters)

		require.NoError(t, env.remediations.SaveTry(ctx, newTry(r.ID, 1, remediation.TryFailed, contractBase.Add(time.Hour))))
		require.NoError(t, env.remediations.SaveCluster(ctx, remediation.Cluster{
			RemediationID: r.ID, Try: 1, Cluster: 1, Kind: workflow.ClusterSingle, TargetNodeID: "old",
			Members: []string{"old"}, Status: remediation.ClusterFellBack, Round: 1,
		}))
		second := newTry(r.ID, 2, remediation.TryDrafting, contractBase.Add(2*time.Hour))
		second.FileRevisions = map[string]string{"a.sql": "r1", "b.sql": "r2"}
		require.NoError(t, env.remediations.SaveTry(ctx, second))
		outcome := &proposal.NodeOutcome{Status: proposal.StatusSkipped, Reason: "no fix"}
		require.NoError(t, env.remediations.SaveCluster(ctx, remediation.Cluster{
			RemediationID: r.ID, Try: 2, Cluster: 2, Kind: workflow.ClusterSharedUpstream, TargetNodeID: "up",
			Members: []string{"a", "b"}, TypeName: "dbt", Status: remediation.ClusterHandedOver, Round: 3, Outcome: outcome,
			UpdatedAt: contractBase,
		}))
		require.NoError(t, env.remediations.SaveCluster(ctx, remediation.Cluster{
			RemediationID: r.ID, Try: 2, Cluster: 1, Kind: workflow.ClusterSingle, TargetNodeID: "x",
			Members: []string{"x"}, Status: remediation.ClusterGathering, Round: 1,
		}))
		r.CurrentTry = 2
		require.NoError(t, env.remediations.SaveRemediation(ctx, r))

		snap, err = env.remediations.Load(ctx, r.ID)
		require.NoError(t, err)
		require.Equal(t, 2, snap.Remediation.CurrentTry)
		require.NotNil(t, snap.Try)
		require.Equal(t, 2, snap.Try.Try)
		require.Equal(t, second.FileRevisions, snap.Try.FileRevisions)
		sameInstant(t, second.DeadlineAt, snap.Try.DeadlineAt, "deadline_at")
		require.Len(t, snap.Clusters, 2, "only the current try's clusters")
		require.Equal(t, 1, snap.Clusters[0].Cluster, "clusters are ordered by cluster")
		require.Equal(t, 2, snap.Clusters[1].Cluster)
		require.Equal(t, []string{"a", "b"}, snap.Clusters[1].Members)
		require.Equal(t, "dbt", snap.Clusters[1].TypeName)
		require.Equal(t, 3, snap.Clusters[1].Round)
		require.Equal(t, workflow.ClusterSharedUpstream, snap.Clusters[1].Kind)
		require.Equal(t, outcome, snap.Clusters[1].Outcome)
		require.Nil(t, snap.Clusters[0].Outcome)

		// Saving a try and a cluster again updates them.
		second.Status = remediation.TryChecking
		require.NoError(t, env.remediations.SaveTry(ctx, second))
		snap, err = env.remediations.Load(ctx, r.ID)
		require.NoError(t, err)
		require.Equal(t, remediation.TryChecking, snap.Try.Status)

		_, err = env.remediations.Load(ctx, uuid.New())
		require.ErrorIs(t, err, repository.ErrRemediationNotFound)
	})
}

func TestRemediationRepository_CreateRefusesAnUnsetID(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		r := newRemediation("rel-noid", 1, contractBase)
		r.ID = uuid.Nil
		created, err := env.remediations.Create(context.Background(), r)
		require.Error(t, err)
		require.False(t, created)
		_, found, err := env.remediations.FindByRelease(context.Background(), "default", "rel-noid", 1)
		require.NoError(t, err)
		require.False(t, found, "nothing is written")
	})
}

func TestRemediationRepository_LoadErrorsWhenTheCurrentTryHasNoRow(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		r := mustCreate(t, env, newRemediation("rel-notry-row", 1, contractBase))
		r.CurrentTry = 2
		require.NoError(t, env.remediations.SaveRemediation(ctx, r))
		_, err := env.remediations.Load(ctx, r.ID)
		require.Error(t, err)
		require.NotErrorIs(t, err, repository.ErrRemediationNotFound)
	})
}

func TestRemediationRepository_ClusterAndStepKindsAreChecked(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		r := mustCreate(t, env, newRemediation("rel-kinds", 1, contractBase))
		startTry(t, env, r, remediation.TryDrafting, contractBase.Add(time.Hour))
		require.Error(t, env.remediations.SaveCluster(ctx, remediation.Cluster{
			RemediationID: r.ID, Try: 1, Cluster: 1, Kind: "bogus", TargetNodeID: "n", Members: []string{"n"},
			Status: remediation.ClusterGathering, Round: 1,
		}))
		require.Error(t, env.remediations.SaveStep(ctx, remediation.StepRun{
			RemediationID: r.ID, Try: 1, Step: "s", Kind: "bogus", Round: 1, Status: remediation.StepRunning, StartedAt: contractBase,
		}))
	})
}

func TestRemediationRepository_SaveRemediationWritesTheRunAndLeavesTheLeaseAlone(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		r := mustCreate(t, env, newRemediation("rel-save", 1, contractBase))
		_, found, err := env.remediations.ClaimNext(ctx, "owner", contractBase, time.Minute)
		require.NoError(t, err)
		require.True(t, found)

		r.Status = remediation.StatusSystemFailure
		r.Failure = "step failed 5 times"
		r.UpdatedAt = contractBase.Add(time.Hour)
		require.NoError(t, env.remediations.SaveRemediation(ctx, r))

		snap, err := env.remediations.Load(ctx, r.ID)
		require.NoError(t, err)
		require.Equal(t, remediation.StatusSystemFailure, snap.Remediation.Status)
		require.Equal(t, "step failed 5 times", snap.Remediation.Failure)
		sameInstant(t, contractBase.Add(time.Hour), snap.Remediation.UpdatedAt, "updated_at")
		require.Equal(t, "owner", snap.Remediation.LeaseOwner, "SaveRemediation never touches the lease")

		missing := newRemediation("rel-missing", 1, contractBase)
		require.ErrorIs(t, env.remediations.SaveRemediation(ctx, missing), repository.ErrRemediationNotFound)
	})
}

func TestRemediationRepository_ListPastDeadline(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		now := contractBase.Add(10 * time.Hour)
		mk := func(release string, status remediation.TryStatus, deadline time.Time) uuid.UUID {
			r := mustCreate(t, env, newRemediation(release, 1, contractBase))
			startTry(t, env, r, status, deadline)
			return r.ID
		}
		late := mk("rel-late", remediation.TryDrafting, now.Add(-2*time.Hour))
		later := mk("rel-later", remediation.TryVerifying, now.Add(-time.Hour))
		mk("rel-ontime", remediation.TryChecking, now.Add(time.Hour))
		mk("rel-passed", remediation.TryPassed, now.Add(-3*time.Hour))
		finished := newRemediation("rel-done", 1, contractBase)
		finished.Status = remediation.StatusDone
		mustCreate(t, env, finished)
		startTry(t, env, finished, remediation.TryDrafting, now.Add(-4*time.Hour))
		mustCreate(t, env, newRemediation("rel-notry", 1, contractBase))

		ids, err := env.remediations.ListPastDeadline(ctx, now, 10)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{late, later}, ids, "open tries of running remediations past their deadline, oldest first")

		ids, err = env.remediations.ListPastDeadline(ctx, now, 1)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{late}, ids)
	})
}

func TestRemediationRepository_DeleteFinishedBeforeCascadesAndClearsProposals(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		cutoff := contractBase.Add(24 * time.Hour)

		finish := func(release string, status remediation.RemediationStatus, updatedAt time.Time) remediation.Remediation {
			r := mustCreate(t, env, newRemediation(release, 1, contractBase))
			startTry(t, env, r, remediation.TryPassed, contractBase.Add(time.Hour))
			require.NoError(t, env.remediations.SaveCluster(ctx, remediation.Cluster{
				RemediationID: r.ID, Try: 1, Cluster: 1, Kind: workflow.ClusterSingle, TargetNodeID: "n",
				Members: []string{"n"}, Status: remediation.ClusterChecked, Round: 1,
			}))
			require.NoError(t, env.remediations.SaveStep(ctx, remediation.StepRun{
				RemediationID: r.ID, Try: 1, Cluster: 1, Step: "draft", Kind: "prompt", Round: 1,
				Status: remediation.StepDone, StartedAt: contractBase,
			}))
			r.CurrentTry = 1
			r.Status = status
			r.UpdatedAt = updatedAt
			require.NoError(t, env.remediations.SaveRemediation(ctx, r))
			return r
		}
		oldDone := finish("rel-old-done", remediation.StatusDone, cutoff.Add(-time.Hour))
		oldFailed := finish("rel-old-failed", remediation.StatusSystemFailure, cutoff.Add(-time.Hour))
		recentDone := finish("rel-recent-done", remediation.StatusDone, cutoff.Add(time.Hour))
		running := mustCreate(t, env, newRemediation("rel-running", 1, contractBase))
		running.UpdatedAt = cutoff.Add(-time.Hour)
		require.NoError(t, env.remediations.SaveRemediation(ctx, running))

		// A proposal the runner wrote for the deleted remediation survives it.
		insertRunnerProposal(t, env, "rel-old-done", 1, oldDone.ID)
		keptProposal := insertRunnerProposal(t, env, "rel-recent-done", 1, recentDone.ID)
		require.NotZero(t, env.childRows(t, oldDone.ID))

		n, err := env.remediations.DeleteFinishedBefore(ctx, cutoff)
		require.NoError(t, err)
		require.EqualValues(t, 2, n, "the done and the system_failure remediation older than the cutoff")

		for _, gone := range []uuid.UUID{oldDone.ID, oldFailed.ID} {
			_, err := env.remediations.Load(ctx, gone)
			require.ErrorIs(t, err, repository.ErrRemediationNotFound)
			require.Zero(t, env.childRows(t, gone), "tries, clusters and steps go with their remediation")
		}
		for _, kept := range []uuid.UUID{recentDone.ID, running.ID} {
			_, err := env.remediations.Load(ctx, kept)
			require.NoError(t, err)
		}
		require.NotZero(t, env.childRows(t, recentDone.ID))

		require.Len(t, env.proposalIDs(t), 2, "no proposal is deleted")
		for _, id := range env.proposalIDs(t) {
			if id == keptProposal {
				require.Equal(t, recentDone.ID.String(), env.proposalRemediation(t, id))
			} else {
				require.Empty(t, env.proposalRemediation(t, id), "the proposal of a deleted remediation has no remediation")
			}
		}
	})
}

func TestRemediationRepository_InsideAUnitOfWorkHoldAndSavesCommitTogether(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		r := mustCreate(t, env, newRemediation("rel-uow", 1, contractBase))
		_, found, err := env.remediations.ClaimNext(ctx, "owner", contractBase, time.Minute)
		require.NoError(t, err)
		require.True(t, found)

		// A commit by a worker that lost its lease is refused, and writes nothing.
		u := env.newUoW()
		require.NoError(t, u.Begin(ctx))
		err = u.RemediationRepo().Hold(ctx, r.ID, "intruder", contractBase, time.Minute)
		require.ErrorIs(t, err, repository.ErrLeaseLost)
		require.NoError(t, u.Rollback())

		// A rolled back unit of work leaves no trace.
		u = env.newUoW()
		require.NoError(t, u.Begin(ctx))
		repo := u.RemediationRepo()
		require.NoError(t, repo.Hold(ctx, r.ID, "owner", contractBase, time.Minute))
		require.NoError(t, repo.SaveTry(ctx, newTry(r.ID, 1, remediation.TryDrafting, contractBase.Add(time.Hour))))
		r.CurrentTry = 1
		require.NoError(t, repo.SaveRemediation(ctx, r))
		require.NoError(t, u.Rollback())
		snap, err := env.remediations.Load(ctx, r.ID)
		require.NoError(t, err)
		require.Nil(t, snap.Try)
		require.Zero(t, snap.Remediation.CurrentTry)

		// A committed one publishes the try and the new current try together.
		u = env.newUoW()
		require.NoError(t, u.Begin(ctx))
		repo = u.RemediationRepo()
		require.NoError(t, repo.Hold(ctx, r.ID, "owner", contractBase, time.Minute))
		require.NoError(t, repo.SaveTry(ctx, newTry(r.ID, 1, remediation.TryDrafting, contractBase.Add(time.Hour))))
		require.NoError(t, repo.SaveRemediation(ctx, r))
		require.NoError(t, u.Commit())
		snap, err = env.remediations.Load(ctx, r.ID)
		require.NoError(t, err)
		require.NotNil(t, snap.Try)
		require.Equal(t, 1, snap.Remediation.CurrentTry)
	})
}

// insertRunnerProposal stores an in-flight proposal that belongs to a
// remediation and returns its id.
func insertRunnerProposal(t *testing.T, env contractEnv, release string, attempt int, remediationID uuid.UUID) string {
	t.Helper()
	before := env.proposalIDs(t)
	require.NoError(t, env.proposals.InsertGenerating(context.Background(), proposal.Proposal{
		Source:        "validation",
		ReleaseID:     release,
		RemediationID: remediationID.String(),
		NodeID:        "model.a",
		Attempt:       attempt,
		Status:        proposal.StatusGenerating,
		CreatedAt:     contractBase,
	}))
	var added []string
	for _, id := range env.proposalIDs(t) {
		if !slices.Contains(before, id) {
			added = append(added, id)
		}
	}
	require.Len(t, added, 1, "the insert adds exactly one proposal")
	return added[0]
}

func TestProposalRepository_ListVerifyingSkipsRunnerRows(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		r := mustCreate(t, env, newRemediation("rel-runner", 1, contractBase))
		verifying := func(release string, remediationID string) proposal.Proposal {
			return proposal.Proposal{
				Source: "validation", ReleaseID: release, RemediationID: remediationID, NodeID: "model.a",
				Attempt: 1, Status: proposal.StatusVerifying, VerificationRunID: "run-" + release,
				CreatedAt: contractBase,
			}
		}
		require.NoError(t, env.proposals.Upsert(ctx, verifying("rel-driver", "")))
		require.NoError(t, env.proposals.Upsert(ctx, verifying("rel-runner", r.ID.String())))

		listed, err := env.proposals.ListVerifying(ctx)
		require.NoError(t, err)
		require.Len(t, listed, 1, "the runner finalizes its own proposals, so the driver's reconciler must not list them")
		require.Equal(t, "rel-driver", listed[0].ReleaseID)

		// An upsert that names no remediation leaves the stored one as it was.
		require.NoError(t, env.proposals.Upsert(ctx, verifying("rel-runner", "")))
		listed, err = env.proposals.ListVerifying(ctx)
		require.NoError(t, err)
		require.Len(t, listed, 1)
	})
}

func TestProposalRepository_UpsertAndInsertGeneratingRecordTheRemediation(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		r := mustCreate(t, env, newRemediation("rel-rid", 1, contractBase))
		id := insertRunnerProposal(t, env, "rel-rid", 1, r.ID)
		require.Equal(t, r.ID.String(), env.proposalRemediation(t, id))

		require.NoError(t, env.proposals.Upsert(ctx, proposal.Proposal{
			Source: "validation", ReleaseID: "rel-rid", NodeID: "model.a", Attempt: 1, Status: proposal.StatusProposed,
			CreatedAt: contractBase,
		}))
		require.Equal(t, r.ID.String(), env.proposalRemediation(t, id), "finalizing without a remediation keeps it")

		require.NoError(t, env.proposals.Upsert(ctx, proposal.Proposal{
			Source: "validation", ReleaseID: "rel-plain", NodeID: "model.b", Attempt: 1, Status: proposal.StatusFailed,
			CreatedAt: contractBase,
		}))
		for _, got := range env.proposalIDs(t) {
			if got != id {
				require.Empty(t, env.proposalRemediation(t, got), "a driver proposal belongs to no remediation")
			}
		}

		err := env.proposals.Upsert(ctx, proposal.Proposal{
			Source: "validation", ReleaseID: "rel-unknown", RemediationID: uuid.NewString(), NodeID: "model.c",
			Attempt: 1, Status: proposal.StatusFailed, CreatedAt: contractBase,
		})
		require.Error(t, err, "a proposal names a remediation that exists")
	})
}

func TestProposalRepository_MarkFixVerifiedRefusesAZeroVerified(t *testing.T) {
	forEachImpl(t, func(t *testing.T, env contractEnv) {
		ctx := context.Background()
		r := mustCreate(t, env, newRemediation("rel-fix", 1, contractBase))
		require.NoError(t, env.proposals.Upsert(ctx, proposal.Proposal{
			Source: "validation", ReleaseID: "rel-fix", RemediationID: r.ID.String(), NodeID: "model.a",
			ResolvedNodeIDs: []string{"model.a"}, Attempt: 1, Status: proposal.StatusVerifying,
			NodeOutcomes:  map[string]proposal.NodeOutcome{"model.a": {Status: proposal.StatusVerifying}},
			Verifications: []proposal.Verification{{Service: "svc", Kind: "dbt", RunID: "run-1"}},
			CreatedAt:     contractBase,
		}))
		ids := env.proposalIDs(t)
		require.Len(t, ids, 1)
		id := ids[0]

		hit, err := env.proposals.MarkFixVerified(ctx, remediation.Verified{})
		require.Error(t, err)
		require.False(t, hit)
		view, err := env.proposals.Get(ctx, id)
		require.NoError(t, err)
		require.Equal(t, proposal.StatusVerifying, view.Status, "a refused call writes nothing")

		checked, err := remediation.NewChecked(remediation.Draft{}, nil)
		require.NoError(t, err)
		final := []proposal.Verification{{Service: "svc", Kind: "dbt", RunID: "run-1", Phase: proposal.PhasePassed}}
		verified, err := remediation.NewVerified(id, []remediation.Checked{checked}, nil, final)
		require.NoError(t, err)

		hit, err = env.proposals.MarkFixVerified(ctx, verified)
		require.NoError(t, err)
		require.True(t, hit)
		view, err = env.proposals.Get(ctx, id)
		require.NoError(t, err)
		require.Equal(t, proposal.StatusProposed, view.Status)
		require.Equal(t, proposal.StatusProposed, view.NodeOutcomes["model.a"].Status)
		require.Equal(t, final, view.Verifications)

		hit, err = env.proposals.MarkFixVerified(ctx, verified)
		require.NoError(t, err)
		require.False(t, hit, "the compare-and-set fires once")
	})
}

func TestRemediationRepository_SaveStepStoresJSONHoldingANUL(t *testing.T) {
	env := postgresEnv(t)
	ctx := context.Background()
	r := mustCreate(t, env, newRemediation("rel-nul", 1, contractBase))
	startTry(t, env, r, remediation.TryDrafting, contractBase.Add(time.Hour))

	output, err := json.Marshal(map[string]string{"source": "select 1\x00", "path": `C:\u0000`})
	require.NoError(t, err)
	step := remediation.StepRun{
		RemediationID: r.ID, Try: 1, Cluster: 1, Step: "failing_node_source", Kind: "context", Round: 1,
		Status: remediation.StepDone, Output: output, StartedAt: contractBase,
		Findings: []remediation.Finding{{Code: "c1", Message: "bad\x00byte"}},
	}
	require.NoError(t, env.remediations.SaveStep(ctx, step), "JSONB refuses \\u0000, so the adapter replaces it")

	snap, err := env.remediations.Load(ctx, r.ID)
	require.NoError(t, err)
	require.Len(t, snap.Steps, 1)
	var got map[string]string
	require.NoError(t, json.Unmarshal(snap.Steps[0].Output, &got))
	assert.Equal(t, "select 1\uFFFD", got["source"])
	assert.Equal(t, `C:\u0000`, got["path"], "a backslash the text holds is kept")
	require.Len(t, snap.Steps[0].Findings, 1)
	assert.Equal(t, "bad\uFFFDbyte", snap.Steps[0].Findings[0].Message)
}
