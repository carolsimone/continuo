//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/commandrequest"
	"github.com/carolsimone/continuo/state/domain/repository"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ repository.CommandRequestRepository = (*postgres.CommandRequestRepository)(nil)

// commandTenant returns a tenant no other test uses, whose runs (and with them
// its command requests) are deleted when the test ends.
func commandTenant(t *testing.T, db *sqlx.DB) string {
	t.Helper()
	tenant := "t-" + uuid.NewString()
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM runs WHERE tenant_id = $1`, tenant) })
	return tenant
}

// insertCommandRun inserts a running run of tenant and returns its id.
func insertCommandRun(t *testing.T, db *sqlx.DB, tenant string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(`
		INSERT INTO runs (tenant_id, run_id, schedule_name, status, created_at, kind)
		VALUES ($1, $2, $3, 'running', now(), 'trigger')`, tenant, id, "cmd-"+id.String()[:8])
	require.NoError(t, err)
	return id
}

func commandRecord(tenant string, runID uuid.UUID, createdAt time.Time) commandrequest.Record {
	return commandrequest.Record{
		TenantID:    tenant,
		RequestID:   uuid.New(),
		Command:     "start_run",
		Fingerprint: commandrequest.Fingerprint("start_run", "e2e-schedule", "run"),
		RunID:       runID,
		CreatedAt:   createdAt,
	}
}

func TestCommandRequests_ClaimStoresTheRecordAndFindReadsItBack(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	repo := postgres.NewCommandRequestRepository(db, nil)
	tenant := commandTenant(t, db)
	runID := insertCommandRun(t, db, tenant)
	rec := commandRecord(tenant, runID, time.Now().UTC().Truncate(time.Microsecond))

	_, found, err := repo.Find(ctx, tenant, rec.RequestID)
	require.NoError(t, err)
	assert.False(t, found, "no record before the claim")

	claimed, err := repo.Claim(ctx, rec)
	require.NoError(t, err)
	assert.True(t, claimed)

	got, found, err := repo.Find(ctx, tenant, rec.RequestID)
	require.NoError(t, err)
	require.True(t, found)
	got.CreatedAt = got.CreatedAt.UTC()
	assert.Equal(t, rec, got)
}

// A second claim of the same request id inserts nothing, whatever it carries,
// and leaves the first record as it was.
func TestCommandRequests_ASecondClaimOfTheSameIDInsertsNothing(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	repo := postgres.NewCommandRequestRepository(db, nil)
	tenant := commandTenant(t, db)
	first := commandRecord(tenant, insertCommandRun(t, db, tenant), time.Now())
	require.True(t, mustClaim(t, repo, first))

	second := first
	second.Fingerprint = commandrequest.Fingerprint("start_run", "another-schedule", "run")
	second.RunID = insertCommandRun(t, db, tenant)
	claimed, err := repo.Claim(ctx, second)
	require.NoError(t, err)
	assert.False(t, claimed)

	got, found, err := repo.Find(ctx, tenant, first.RequestID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, first.Fingerprint, got.Fingerprint)
	assert.Equal(t, first.RunID, got.RunID)
}

func mustClaim(t *testing.T, repo repository.CommandRequestRepository, rec commandrequest.Record) bool {
	t.Helper()
	claimed, err := repo.Claim(context.Background(), rec)
	require.NoError(t, err)
	return claimed
}

// A request id is scoped to its tenant: the same id in two tenants is two
// records, and each tenant finds only its own.
func TestCommandRequests_TheSameIDInTwoTenantsIsTwoRecords(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	repo := postgres.NewCommandRequestRepository(db, nil)
	tenantA, tenantB := commandTenant(t, db), commandTenant(t, db)
	runA, runB := insertCommandRun(t, db, tenantA), insertCommandRun(t, db, tenantB)

	a := commandRecord(tenantA, runA, time.Now())
	b := a
	b.TenantID, b.RunID = tenantB, runB
	b.Fingerprint = commandrequest.Fingerprint("start_run", "other", "run")

	assert.True(t, mustClaim(t, repo, a))
	assert.True(t, mustClaim(t, repo, b), "the other tenant's claim of the same id is not a conflict")

	gotA, foundA, err := repo.Find(ctx, tenantA, a.RequestID)
	require.NoError(t, err)
	require.True(t, foundA)
	assert.Equal(t, runA, gotA.RunID)
	gotB, foundB, err := repo.Find(ctx, tenantB, a.RequestID)
	require.NoError(t, err)
	require.True(t, foundB)
	assert.Equal(t, runB, gotB.RunID)
	assert.NotEqual(t, gotA.Fingerprint, gotB.Fingerprint)

	_, foundC, err := repo.Find(ctx, commandTenant(t, db), a.RequestID)
	require.NoError(t, err)
	assert.False(t, foundC, "a third tenant has no record of the id")
}

// A record older than the age goes; a younger one stays. Only the rows older
// than the replay horizon are deleted by the pruner's cutoff.
func TestCommandRequests_PrunedAfterTheReplayHorizon(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	repo := postgres.NewCommandRequestRepository(db, nil)
	tenant := commandTenant(t, db)
	runID := insertCommandRun(t, db, tenant)

	old := commandRecord(tenant, runID, time.Now().Add(-model.ReplayHorizon-time.Hour))
	recent := commandRecord(tenant, runID, time.Now().Add(-model.ReplayHorizon+time.Hour))
	require.True(t, mustClaim(t, repo, old))
	require.True(t, mustClaim(t, repo, recent))

	deleted, err := repo.DeleteOlderThan(ctx, model.ReplayHorizon)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, deleted, int64(1))

	_, foundOld, err := repo.Find(ctx, tenant, old.RequestID)
	require.NoError(t, err)
	assert.False(t, foundOld, "a record older than the horizon is pruned, so its id may be used again")
	_, foundRecent, err := repo.Find(ctx, tenant, recent.RequestID)
	require.NoError(t, err)
	assert.True(t, foundRecent, "a record inside the horizon stays")
}

// The run key is checked when the transaction commits, so a transaction may
// claim a request before it creates the run; a claim for a run that never
// appears fails the commit; and deleting a run deletes its records.
func TestCommandRequests_TheRunKeyIsCheckedAtCommit(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := commandTenant(t, db)

	t.Run("the claim may come before the run", func(t *testing.T) {
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		runID := uuid.New()
		rec := commandRecord(tenant, runID, time.Now())
		claimed, err := postgres.NewCommandRequestRepository(db, tx).Claim(ctx, rec)
		require.NoError(t, err)
		require.True(t, claimed)
		_, err = tx.Exec(`INSERT INTO runs (tenant_id, run_id, schedule_name, status, created_at, kind)
			VALUES ($1, $2, 'late-run', 'running', now(), 'trigger')`, tenant, runID)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		_, found, err := postgres.NewCommandRequestRepository(db, nil).Find(ctx, tenant, rec.RequestID)
		require.NoError(t, err)
		assert.True(t, found)
	})

	t.Run("a claim for a run that never appears fails the commit", func(t *testing.T) {
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)
		rec := commandRecord(tenant, uuid.New(), time.Now())
		claimed, err := postgres.NewCommandRequestRepository(db, tx).Claim(ctx, rec)
		require.NoError(t, err)
		require.True(t, claimed)
		assert.Error(t, tx.Commit())
	})

	t.Run("deleting the run deletes its records", func(t *testing.T) {
		runID := insertCommandRun(t, db, tenant)
		rec := commandRecord(tenant, runID, time.Now())
		require.True(t, mustClaim(t, postgres.NewCommandRequestRepository(db, nil), rec))
		_, err := db.Exec(`DELETE FROM runs WHERE tenant_id = $1 AND run_id = $2`, tenant, runID)
		require.NoError(t, err)
		_, found, err := postgres.NewCommandRequestRepository(db, nil).Find(ctx, tenant, rec.RequestID)
		require.NoError(t, err)
		assert.False(t, found)
	})
}

// A claim in an open transaction is invisible to others until it commits, and
// a competing claim of the same id waits for it: after a commit it inserts
// nothing; after a rollback it inserts.
func TestCommandRequests_ACompetingClaimWaitsForTheOpenOne(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := commandTenant(t, db)
	runID := insertCommandRun(t, db, tenant)
	rec := commandRecord(tenant, runID, time.Now())

	for _, commit := range []bool{true, false} {
		rec.RequestID = uuid.New()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)
		claimed, err := postgres.NewCommandRequestRepository(db, tx).Claim(ctx, rec)
		require.NoError(t, err)
		require.True(t, claimed)

		result := make(chan bool, 1)
		go func() {
			ok, claimErr := postgres.NewCommandRequestRepository(db, nil).Claim(ctx, rec)
			assert.NoError(t, claimErr)
			result <- ok
		}()
		select {
		case <-result:
			t.Fatal("the competing claim returned before the first transaction ended")
		case <-time.After(300 * time.Millisecond):
		}
		if commit {
			require.NoError(t, tx.Commit())
		} else {
			require.NoError(t, tx.Rollback())
		}
		assert.Equal(t, !commit, <-result, "commit=%v", commit)
	}
}

// The migrated schema keys command_requests by tenant and request id, checks
// its run key at commit, and no longer gives a run an initialization status.
func TestCommandRequests_SchemaKeysTheTableAndDropsTheRunInitializationStatus(t *testing.T) {
	db := newTestDB(t)

	var pk string
	require.NoError(t, db.Get(&pk, `
		SELECT string_agg(a.attname, ',' ORDER BY k.n)
		  FROM pg_constraint c
		  CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, n)
		  JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
		 WHERE c.conrelid = 'public.command_requests'::regclass AND c.contype = 'p'`))
	assert.Equal(t, "tenant_id,request_id", pk)

	var fk struct {
		Deferrable bool   `db:"condeferrable"`
		Deferred   bool   `db:"condeferred"`
		OnDelete   string `db:"confdeltype"`
		Target     string `db:"target"`
	}
	require.NoError(t, db.Get(&fk, `
		SELECT condeferrable, condeferred, confdeltype::text, confrelid::regclass::text AS target
		  FROM pg_constraint
		 WHERE conrelid = 'public.command_requests'::regclass AND contype = 'f'`))
	assert.True(t, fk.Deferrable)
	assert.True(t, fk.Deferred)
	assert.Equal(t, "c", fk.OnDelete, "deleting a run deletes its records")
	assert.Equal(t, "runs", fk.Target)

	var n int
	require.NoError(t, db.Get(&n, `
		SELECT count(*) FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = 'runs' AND column_name = 'initialization_status'`))
	assert.Zero(t, n)
}
