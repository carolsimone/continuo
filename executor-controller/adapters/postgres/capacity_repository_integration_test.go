//go:build integration

package postgres_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tokenSHA(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// addWorkerDeployment inserts a due pending worker-mode deployment for poolKey.
func addWorkerDeployment(t *testing.T, db *sqlx.DB, poolKey string, now time.Time) *model.Deployment {
	t.Helper()
	dep := model.NewWorkerDeployment(validCmd(), uuid.Nil, poolKey, now)
	require.NoError(t, postgres.NewDeploymentsRepository(db, testLogger()).Add(context.Background(), dep))
	return dep
}

// seedPool registers a worker pool so ListPoolDemand reports it.
func seedPool(t *testing.T, db *sqlx.DB, poolKey, service, imageTag string) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO executor_worker_pools (
			pool_key, service_name, image_tag,
			runtime_manifest_uri, runtime_manifest_sha256,
			runtime_manifest_dbt_version, runtime_manifest_parse_context_sha256,
			credential_sha256, desired_replicas, last_activity_at, created_at, updated_at
		) VALUES ($1, $2, $3, 's3://artifacts/manifest.json', 'abc123', '1.12.0b1', 'ctx123',
		          'cred123', 0, NOW(), NOW(), NOW())`,
		poolKey, service, imageTag)
	require.NoError(t, err)
}

func TestWorkerLeaseRepository_ClaimRoundTrips(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now().UTC().Truncate(time.Millisecond)
	dep := addWorkerDeployment(t, db, "dbt:sha-abc", now)

	leaseID, token := uuid.New(), "raw-secret-token"
	require.NoError(t, dep.Claim(leaseID, tokenSHA(token), "worker-1", "pod-a", "uid-a",
		now, now.Add(time.Minute), []string{"dbt", "run", "--select", "orders"}, model.ExecutionPathNative))
	require.NoError(t, repo.Save(ctx, dep))

	loaded, err := repo.GetByID(ctx, dep.ID())
	require.NoError(t, err)

	assert.Equal(t, model.StatusLeased, loaded.Status())
	assert.Equal(t, model.ExecutionModeWorkers, loaded.ExecutionMode())
	assert.Equal(t, "dbt:sha-abc", loaded.PoolKey())
	assert.Equal(t, []string{"dbt", "run", "--select", "orders"}, loaded.ResolvedArgv())
	assert.Equal(t, model.ExecutionPathNative, loaded.ExecutionPath())

	lease := loaded.ActiveLease()
	require.NotNil(t, lease)
	assert.Equal(t, leaseID, lease.ID)
	assert.Equal(t, tokenSHA(token), lease.TokenSHA256)
	assert.Equal(t, "worker-1", lease.Owner)
	assert.Equal(t, "pod-a", lease.PodName)
	assert.Equal(t, "uid-a", lease.PodUID)
	assert.Equal(t, 1, lease.Attempt)
	assert.NotNil(t, loaded.Reservation().ReservedAt)
	assert.Nil(t, loaded.SlotReleasedAt())

	// The stale-token fence survives the round trip through Postgres.
	assert.ErrorIs(t, loaded.Complete(leaseID, tokenSHA("wrong"),
		model.WorkerResult{Succeeded: true}, now), model.ErrStaleLease)
	require.NoError(t, loaded.Complete(leaseID, tokenSHA(token),
		model.WorkerResult{Succeeded: true, ExecutionSeconds: 3.5}, now))
	require.NoError(t, repo.Save(ctx, loaded))

	done, err := repo.GetByID(ctx, dep.ID())
	require.NoError(t, err)
	assert.Equal(t, model.StatusSucceeded, done.Status())
	assert.NotNil(t, done.SlotReleasedAt())
	require.NotNil(t, done.TerminalResult())
	assert.Equal(t, 3.5, done.TerminalResult().ExecutionSeconds)
}

// TestWorkerLeaseRepository_RawTokenNeverStored proves the raw lease token
// appears in no column of the row.
func TestWorkerLeaseRepository_RawTokenNeverStored(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now().UTC()
	dep := addWorkerDeployment(t, db, "dbt:sha-abc", now)
	const token = "super-secret-raw-token"
	require.NoError(t, dep.Claim(uuid.New(), tokenSHA(token), "worker-1", "pod-a", "uid-a",
		now, now.Add(time.Minute), []string{"dbt", "run"}, model.ExecutionPathNative))
	require.NoError(t, repo.Save(ctx, dep))

	var found int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM executor_deployments WHERE id = $1 AND $2 = ANY(
		     ARRAY[lease_token_sha256, lease_owner, lease_pod_name, lease_pod_uid, error_message]
		 )`, dep.ID(), token).Scan(&found))
	assert.Zero(t, found, "the raw token must not be persisted in any lease column")

	var stored string
	require.NoError(t, db.QueryRow(
		`SELECT lease_token_sha256 FROM executor_deployments WHERE id = $1`, dep.ID()).Scan(&stored))
	assert.Equal(t, tokenSHA(token), stored)
}

func TestWorkerLeaseRepository_GetDueWorkerForPoolIsPoolScoped(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now().UTC()
	want := addWorkerDeployment(t, db, "pool-a", now.Add(-time.Minute))
	addWorkerDeployment(t, db, "pool-b", now.Add(-time.Minute))

	got, err := repo.GetDueWorkerForPool(ctx, "pool-a")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, want.ID(), got.ID())

	empty, err := repo.GetDueWorkerForPool(ctx, "pool-c")
	require.NoError(t, err)
	assert.Nil(t, empty, "a pool with no work yields no deployment")
}

func TestWorkerLeaseRepository_GetDueWorkerForPoolSkipsNotYetDue(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	addWorkerDeployment(t, db, "pool-a", time.Now().UTC().Add(10*time.Minute))

	got, err := repo.GetDueWorkerForPool(ctx, "pool-a")
	require.NoError(t, err)
	assert.Nil(t, got, "a task still in backoff is not claimable")
}

// TestGetDueJobs_ExcludesWorkerRows keeps worker-mode work off the Jobs
// dispatcher: only the Jobs-mode row is due for a Kubernetes Job.
func TestGetDueJobs_ExcludesWorkerRows(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now().UTC().Add(-time.Minute)
	jobsDep := model.NewDeployment(validCmd(), nil, now)
	require.NoError(t, repo.Add(ctx, jobsDep))
	addWorkerDeployment(t, db, "pool-a", now)

	due, err := repo.GetDueJobs(ctx, 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	assert.Equal(t, jobsDep.ID(), due[0].ID())
	assert.Equal(t, model.ExecutionModeJobs, due[0].ExecutionMode())
}

func TestGetExpiredLeaseForUpdate(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now().UTC()
	live := addWorkerDeployment(t, db, "pool-a", now)
	require.NoError(t, live.Claim(uuid.New(), tokenSHA("t1"), "w1", "pod-live", "uid-live",
		now, now.Add(time.Hour), []string{"dbt", "run"}, model.ExecutionPathNative))
	require.NoError(t, repo.Save(ctx, live))

	expired := addWorkerDeployment(t, db, "pool-a", now)
	require.NoError(t, expired.Claim(uuid.New(), tokenSHA("t2"), "w2", "pod-dead", "uid-dead",
		now, now.Add(-time.Minute), []string{"dbt", "run"}, model.ExecutionPathNative))
	require.NoError(t, repo.Save(ctx, expired))

	got, err := repo.GetExpiredLeaseForUpdate(ctx, now)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, expired.ID(), got.ID(), "only the lease past its deadline is reaped")
	assert.Equal(t, "pod-dead", got.ActiveLease().PodName)
}

func TestGetStaleDispatchingForUpdate(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now().UTC()
	stale := model.NewDeployment(validCmd(), nil, now.Add(-time.Hour))
	require.NoError(t, repo.Add(ctx, stale))
	require.NoError(t, stale.ReserveForDispatch(now.Add(-time.Hour)))
	require.NoError(t, repo.Save(ctx, stale))

	fresh := model.NewDeployment(validCmd(), nil, now)
	require.NoError(t, repo.Add(ctx, fresh))
	require.NoError(t, fresh.ReserveForDispatch(now))
	require.NoError(t, repo.Save(ctx, fresh))

	got, err := repo.GetStaleDispatchingForUpdate(ctx, now.Add(-time.Minute))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, stale.ID(), got.ID())
}

func TestReleaseSlot_IsIdempotentForJobs(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now().UTC()
	dep := model.NewDeployment(validCmd(), nil, now)
	require.NoError(t, repo.Add(ctx, dep))
	require.NoError(t, dep.ReserveForDispatch(now))
	require.NoError(t, repo.Save(ctx, dep))

	count, err := repo.ActiveSlotCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "a dispatching Job holds a slot")

	released, err := repo.ReleaseSlot(ctx, dep.ID(), now)
	require.NoError(t, err)
	assert.True(t, released)

	released, err = repo.ReleaseSlot(ctx, dep.ID(), now)
	require.NoError(t, err)
	assert.False(t, released, "a duplicate Job terminal event releases nothing")

	count, err = repo.ActiveSlotCount(ctx)
	require.NoError(t, err)
	assert.Zero(t, count)
}

// TestActiveSlotCount_CountsBothExecutionModes pins the shared capacity pool:
// a Kubernetes Job and a worker lease each consume one slot.
func TestActiveSlotCount_CountsBothExecutionModes(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now().UTC()
	job := model.NewDeployment(validCmd(), nil, now)
	require.NoError(t, repo.Add(ctx, job))
	require.NoError(t, job.ReserveForDispatch(now))
	require.NoError(t, repo.Save(ctx, job))

	worker := addWorkerDeployment(t, db, "pool-a", now)
	require.NoError(t, worker.Claim(uuid.New(), tokenSHA("t"), "w", "pod", "uid",
		now, now.Add(time.Minute), []string{"dbt", "run"}, model.ExecutionPathNative))
	require.NoError(t, repo.Save(ctx, worker))

	count, err := repo.ActiveSlotCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count, "Jobs and workers draw on one capacity pool")

	// A worker transition releases its own slot; the Job's is untouched.
	require.NoError(t, worker.Complete(worker.ActiveLease().ID, tokenSHA("t"),
		model.WorkerResult{Succeeded: true}, now))
	require.NoError(t, repo.Save(ctx, worker))

	count, err = repo.ActiveSlotCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestDemotePendingPoolToJobs(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now().UTC()
	pending := addWorkerDeployment(t, db, "pool-a", now)
	other := addWorkerDeployment(t, db, "pool-b", now)

	leased := addWorkerDeployment(t, db, "pool-a", now)
	require.NoError(t, leased.Claim(uuid.New(), tokenSHA("t"), "w", "pod", "uid",
		now, now.Add(time.Minute), []string{"dbt", "run"}, model.ExecutionPathNative))
	require.NoError(t, repo.Save(ctx, leased))

	n, err := repo.DemotePendingPoolToJobs(ctx, "pool-a", now)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n, "only pool-a's pending row is demoted")

	moved, err := repo.GetByID(ctx, pending.ID())
	require.NoError(t, err)
	assert.Equal(t, model.ExecutionModeJobs, moved.ExecutionMode())
	assert.Empty(t, moved.PoolKey())

	stillLeased, err := repo.GetByID(ctx, leased.ID())
	require.NoError(t, err)
	assert.Equal(t, model.ExecutionModeWorkers, stillLeased.ExecutionMode(),
		"running work must finish or be fenced, never silently demoted")

	untouched, err := repo.GetByID(ctx, other.ID())
	require.NoError(t, err)
	assert.Equal(t, "pool-b", untouched.PoolKey())
}

func TestCancelSchedule_ReportsActiveLeasesAndReleasesSlots(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now().UTC()
	cmd := validCmd()
	scheduleID := uuid.MustParse(cmd.ScheduleID)

	leased := model.NewWorkerDeployment(cmd, uuid.Nil, "pool-a", now)
	require.NoError(t, repo.Add(ctx, leased))
	leaseID := uuid.New()
	require.NoError(t, leased.Claim(leaseID, tokenSHA("t"), "w", "pod-x", "uid-x",
		now, now.Add(time.Minute), []string{"dbt", "run"}, model.ExecutionPathNative))
	require.NoError(t, repo.Save(ctx, leased))

	pendingCmd := cmd
	pendingCmd.TaskID = uuid.New().String()
	pending := model.NewWorkerDeployment(pendingCmd, uuid.Nil, "pool-a", now)
	require.NoError(t, repo.Add(ctx, pending))

	leases, err := repo.CancelSchedule(ctx, scheduleID, now)
	require.NoError(t, err)

	require.Len(t, leases, 1, "only the leased row names a pod to terminate")
	assert.Equal(t, leased.ID(), leases[0].DeploymentID)
	assert.Equal(t, leaseID, leases[0].LeaseID)
	assert.Equal(t, "pod-x", leases[0].PodName)
	assert.Equal(t, "uid-x", leases[0].PodUID)

	for _, id := range []uuid.UUID{leased.ID(), pending.ID()} {
		got, err := repo.GetByID(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, model.StatusCancelled, got.Status())
	}

	count, err := repo.ActiveSlotCount(ctx)
	require.NoError(t, err)
	assert.Zero(t, count, "cancelling releases the leased row's slot")
}

func TestListPoolDemand(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now().UTC()
	seedPool(t, db, "pool-a", "dbt", "sha-abc")
	seedPool(t, db, "pool-idle", "dbt", "sha-idle")

	oldest := now.Add(-5 * time.Minute)
	addWorkerDeployment(t, db, "pool-a", oldest)
	addWorkerDeployment(t, db, "pool-a", now.Add(-time.Minute))
	addWorkerDeployment(t, db, "pool-a", now.Add(time.Hour)) // still in backoff

	leased := addWorkerDeployment(t, db, "pool-a", now)
	require.NoError(t, leased.Claim(uuid.New(), tokenSHA("t"), "w", "pod", "uid",
		now, now.Add(time.Minute), []string{"dbt", "run"}, model.ExecutionPathNative))
	require.NoError(t, repo.Save(ctx, leased))

	demand, err := repo.ListPoolDemand(ctx, now)
	require.NoError(t, err)
	require.Len(t, demand, 2)

	byKey := map[string]model.PoolDemand{}
	for _, d := range demand {
		byKey[d.PoolKey] = d
	}

	a := byKey["pool-a"]
	assert.Equal(t, 2, a.Pending, "only due pending work is backlog")
	assert.Equal(t, 1, a.ActiveLeases)
	assert.WithinDuration(t, oldest, a.OldestReadyAt, time.Second)
	assert.Equal(t, "dbt", a.ServiceName)
	assert.Equal(t, "sha-abc", a.ImageTag)
	assert.Equal(t, "s3://artifacts/manifest.json", a.RuntimeManifest.RuntimeManifestURI)
	assert.Equal(t, "abc123", a.RuntimeManifest.RuntimeManifestSHA256)
	assert.True(t, a.RuntimeManifest.Complete(), "a pool serves one complete runtime manifest")

	idle := byKey["pool-idle"]
	assert.Zero(t, idle.Pending)
	assert.Zero(t, idle.ActiveLeases)
}

func TestGetByID_NotFound(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	_, err := postgres.NewDeploymentsRepository(db, testLogger()).GetByID(context.Background(), uuid.New())
	assert.ErrorIs(t, err, sql.ErrNoRows)
}

// TestConcurrentCapacityNeverOvershoots runs MAX+10 concurrent reservations
// across two worker pools and the Jobs path against one real database. Each
// caller does what the application services will: lock capacity, count held
// slots, and reserve only if one is free. The cap must hold, and no deployment
// or lease may be handed out twice.
func TestConcurrentCapacityNeverOvershoots(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	const limit = 4
	const callers = limit + 10

	now := time.Now().UTC().Add(-time.Minute)
	// Enough due work on every path that capacity, not supply, is the limit.
	for i := 0; i < callers; i++ {
		addWorkerDeployment(t, db, "pool-a", now)
		addWorkerDeployment(t, db, "pool-b", now)
		require.NoError(t, postgres.NewDeploymentsRepository(db, testLogger()).
			Add(ctx, model.NewDeployment(validCmd(), nil, now)))
	}

	var (
		mu                    sync.Mutex
		claimedDeploymentIDs  = map[uuid.UUID]struct{}{}
		claimedLeaseIDs       = map[uuid.UUID]struct{}{}
		maxObservedActiveSlot int64
		workerClaims          int64
		start                 = make(chan struct{})
		wg                    sync.WaitGroup
	)

	observe := func(n int) {
		for {
			cur := atomic.LoadInt64(&maxObservedActiveSlot)
			if int64(n) <= cur || atomic.CompareAndSwapInt64(&maxObservedActiveSlot, cur, int64(n)) {
				return
			}
		}
	}

	// reserve mirrors one application-service transaction.
	reserve := func(pool string) error {
		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		repo := postgres.NewDeploymentsRepository(tx, testLogger())
		if err := repo.LockCapacity(ctx); err != nil {
			return err
		}
		active, err := repo.ActiveSlotCount(ctx)
		if err != nil {
			return err
		}
		if active >= limit {
			return nil // no capacity: the caller backs off
		}

		var dep *model.Deployment
		leaseID := uuid.New()
		if pool == "" {
			due, err := repo.GetDueJobs(ctx, 1)
			if err != nil || len(due) == 0 {
				return err
			}
			dep = due[0]
			if err := dep.ReserveForDispatch(time.Now()); err != nil {
				return err
			}
		} else {
			if dep, err = repo.GetDueWorkerForPool(ctx, pool); err != nil || dep == nil {
				return err
			}
			if err := dep.Claim(leaseID, tokenSHA(leaseID.String()), "w", "pod", "uid",
				time.Now(), time.Now().Add(time.Minute), []string{"dbt", "run"},
				model.ExecutionPathNative); err != nil {
				return err
			}
		}
		if err := repo.Save(ctx, dep); err != nil {
			return err
		}
		// Counted while the capacity lock is still held, so the observation is a
		// true serialized reading of the cap.
		after, err := repo.ActiveSlotCount(ctx)
		if err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}

		observe(after)
		mu.Lock()
		defer mu.Unlock()
		claimedDeploymentIDs[dep.ID()] = struct{}{}
		if pool != "" {
			claimedLeaseIDs[leaseID] = struct{}{}
			atomic.AddInt64(&workerClaims, 1)
		}
		return nil
	}

	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		pool := []string{"", "pool-a", "pool-b"}[i%3]
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := reserve(pool); err != nil {
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	assert.LessOrEqual(t, int(maxObservedActiveSlot), limit, "the concurrency cap must never be exceeded")
	assert.Len(t, claimedDeploymentIDs, limit, "exactly the cap's worth of distinct deployments reserved")
	assert.Len(t, claimedLeaseIDs, int(workerClaims), "every worker claim minted a distinct lease")

	final, err := postgres.NewDeploymentsRepository(db, testLogger()).ActiveSlotCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, limit, final, "all reservations committed and none leaked")
}
