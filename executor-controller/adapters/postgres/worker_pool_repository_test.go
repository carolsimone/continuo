//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func poolFixture(poolKey string, now time.Time) model.WorkerPool {
	return model.WorkerPool{
		PoolKey:     poolKey,
		ServiceName: "finance",
		ImageTag:    "sha-abc",
		RuntimeManifest: pkgmodel.RuntimeManifestRef{
			RuntimeManifestURI:                "s3://continuo/artifacts/finance/manifest.msgpack",
			RuntimeManifestSHA256:             "a1b2",
			RuntimeManifestDBTVersion:         "1.12.0b1",
			RuntimeManifestParseContextSHA256: "c3d4",
		},
		CredentialSHA256: "credential-digest",
		DesiredReplicas:  2,
		LastActivityAt:   now,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}

func TestWorkerPoolRepository_AddAndGetRoundTrip(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewWorkerPoolRepository(db)

	now := time.Now().UTC().Truncate(time.Millisecond)
	pool := poolFixture("pool-abc", now)
	require.NoError(t, repo.Add(ctx, pool))

	loaded, err := repo.Get(ctx, "pool-abc")
	require.NoError(t, err)
	require.NotNil(t, loaded)

	assert.Equal(t, "pool-abc", loaded.PoolKey)
	assert.Equal(t, "finance", loaded.ServiceName)
	assert.Equal(t, "sha-abc", loaded.ImageTag)
	assert.Equal(t, pool.RuntimeManifest, loaded.RuntimeManifest)
	assert.Equal(t, "credential-digest", loaded.CredentialSHA256)
	assert.Equal(t, 2, loaded.DesiredReplicas)
	assert.True(t, loaded.Ready())
	assert.Empty(t, loaded.InitializationError)
}

// TestWorkerPoolRepository_GetUnknownPoolIsNotAnError lets the authenticator
// treat an unregistered pool exactly like a wrong credential.
func TestWorkerPoolRepository_GetUnknownPoolIsNotAnError(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	loaded, err := postgres.NewWorkerPoolRepository(db).Get(context.Background(), "pool-missing")

	require.NoError(t, err)
	assert.Nil(t, loaded)
}

// TestWorkerPoolRepository_SaveRoundTripsTheInitializationError pins that a
// pool's readiness survives a write and a read: a NULL column and an empty
// string must both read as ready.
func TestWorkerPoolRepository_SaveRoundTripsTheInitializationError(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewWorkerPoolRepository(db)

	now := time.Now().UTC().Truncate(time.Millisecond)
	pool := poolFixture("pool-abc", now)
	require.NoError(t, repo.Add(ctx, pool))

	pool.RecordInitializationFailure("artifact_rejected", "sha256 mismatch", now)
	pool.DesiredReplicas = 1
	require.NoError(t, repo.Save(ctx, pool))

	loaded, err := repo.Get(ctx, "pool-abc")
	require.NoError(t, err)
	assert.False(t, loaded.Ready())
	assert.Equal(t, "artifact_rejected: sha256 mismatch", loaded.InitializationError)
	assert.Equal(t, 1, loaded.DesiredReplicas)

	pool.ClearInitializationError(now)
	require.NoError(t, repo.Save(ctx, pool))

	loaded, err = repo.Get(ctx, "pool-abc")
	require.NoError(t, err)
	assert.True(t, loaded.Ready())
	assert.Empty(t, loaded.InitializationError)
}

// TestWorkerPoolRepository_SaveInitializationErrorLeavesTheRestOfTheRow is what
// keeps a worker's own report inside what the report owns. A rotation of the
// pool's credential that lands between the report's read and its write must
// survive it: restoring a retired digest would let it authenticate again.
func TestWorkerPoolRepository_SaveInitializationErrorLeavesTheRestOfTheRow(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewWorkerPoolRepository(db)

	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, repo.Add(ctx, poolFixture("pool-abc", now)))

	// A rotation lands after a report has read the pool.
	rotated := poolFixture("pool-abc", now)
	rotated.CredentialSHA256 = "rotated-digest"
	rotated.DesiredReplicas = 7
	require.NoError(t, repo.Save(ctx, rotated))

	later := now.Add(time.Minute)
	require.NoError(t, repo.SaveInitializationError(ctx, "pool-abc", "artifact_rejected: sha256 mismatch", later))

	loaded, err := repo.Get(ctx, "pool-abc")
	require.NoError(t, err)
	assert.Equal(t, "artifact_rejected: sha256 mismatch", loaded.InitializationError)
	assert.False(t, loaded.Ready())
	assert.Equal(t, later, loaded.UpdatedAt.UTC())
	// Everything the report does not own is untouched.
	assert.Equal(t, "rotated-digest", loaded.CredentialSHA256)
	assert.Equal(t, 7, loaded.DesiredReplicas)
	assert.Equal(t, rotated.RuntimeManifest, loaded.RuntimeManifest)
}

// TestWorkerPoolRepository_SaveInitializationErrorClearsToNull keeps a
// recovered pool readable as ready: an empty error must reach the column as
// NULL, which is what Ready reads back.
func TestWorkerPoolRepository_SaveInitializationErrorClearsToNull(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewWorkerPoolRepository(db)

	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, repo.Add(ctx, poolFixture("pool-abc", now)))
	require.NoError(t, repo.SaveInitializationError(ctx, "pool-abc", "artifact_rejected", now))

	require.NoError(t, repo.SaveInitializationError(ctx, "pool-abc", "", now))

	loaded, err := repo.Get(ctx, "pool-abc")
	require.NoError(t, err)
	assert.True(t, loaded.Ready())
	assert.Empty(t, loaded.InitializationError)
}

// TestWorkerPoolRepository_SaveInitializationErrorUnknownPoolFails stops a
// report naming a pool that was never registered from silently doing nothing.
func TestWorkerPoolRepository_SaveInitializationErrorUnknownPoolFails(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	err := postgres.NewWorkerPoolRepository(db).SaveInitializationError(
		context.Background(), "pool-missing", "artifact_rejected", time.Now().UTC())

	require.Error(t, err)
}

// TestWorkerPoolRepository_SaveUnknownPoolFails stops a report for a pool that
// was never registered from silently doing nothing.
func TestWorkerPoolRepository_SaveUnknownPoolFails(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	now := time.Now().UTC().Truncate(time.Millisecond)
	err := postgres.NewWorkerPoolRepository(db).Save(context.Background(), poolFixture("pool-missing", now))

	require.Error(t, err)
}

func TestWorkerPoolRepository_ListReturnsEveryPool(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewWorkerPoolRepository(db)

	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, repo.Add(ctx, poolFixture("pool-b", now)))
	require.NoError(t, repo.Add(ctx, poolFixture("pool-a", now)))

	pools, err := repo.List(ctx)
	require.NoError(t, err)
	require.Len(t, pools, 2)
	assert.Equal(t, "pool-a", pools[0].PoolKey)
	assert.Equal(t, "pool-b", pools[1].PoolKey)
}

func TestWorkerPoolRepository_ListEmptyIsNotAnError(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	pools, err := postgres.NewWorkerPoolRepository(db).List(context.Background())

	require.NoError(t, err)
	assert.Empty(t, pools)
}

// workerDeploymentFixture is a worker-mode task naming poolKey, carrying the
// runtime manifest a pool registered from it must serve.
func workerDeploymentFixture(poolKey, service, imageTag, manifestSHA string) command.DeployTask {
	cmd := validCmd()
	cmd.ServiceName = service
	cmd.ImageTag = imageTag
	cmd.DBTUniqueID = "model." + service + ".orders"
	cmd.RuntimeManifestRef = pkgmodel.RuntimeManifestRef{
		RuntimeManifestURI:                "s3://continuo/artifacts/" + service + "/partial_parse.msgpack",
		RuntimeManifestSHA256:             manifestSHA,
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: "ctx-" + manifestSHA,
	}
	return cmd
}

// addWorkerTask enqueues one worker-mode task against poolKey.
func addWorkerTask(t *testing.T, db *sqlx.DB, poolKey string, cmd command.DeployTask) *model.Deployment {
	t.Helper()
	dep := model.NewWorkerDeployment(cmd, uuid.Nil, poolKey, time.Now().UTC())
	require.NoError(t, postgres.NewDeploymentsRepository(db, testLogger()).
		Add(context.Background(), dep))
	return dep
}

// TestWorkerPoolRepository_ListUnregisteredDiscoversAPoolFromItsWaitingWork is
// how a pool ever comes to exist: nothing declares pools, so the work already
// routed to one is what says the pool must be registered.
func TestWorkerPoolRepository_ListUnregisteredDiscoversAPoolFromItsWaitingWork(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	poolKey := pkgmodel.WorkerPoolKey("finance", "sha-abc", "man-1")
	addWorkerTask(t, db, poolKey, workerDeploymentFixture(poolKey, "finance", "sha-abc", "man-1"))

	got, err := postgres.NewWorkerPoolRepository(db).ListUnregistered(ctx)

	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, poolKey, got[0].PoolKey)
	assert.Equal(t, "finance", got[0].ServiceName)
	assert.Equal(t, "sha-abc", got[0].ImageTag)
	assert.Equal(t, pkgmodel.RuntimeManifestRef{
		RuntimeManifestURI:                "s3://continuo/artifacts/finance/partial_parse.msgpack",
		RuntimeManifestSHA256:             "man-1",
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: "ctx-man-1",
	}, got[0].RuntimeManifest, "the pool serves the artifact its work names")
}

// TestWorkerPoolRepository_ListUnregisteredSkipsARegisteredPool keeps a tick
// from re-registering — and so re-credentialing — a pool that already exists.
func TestWorkerPoolRepository_ListUnregisteredSkipsARegisteredPool(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewWorkerPoolRepository(db)

	poolKey := pkgmodel.WorkerPoolKey("finance", "sha-abc", "man-1")
	addWorkerTask(t, db, poolKey, workerDeploymentFixture(poolKey, "finance", "sha-abc", "man-1"))
	require.NoError(t, repo.Add(ctx, poolFixture(poolKey, time.Now().UTC())))

	got, err := repo.ListUnregistered(ctx)

	require.NoError(t, err)
	assert.Empty(t, got, "a pool that exists is not discovered again")
}

// TestWorkerPoolRepository_ListUnregisteredCollapsesAPoolsManyTasks proves the
// read is about pools, not tasks: a backlog of a thousand tasks is one pool.
func TestWorkerPoolRepository_ListUnregisteredCollapsesAPoolsManyTasks(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	poolKey := pkgmodel.WorkerPoolKey("finance", "sha-abc", "man-1")
	for i := 0; i < 3; i++ {
		addWorkerTask(t, db, poolKey, workerDeploymentFixture(poolKey, "finance", "sha-abc", "man-1"))
	}

	got, err := postgres.NewWorkerPoolRepository(db).ListUnregistered(context.Background())

	require.NoError(t, err)
	assert.Len(t, got, 1, "many tasks of one pool discover one pool")
}

// TestWorkerPoolRepository_ListUnregisteredSeparatesPoolsOfOneService proves a
// service running two image tags or two runtime manifests needs two pools: a
// worker can only serve the exact artifact it hydrated.
func TestWorkerPoolRepository_ListUnregisteredSeparatesPoolsOfOneService(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	newImage := pkgmodel.WorkerPoolKey("finance", "sha-xyz", "man-1")
	newManifest := pkgmodel.WorkerPoolKey("finance", "sha-abc", "man-2")
	addWorkerTask(t, db, newImage, workerDeploymentFixture(newImage, "finance", "sha-xyz", "man-1"))
	addWorkerTask(t, db, newManifest, workerDeploymentFixture(newManifest, "finance", "sha-abc", "man-2"))

	got, err := postgres.NewWorkerPoolRepository(db).ListUnregistered(context.Background())

	require.NoError(t, err)
	require.Len(t, got, 2, "one service, two pools")
	byKey := map[string]model.PoolIdentity{got[0].PoolKey: got[0], got[1].PoolKey: got[1]}
	assert.Equal(t, "sha-xyz", byKey[newImage].ImageTag)
	assert.Equal(t, "man-2", byKey[newManifest].RuntimeManifest.RuntimeManifestSHA256)
}

// TestWorkerPoolRepository_ListUnregisteredIgnoresJobsModeWork proves the Jobs
// path never conjures a pool. Every record on a jobs-mode executor is one of
// these, so a pool discovered from one would be a pool nothing could ever use.
func TestWorkerPoolRepository_ListUnregisteredIgnoresJobsModeWork(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	require.NoError(t, postgres.NewDeploymentsRepository(db, testLogger()).Add(
		context.Background(), model.NewDeployment(validCmd(), nil, time.Now().UTC())))

	got, err := postgres.NewWorkerPoolRepository(db).ListUnregistered(context.Background())

	require.NoError(t, err)
	assert.Empty(t, got, "a Jobs-mode record implies no pool")
}

// TestWorkerPoolRepository_ListUnregisteredDiscoversAPoolForParkedRetries proves
// a task waiting out its retry backoff still holds its pool open. It is going to
// run, so retiring the pool underneath it would strand it.
func TestWorkerPoolRepository_ListUnregisteredDiscoversAPoolForParkedRetries(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	poolKey := pkgmodel.WorkerPoolKey("finance", "sha-abc", "man-1")
	dep := addWorkerTask(t, db, poolKey, workerDeploymentFixture(poolKey, "finance", "sha-abc", "man-1"))
	_, err := db.Exec(`UPDATE executor_deployments SET status = 'retry_pending' WHERE id = $1`, dep.ID())
	require.NoError(t, err)

	got, err := postgres.NewWorkerPoolRepository(db).ListUnregistered(context.Background())

	require.NoError(t, err)
	require.Len(t, got, 1, "a task parked for retry still needs its pool")
}

// TestWorkerPoolRepository_ListUnregisteredIgnoresSettledWork proves a pool is
// not resurrected by history: work that has already finished needs no worker.
func TestWorkerPoolRepository_ListUnregisteredIgnoresSettledWork(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	poolKey := pkgmodel.WorkerPoolKey("finance", "sha-abc", "man-1")
	dep := addWorkerTask(t, db, poolKey, workerDeploymentFixture(poolKey, "finance", "sha-abc", "man-1"))
	_, err := db.Exec(`UPDATE executor_deployments SET status = 'succeeded' WHERE id = $1`, dep.ID())
	require.NoError(t, err)

	got, err := postgres.NewWorkerPoolRepository(db).ListUnregistered(context.Background())

	require.NoError(t, err)
	assert.Empty(t, got, "a settled task does not keep a pool alive")
}
