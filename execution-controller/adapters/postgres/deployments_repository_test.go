//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/validation"
	executortest "github.com/carolsimone/continuo/execution-controller/test"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupPostgres(t *testing.T) (*sqlx.DB, func()) {
	return executortest.SetupPostgres(t)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func validCmd() command.DeployTask {
	return command.DeployTask{
		TaskID: uuid.New().String(), ScheduleID: uuid.New().String(),
		ScheduleName: "daily", ServiceName: "dbt", SchemaName: "public", TableName: "orders",
		JobName: "dbt-public-orders", NodeType: "dbt-model", ImageTag: "sha-abc",
		TaskRetryCount: 0, TaskMaxRetries: 2,
	}
}

func validValidationCmd(releaseID, nodeID string) command.ValidationDeployTask {
	return command.ValidationDeployTask{
		ReleaseID: releaseID, NodeID: nodeID,
		ServiceName: "dbt", SchemaName: "public", TableName: "orders",
		NodeType: "dbt-model", ImageTag: "sha-cand",
		JobName: "dbt-validate-public-orders", CandidateSchema: "candidate_rel1",
	}
}

// seedDue inserts a raw pending row (empty job_params) with a chosen due time.
func seedDue(t *testing.T, db *sqlx.DB, nextAttempt time.Time) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Exec(
		`INSERT INTO deployments (id, task_id, schedule_id, job_params, next_attempt_at)
		 VALUES ($1, $2, $3, '{}'::jsonb, $4)`,
		id, uuid.New(), uuid.New(), nextAttempt)
	require.NoError(t, err)
	return id
}

func TestRepo_Add_PersistsPendingAggregate(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now()
	cmd := validCmd()
	dep := model.NewDeployment(cmd, nil, now)
	require.NoError(t, repo.Add(context.Background(), dep))

	var (
		status     string
		maxRetries int
		taskID     uuid.UUID
		jobParams  []byte
		nextAt     time.Time
	)
	require.NoError(t, db.QueryRow(
		`SELECT status, max_retries, task_id, job_params, next_attempt_at FROM deployments WHERE id=$1`, dep.ID(),
	).Scan(&status, &maxRetries, &taskID, &jobParams, &nextAt))
	assert.Equal(t, "pending", status)
	assert.Equal(t, 3, maxRetries)
	assert.Equal(t, cmd.TaskID, taskID.String(), "task_id column populated from the command identity")
	assert.Contains(t, string(jobParams), "dbt-public-orders", "command serialized into job_params")
	assert.False(t, nextAt.IsZero())
}

func TestRepo_Save_MarkStarted(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now()
	dep := model.NewDeployment(validCmd(), nil, now)
	require.NoError(t, repo.Add(context.Background(), dep))

	require.NoError(t, dep.Reserve())
	require.NoError(t, dep.MarkStarted(now))
	require.NoError(t, repo.Save(context.Background(), dep))

	var status string
	var deployedAt *time.Time
	require.NoError(t, db.QueryRow(`SELECT status, deployed_at FROM deployments WHERE id=$1`, dep.ID()).Scan(&status, &deployedAt))
	assert.Equal(t, "starting", status)
	assert.NotNil(t, deployedAt)
}

func TestRepo_Save_StampsStateChangedAtOnStatusChangeOnly(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	ctx := context.Background()

	dep := model.NewDeployment(validCmd(), nil, time.Now())
	require.NoError(t, repo.Add(ctx, dep))
	_, err := db.Exec(`UPDATE deployments SET state_changed_at = NOW() - interval '1 hour' WHERE id = $1`, dep.ID())
	require.NoError(t, err)
	age := func() time.Duration {
		var secs float64
		require.NoError(t, db.QueryRow(`SELECT EXTRACT(EPOCH FROM NOW() - state_changed_at) FROM deployments WHERE id = $1`, dep.ID()).Scan(&secs))
		return time.Duration(secs * float64(time.Second))
	}

	require.NoError(t, repo.Save(ctx, dep)) // still pending
	require.Greater(t, age(), 50*time.Minute, "a save without a status change keeps the clock")

	require.NoError(t, dep.Reserve())
	require.NoError(t, repo.Save(ctx, dep))
	require.Less(t, age(), time.Minute, "a status change restarts the clock")
}

func TestRepo_PendingValidationCount_CountsReleasedRowsWithoutOutcome(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	ctx := context.Background()

	dep := model.NewValidationDeployment(validValidationCmd("rel-count", "n1"), nil, time.Now(), false)
	require.NoError(t, repo.Add(ctx, dep))
	_, err := db.Exec(`UPDATE deployments SET status = 'done' WHERE id = $1`, dep.ID())
	require.NoError(t, err)

	n, err := repo.PendingValidationCount(ctx, "rel-count", model.ModeValidation)
	require.NoError(t, err)
	require.Equal(t, 1, n, "a node whose slot was released without an outcome still holds the leg open")
}

func TestRepo_Save_RescheduleAndFail(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	backoff := model.BackoffPolicy{Base: 20 * time.Second, Cap: 2 * time.Minute}

	now := time.Now()
	dep := model.NewDeployment(validCmd(), nil, now) // maxRetries 3
	require.NoError(t, repo.Add(context.Background(), dep))

	// Transient failure → reschedule (stays pending, retry_count 1).
	require.NoError(t, dep.Reserve())
	terminal, err := dep.RegisterFailure(now, false, "boom", backoff)
	require.NoError(t, err)
	require.False(t, terminal)
	require.NoError(t, repo.Save(context.Background(), dep))

	var status, errMsg string
	var rc int
	var nextAt time.Time
	require.NoError(t, db.QueryRow(
		`SELECT status, retry_count, error_message, next_attempt_at FROM deployments WHERE id=$1`, dep.ID()).
		Scan(&status, &rc, &errMsg, &nextAt))
	assert.Equal(t, "pending", status)
	assert.Equal(t, 1, rc)
	assert.Equal(t, "boom", errMsg)
	assert.True(t, nextAt.After(now))

	// Permanent failure → terminal.
	require.NoError(t, dep.Reserve())
	terminal, err = dep.RegisterFailure(now, true, "fatal", backoff)
	require.NoError(t, err)
	require.True(t, terminal)
	require.NoError(t, repo.Save(context.Background(), dep))
	require.NoError(t, db.QueryRow(`SELECT status, error_message FROM deployments WHERE id=$1`, dep.ID()).Scan(&status, &errMsg))
	assert.Equal(t, "failed", status)
	assert.Equal(t, "fatal", errMsg)
}

func TestAdd_ValidationRow_RoundTrip(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now()
	cmd := validValidationCmd("rel-1", "node-A")
	dep := model.NewValidationDeployment(cmd, nil, now, false)
	require.NoError(t, repo.Add(context.Background(), dep))

	// The mode/release/node columns and synthetic NOT NULL ids are persisted.
	var (
		mode             string
		releaseID        string
		nodeID           string
		taskID, schedule uuid.UUID
		jobParams        []byte
	)
	require.NoError(t, db.QueryRow(
		`SELECT mode, release_id, node_id, task_id, schedule_id, job_params
		 FROM deployments WHERE id=$1`, dep.ID(),
	).Scan(&mode, &releaseID, &nodeID, &taskID, &schedule, &jobParams))
	assert.Equal(t, "validation", mode)
	assert.Equal(t, "rel-1", releaseID)
	assert.Equal(t, "node-A", nodeID)
	assert.Contains(t, string(jobParams), "dbt-validate-public-orders", "validation command serialized into job_params")
	require.NotEqual(t, uuid.Nil, taskID)
	require.NotEqual(t, uuid.Nil, schedule)
	assert.NotEqual(t, taskID, schedule, "task and schedule synthetic ids must differ")

	// Synthetic ids are deterministic across re-adds of the same (release,node).
	dep2 := model.NewValidationDeployment(cmd, nil, now, false)
	_, delErr := db.Exec(`DELETE FROM deployments WHERE id=$1`, dep.ID())
	require.NoError(t, delErr)
	require.NoError(t, repo.Add(context.Background(), dep2))
	var taskID2, schedule2 uuid.UUID
	require.NoError(t, db.QueryRow(
		`SELECT task_id, schedule_id FROM deployments WHERE id=$1`, dep2.ID(),
	).Scan(&taskID2, &schedule2))
	assert.Equal(t, taskID, taskID2, "synthetic task_id deterministic for same (release,node)")
	assert.Equal(t, schedule, schedule2, "synthetic schedule_id deterministic for same (release,node)")

	// Reconstitution via GetByReleaseNode rebuilds the validation aggregate.
	got, err := repo.GetByReleaseNode(context.Background(), "rel-1", "node-A", model.ModeValidation)
	require.NoError(t, err)
	assert.Equal(t, model.ModeValidation, got.Mode())
	assert.Equal(t, cmd, got.ValidationCommand())
	assert.Equal(t, "rel-1", got.ReleaseID())
	assert.Equal(t, "node-A", got.NodeID())
}

func TestGetByReleaseNode_HitAndMiss(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	dep := model.NewValidationDeployment(validValidationCmd("rel-2", "node-X"), nil, time.Now(), false)
	require.NoError(t, repo.Add(context.Background(), dep))

	got, err := repo.GetByReleaseNode(context.Background(), "rel-2", "node-X", model.ModeValidation)
	require.NoError(t, err)
	assert.Equal(t, dep.ID(), got.ID())

	_, err = repo.GetByReleaseNode(context.Background(), "rel-2", "node-MISSING", model.ModeValidation)
	assert.ErrorIs(t, err, sql.ErrNoRows, "miss signals sql.ErrNoRows")
}

func TestPendingValidationCount_PendingDeployedDoneMix(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	ctx := context.Background()
	now := time.Now()

	// pending row (counts)
	pending := model.NewValidationDeployment(validValidationCmd("rel-3", "n1"), nil, now, false)
	require.NoError(t, repo.Add(ctx, pending))

	// started, outcome not yet recorded (counts)
	deployed := model.NewValidationDeployment(validValidationCmd("rel-3", "n2"), nil, now, false)
	require.NoError(t, repo.Add(ctx, deployed))
	require.NoError(t, deployed.Reserve())
	require.NoError(t, deployed.MarkStarted(now))
	require.NoError(t, repo.Save(ctx, deployed))

	// started + outcome recorded (does NOT count — terminal)
	done := model.NewValidationDeployment(validValidationCmd("rel-3", "n3"), nil, now, false)
	require.NoError(t, repo.Add(ctx, done))
	require.NoError(t, done.Reserve())
	require.NoError(t, done.MarkStarted(now))
	require.NoError(t, done.RecordOutcome("ok", "s3://logs/n3", "", "", now))
	require.NoError(t, repo.Save(ctx, done))

	// a different release's pending row must not leak into the count
	other := model.NewValidationDeployment(validValidationCmd("rel-OTHER", "n1"), nil, now, false)
	require.NoError(t, repo.Add(ctx, other))

	count, err := repo.PendingValidationCount(ctx, "rel-3", model.ModeValidation)
	require.NoError(t, err)
	assert.Equal(t, 2, count, "pending + started-without-outcome count; outcomed row excluded")
}

func TestListValidationResults_OnlyOutcomedRows(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	ctx := context.Background()
	now := time.Now()

	// outcomed ok
	okDep := model.NewValidationDeployment(validValidationCmd("rel-4", "n1"), nil, now, false)
	require.NoError(t, repo.Add(ctx, okDep))
	require.NoError(t, okDep.Reserve())
	require.NoError(t, okDep.MarkStarted(now))
	require.NoError(t, okDep.RecordOutcome("ok", "s3://logs/n1", "", "", now))
	require.NoError(t, repo.Save(ctx, okDep))

	// outcomed failed (later outcome_at so it orders second)
	failDep := model.NewValidationDeployment(validValidationCmd("rel-4", "n2"), nil, now, false)
	require.NoError(t, repo.Add(ctx, failDep))
	require.NoError(t, failDep.Reserve())
	require.NoError(t, failDep.MarkStarted(now))
	require.NoError(t, failDep.RecordOutcome("failed", "s3://logs/n2", "run-results/n2.json", "", now.Add(time.Second)))
	require.NoError(t, repo.Save(ctx, failDep))

	// pending, no outcome — excluded
	pending := model.NewValidationDeployment(validValidationCmd("rel-4", "n3"), nil, now, false)
	require.NoError(t, repo.Add(ctx, pending))

	results, err := repo.ListValidationResults(ctx, "rel-4", model.ModeValidation)
	require.NoError(t, err)
	require.Len(t, results, 2, "only outcomed rows returned")
	assert.Equal(t, "ok", results[0].Outcome(), "ordered by outcome_at ASC")
	assert.Equal(t, "s3://logs/n1", results[0].DBTLogURI())
	require.NotNil(t, results[0].OutcomeAt())
	assert.Equal(t, "failed", results[1].Outcome())
	assert.Equal(t, "n2", results[1].NodeID())
	assert.Equal(t, "run-results/n2.json", results[1].DBTRunResultsURI(), "run_results_uri round-trips")
	assert.Equal(t, "", results[0].DBTRunResultsURI(), "absent run_results_uri reconstitutes empty")
}

func TestClaimEmission_FirstCallerWins_SecondReturnsFalse(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewValidationAggregateRepository(db)
	ctx := context.Background()
	now := time.Now()

	won, err := repo.ClaimEmission(ctx, "rel-5", model.ModeValidation, now)
	require.NoError(t, err)
	assert.True(t, won, "first caller claims emission")

	won2, err := repo.ClaimEmission(ctx, "rel-5", model.ModeValidation, now.Add(time.Second))
	require.NoError(t, err)
	assert.False(t, won2, "second caller loses on PK conflict")

	// a distinct release is independent
	wonOther, err := repo.ClaimEmission(ctx, "rel-6", model.ModeValidation, now)
	require.NoError(t, err)
	assert.True(t, wonOther)
}

// seedDeployedValidationNode inserts a mode=validation row in status=starting
// with no outcome yet — i.e. one that PendingValidationCount counts as pending.
func seedDeployedValidationNode(t *testing.T, db *sqlx.DB, releaseID, nodeID string, now time.Time) {
	t.Helper()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	dep := model.NewValidationDeployment(validValidationCmd(releaseID, nodeID), nil, now, false)
	require.NoError(t, repo.Add(context.Background(), dep))
	require.NoError(t, dep.Reserve())
	require.NoError(t, dep.MarkStarted(now))
	require.NoError(t, repo.Save(context.Background(), dep))
}

// runGateInTx mirrors a production call site: inside one transaction it records
// the terminal outcome for (releaseID, nodeID) and runs the aggregate-emit gate
// (LockRelease -> PendingValidationCount -> ClaimEmission -> emit) over the
// execution_outbox. The caller decides when to commit so the test can hold one
// tx's advisory lock open while a second tx blocks on it.
func runGateInTx(t *testing.T, tx *sqlx.Tx, releaseID, nodeID string, now time.Time) error {
	t.Helper()
	logger := testLogger()
	depRepo := postgres.NewDeploymentsRepository(tx, logger)
	dep, err := depRepo.GetByReleaseNode(context.Background(), releaseID, nodeID, model.ModeValidation)
	require.NoError(t, err)
	require.NoError(t, dep.RecordOutcome("ok", "", "", "", now))
	require.NoError(t, depRepo.Save(context.Background(), dep))

	return validation.EmitValidationAggregateIfComplete(
		context.Background(),
		depRepo,
		outbox.NewPostgresRepository(tx, postgres.OutboxTable, logger),
		postgres.NewValidationAggregateRepository(tx),
		validation.DedupNamespace,
		releaseID,
		now,
	)
}

// countValidationCompletedOutbox counts the terminal (kind=complete) rows for
// releaseID on the shared validation.result:v1 stream. It filters by kind
// because the stream also carries per-node (kind=node) rows for the same
// release_id; callers that only exercise EmitValidationAggregateIfComplete
// directly (never SettleNodeTerminal's per-node emission) would still get a
// correct count without the filter, but it stays kind-scoped so this helper
// means exactly what its name says.
func countValidationCompletedOutbox(t *testing.T, db *sqlx.DB, releaseID string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(
		`SELECT count(*) FROM execution_outbox WHERE stream_name = $1 AND payload::jsonb->>'release_id' = $2 AND payload::jsonb->>'kind' = 'complete'`,
		streams.ValidationResultV1, releaseID,
	).Scan(&n))
	return n
}

// TestAggregateGate_ConcurrentLastNodes_EmitsExactlyOnce is the I1 regression
// guard. Two mode=validation nodes for one release are both deployed-without-
// outcome (PendingValidationCount == 2). Two concurrent transactions each mark
// their node terminal and run the gate — the dispatcher-vs-consumer overlap that
// previously lost the emission: under READ COMMITTED both counts saw the other
// node as still pending and both no-op'd.
//
// The per-release advisory lock taken at the top of the gate serializes the two
// transactions: tx_B blocks on LockRelease until tx_A commits, then sees
// pending==0 and the sentinel resolves it to exactly one emission. The test
// drives that interleaving deterministically — tx_A holds the lock until a
// barrier confirms tx_B is parked on it — and asserts EXACTLY ONE
// validation.result:v1 (kind=complete) row results (never zero, never two).
func TestDeploymentsRepository_ListValidationByRelease_And_BlockedPending(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	ctx := context.Background()
	now := time.Now()

	const releaseID = "rel-listval"
	root := command.ValidationDeployTask{
		ReleaseID: releaseID, NodeID: "a1", JobName: "ja1",
		ServiceName: "s", SchemaName: "sc", TableName: "a1",
		NodeType: "dbt-model", ImageTag: "t", CandidateSchema: "_candidate_rel",
	}
	child := command.ValidationDeployTask{
		ReleaseID: releaseID, NodeID: "a2", JobName: "ja2",
		ServiceName: "s", SchemaName: "sc", TableName: "a2",
		NodeType: "dbt-model", ImageTag: "t", CandidateSchema: "_candidate_rel",
		UpstreamNodeIDs: []string{"a1"},
	}
	require.NoError(t, repo.Add(ctx, model.NewValidationDeployment(root, nil, now, false)))
	require.NoError(t, repo.Add(ctx, model.NewValidationDeployment(child, nil, now, true)))

	// Blocked child counts toward "pending" so the aggregate gate does not fire early.
	n, err := repo.PendingValidationCount(ctx, releaseID, model.ModeValidation)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	rows, err := repo.ListValidationByRelease(ctx, releaseID, model.ModeValidation)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	byNode := map[string]*model.Deployment{}
	for _, d := range rows {
		byNode[d.NodeID()] = d
	}
	require.Equal(t, model.StatusPending, byNode["a1"].Status())
	require.Equal(t, model.StatusBlocked, byNode["a2"].Status())
	require.Equal(t, []string{"a1"}, byNode["a2"].ValidationCommand().UpstreamNodeIDs)
}

func validSeedBuildCmd(releaseID, nodeID string) command.ValidationDeployTask {
	return command.ValidationDeployTask{
		ReleaseID: releaseID, NodeID: nodeID,
		ServiceName: "dbt", SchemaName: "public", TableName: nodeID,
		NodeType: "dbt-seed", ImageTag: "sha-seed",
		JobName: "dbt-seed-" + nodeID, CandidateSchema: "_candidate_" + releaseID,
	}
}

func TestAdd_SeedBuildRow_RoundTrip(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewDeploymentsRepository(db, testLogger())

	now := time.Now()
	cmd := validSeedBuildCmd("rel-seed-1", "seed.orders")
	dep := model.NewSeedBuildDeployment(cmd, nil, now)
	require.NoError(t, repo.Add(context.Background(), dep))

	// Verify the raw row persisted with mode=seed_build and job_params contain the command.
	var (
		mode      string
		releaseID string
		nodeID    string
		jobParams []byte
	)
	require.NoError(t, db.QueryRow(
		`SELECT mode, release_id, node_id, job_params FROM deployments WHERE id=$1`, dep.ID(),
	).Scan(&mode, &releaseID, &nodeID, &jobParams))
	assert.Equal(t, "seed_build", mode)
	assert.Equal(t, "rel-seed-1", releaseID)
	assert.Equal(t, "seed.orders", nodeID)
	assert.Contains(t, string(jobParams), "dbt-seed-seed.orders", "seed command serialized into job_params")

	// Rehydrate via GetByReleaseNode scoped to ModeSeedBuild.
	got, err := repo.GetByReleaseNode(context.Background(), "rel-seed-1", "seed.orders", model.ModeSeedBuild)
	require.NoError(t, err)
	assert.Equal(t, model.ModeSeedBuild, got.Mode())
	assert.Equal(t, cmd, got.ValidationCommand(), "command round-trips intact")
	assert.Equal(t, "rel-seed-1", got.ReleaseID())
	assert.Equal(t, "seed.orders", got.NodeID())
	assert.Equal(t, model.StatusPending, got.Status())

	// A ModeValidation lookup for the same (release, node) must miss — the legs
	// share release_id but GetByReleaseNode is mode-scoped.
	_, err = repo.GetByReleaseNode(context.Background(), "rel-seed-1", "seed.orders", model.ModeValidation)
	assert.ErrorIs(t, err, sql.ErrNoRows, "mode scoping isolates the seed-build row from a validation lookup")
}

// TestCrossModeIsolation_SameReleaseID is the B12 mode-scoping regression guard:
// a single release_id carries both a seed-build leg and a validation leg
// (sequential phases). The per-mode pending-count, results, and aggregate-emit
// sentinel must treat the two legs independently — a seed-build claim must not
// block the later validation claim, and neither leg's rows must leak into the
// other's count/results.
func TestCrossModeIsolation_SameReleaseID(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	aggRepo := postgres.NewValidationAggregateRepository(db)
	ctx := context.Background()
	now := time.Now()
	const releaseID = "rel-both"

	// One terminal-ok seed-build row.
	seed := model.NewSeedBuildDeployment(validSeedBuildCmd(releaseID, "seed.fx"), nil, now)
	require.NoError(t, repo.Add(ctx, seed))
	require.NoError(t, seed.Reserve())
	require.NoError(t, seed.MarkStarted(now))
	require.NoError(t, seed.RecordOutcome("ok", "", "", "", now))
	require.NoError(t, repo.Save(ctx, seed))

	// One still-pending validation row for the SAME release.
	val := model.NewValidationDeployment(validValidationCmd(releaseID, "model.x"), nil, now, false)
	require.NoError(t, repo.Add(ctx, val))

	// Per-mode pending counts: seed-build has 0 (its row is terminal), validation has 1.
	seedPending, err := repo.PendingValidationCount(ctx, releaseID, model.ModeSeedBuild)
	require.NoError(t, err)
	assert.Equal(t, 0, seedPending, "seed-build leg fully settled")
	valPending, err := repo.PendingValidationCount(ctx, releaseID, model.ModeValidation)
	require.NoError(t, err)
	assert.Equal(t, 1, valPending, "validation leg still pending, unaffected by seed-build settle")

	// Per-mode results lists do not cross-contaminate.
	seedResults, err := repo.ListValidationResults(ctx, releaseID, model.ModeSeedBuild)
	require.NoError(t, err)
	require.Len(t, seedResults, 1)
	assert.Equal(t, "seed.fx", seedResults[0].NodeID())
	valResults, err := repo.ListValidationResults(ctx, releaseID, model.ModeValidation)
	require.NoError(t, err)
	assert.Empty(t, valResults, "validation row has no outcome yet")

	// The aggregate-emit sentinel is keyed on (release, mode): the seed-build
	// claim must NOT block the later validation claim for the same release.
	wonSeed, err := aggRepo.ClaimEmission(ctx, releaseID, model.ModeSeedBuild, now)
	require.NoError(t, err)
	assert.True(t, wonSeed, "seed-build leg claims its emission")
	wonVal, err := aggRepo.ClaimEmission(ctx, releaseID, model.ModeValidation, now)
	require.NoError(t, err)
	assert.True(t, wonVal, "validation leg claims independently despite same release_id")

	// Re-claiming the same (release, mode) loses, as before.
	wonSeed2, err := aggRepo.ClaimEmission(ctx, releaseID, model.ModeSeedBuild, now)
	require.NoError(t, err)
	assert.False(t, wonSeed2, "second seed-build claim loses on (release, mode) conflict")
}

func countSeedBuildCompletedOutbox(t *testing.T, db *sqlx.DB, releaseID string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(
		`SELECT count(*) FROM execution_outbox WHERE stream_name = $1 AND payload::jsonb->>'release_id' = $2`,
		streams.SeedBuildCompletedV1, releaseID,
	).Scan(&n))
	return n
}

// TestSeedBuildAggregateGate_EmitsCompletion drives the seed-build settle path
// end-to-end against Postgres: a single terminal seed row -> the gate emits
// exactly one seed.build.completed:v1 row with status=ok.
func TestSeedBuildAggregateGate_EmitsCompletion(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	now := time.Now()
	const releaseID = "rel-seedgate"
	logger := testLogger()

	repo := postgres.NewDeploymentsRepository(db, logger)
	seed := model.NewSeedBuildDeployment(validSeedBuildCmd(releaseID, "seed.fx"), nil, now)
	require.NoError(t, repo.Add(ctx, seed))
	require.NoError(t, seed.Reserve())
	require.NoError(t, seed.MarkStarted(now))
	require.NoError(t, repo.Save(ctx, seed))

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	txRepo := postgres.NewDeploymentsRepository(tx, logger)
	dep, err := txRepo.GetByReleaseNode(ctx, releaseID, "seed.fx", model.ModeSeedBuild)
	require.NoError(t, err)
	require.NoError(t, dep.RecordOutcome("ok", "", "", "", now))
	require.NoError(t, txRepo.Save(ctx, dep))
	require.NoError(t, validation.SettleSeedBuildNodeTerminal(
		ctx, txRepo,
		outbox.NewPostgresRepository(tx, postgres.OutboxTable, logger),
		postgres.NewValidationAggregateRepository(tx),
		releaseID, "seed.fx", "ok", now,
	))
	require.NoError(t, tx.Commit())

	assert.Equal(t, 1, countSeedBuildCompletedOutbox(t, db, releaseID),
		"exactly one seed.build.completed:v1 row")
}

func TestAggregateGate_ConcurrentLastNodes_EmitsExactlyOnce(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	now := time.Now()
	const releaseID = "rel-race"

	seedDeployedValidationNode(t, db, releaseID, "n1", now)
	seedDeployedValidationNode(t, db, releaseID, "n2", now)

	txA, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	txB, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)

	// tx_A records n1's outcome and runs the gate, taking the advisory lock. It
	// sees n2 as still pending (n2's outcome is uncommitted in tx_B), so it does
	// NOT emit yet. It keeps the lock until commit.
	require.NoError(t, runGateInTx(t, txA, releaseID, "n1", now))

	// tx_B runs the gate in a goroutine; LockRelease must block on tx_A's lock.
	bDone := make(chan error, 1)
	go func() { bDone <- runGateInTx(t, txB, releaseID, "n2", now) }()

	// Confirm tx_B is genuinely parked on the lock (not racing ahead): it must
	// NOT finish while tx_A still holds the advisory lock.
	select {
	case err := <-bDone:
		t.Fatalf("tx_B gate returned before tx_A committed — advisory lock did not serialize (err=%v)", err)
	case <-time.After(500 * time.Millisecond):
		// blocked as required
	}

	// Releasing tx_A unblocks tx_B; it now sees both nodes terminal (n1 committed,
	// n2 its own) and wins the sentinel, emitting exactly once.
	require.NoError(t, txA.Commit())

	select {
	case err := <-bDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("tx_B gate did not complete after tx_A committed")
	}
	require.NoError(t, txB.Commit())

	assert.Equal(t, 1, countValidationCompletedOutbox(t, db, releaseID),
		"exactly one validation terminal (kind=complete) row — never zero (lost) and never two (double)")
}

// TestMigration_ExecutorDeploymentsHasFailedContainerColumn asserts flyway
// migration V21 ran and created the failed_container column on
// deployments (the table the mode CHECK constraint targets).
func TestMigration_ExecutorDeploymentsHasFailedContainerColumn(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	var dataType string
	err := db.QueryRow(
		`SELECT data_type FROM information_schema.columns
		 WHERE table_name = 'deployments' AND column_name = 'failed_container'`,
	).Scan(&dataType)
	require.NoError(t, err, "V21 must add deployments.failed_container")
	assert.Equal(t, "text", dataType)
}

func validCompileCmd(releaseID, nodeID string) command.ValidationDeployTask {
	return command.ValidationDeployTask{
		ReleaseID: releaseID, NodeID: nodeID,
		ServiceName: "dbt", ImageTag: "sha-compile",
		JobName: "compile-" + nodeID, ManifestS3URI: "s3://bucket/" + releaseID + "/manifest.json",
	}
}

// TestAdd_CompileRow_FailedContainer_RoundTrip verifies failed_container
// persists through Save and rehydrates via GetByReleaseNode: absent by
// default, and carrying the failing container's name once RecordOutcome
// records one.
func TestAdd_CompileRow_FailedContainer_RoundTrip(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	repo := postgres.NewDeploymentsRepository(db, testLogger())
	ctx := context.Background()
	now := time.Now()

	dep := model.NewCompileDeployment(validCompileCmd("rel-fc", "compile.svc"), nil, now)
	require.NoError(t, repo.Add(ctx, dep))
	require.NoError(t, dep.Reserve())
	require.NoError(t, dep.MarkStarted(now))
	require.NoError(t, repo.Save(ctx, dep))

	// Before RecordOutcome, failed_container reads back empty.
	pending, err := repo.GetByReleaseNode(ctx, "rel-fc", "compile.svc", model.ModeCompile)
	require.NoError(t, err)
	assert.Equal(t, "", pending.FailedContainer(), "no outcome recorded yet")

	require.NoError(t, dep.RecordOutcome("failed", "s3://logs/compile.svc", "", "parse-prod", now))
	require.NoError(t, repo.Save(ctx, dep))

	got, err := repo.GetByReleaseNode(ctx, "rel-fc", "compile.svc", model.ModeCompile)
	require.NoError(t, err)
	assert.Equal(t, "failed", got.Outcome())
	assert.Equal(t, "parse-prod", got.FailedContainer(), "failed_container round-trips through Save/GetByReleaseNode")

	// Raw column check: confirms the value actually landed in the new column,
	// not just in job_params.
	var raw sql.NullString
	require.NoError(t, db.QueryRow(
		`SELECT failed_container FROM deployments WHERE id=$1`, dep.ID(),
	).Scan(&raw))
	require.True(t, raw.Valid)
	assert.Equal(t, "parse-prod", raw.String)
}
