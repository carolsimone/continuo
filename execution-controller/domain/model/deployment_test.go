package model_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func deployableCmd() command.DeployTask {
	return command.DeployTask{
		TaskID: uuid.New().String(), ScheduleID: uuid.New().String(),
		JobName: "dbt-public-orders", NodeType: "dbt-model",
		TaskRetryCount: 0, TaskMaxRetries: 2,
	}
}

func TestNewDeployment_Defaults(t *testing.T) {
	now := time.Now()
	d := model.NewDeployment(deployableCmd(), nil, now)

	assert.NotEqual(t, uuid.Nil, d.ID())
	assert.Equal(t, model.StatusPending, d.Status())
	assert.Equal(t, 0, d.RetryCount())
	assert.Equal(t, 3, d.MaxRetries(), "default deploy-attempt budget")
	assert.Equal(t, now, d.NextAttemptAt(), "due immediately")
	assert.Nil(t, d.DeployedAt())
	assert.True(t, d.IsDeployable())
}

func TestIsDeployable_FalseWhenIdentityOnly(t *testing.T) {
	// Mimics a corrupt row recovered with only task/schedule identity.
	cmd := command.DeployTask{TaskID: uuid.New().String(), ScheduleID: uuid.New().String()}
	d := model.NewDeployment(cmd, nil, time.Now())
	assert.False(t, d.IsDeployable(), "missing JobName/NodeType => not deployable")
}

func TestMarkStarted_OnlyFromReserved(t *testing.T) {
	now := time.Now()
	d := model.NewDeployment(deployableCmd(), nil, now)

	assert.Error(t, d.MarkStarted(now), "a pending deployment holds no slot to start in")
	require.NoError(t, d.Reserve())
	require.NoError(t, d.MarkStarted(now))
	assert.Equal(t, model.StatusStarting, d.Status())
	require.NotNil(t, d.DeployedAt())
	assert.Equal(t, now, *d.DeployedAt())

	// A second transition is rejected.
	assert.Error(t, d.MarkStarted(now))
}

func TestRegisterFailure_ReschedulesWhileBudgetRemains(t *testing.T) {
	now := time.Now()
	d := model.NewDeployment(deployableCmd(), nil, now) // maxRetries 3, retryCount 0
	require.NoError(t, d.Reserve())
	backoff := model.BackoffPolicy{Base: 5 * time.Second, Cap: 2 * time.Minute}

	terminal, err := d.RegisterFailure(now, false, "apiserver down", backoff)
	require.NoError(t, err)
	assert.False(t, terminal, "0+1 < 3 => retry")
	assert.Equal(t, model.StatusPending, d.Status())
	assert.Equal(t, 1, d.RetryCount())
	assert.Equal(t, now.Add(5*time.Second), d.NextAttemptAt(), "first backoff = base<<0")
	require.NotNil(t, d.ErrorMessage())
	assert.Equal(t, "apiserver down", *d.ErrorMessage())
}

func TestRegisterFailure_FailsWhenBudgetExhausted(t *testing.T) {
	now := time.Now()
	// retryCount 2, maxRetries 3 => 2+1 == 3, exhausted.
	d := model.Reconstitute(uuid.New(), nil, deployableCmd(), model.StatusReserved, 2, 3, now, now, nil, nil)
	backoff := model.BackoffPolicy{Base: 5 * time.Second, Cap: 2 * time.Minute}

	terminal, err := d.RegisterFailure(now, false, "boom", backoff)
	require.NoError(t, err)
	assert.True(t, terminal)
	assert.Equal(t, model.StatusFailed, d.Status())
	assert.Equal(t, 2, d.RetryCount(), "no further retry once exhausted")
}

func TestRegisterFailure_PermanentFailsImmediately(t *testing.T) {
	now := time.Now()
	d := model.NewDeployment(deployableCmd(), nil, now) // budget remains
	require.NoError(t, d.Reserve())
	backoff := model.BackoffPolicy{Base: 5 * time.Second, Cap: 2 * time.Minute}

	terminal, err := d.RegisterFailure(now, true, "invalid node type", backoff)
	require.NoError(t, err)
	assert.True(t, terminal, "permanent error skips the retry budget")
	assert.Equal(t, model.StatusFailed, d.Status())
}

func validationCmd() command.ValidationDeployTask {
	return command.ValidationDeployTask{
		ReleaseID: uuid.New().String(), NodeID: uuid.New().String(),
		JobName: "dbt-public-orders", NodeType: "dbt-model",
		ImageTag: "sha-abc123", CandidateSchema: "candidate_orders",
	}
}

func TestNewValidationDeployment_Defaults(t *testing.T) {
	now := time.Now()
	cmd := validationCmd()
	d := model.NewValidationDeployment(cmd, nil, now, false)

	assert.NotEqual(t, uuid.Nil, d.ID())
	assert.Equal(t, model.ModeValidation, d.Mode())
	assert.Equal(t, cmd.ReleaseID, d.ReleaseID())
	assert.Equal(t, cmd.NodeID, d.NodeID())
	assert.Equal(t, model.StatusPending, d.Status())
	assert.Equal(t, 3, d.MaxRetries(), "default deploy-attempt budget")
	assert.Equal(t, now, d.NextAttemptAt(), "due immediately")
	assert.Equal(t, "", d.Outcome(), "no outcome until recorded")
	assert.Nil(t, d.OutcomeAt())
	assert.True(t, d.IsDeployable())
}

func TestNewDeployment_ModeProduction(t *testing.T) {
	d := model.NewDeployment(deployableCmd(), nil, time.Now())
	assert.Equal(t, model.ModeProduction, d.Mode(), "production constructor sets mode explicitly")
}

func TestValidationIsDeployable_FalseWhenIncomplete(t *testing.T) {
	cmd := command.ValidationDeployTask{ReleaseID: uuid.New().String(), NodeID: uuid.New().String()}
	d := model.NewValidationDeployment(cmd, nil, time.Now(), false)
	assert.False(t, d.IsDeployable(), "missing JobName/NodeType/ImageTag => not deployable")
}

func TestRecordOutcome_OnlyOnceStarted(t *testing.T) {
	now := time.Now()
	d := model.NewValidationDeployment(validationCmd(), nil, now, false)

	// pending => rejected
	assert.Error(t, d.RecordOutcome("ok", "s3://logs/run", "", "", now))

	require.NoError(t, d.Reserve())
	require.NoError(t, d.MarkStarted(now))
	require.NoError(t, d.RecordOutcome("ok", "s3://logs/run", "", "", now))
	assert.Equal(t, "ok", d.Outcome())
	assert.Equal(t, "s3://logs/run", d.DBTLogURI())
	require.NotNil(t, d.OutcomeAt())
	assert.Equal(t, now, *d.OutcomeAt())
}

func TestRecordOutcome_AcceptsFailed(t *testing.T) {
	now := time.Now()
	d := model.NewValidationDeployment(validationCmd(), nil, now, false)
	require.NoError(t, d.Reserve())
	require.NoError(t, d.MarkStarted(now))
	require.NoError(t, d.RecordOutcome("failed", "s3://logs/run", "", "", now))
	assert.Equal(t, "failed", d.Outcome())
}

func TestRecordOutcome_StoresRunResultsURI(t *testing.T) {
	now := time.Now()
	d := model.NewValidationDeployment(validationCmd(), nil, now, false)
	require.NoError(t, d.Reserve())
	require.NoError(t, d.MarkStarted(now))
	require.NoError(t, d.RecordOutcome("failed", "s3://logs/run", "run-results/x.json", "", now))
	assert.Equal(t, "run-results/x.json", d.DBTRunResultsURI())
}

// TestRecordOutcome_StoresFailedContainer pins the compile-leg failure
// attribution: RecordOutcome's fourth parameter is stored verbatim and read
// back via FailedContainer(). Only the compile leg ever passes a non-empty
// value (validation/seed-build always pass "").
func TestRecordOutcome_StoresFailedContainer(t *testing.T) {
	now := time.Now()
	d := model.NewCompileDeployment(compileCmd(), nil, now)
	require.NoError(t, d.Reserve())
	require.NoError(t, d.MarkStarted(now))
	require.NoError(t, d.RecordOutcome("failed", "", "", "parse-prod", now))
	assert.Equal(t, "parse-prod", d.FailedContainer())
}

// TestRecordOutcome_FailedContainerEmptyByDefault verifies a node that fails
// without container attribution (or succeeds) round-trips an empty string.
func TestRecordOutcome_FailedContainerEmptyByDefault(t *testing.T) {
	now := time.Now()
	d := model.NewValidationDeployment(validationCmd(), nil, now, false)
	require.NoError(t, d.Reserve())
	require.NoError(t, d.MarkStarted(now))
	require.NoError(t, d.RecordOutcome("ok", "s3://logs/run", "", "", now))
	assert.Equal(t, "", d.FailedContainer())
}

func TestRecordOutcome_RejectsInvalidOutcome(t *testing.T) {
	now := time.Now()
	d := model.NewValidationDeployment(validationCmd(), nil, now, false)
	require.NoError(t, d.Reserve())
	require.NoError(t, d.MarkStarted(now))
	assert.Error(t, d.RecordOutcome("maybe", "s3://logs/run", "", "", now))
	assert.Equal(t, "", d.Outcome(), "rejected outcome is not stored")
	assert.Nil(t, d.OutcomeAt())
}

func TestRecordOutcome_RejectsSecondRecording(t *testing.T) {
	now := time.Now()
	d := model.NewValidationDeployment(validationCmd(), nil, now, false)
	require.NoError(t, d.Reserve())
	require.NoError(t, d.MarkStarted(now))
	require.NoError(t, d.RecordOutcome("ok", "s3://logs/first", "", "", now))

	// A second recording is rejected and leaves the first outcome intact.
	later := now.Add(time.Minute)
	assert.Error(t, d.RecordOutcome("failed", "s3://logs/second", "", "", later), "outcome recorded once")
	assert.Equal(t, "ok", d.Outcome(), "first outcome unchanged")
	assert.Equal(t, "s3://logs/first", d.DBTLogURI(), "first logURI unchanged")
	require.NotNil(t, d.OutcomeAt())
	assert.Equal(t, now, *d.OutcomeAt(), "first timestamp unchanged")
}

func TestRecordOutcome_RejectsOnProductionDeployment(t *testing.T) {
	now := time.Now()
	d := model.NewDeployment(deployableCmd(), nil, now)
	require.NoError(t, d.Reserve())
	require.NoError(t, d.MarkStarted(now))
	assert.Error(t, d.RecordOutcome("ok", "s3://logs/run", "", "", now), "RecordOutcome is validation-only")
}

func TestReconstituteValidation_RestoresOutcome(t *testing.T) {
	now := time.Now()
	cmd := validationCmd()
	ts := now
	d := model.ReconstituteValidation(
		uuid.New(), nil, cmd, model.StatusDone, 0, 3, now, now, &now, nil,
		"ok", "s3://logs/run", "run-results/run.json", "", &ts,
	)
	assert.Equal(t, model.ModeValidation, d.Mode())
	assert.Equal(t, cmd.ReleaseID, d.ReleaseID())
	assert.Equal(t, cmd.NodeID, d.NodeID())
	assert.Equal(t, "ok", d.Outcome())
	assert.Equal(t, "s3://logs/run", d.DBTLogURI())
	assert.Equal(t, "run-results/run.json", d.DBTRunResultsURI())
	require.NotNil(t, d.OutcomeAt())
	assert.Equal(t, cmd, d.ValidationCommand())
}

func TestFailValidation_FromPendingSetsTerminalFailedOutcome(t *testing.T) {
	now := time.Now()
	d := model.NewValidationDeployment(validationCmd(), nil, now, false)

	// Reserved but never started — RecordOutcome would reject this.
	require.NoError(t, d.Reserve())
	require.NoError(t, d.FailValidation("not deployable", now))
	assert.Equal(t, model.StatusFailed, d.Status())
	assert.Equal(t, "failed", d.Outcome())
	require.NotNil(t, d.OutcomeAt())
	assert.Equal(t, now, *d.OutcomeAt())
	require.NotNil(t, d.ErrorMessage())
	assert.Equal(t, "not deployable", *d.ErrorMessage())
}

func TestFailValidation_RejectsSecondRecording(t *testing.T) {
	now := time.Now()
	d := model.NewValidationDeployment(validationCmd(), nil, now, false)
	require.NoError(t, d.Reserve())
	require.NoError(t, d.FailValidation("first", now))
	assert.Error(t, d.FailValidation("second", now.Add(time.Minute)), "outcome recorded once")
	assert.Equal(t, "first", *d.ErrorMessage(), "first reason unchanged")
}

func TestFailValidation_RejectsOnProductionDeployment(t *testing.T) {
	d := model.NewDeployment(deployableCmd(), nil, time.Now())
	assert.Error(t, d.FailValidation("x", time.Now()), "FailValidation is validation-only")
}

func TestNewValidationDeployment_InitialState(t *testing.T) {
	cmd := command.ValidationDeployTask{ReleaseID: "r", NodeID: "n", JobName: "j", NodeType: "dbt-model", ImageTag: "t"}
	now := time.Now()
	blocked := model.NewValidationDeployment(cmd, nil, now, true)
	assert.Equal(t, model.StatusBlocked, blocked.Status(), "hasUpstreams=true => blocked")
	root := model.NewValidationDeployment(cmd, nil, now, false)
	assert.Equal(t, model.StatusPending, root.Status(), "hasUpstreams=false => pending")
}

func TestDeployment_Unblock(t *testing.T) {
	cmd := command.ValidationDeployTask{ReleaseID: "r", NodeID: "n", JobName: "j", NodeType: "dbt-model", ImageTag: "t"}
	now := time.Now()
	d := model.NewValidationDeployment(cmd, nil, now, true)
	require.NoError(t, d.Unblock(now))
	assert.Equal(t, model.StatusPending, d.Status(), "after Unblock => pending")
	assert.Error(t, d.Unblock(now), "Unblock from pending should error")
}

func TestDeployment_Skip(t *testing.T) {
	cmd := command.ValidationDeployTask{ReleaseID: "r", NodeID: "n", JobName: "j", NodeType: "dbt-model", ImageTag: "t"}
	now := time.Now()
	d := model.NewValidationDeployment(cmd, nil, now, true)
	require.NoError(t, d.Skip("upstream a1 failed", now))
	assert.Equal(t, model.StatusSkipped, d.Status())
	assert.Equal(t, "skipped", d.Outcome())
	require.NotNil(t, d.OutcomeAt())
	assert.Error(t, d.Skip("x", now), "Skip from skipped should error")
}

func TestNewSeedBuildDeployment_IsPendingValidationMode(t *testing.T) {
	t0 := time.Now()
	cmd := command.ValidationDeployTask{ReleaseID: "r", NodeID: "seed.a", ServiceName: "s",
		SchemaName: "sc", TableName: "a", NodeType: "dbt-seed", ImageTag: "t", JobName: "j",
		CandidateSchema: "_candidate_r"}
	dep := model.NewSeedBuildDeployment(cmd, nil, t0)
	assert.Equal(t, model.ModeSeedBuild, dep.Mode())
	assert.Equal(t, model.StatusPending, dep.Status())
	assert.Equal(t, "seed.a", dep.NodeID())
}

func TestReconstituteSeedBuild_RestoresMode(t *testing.T) {
	now := time.Now()
	cmd := command.ValidationDeployTask{ReleaseID: "r2", NodeID: "seed.b", JobName: "j2",
		NodeType: "dbt-seed", ImageTag: "t2", CandidateSchema: "_candidate_r2"}
	ts := now
	d := model.ReconstituteSeedBuild(
		uuid.New(), nil, cmd, model.StatusDone, 0, 3, now, now, &now, nil,
		"ok", "s3://logs/seed", "run-results/seed.json", "", &ts,
	)
	assert.Equal(t, model.ModeSeedBuild, d.Mode())
	assert.Equal(t, cmd, d.ValidationCommand())
	assert.Equal(t, "r2", d.ReleaseID())
	assert.Equal(t, "seed.b", d.NodeID())
	assert.Equal(t, "ok", d.Outcome())
}

func TestBackoff_CapAndOverflow(t *testing.T) {
	now := time.Now()
	backoff := model.BackoffPolicy{Base: 5 * time.Second, Cap: 30 * time.Second}
	// retryCount climbs; delay must never exceed Cap and never go negative.
	d := model.Reconstitute(uuid.New(), nil, deployableCmd(), model.StatusPending, 0, 100, now, now, nil, nil)
	prev := now
	for i := 0; i < 70; i++ {
		require.NoError(t, d.Reserve())
		_, err := d.RegisterFailure(now, false, "x", backoff)
		require.NoError(t, err)
		gap := d.NextAttemptAt().Sub(now)
		assert.LessOrEqual(t, gap, 30*time.Second, "delay capped")
		assert.GreaterOrEqual(t, gap, time.Duration(0), "delay never negative (overflow guard)")
		_ = prev
	}
}

func productionDeployment(t *testing.T) *model.Deployment {
	t.Helper()
	return model.NewDeployment(command.DeployTask{
		TaskID: "11111111-1111-1111-1111-111111111111", ScheduleID: "22222222-2222-2222-2222-222222222222",
		JobName: "dbt-public-orders", NodeType: "dbt-model",
	}, nil, time.Now())
}

func TestDeployment_SlotLifecycle(t *testing.T) {
	now := time.Now()
	d := productionDeployment(t)

	require.Error(t, d.MarkStarted(now), "a pending deployment holds no slot to start in")
	require.NoError(t, d.Reserve())
	assert.Equal(t, model.StatusReserved, d.Status())
	require.Error(t, d.Reserve(), "reserve is only from pending")

	require.NoError(t, d.MarkStarted(now))
	assert.Equal(t, model.StatusStarting, d.Status())
	require.NotNil(t, d.DeployedAt(), "deployed_at records when the Job was created")

	changed, err := d.MarkRunning()
	require.NoError(t, err)
	assert.True(t, changed)
	changed, err = d.MarkRunning()
	require.NoError(t, err)
	assert.False(t, changed, "a second running observation changes nothing")

	require.NoError(t, d.Finish("ok", now))
	assert.Equal(t, model.StatusDone, d.Status())
	assert.True(t, d.HasOutcome())
	assert.Equal(t, "ok", d.Outcome())
	require.Error(t, d.Finish("failed", now), "an outcome is recorded once")
}

func TestDeployment_FinishAcceptsADoneRowWithoutOutcome(t *testing.T) {
	now := time.Now()
	d := productionDeployment(t)
	require.NoError(t, d.Reserve())
	require.NoError(t, d.MarkStarted(now))
	require.NoError(t, d.ReleaseSlot())
	assert.Equal(t, model.StatusDone, d.Status())
	assert.False(t, d.HasOutcome())

	require.NoError(t, d.Finish("failed", now), "a status check that arrives after reconciliation released the slot still records the outcome")
	assert.Equal(t, "failed", d.Outcome())
}

func TestDeployment_FinishRejections(t *testing.T) {
	now := time.Now()
	d := productionDeployment(t)
	require.Error(t, d.Finish("ok", now), "a pending deployment has no Job to finish")
	require.NoError(t, d.Reserve())
	require.NoError(t, d.MarkStarted(now))
	require.Error(t, d.Finish("skipped", now), "only ok and failed")

	v := model.NewCompileDeployment(command.ValidationDeployTask{ReleaseID: "r", NodeID: "n", JobName: "j", ImageTag: "i"}, nil, now)
	require.NoError(t, v.Reserve())
	require.NoError(t, v.MarkStarted(now))
	require.Error(t, v.Finish("ok", now), "candidate deployments record through RecordOutcome")
}

func TestDeployment_ReleaseSlotOnlyFromStartedStates(t *testing.T) {
	d := productionDeployment(t)
	require.Error(t, d.ReleaseSlot())
	require.NoError(t, d.Reserve())
	require.Error(t, d.ReleaseSlot(), "a reserved row is launched, not released")
}

func TestDeployment_TransientFailureReturnsTheSlot(t *testing.T) {
	now := time.Now()
	d := productionDeployment(t)
	_, err := d.RegisterFailure(now, false, "api timeout", model.BackoffPolicy{Base: time.Second, Cap: time.Minute})
	require.Error(t, err, "only a reserved deployment attempts a deploy")
	require.NoError(t, d.Reserve())
	terminal, err := d.RegisterFailure(now, false, "api timeout", model.BackoffPolicy{Base: time.Second, Cap: time.Minute})
	require.NoError(t, err)
	assert.False(t, terminal)
	assert.Equal(t, model.StatusPending, d.Status(), "a transient failure gives the slot back and waits out the backoff")
	assert.True(t, d.NextAttemptAt().After(now))
}

func TestDeployment_RecordOutcomeFromInFlightOrReleased(t *testing.T) {
	now := time.Now()
	v := model.NewValidationDeployment(command.ValidationDeployTask{
		ReleaseID: "r", NodeID: "n", JobName: "j", NodeType: "dbt-model", ImageTag: "i",
	}, nil, now, false)
	require.Error(t, v.RecordOutcome("ok", "", "", "", now), "pending has no Job")
	require.NoError(t, v.Reserve())
	require.NoError(t, v.MarkStarted(now))
	require.NoError(t, v.RecordOutcome("ok", "log", "rr", "", now))
	assert.Equal(t, model.StatusDone, v.Status())
	assert.True(t, v.HasOutcome())
}

func TestDeployment_JobName(t *testing.T) {
	assert.Equal(t, "dbt-public-orders", productionDeployment(t).JobName())
	v := model.NewSeedBuildDeployment(command.ValidationDeployTask{ReleaseID: "r", NodeID: "n", JobName: "seed-j"}, nil, time.Now())
	assert.Equal(t, "seed-j", v.JobName())
}
