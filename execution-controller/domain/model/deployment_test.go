package model_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func deployableCmd() command.DeployTask {
	return command.DeployTask{
		TaskID: uuid.New().String(), ScheduleID: uuid.New().String(),
		JobName: "dbt-public-orders", NodeType: "dbt-model",
		Attempt: 0,
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

// A transient failure leaves another try only while the budget has one.
func TestRetriesLeft_FollowsTheBudget(t *testing.T) {
	now := time.Now()
	for retryCount, want := range map[int]bool{0: true, 1: true, 2: false} {
		d := model.Reconstitute(uuid.New(), nil, deployableCmd(), model.StatusReserved, retryCount, 3, now, now, nil, nil)
		assert.Equal(t, want, d.RetriesLeft(), "retry_count %d of a budget of 3", retryCount)
	}
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
		"ok", "s3://logs/run", "run-results/run.json", "", &ts, model.AttemptFacts{},
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
		"ok", "s3://logs/seed", "run-results/seed.json", "", &ts, model.AttemptFacts{},
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

func TestDeployment_RecordOutcomeFromInFlight(t *testing.T) {
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

// Execution-controller ends an attempt itself by recording why and moving the
// deployment to terminating, which keeps its slot until the pods are gone.
func TestDeployment_BeginTerminationHoldsTheSlotAndKeepsTheFirstReason(t *testing.T) {
	now := time.Now()
	d := productionDeployment(t)
	require.Error(t, d.BeginTermination(pkgmodel.FailureReasonStartTimeout, "not started"), "a pending deployment has no Job to end")
	require.NoError(t, d.Reserve())
	require.NoError(t, d.MarkStarted(now))

	require.NoError(t, d.BeginTermination(pkgmodel.FailureReasonStartTimeout, "not started after 15m0s"))
	assert.Equal(t, model.StatusTerminating, d.Status())
	assert.True(t, d.Status().InFlight(), "a terminating deployment holds its slot")
	assert.Equal(t, pkgmodel.FailureReasonStartTimeout, d.Facts().Reason)
	assert.Equal(t, "not started after 15m0s", d.Facts().Message)

	require.NoError(t, d.BeginTermination(pkgmodel.FailureReasonImagePull, "pull failed"), "ending it again is a no-op")
	assert.Equal(t, pkgmodel.FailureReasonStartTimeout, d.Facts().Reason, "the first reason stands")

	_, err := d.MarkRunning()
	require.Error(t, err, "a terminating attempt never runs again")
	require.NoError(t, d.Finish("failed", now))
	assert.Equal(t, model.StatusDone, d.Status())
}

func TestDeployment_ObserveAddsFacts(t *testing.T) {
	d := productionDeployment(t)
	start := time.Now().UTC()
	assert.True(t, d.Observe(model.AttemptFacts{StartedAt: &start}))
	assert.False(t, d.Observe(model.AttemptFacts{StartedAt: &start}), "the same fact again changes nothing")
	assert.Equal(t, &start, d.Facts().StartedAt)
}

func TestDeployment_CandidateRecordsItsOutcomeFromTerminating(t *testing.T) {
	now := time.Now()
	v := model.NewCompileDeployment(command.ValidationDeployTask{ReleaseID: "r", NodeID: "n", JobName: "j", ImageTag: "i"}, nil, now)
	require.NoError(t, v.Reserve())
	require.NoError(t, v.MarkStarted(now))
	require.NoError(t, v.BeginTermination(pkgmodel.FailureReasonImagePull, "ErrImagePull"))
	require.NoError(t, v.RecordOutcome("failed", "", "", "", now))
	assert.Equal(t, model.StatusDone, v.Status())
}

func TestDeployment_AwaitingResult(t *testing.T) {
	now := time.Now()
	d := productionDeployment(t)
	assert.False(t, d.AwaitingResult(), "pending: no attempt started")
	require.NoError(t, d.Reserve())
	assert.False(t, d.AwaitingResult(), "reserved: no Job yet")
	require.NoError(t, d.MarkStarted(now))
	assert.True(t, d.AwaitingResult(), "starting")
	require.NoError(t, d.BeginTermination(pkgmodel.FailureReasonImagePull, ""))
	assert.True(t, d.AwaitingResult(), "terminating")
	require.NoError(t, d.Finish("failed", now))
	assert.False(t, d.AwaitingResult(), "done with its outcome recorded")
}

// A row waiting for its launch, pending or reserved, records no Job, yet one
// of its attempt may exist (a launch that created it was lost before its
// commit), so Stop refuses it: StopBeforeLaunch stops it from a look for that
// Job.
func TestDeployment_StopRefusesARowWaitingForItsLaunch(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	for _, reserve := range []bool{false, true} {
		d := productionDeployment(t)
		if reserve {
			require.NoError(t, d.Reserve())
		}
		want := d.Status()
		assert.True(t, d.AwaitsLaunch())

		_, err := d.Stop(now)

		require.Error(t, err, "reserved=%v", reserve)
		assert.Equal(t, want, d.Status())
		assert.Nil(t, d.Facts().StopRequestedAt)
	}
}

// A row waiting for its launch whose attempt has neither a Job nor a pod
// left ends at once: the attempt never ran, so it is done with the outcome
// stopped and holds no slot.
func TestDeployment_StopBeforeLaunchEndsARowWithNoJob(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	for _, reserve := range []bool{false, true} {
		d := productionDeployment(t)
		if reserve {
			require.NoError(t, d.Reserve())
		}

		effect, err := d.StopBeforeLaunch(now, model.StoreStopped)

		require.NoError(t, err)
		assert.Equal(t, model.StopEnded, effect, "reserved=%v", reserve)
		assert.Equal(t, model.StatusDone, d.Status())
		assert.Equal(t, "stopped", d.Outcome())
		assert.True(t, d.HasOutcome())
		assert.Equal(t, &now, d.Facts().StopRequestedAt)
		assert.False(t, d.Status().InFlight())
		assert.Nil(t, d.DeployedAt())
	}
}

// A row waiting for its launch whose attempt's Job exists adopts the Job: it
// moves to terminating with the stop recorded and the Job recorded as
// deployed, holding a slot until the Job's pods are gone, and the Job is to
// be deleted.
func TestDeployment_StopBeforeLaunchAdoptsAnExistingJob(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	for _, reserve := range []bool{false, true} {
		d := productionDeployment(t)
		if reserve {
			require.NoError(t, d.Reserve())
		}

		effect, err := d.StopBeforeLaunch(now, model.StopAdopted)

		require.NoError(t, err)
		assert.Equal(t, model.StopTerminate, effect, "reserved=%v", reserve)
		assert.Equal(t, model.StatusTerminating, d.Status())
		assert.True(t, d.Status().InFlight(), "it holds a slot until its pods are gone")
		assert.True(t, d.AwaitingResult(), "the observer follows the Job to its end")
		assert.Equal(t, &now, d.DeployedAt())
		assert.Equal(t, &now, d.Facts().StopRequestedAt)
		assert.False(t, d.HasOutcome(), "the outcome is recorded once the pods are gone")
		assert.Empty(t, d.Facts().Reason, "a stop is not a failure reason")
	}
}

// A row waiting for its launch whose attempt has no Job but a pod that has
// not finished changes nothing: no stop can be reported while the pod may
// still run.
func TestDeployment_StopBeforeLaunchAwaitsAnUnfinishedPod(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	for _, reserve := range []bool{false, true} {
		d := productionDeployment(t)
		if reserve {
			require.NoError(t, d.Reserve())
		}
		want := d.Status()

		effect, err := d.StopBeforeLaunch(now, model.AwaitFencedPod)

		require.NoError(t, err)
		assert.Equal(t, model.StopAwaitPod, effect, "reserved=%v", reserve)
		assert.Equal(t, want, d.Status())
		assert.Nil(t, d.Facts().StopRequestedAt, "nothing is recorded")
		assert.False(t, d.HasOutcome())
	}
}

// StopBeforeLaunch stops only a task's attempt that waits for its launch.
func TestDeployment_StopBeforeLaunchRefusesAnyOtherRow(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	started := productionDeployment(t)
	require.NoError(t, started.Reserve())
	require.NoError(t, started.MarkStarted(now))
	assert.False(t, started.AwaitsLaunch())
	_, err := started.StopBeforeLaunch(now, model.StoreStopped)
	require.Error(t, err)
	assert.Equal(t, model.StatusStarting, started.Status())

	v := model.NewValidationDeployment(command.ValidationDeployTask{ReleaseID: "r", NodeID: "n", JobName: "j", ImageTag: "i"}, nil, now, false)
	_, err = v.StopBeforeLaunch(now, model.StoreStopped)
	require.Error(t, err)
	assert.Equal(t, model.StatusPending, v.Status())

	_, err = productionDeployment(t).StopBeforeLaunch(now, model.FencedAction(0))
	require.Error(t, err, "an unknown action is refused")
}

// A started row keeps its slot until its pods are gone, so the stop moves it
// to terminating and asks for its Job to be deleted.
func TestDeployment_StopMovesAStartedRowToTerminating(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	for _, running := range []bool{false, true} {
		d := productionDeployment(t)
		require.NoError(t, d.Reserve())
		require.NoError(t, d.MarkStarted(now))
		if running {
			_, err := d.MarkRunning()
			require.NoError(t, err)
		}

		effect, err := d.Stop(now.Add(time.Minute))
		require.NoError(t, err)
		assert.Equal(t, model.StopTerminate, effect, "running=%v", running)
		assert.Equal(t, model.StatusTerminating, d.Status())
		assert.True(t, d.Status().InFlight(), "it keeps its slot until its pods are gone")
		stopAt := now.Add(time.Minute)
		assert.Equal(t, &stopAt, d.Facts().StopRequestedAt)
		assert.False(t, d.HasOutcome(), "the outcome is recorded once the pods are gone")
		assert.Empty(t, d.Facts().Reason, "a stop is not a failure reason")
	}
}

// A row execution-controller already ends keeps the reason it decided; the
// stop is recorded beside it and a repeated stop changes nothing.
func TestDeployment_StopKeepsATerminatingRowsReason(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	d := productionDeployment(t)
	require.NoError(t, d.Reserve())
	require.NoError(t, d.MarkStarted(now))
	require.NoError(t, d.BeginTermination(pkgmodel.FailureReasonStartTimeout, "not started"))

	effect, err := d.Stop(now.Add(time.Minute))
	require.NoError(t, err)
	assert.Equal(t, model.StopNone, effect)
	assert.Equal(t, model.StatusTerminating, d.Status())
	assert.Equal(t, pkgmodel.FailureReasonStartTimeout, d.Facts().Reason)
	stopAt := now.Add(time.Minute)
	assert.Equal(t, &stopAt, d.Facts().StopRequestedAt)

	effect, err = d.Stop(now.Add(time.Hour))
	require.NoError(t, err)
	assert.Equal(t, model.StopNone, effect)
	assert.Equal(t, &stopAt, d.Facts().StopRequestedAt, "the first stop request stands")
}

// A row that already ended records nothing: there is nothing left to stop.
func TestDeployment_StopOfAnEndedRowChangesNothing(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	d := productionDeployment(t)
	require.NoError(t, d.Reserve())
	require.NoError(t, d.MarkStarted(now))
	require.NoError(t, d.Finish("ok", now))

	effect, err := d.Stop(now.Add(time.Minute))
	require.NoError(t, err)
	assert.Equal(t, model.StopNone, effect)
	assert.Equal(t, "ok", d.Outcome())
	assert.Nil(t, d.Facts().StopRequestedAt)
}

// Only a task's attempt is stopped; a release leg's row is not.
func TestDeployment_StopRefusesACandidateRow(t *testing.T) {
	v := model.NewValidationDeployment(command.ValidationDeployTask{ReleaseID: "r", NodeID: "n", JobName: "j", ImageTag: "i"}, nil, time.Now(), false)
	_, err := v.Stop(time.Now())
	require.Error(t, err)
	assert.Equal(t, model.StatusPending, v.Status())
}

// Finish records "stopped" as the outcome of an attempt that was stopped.
func TestDeployment_FinishAcceptsStopped(t *testing.T) {
	now := time.Now()
	d := productionDeployment(t)
	require.NoError(t, d.Reserve())
	require.NoError(t, d.MarkStarted(now))
	_, err := d.Stop(now)
	require.NoError(t, err)
	require.NoError(t, d.Finish("stopped", now))
	assert.Equal(t, "stopped", d.Outcome())
	assert.Equal(t, model.StatusDone, d.Status())
	require.Error(t, productionDeployment(t).Finish("cancelled", now), "no other outcome is accepted")
}

// The record of an attempt dispatched after its run was cancelled: it ended
// stopped before any Job existed.
func TestNewStoppedDeployment_IsDoneAndStopped(t *testing.T) {
	now := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	d := model.NewStoppedDeployment(deployableCmd(), nil, now)

	assert.Equal(t, model.StatusDone, d.Status())
	assert.Equal(t, "stopped", d.Outcome())
	require.NotNil(t, d.OutcomeAt())
	assert.Equal(t, now, *d.OutcomeAt())
	assert.Equal(t, &now, d.Facts().StopRequestedAt)
	assert.Equal(t, model.ModeProduction, d.Mode())
	assert.False(t, d.Status().InFlight())
	assert.Nil(t, d.DeployedAt(), "no Job was created")
}
