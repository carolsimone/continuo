package handlers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// synthesiseDerivedRun is shared by RerunHandler and RebaseHandler. These
// tests bite on the contract — both wrappers trust the helper for eligibility
// and atomic-write semantics, and only assert their own kind/stream/event
// constants in their own tests.

func TestSynthesiseDerivedRun_HappyPath(t *testing.T) {
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	scheduleName := "synth-happy-" + uuid.New().String()[:8]
	srcID := seedTerminalRunWithFailedTask(t, fx, scheduleName, model.SchedulerStatusFailed)

	newID, gotSchedName, err := synthesiseDerivedRun(
		context.Background(),
		fx.DB, fx.SchedulerRepo, fx.TaskRepo, fx.OutboxRepo,
		newTestLogger(),
		srcID,
		derivedRunSpec{Kind: "rerun", StreamName: "trigger.rerun:v1", EventType: "rerun"},
	)
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, newID)
	require.Equal(t, scheduleName, gotSchedName)
	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), "DELETE FROM state_outbox WHERE aggregate_id = $1", newID)
		fx.DB.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", newID)
	})

	tracker, err := fx.SchedulerRepo.GetByID(context.Background(), newID)
	require.NoError(t, err)
	require.Equal(t, "rerun", tracker.Kind)
	require.NotNil(t, tracker.SourceRunID)
	require.Equal(t, srcID, *tracker.SourceRunID)
	require.Equal(t, model.SchedulerStatusPending, tracker.Status)

	var payloadRaw []byte
	require.NoError(t, fx.DB.GetContext(context.Background(), &payloadRaw,
		`SELECT payload FROM state_outbox WHERE aggregate_id = $1 AND stream_name = 'trigger.rerun:v1' LIMIT 1`,
		newID,
	))
	var payload map[string]string
	require.NoError(t, json.Unmarshal(payloadRaw, &payload))
	require.Equal(t, newID.String(), payload["schedule_id"])
	require.Equal(t, scheduleName, payload["schedule_name"])
	require.Equal(t, "rerun", payload["kind"])
	require.Equal(t, srcID.String(), payload["source_run_id"])
}

func TestSynthesiseDerivedRun_RejectsSourceNotFound(t *testing.T) {
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	_, _, err := synthesiseDerivedRun(
		context.Background(),
		fx.DB, fx.SchedulerRepo, fx.TaskRepo, fx.OutboxRepo,
		newTestLogger(),
		uuid.New(),
		derivedRunSpec{Kind: "rerun", StreamName: "trigger.rerun:v1", EventType: "rerun"},
	)
	requireGRPCCode(t, err, codes.NotFound)
}

func TestSynthesiseDerivedRun_RejectsSourceNotTerminal(t *testing.T) {
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	srcID := seedRunWithTaskStatus(t, fx, "synth-rej-running-"+uuid.New().String()[:8],
		model.SchedulerStatusRunning, model.TaskStatusRunning)
	_, _, err := synthesiseDerivedRun(
		context.Background(),
		fx.DB, fx.SchedulerRepo, fx.TaskRepo, fx.OutboxRepo,
		newTestLogger(),
		srcID,
		derivedRunSpec{Kind: "rerun", StreamName: "trigger.rerun:v1", EventType: "rerun"},
	)
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

func TestSynthesiseDerivedRun_RejectsAllSucceededTasks(t *testing.T) {
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	srcID := seedRunWithTaskStatus(t, fx, "synth-rej-allsucc-"+uuid.New().String()[:8],
		model.SchedulerStatusFailed, model.TaskStatusSucceeded)
	_, _, err := synthesiseDerivedRun(
		context.Background(),
		fx.DB, fx.SchedulerRepo, fx.TaskRepo, fx.OutboxRepo,
		newTestLogger(),
		srcID,
		derivedRunSpec{Kind: "rerun", StreamName: "trigger.rerun:v1", EventType: "rerun"},
	)
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

func TestSynthesiseDerivedRun_RejectsActiveRunOnSameSchedule(t *testing.T) {
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	scheduleName := "synth-rej-concurrent-" + uuid.New().String()[:8]
	srcID := seedRunWithTaskStatus(t, fx, scheduleName, model.SchedulerStatusFailed, model.TaskStatusFailed)
	_ = seedRunningSchedulerOnSchedule(t, fx, scheduleName)

	_, _, err := synthesiseDerivedRun(
		context.Background(),
		fx.DB, fx.SchedulerRepo, fx.TaskRepo, fx.OutboxRepo,
		newTestLogger(),
		srcID,
		derivedRunSpec{Kind: "rerun", StreamName: "trigger.rerun:v1", EventType: "rerun"},
	)
	requireGRPCCode(t, err, codes.FailedPrecondition)
}

func TestSynthesiseDerivedRun_HonoursSpecForRebaseKind(t *testing.T) {
	fx := setupRebaseFixture(t)
	defer fx.Cleanup()

	scheduleName := "synth-rebase-" + uuid.New().String()[:8]
	srcID := seedTerminalRunWithFailedTask(t, fx, scheduleName, model.SchedulerStatusFailed)

	newID, _, err := synthesiseDerivedRun(
		context.Background(),
		fx.DB, fx.SchedulerRepo, fx.TaskRepo, fx.OutboxRepo,
		newTestLogger(),
		srcID,
		derivedRunSpec{Kind: "rebase", StreamName: "trigger.rebase:v1", EventType: "rebase"},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		fx.DB.ExecContext(context.Background(), "DELETE FROM state_outbox WHERE aggregate_id = $1", newID)
		fx.DB.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", newID)
	})

	tracker, err := fx.SchedulerRepo.GetByID(context.Background(), newID)
	require.NoError(t, err)
	require.Equal(t, "rebase", tracker.Kind)

	var streamName string
	require.NoError(t, fx.DB.GetContext(context.Background(), &streamName,
		`SELECT stream_name FROM state_outbox WHERE aggregate_id = $1 LIMIT 1`, newID,
	))
	require.Equal(t, "trigger.rebase:v1", streamName)
}
