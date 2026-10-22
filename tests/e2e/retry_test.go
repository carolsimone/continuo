package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const (
	// retryScheduleName is the schedule of the fail-once fixture and nothing
	// else.
	retryScheduleName = "e2e-retry"
	// retryTable is service-1's ftable_retry: its SQL fails while the pod's
	// TASK_ATTEMPT is 0 and succeeds on every later attempt.
	retryTable = "ftable_retry"
)

// TestE2E_Retry_AFailOnceModelSucceedsOnItsSecondAttempt proves that state
// retries a task's own failure and that execution-controller runs each attempt
// as a Job of its own that knows its attempt number:
//
//	TriggerSchedule(e2e-retry) → state dispatches attempt 0 of ftable_retry
//	→ its Job runs with TASK_ATTEMPT=0, the model's cast fails, dbt exits 1
//	→ execution-controller reports attempt 0 failed (task, exit_code)
//	→ state records it and, in the same transaction, dispatches attempt 1
//	→ its Job, <attempt 0's name>-r1, runs with TASK_ATTEMPT=1 and succeeds
//	→ the task and the run succeed.
func TestE2E_Retry_AFailOnceModelSucceedsOnItsSecondAttempt(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// Topology (2m) + two dbt attempts on a cold stack (10m) + the run's
	// finalization (2m), with headroom.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)
	defer cleanupTestData(t, ctx, clients, retryScheduleName)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	cleanupTestData(t, ctx, clients, retryScheduleName)

	waitForStateLiveRelease(t, ctx, clients, seedTopology(t, ctx, clients))

	resp, err := clients.stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{ScheduleName: retryScheduleName})
	require.NoError(t, err, "TriggerSchedule(%s)", retryScheduleName)
	runID, err := uuid.Parse(resp.ScheduleId)
	require.NoError(t, err, "TriggerSchedule must return the run's id")
	t.Logf("run %s of %s triggered", runID, retryScheduleName)
	taskID := taskOfRun(t, ctx, clients, runID, retryTable)

	ended := waitForTaskEnded(t, ctx, clients, taskID, retryTable, 10*time.Minute)
	require.Equal(t, []string{
		fmt.Sprintf("0:failed:%s:%s", model.FailureClassTask, model.FailureReasonExitCode),
		"1:succeeded",
	}, ended.attempts, "attempt 0 fails on the model's own error and attempt 1 succeeds")
	require.Equal(t, "succeeded", ended.status, "the task succeeds on its second attempt")
	require.Equal(t, 1, ended.attempt, "the task's current attempt is the one that succeeded")
	verifySchedulerSucceeded(t, ctx, clients, runID)

	// Each attempt ran as a Job of its own: attempt 1's is attempt 0's name with
	// -r1, and each Job carries its attempt in its labels.
	var jobNames []string
	require.NoError(t, clients.stateDB.SelectContext(ctx, &jobNames,
		`SELECT job_name FROM task_attempts WHERE task_id = $1 ORDER BY attempt`, taskID))
	require.Len(t, jobNames, 2)
	require.Equal(t, jobNames[0]+"-r1", jobNames[1], "attempt 1's Job is named after attempt 0's")
	jobs, err := getK8sJobs(ctx, "task-id="+taskID.String())
	require.NoError(t, err, "list the task's Jobs")
	byAttempt := map[int]string{}
	for _, job := range jobs.Items {
		byAttempt[attemptLabel(job.Metadata.Labels)] = job.Metadata.Name
	}
	require.Equal(t, map[int]string{0: jobNames[0], 1: jobNames[1]}, byAttempt,
		"the task has one Job per attempt, labelled with its attempt")
	t.Logf("✅ %s failed attempt 0 (%s) and succeeded on attempt 1 (%s)", retryTable, jobNames[0], jobNames[1])
}
