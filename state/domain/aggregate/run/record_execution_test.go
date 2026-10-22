package run_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordedAt is the dispatch time the tasks of these tests were stamped with.
var recordedAt = time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)

// heldTask stores one running task with Job name "job-x" in a fresh fake and
// returns both.
func heldTask() (*fakeTaskCollection, uuid.UUID) {
	tc := newFakeTaskCollection()
	id := uuid.New()
	dispatched := recordedAt
	tc.statuses[id] = run.TaskStatusRunning
	tc.created[id] = run.Task{TaskID: id, JobName: "job-x", DispatchedAt: &dispatched}
	return tc, id
}

// executionRecord returns a record of Job jobName of task id carrying every fact.
func executionRecord(id uuid.UUID, jobName string) run.ExecutionRecord {
	started := recordedAt.Add(time.Minute)
	completed := started.Add(30 * time.Second)
	secs := 30.0
	msg, logKey, results := "boom", "logs/job.log", "run-results/job.json"
	cache, reason := "miss", "manifest changed"
	return run.ExecutionRecord{
		TaskID: id, JobName: jobName,
		StartedAt: &started, CompletedAt: &completed, ExecutionTimeSeconds: &secs,
		ErrorMessage: &msg, LogS3Key: &logKey, RunResultsURI: &results,
		ParseCache: &cache, ParseCacheReason: &reason,
	}
}

// assertFacts checks a carries every fact rec reports.
func assertFacts(t *testing.T, rec run.ExecutionRecord, a run.Attempt) {
	t.Helper()
	assert.Equal(t, rec.StartedAt, a.StartedAt)
	assert.Equal(t, rec.CompletedAt, a.FinishedAt)
	assert.Equal(t, rec.ExecutionTimeSeconds, a.ExecutionSeconds)
	assert.Equal(t, rec.ErrorMessage, a.ErrorMessage)
	assert.Equal(t, rec.LogS3Key, a.LogS3Key)
	assert.Equal(t, rec.RunResultsURI, a.RunResultsURI)
	assert.Equal(t, rec.ParseCache, a.ParseCache)
	assert.Equal(t, rec.ParseCacheReason, a.ParseCacheReason)
}

// A record of Job "<job>-r1" fills attempt 1 with its facts.
func TestRecordTaskExecution_FillsTheAttemptItsJobNameNames(t *testing.T) {
	tc, id := heldTask()
	rec := executionRecord(id, "job-x-r1")

	require.NoError(t, run.RecordTaskExecution(context.Background(), tc, rec))

	require.Len(t, tc.saves, 1)
	a := tc.saves[0]
	assert.Equal(t, id, a.TaskID)
	assert.Equal(t, int32(1), a.Number)
	assert.Equal(t, "job-x-r1", a.JobName)
	assert.Nil(t, a.DispatchedAt, "a later attempt is stamped by the database clock")
	assertFacts(t, rec, a)
	assert.Equal(t, a, tc.attemptRows[attemptKey{id, 1}])
}

// A record that arrives before any status of its attempt creates the attempt
// running; attempt 0 takes the task's dispatch time.
func TestRecordTaskExecution_ARecordBeforeItsStatusCreatesARunningAttempt(t *testing.T) {
	tc, id := heldTask()
	rec := executionRecord(id, "job-x")

	require.NoError(t, run.RecordTaskExecution(context.Background(), tc, rec))

	a, ok := tc.attemptRows[attemptKey{id, 0}]
	require.True(t, ok)
	assert.Equal(t, run.AttemptRunning, a.Status)
	assert.Equal(t, "job-x", a.JobName)
	require.NotNil(t, a.DispatchedAt)
	assert.True(t, a.DispatchedAt.Equal(recordedAt))
	assertFacts(t, rec, a)
}

// A record that arrives after its attempt ended leaves the final status and
// the stored dispatch time as they are.
func TestRecordTaskExecution_ARecordAfterItsStatusKeepsTheFinalStatus(t *testing.T) {
	tc, id := heldTask()
	dispatched := recordedAt.Add(5 * time.Minute)
	reason := "OOMKilled"
	tc.attemptRows[attemptKey{id, 1}] = run.Attempt{
		TaskID: id, Number: 1, JobName: "job-x-r1", Status: run.AttemptFailed,
		DispatchedAt: &dispatched, FailureReason: &reason,
	}
	rec := executionRecord(id, "job-x-r1")

	require.NoError(t, run.RecordTaskExecution(context.Background(), tc, rec))

	a := tc.attemptRows[attemptKey{id, 1}]
	assert.Equal(t, run.AttemptFailed, a.Status)
	assert.Equal(t, &dispatched, a.DispatchedAt)
	assert.Equal(t, &reason, a.FailureReason)
	assertFacts(t, rec, a)
}

// A record of a task no run holds is not held, and writes nothing.
func TestRecordTaskExecution_ATaskStateDoesNotHoldIsNotHeld(t *testing.T) {
	tc := newFakeTaskCollection()

	err := run.RecordTaskExecution(context.Background(), tc, executionRecord(uuid.New(), "job-x"))

	assert.True(t, errors.Is(err, run.ErrTaskNotHeld), "got %v", err)
	assert.Empty(t, tc.saves)
}

// The task row is locked before the attempt row, as the status transaction
// locks them.
func TestRecordTaskExecution_LocksTheTaskBeforeTheAttempt(t *testing.T) {
	tc, id := heldTask()

	require.NoError(t, run.RecordTaskExecution(context.Background(), tc, executionRecord(id, "job-x-r1")))

	assert.Equal(t, []string{"task " + id.String(), "attempt " + id.String() + "/1"}, tc.lockOrder)
}

// A record for a task that was cancelled before its attempt had a row creates
// the attempt cancelled, with the record's facts, so a cancelled run is left
// with no open attempt.
func TestRecordTaskExecution_ARecordForACancelledTaskCreatesACancelledAttempt(t *testing.T) {
	tc, id := heldTask()
	tc.statuses[id] = run.TaskStatusCancelled
	rec := executionRecord(id, "job-x-r1")

	require.NoError(t, run.RecordTaskExecution(context.Background(), tc, rec))

	a, ok := tc.attemptRows[attemptKey{id, 1}]
	require.True(t, ok)
	assert.Equal(t, run.AttemptCancelled, a.Status)
	assert.Equal(t, "job-x-r1", a.JobName)
	assertFacts(t, rec, a)
}
