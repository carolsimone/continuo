package run

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAttemptStatus_RanksDispatchedQueuedRunningThenFinal(t *testing.T) {
	assert.Equal(t, 0, AttemptDispatched.Rank())
	assert.Equal(t, 1, AttemptQueued.Rank())
	assert.Equal(t, 2, AttemptRunning.Rank())
	for _, s := range []AttemptStatus{AttemptSucceeded, AttemptFailed, AttemptCancelled} {
		assert.Equal(t, 3, s.Rank(), s)
		assert.True(t, s.IsFinal(), s)
	}
	for _, s := range []AttemptStatus{AttemptDispatched, AttemptQueued, AttemptRunning} {
		assert.False(t, s.IsFinal(), s)
	}
	assert.Equal(t, -1, AttemptStatus("bogus").Rank())
	assert.False(t, AttemptStatus("bogus").IsFinal())
}

func TestAttempt_AdvanceMovesOnlyToAHigherRank(t *testing.T) {
	tests := []struct {
		from, to AttemptStatus
		moved    bool
		want     AttemptStatus
	}{
		{AttemptDispatched, AttemptRunning, true, AttemptRunning},
		{AttemptRunning, AttemptFailed, true, AttemptFailed},
		{AttemptFailed, AttemptRunning, false, AttemptFailed},
		{AttemptFailed, AttemptSucceeded, false, AttemptFailed},
		{AttemptCancelled, AttemptFailed, false, AttemptCancelled},
		{AttemptRunning, AttemptRunning, false, AttemptRunning},
	}
	for _, tc := range tests {
		t.Run(string(tc.from)+"_to_"+string(tc.to), func(t *testing.T) {
			a := Attempt{Status: tc.from}
			assert.Equal(t, tc.moved, a.Advance(tc.to))
			assert.Equal(t, tc.want, a.Status)
		})
	}
}

func TestAttempt_RecordExecutionKeepsWhatTheRecordOmits(t *testing.T) {
	stored, replaced, fresh := "logs/key", "old error", "new error"
	a := Attempt{
		TaskID:       uuid.New(),
		JobName:      "svc-sch-tbl-7f3e1a2b",
		Status:       AttemptRunning,
		LogS3Key:     &stored,
		ErrorMessage: &replaced,
	}

	a.RecordExecution(ExecutionRecord{TaskID: a.TaskID, ErrorMessage: &fresh})

	require.NotNil(t, a.LogS3Key)
	assert.Equal(t, "logs/key", *a.LogS3Key, "a nil fact keeps the stored value")
	require.NotNil(t, a.ErrorMessage)
	assert.Equal(t, "new error", *a.ErrorMessage, "a present fact replaces the stored value")
	assert.Equal(t, "svc-sch-tbl-7f3e1a2b", a.JobName)
	assert.Equal(t, AttemptRunning, a.Status)
}

func TestAttempt_RecordExecutionCopiesEveryFact(t *testing.T) {
	started := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	completed := started.Add(90 * time.Second)
	secs := 1.5
	errMsg, logKey, results, cache, reason := "e", "l", "r", "hit", "why"
	var a Attempt

	a.RecordExecution(ExecutionRecord{
		StartedAt:            &started,
		CompletedAt:          &completed,
		ExecutionTimeSeconds: &secs,
		ErrorMessage:         &errMsg,
		LogS3Key:             &logKey,
		RunResultsURI:        &results,
		ParseCache:           &cache,
		ParseCacheReason:     &reason,
	})

	assert.Equal(t, &started, a.StartedAt)
	assert.Equal(t, &completed, a.FinishedAt)
	assert.Equal(t, &secs, a.ExecutionSeconds)
	assert.Equal(t, &errMsg, a.ErrorMessage)
	assert.Equal(t, &logKey, a.LogS3Key)
	assert.Equal(t, &results, a.RunResultsURI)
	assert.Equal(t, &cache, a.ParseCache)
	assert.Equal(t, &reason, a.ParseCacheReason)
}

func TestAttemptFromJobName(t *testing.T) {
	tests := []struct {
		name string
		want int32
	}{
		{"svc-sch-tbl-7f3e1a2b", 0},
		{"svc-sch-tbl-7f3e1a2b-r1", 1},
		{"svc-sch-tbl-7f3e1a2b--r1", 1},
		{"svc-sch-tbl-7f3e1a2b-r1-r2", 2},
		{"svc-sch-tbl-7f3e1a2b-r10", 10},
		{"", 0},
		{"x-r", 0},
		{"x-rx", 0},
		{"x-r1a", 0},
		{"x-r99999999999", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, AttemptFromJobName(tc.name))
		})
	}
}

func TestAttemptStatusOfReport(t *testing.T) {
	for _, s := range []TaskStatus{TaskStatusRunning, TaskStatusSucceeded, TaskStatusFailed} {
		got, ok := attemptStatusOfReport(s)
		assert.True(t, ok, s)
		assert.Equal(t, AttemptStatus(s), got)
	}
	for _, s := range []TaskStatus{TaskStatusPending, TaskStatusCancelled, TaskStatusSkipped, "bogus"} {
		_, ok := attemptStatusOfReport(s)
		assert.False(t, ok, s)
	}
}

func TestAttemptOfReport(t *testing.T) {
	tests := []struct {
		name   string
		status TaskStatus
		r      int32
		task   TaskAttemptState
		held   bool
		want   int32
	}{
		{"running reports its own attempt", TaskStatusRunning, 1, TaskAttemptState{MaxRetries: 2}, false, 1},
		{"succeeded reports its own attempt", TaskStatusSucceeded, 0, TaskAttemptState{MaxRetries: 2}, false, 0},
		{"failed below the budget reports its own attempt", TaskStatusFailed, 0, TaskAttemptState{MaxRetries: 2}, false, 0},
		{"failed at the budget with a row at the count", TaskStatusFailed, 2, TaskAttemptState{Status: TaskStatusFailed, RetryCount: 0, MaxRetries: 2}, true, 2},
		{"failed at the budget, task still pending at 0", TaskStatusFailed, 2, TaskAttemptState{Status: TaskStatusPending, RetryCount: 0, MaxRetries: 2}, false, 0},
		{"failed at the budget, task failed at 0", TaskStatusFailed, 2, TaskAttemptState{Status: TaskStatusFailed, RetryCount: 0, MaxRetries: 2}, false, 1},
		{"failed at the budget, task failed at 1", TaskStatusFailed, 2, TaskAttemptState{Status: TaskStatusFailed, RetryCount: 1, MaxRetries: 2}, false, 2},
		{"failed at the budget, task running at 1", TaskStatusFailed, 2, TaskAttemptState{Status: TaskStatusRunning, RetryCount: 1, MaxRetries: 2}, false, 1},
		{"never above the reported count", TaskStatusFailed, 2, TaskAttemptState{Status: TaskStatusFailed, RetryCount: 2, MaxRetries: 2}, false, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, attemptOfReport(tc.status, tc.r, tc.task, tc.held))
		})
	}
}
