package run

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/stretchr/testify/assert"
)

func TestRetryable_RetriesTaskResourceInfraAndStartFailures(t *testing.T) {
	decisions := map[model.FailureClass]bool{
		model.FailureClassTask:     true,
		model.FailureClassResource: true,
		model.FailureClassInfra:    true,
		model.FailureClassStart:    true,
		model.FailureClassTimeout:  false,
		model.FailureClassInvalid:  false,
	}
	for _, c := range model.FailureClasses() {
		want, decided := decisions[c]
		assert.True(t, decided, "failure class %q has no retry decision in this test", c)
		assert.Equal(t, want, Retryable(c), "class %q", c)
	}
	assert.False(t, Retryable(""), "a failure without a class is not retried")
}

func TestAttempt_ApplyRanksTheReport(t *testing.T) {
	started := time.Date(2026, 10, 20, 23, 1, 24, 0, time.UTC)
	tests := []struct {
		name            string
		from            AttemptStatus
		report          AttemptStatus
		applied, change bool
		want            AttemptStatus
	}{
		{"queued after dispatched", AttemptDispatched, AttemptQueued, true, true, AttemptQueued},
		{"running after queued", AttemptQueued, AttemptRunning, true, true, AttemptRunning},
		{"a final report after its lost predecessors", AttemptDispatched, AttemptSucceeded, true, true, AttemptSucceeded},
		{"the answer to a question about a running attempt", AttemptRunning, AttemptRunning, true, false, AttemptRunning},
		{"a late queued after running", AttemptRunning, AttemptQueued, false, false, AttemptRunning},
		{"a replayed final", AttemptFailed, AttemptFailed, false, false, AttemptFailed},
		{"a final after another final", AttemptSucceeded, AttemptFailed, false, false, AttemptSucceeded},
		{"a report for a stopped attempt", AttemptStopped, AttemptRunning, false, false, AttemptStopped},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := Attempt{Status: tc.from}
			rep := AttemptReport{Status: tc.report, StartedAt: &started}
			if tc.report == AttemptFailed {
				rep.FailureReason = model.FailureReasonExitCode
			}
			applied, changed := a.Apply(rep)
			assert.Equal(t, tc.applied, applied, "applied")
			assert.Equal(t, tc.change, changed, "status changed")
			assert.Equal(t, tc.want, a.Status)
			if tc.applied {
				assert.Equal(t, &started, a.StartedAt, "an applied report's facts are kept")
			} else {
				assert.Nil(t, a.StartedAt, "a report that is not applied changes nothing")
			}
		})
	}
}

func TestAttempt_ApplyKeepsEveryFactTheReportCarries(t *testing.T) {
	queued := time.Date(2026, 10, 20, 23, 1, 20, 0, time.UTC)
	finished := queued.Add(31 * time.Second)
	secs, code := 27.8, int32(137)
	logKey, msg, stored := "logs/key", "OOMKilled", "parse-cache-reason"
	a := Attempt{Status: AttemptRunning, QueuedAt: &queued, ParseCacheReason: &stored}

	applied, changed := a.Apply(AttemptReport{
		Status: AttemptFailed, FinishedAt: &finished, ExecutionSeconds: &secs, ExitCode: &code,
		FailureReason: model.FailureReasonOOMKilled, ErrorMessage: &msg, LogS3Key: &logKey,
	})

	assert.True(t, applied)
	assert.True(t, changed)
	assert.Equal(t, AttemptFailed, a.Status)
	assert.Equal(t, &queued, a.QueuedAt, "a fact the report omits keeps its stored value")
	assert.Equal(t, &finished, a.FinishedAt)
	assert.Equal(t, &secs, a.ExecutionSeconds)
	assert.Equal(t, &code, a.ExitCode)
	assert.Equal(t, "resource", *a.FailureClass, "the class is the reason's")
	assert.Equal(t, "oom_killed", *a.FailureReason)
	assert.Equal(t, &msg, a.ErrorMessage)
	assert.Equal(t, &logKey, a.LogS3Key)
	assert.Equal(t, &stored, a.ParseCacheReason)
	assert.Zero(t, a.NextCheckIn, "a final attempt is never asked about")
}

// run-controller asks about an attempt it has heard nothing of: an unacknowledged one
// after 5 minutes, then 10, 20, 40 and every 60; an acknowledged one after 30
// minutes of silence, and every 30 after that.
func TestNextCheckAfter(t *testing.T) {
	for checks, want := range map[int]time.Duration{
		0: 5 * time.Minute, 1: 10 * time.Minute, 2: 20 * time.Minute, 3: 40 * time.Minute,
		4: 60 * time.Minute, 9: 60 * time.Minute,
	} {
		assert.Equal(t, want, NextCheckAfter(AttemptDispatched, checks), "dispatched, %d checks sent", checks)
	}
	for _, s := range []AttemptStatus{AttemptQueued, AttemptRunning} {
		assert.Equal(t, 30*time.Minute, NextCheckAfter(s, 0), s)
		assert.Equal(t, 30*time.Minute, NextCheckAfter(s, 7), s)
	}
	for _, s := range []AttemptStatus{AttemptSucceeded, AttemptFailed, AttemptStopped} {
		assert.Zero(t, NextCheckAfter(s, 0), s)
	}
}

// run-controller re-sends the cancel for an attempt it asked to stop after 5 minutes,
// then 10, 20, 40 and every 60.
func TestNextStopCheckAfter(t *testing.T) {
	for checks, want := range map[int]time.Duration{
		0: 5 * time.Minute, 1: 10 * time.Minute, 2: 20 * time.Minute, 3: 40 * time.Minute,
		4: 60 * time.Minute, 9: 60 * time.Minute,
	} {
		assert.Equal(t, want, NextStopCheckAfter(checks), "%d re-sends", checks)
	}
}

func TestAttempt_ApplySetsTheNextCheckOfAnOpenAttempt(t *testing.T) {
	a := Attempt{Status: AttemptQueued}
	a.Apply(AttemptReport{Status: AttemptRunning})
	assert.Equal(t, 30*time.Minute, a.NextCheckIn)
}

// A declared timeout is the timeout, even when it equals the install's
// default; a node that declares none gets the default.
func TestEffectiveTimeout_IsTheDeclaredTimeoutElseTheDefault(t *testing.T) {
	d := TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}
	eightHours, oneDay := 28800, 86400
	cases := map[string]struct {
		declared     *int
		want         time.Duration
		wantDeclared bool
	}{
		"declared":                      {&eightHours, 8 * time.Hour, true},
		"undeclared":                    {nil, 24 * time.Hour, false},
		"declared equal to the default": {&oneDay, 24 * time.Hour, true},
	}
	for name, c := range cases {
		timeout, declared := EffectiveTimeout(c.declared, d)
		assert.Equal(t, c.want, timeout, name)
		assert.Equal(t, c.wantDeclared, declared, name)
	}
}

func TestTaskTimeout_IsTheNodesDeclaredTimeoutElseTheDefault(t *testing.T) {
	d := TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}
	declared := 7200
	assert.Equal(t, 2*time.Hour, TaskTimeout(topology.Node{TimeoutSeconds: &declared}, d))
	assert.Equal(t, 24*time.Hour, TaskTimeout(topology.Node{}, d))
}
