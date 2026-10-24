package run

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAttemptStatus_RanksDispatchedQueuedRunningThenFinal(t *testing.T) {
	assert.Equal(t, 0, AttemptDispatched.Rank())
	assert.Equal(t, 1, AttemptQueued.Rank())
	assert.Equal(t, 2, AttemptRunning.Rank())
	for _, s := range []AttemptStatus{AttemptSucceeded, AttemptFailed, AttemptStopped} {
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
		{AttemptStopped, AttemptFailed, false, AttemptStopped},
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
