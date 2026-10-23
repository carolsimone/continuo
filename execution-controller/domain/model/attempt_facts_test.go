package model_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
)

func at(sec int) *time.Time {
	t := time.Date(2026, 10, 20, 23, 1, 0, 0, time.UTC).Add(time.Duration(sec) * time.Second)
	return &t
}

func code(c int32) *int32 { return &c }

func TestAttemptFacts_MergeAddsWhatIsNewAndKeepsWhatIsKnown(t *testing.T) {
	known := model.AttemptFacts{StartedAt: at(4), LogKey: "logs/a.log"}

	merged, changed := known.Merge(model.AttemptFacts{
		StartedAt: at(9), FinishedAt: at(31), ExitCode: code(137), TerminationReason: "OOMKilled",
		LogKey: "logs/b.log", Reason: pkgmodel.FailureReasonOOMKilled,
	})

	assert.True(t, changed)
	assert.Equal(t, at(4), merged.StartedAt, "a known start is never replaced")
	assert.Equal(t, "logs/a.log", merged.LogKey, "a known log key is never replaced")
	assert.Equal(t, at(31), merged.FinishedAt)
	assert.Equal(t, code(137), merged.ExitCode)
	assert.Equal(t, "OOMKilled", merged.TerminationReason)
	assert.Equal(t, pkgmodel.FailureReasonOOMKilled, merged.Reason)
	assert.Equal(t, pkgmodel.FailureClassResource, merged.Class())
}

func TestAttemptFacts_MergeOfNothingNewReportsNoChange(t *testing.T) {
	known := model.AttemptFacts{StartedAt: at(4), JobSucceeded: true, NothingToDo: true}
	merged, changed := known.Merge(model.AttemptFacts{StartedAt: at(5)})
	assert.False(t, changed)
	assert.Equal(t, known, merged)
}

func TestAttemptFacts_ATrueFlagStaysTrue(t *testing.T) {
	merged, changed := model.AttemptFacts{}.Merge(model.AttemptFacts{JobSucceeded: true, NothingToDo: true})
	assert.True(t, changed)
	merged, changed = merged.Merge(model.AttemptFacts{})
	assert.False(t, changed)
	assert.True(t, merged.JobSucceeded)
	assert.True(t, merged.NothingToDo)
}

func TestAttemptFacts_AStopRequestIsKeptOnceMade(t *testing.T) {
	merged, changed := model.AttemptFacts{}.Merge(model.AttemptFacts{StopRequestedAt: at(5)})
	assert.True(t, changed)
	assert.Equal(t, at(5), merged.StopRequestedAt)
	merged, changed = merged.Merge(model.AttemptFacts{StopRequestedAt: at(9)})
	assert.False(t, changed)
	assert.Equal(t, at(5), merged.StopRequestedAt, "the first stop request stands")
}
