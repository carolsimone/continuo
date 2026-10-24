package model_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name  string
		facts model.AttemptFacts
		want  model.Verdict
	}{
		{"exit 0", model.AttemptFacts{ExitCode: code(0)},
			model.Verdict{Succeeded: true}},
		{"exit 0 but dbt matched no node", model.AttemptFacts{ExitCode: code(0), NothingToDo: true},
			model.Verdict{Reason: pkgmodel.FailureReasonDbtNothingToDo}},
		{"killed for memory", model.AttemptFacts{ExitCode: code(137), TerminationReason: "OOMKilled"},
			model.Verdict{Reason: pkgmodel.FailureReasonOOMKilled}},
		{"evicted", model.AttemptFacts{ExitCode: code(137), PodReason: "Evicted"},
			model.Verdict{Reason: pkgmodel.FailureReasonEvicted}},
		{"past the Job's deadline", model.AttemptFacts{ExitCode: code(143), JobFailureReason: "DeadlineExceeded"},
			model.Verdict{Reason: pkgmodel.FailureReasonDeadlineExceeded}},
		{"non-zero exit", model.AttemptFacts{ExitCode: code(1), FailedContainer: "dbt-job"},
			model.Verdict{Reason: pkgmodel.FailureReasonExitCode}},
		{"an init container failed in place of the main one", model.AttemptFacts{ExitCode: code(2), FailedContainer: "compile"},
			model.Verdict{Reason: pkgmodel.FailureReasonExitCode}},
		{"the Job succeeded with no pod left to read", model.AttemptFacts{JobSucceeded: true},
			model.Verdict{Succeeded: true}},
		{"gone before its main container terminated", model.AttemptFacts{StartedAt: at(4)},
			model.Verdict{Reason: pkgmodel.FailureReasonNodeLost}},
		{"gone before it started", model.AttemptFacts{JobFailureReason: "BackoffLimitExceeded"},
			model.Verdict{Reason: pkgmodel.FailureReasonNodeLost}},
		{"ended by execution-controller at the start timeout", model.AttemptFacts{Reason: pkgmodel.FailureReasonStartTimeout},
			model.Verdict{Reason: pkgmodel.FailureReasonStartTimeout}},
		{"a decided reason wins over what the pod showed", model.AttemptFacts{ExitCode: code(1), Reason: pkgmodel.FailureReasonImagePull},
			model.Verdict{Reason: pkgmodel.FailureReasonImagePull}},
	}
	for _, c := range cases {
		got := model.Classify(c.facts)
		assert.Equal(t, c.want, got, c.name)
		if !got.Succeeded {
			assert.True(t, got.Reason.Class().IsValid(), "%s: %s names a class", c.name, got.Reason)
		}
	}
}

func stopped(f model.AttemptFacts) model.AttemptFacts {
	f.StopRequestedAt = at(60)
	return f
}

func TestClassify_AStoppedAttemptThatExited0Succeeded(t *testing.T) {
	got := model.Classify(stopped(model.AttemptFacts{ExitCode: code(0)}))
	assert.Equal(t, model.Verdict{Succeeded: true}, got)
	got = model.Classify(stopped(model.AttemptFacts{JobSucceeded: true}))
	assert.Equal(t, model.Verdict{Succeeded: true}, got)
}

func TestClassify_AStoppedAttemptKilledBySIGTERMIsStopped(t *testing.T) {
	got := model.Classify(stopped(model.AttemptFacts{StartedAt: at(4), ExitCode: code(143), FailedContainer: "dbt-job"}))
	assert.Equal(t, model.Verdict{Stopped: true}, got)
}

func TestClassify_AStoppedAttemptWhosePodVanishedIsStopped(t *testing.T) {
	got := model.Classify(stopped(model.AttemptFacts{StartedAt: at(4)}))
	assert.Equal(t, model.Verdict{Stopped: true}, got, "a deleted Job's pod is not a lost node")
	got = model.Classify(stopped(model.AttemptFacts{}))
	assert.Equal(t, model.Verdict{Stopped: true}, got, "an attempt that never started")
}

func TestClassify_AReasonRecordedBeforeTheStopStands(t *testing.T) {
	got := model.Classify(stopped(model.AttemptFacts{Reason: pkgmodel.FailureReasonStartTimeout}))
	assert.Equal(t, model.Verdict{Reason: pkgmodel.FailureReasonStartTimeout}, got)
}

// atMs is at(sec) plus ms milliseconds: execution-controller's clock keeps
// the fraction of a second, a Kubernetes container timestamp does not.
func atMs(sec, ms int) *time.Time {
	t := at(sec).Add(time.Duration(ms) * time.Millisecond)
	return &t
}

// A stop request is at(60).250; a container's FinishedAt is a Kubernetes
// timestamp, whole seconds. An attempt that failed before the request reports
// its own failure; one that ended at or after it, or whose pods vanished
// before a terminated state was seen, is stopped; a success is a success.
func TestClassify_AStoppedAttemptKeepsAFailureItReachedBeforeTheStop(t *testing.T) {
	stopAt := atMs(60, 250)
	ended := func(f model.AttemptFacts, finished *time.Time) model.AttemptFacts {
		f.StartedAt, f.FinishedAt, f.StopRequestedAt = at(4), finished, stopAt
		return f
	}
	failedExit1 := model.AttemptFacts{ExitCode: code(1), FailedContainer: "dbt-job"}
	cases := []struct {
		name  string
		facts model.AttemptFacts
		want  model.Verdict
	}{
		{"exit 1 before the stop", ended(failedExit1, at(30)),
			model.Verdict{Reason: pkgmodel.FailureReasonExitCode}},
		{"exit 1 in the second before the stop's", ended(failedExit1, at(59)),
			model.Verdict{Reason: pkgmodel.FailureReasonExitCode}},
		{"exit 1 in the stop's own second is not the stop's signature", ended(failedExit1, at(60)),
			model.Verdict{Reason: pkgmodel.FailureReasonExitCode}},
		{"exit 1 after the stop", ended(failedExit1, at(61)),
			model.Verdict{Stopped: true}},
		{"exit 143 after the stop", ended(model.AttemptFacts{ExitCode: code(143), FailedContainer: "dbt-job"}, at(62)),
			model.Verdict{Stopped: true}},
		{"exit 143 in the stop's own second is the deletion's SIGTERM",
			ended(model.AttemptFacts{ExitCode: code(143), FailedContainer: "dbt-job"}, at(60)),
			model.Verdict{Stopped: true}},
		{"exit 137 in the stop's own second is the kill after the grace period",
			ended(model.AttemptFacts{ExitCode: code(137), FailedContainer: "dbt-job"}, at(60)),
			model.Verdict{Stopped: true}},
		{"exit 143 well before the stop is a failure of its own", ended(model.AttemptFacts{ExitCode: code(143), FailedContainer: "dbt-job"}, at(20)),
			model.Verdict{Reason: pkgmodel.FailureReasonExitCode}},
		{"pods vanished with no terminated state", stopped(model.AttemptFacts{StartedAt: at(4)}),
			model.Verdict{Stopped: true}},
		{"exit 0 before the stop", ended(model.AttemptFacts{ExitCode: code(0)}, at(30)),
			model.Verdict{Succeeded: true}},
		{"exit 0 after the stop", ended(model.AttemptFacts{ExitCode: code(0)}, at(90)),
			model.Verdict{Succeeded: true}},
		{"a recorded reason stands", ended(model.AttemptFacts{ExitCode: code(1), Reason: pkgmodel.FailureReasonStartTimeout}, at(90)),
			model.Verdict{Reason: pkgmodel.FailureReasonStartTimeout}},
		{"killed for memory before the stop", ended(model.AttemptFacts{ExitCode: code(137), TerminationReason: "OOMKilled"}, at(40)),
			model.Verdict{Reason: pkgmodel.FailureReasonOOMKilled}},
		{"killed for memory in the stop's own second", ended(model.AttemptFacts{ExitCode: code(137), TerminationReason: "OOMKilled"}, at(60)),
			model.Verdict{Reason: pkgmodel.FailureReasonOOMKilled}},
		{"killed for memory after the stop", ended(model.AttemptFacts{ExitCode: code(137), TerminationReason: "OOMKilled"}, at(61)),
			model.Verdict{Stopped: true}},
		{"an init container failed before the stop", ended(model.AttemptFacts{ExitCode: code(2), FailedContainer: "compile"}, at(10)),
			model.Verdict{Reason: pkgmodel.FailureReasonExitCode}},
	}
	for _, c := range cases {
		got := model.Classify(c.facts)
		assert.Equal(t, c.want, got, c.name)
		if !got.Succeeded && !got.Stopped {
			assert.True(t, got.Reason.Class().IsValid(), "%s: %s names a class", c.name, got.Reason)
		}
	}
	assert.Equal(t, pkgmodel.FailureClassResource,
		model.Classify(ended(model.AttemptFacts{ExitCode: code(137), TerminationReason: "OOMKilled"}, at(40))).Reason.Class(),
		"a memory kill before the stop keeps its class")
}
