package model_test

import (
	"testing"

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
