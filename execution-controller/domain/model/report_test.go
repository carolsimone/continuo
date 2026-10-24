package model

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestReportOf_AQueuedAttemptIsQueuedWhenItWasAccepted(t *testing.T) {
	accepted := time.Date(2026, 10, 20, 23, 1, 20, 0, time.UTC)
	d := NewDeployment(command.DeployTask{TaskID: uuid.NewString(), Attempt: 1}, nil, accepted)

	r := d.ReportOf(pkgmodel.AttemptStatusQueued, "ignored")

	assert.Equal(t, Report{Status: pkgmodel.AttemptStatusQueued, QueuedAt: &accepted}, r, "only a failure carries an error message")
}

// A report carries every fact seen of the attempt: a failure its reason and
// message, a finished attempt its run time.
func TestReportOf_CarriesEveryFactSeenOfTheAttempt(t *testing.T) {
	accepted := time.Date(2026, 10, 20, 23, 1, 20, 0, time.UTC)
	started, finished := accepted.Add(4*time.Second), accepted.Add(31*time.Second)
	code := int32(137)
	d := NewDeployment(command.DeployTask{TaskID: uuid.NewString()}, nil, accepted)
	d.Observe(AttemptFacts{StartedAt: &started, FinishedAt: &finished, ExitCode: &code, LogKey: "logs/k",
		RunResultsKey: "results/k", ParseCache: "degraded", ParseCacheReason: "missing", Reason: pkgmodel.FailureReasonOOMKilled})

	r := d.ReportOf(pkgmodel.AttemptStatusFailed, "OOMKilled")

	secs := 27.0
	assert.Equal(t, Report{Status: pkgmodel.AttemptStatusFailed, QueuedAt: &accepted, StartedAt: &started, FinishedAt: &finished,
		ExecutionSeconds: &secs, ExitCode: &code, FailureReason: pkgmodel.FailureReasonOOMKilled, ErrorMessage: "OOMKilled",
		LogKey: "logs/k", RunResultsKey: "results/k", ParseCache: "degraded", ParseCacheReason: "missing"}, r)
}

// The report last emitted is kept with the attempt.
func TestDeployment_RecordReportKeepsTheLastReport(t *testing.T) {
	d := NewDeployment(command.DeployTask{TaskID: uuid.NewString()}, nil, time.Now())
	assert.Nil(t, d.LastReport())
	d.RecordReport(Report{Status: pkgmodel.AttemptStatusQueued})
	d.RecordReport(Report{Status: pkgmodel.AttemptStatusRunning})
	assert.Equal(t, &Report{Status: pkgmodel.AttemptStatusRunning}, d.LastReport())
}

// A stopped attempt reports what was seen of it and no failure.
func TestReportOf_AStoppedAttemptCarriesItsFactsButNoFailure(t *testing.T) {
	accepted := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	started, finished := accepted.Add(4*time.Second), accepted.Add(31*time.Second)
	stop := accepted.Add(30 * time.Second)
	code := int32(143)
	d := NewDeployment(command.DeployTask{TaskID: uuid.NewString()}, nil, accepted)
	d.Observe(AttemptFacts{StartedAt: &started, FinishedAt: &finished, ExitCode: &code, LogKey: "logs/k", StopRequestedAt: &stop})

	r := d.ReportOf(pkgmodel.AttemptStatusStopped, "ignored")

	secs := 27.0
	assert.Equal(t, Report{Status: pkgmodel.AttemptStatusStopped, QueuedAt: &accepted, StartedAt: &started, FinishedAt: &finished,
		ExecutionSeconds: &secs, ExitCode: &code, LogKey: "logs/k"}, r)
}
