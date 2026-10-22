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

	r := d.ReportOf(ReportQueued, "ignored")

	assert.Equal(t, Report{Status: ReportQueued, QueuedAt: &accepted}, r, "only a failure carries an error message")
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

	r := d.ReportOf(ReportFailed, "OOMKilled")

	secs := 27.0
	assert.Equal(t, Report{Status: ReportFailed, QueuedAt: &accepted, StartedAt: &started, FinishedAt: &finished,
		ExecutionSeconds: &secs, ExitCode: &code, FailureReason: pkgmodel.FailureReasonOOMKilled, ErrorMessage: "OOMKilled",
		LogKey: "logs/k", RunResultsKey: "results/k", ParseCache: "degraded", ParseCacheReason: "missing"}, r)
}

// The report last emitted is kept with the attempt.
func TestDeployment_RecordReportKeepsTheLastReport(t *testing.T) {
	d := NewDeployment(command.DeployTask{TaskID: uuid.NewString()}, nil, time.Now())
	assert.Nil(t, d.LastReport())
	d.RecordReport(Report{Status: ReportQueued})
	d.RecordReport(Report{Status: ReportRunning})
	assert.Equal(t, &Report{Status: ReportRunning}, d.LastReport())
}
