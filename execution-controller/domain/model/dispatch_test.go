package model

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// The design's table of what a dispatch leads to: only a dispatch state still
// knows as dispatched, with neither a record nor a Job of the attempt, creates
// the attempt.
func TestDecideDispatch(t *testing.T) {
	cases := []struct {
		name             string
		held, later, job bool
		problem          pkgmodel.FailureReason
		known            string
		want             DispatchAction
	}{
		{"its record is held", true, false, false, "", KnownRunning, ResendReport},
		{"its record is held, whatever else", true, true, true, pkgmodel.FailureReasonMalformedDispatch, KnownDispatched, ResendReport},
		{"a later attempt is held", false, true, false, "", KnownDispatched, IgnoreOlder},
		{"the dispatch cannot run", false, false, false, pkgmodel.FailureReasonInvalidNodeType, KnownDispatched, RefuseInvalid},
		{"its Job exists", false, false, true, "", KnownRunning, AdoptJob},
		{"a new dispatch", false, false, false, "", KnownDispatched, CreateAttempt},
		{"a question about a queued attempt it lost", false, false, false, "", KnownQueued, ReportLost},
		{"a question about a running attempt it lost", false, false, false, "", KnownRunning, ReportLost},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, DecideDispatch(c.held, c.later, c.problem, c.job, c.known), c.name)
	}
}

func validDispatch() command.DeployTask {
	return command.DeployTask{TaskID: uuid.NewString(), ScheduleID: uuid.NewString(), NodeType: "dbt-model",
		JobName: "svc-sch-tbl-7f3e1a2b", ImageTag: "v1", Operation: "run", TimeoutSeconds: 3600}
}

// Every field that stops a dispatch from running names its reason.
func TestDispatchProblem(t *testing.T) {
	reason, _ := DispatchProblem(validDispatch(), KnownDispatched)
	assert.Empty(t, reason, "a valid dispatch")
	for name, c := range map[string]struct {
		edit  func(*command.DeployTask)
		known string
		want  pkgmodel.FailureReason
	}{
		"undeclared node type": {func(c *command.DeployTask) { c.NodeType = "dbt-macro" }, KnownDispatched, pkgmodel.FailureReasonInvalidNodeType},
		"unknown known status": {func(*command.DeployTask) {}, "lost", pkgmodel.FailureReasonMalformedDispatch},
		"no job name":          {func(c *command.DeployTask) { c.JobName = "" }, KnownDispatched, pkgmodel.FailureReasonMalformedDispatch},
		"a job name too long":  {func(c *command.DeployTask) { c.JobName = string(make([]byte, 64)) }, KnownDispatched, pkgmodel.FailureReasonMalformedDispatch},
		"no image":             {func(c *command.DeployTask) { c.ImageTag = "" }, KnownDispatched, pkgmodel.FailureReasonMalformedDispatch},
		"no timeout":           {func(c *command.DeployTask) { c.TimeoutSeconds = 0 }, KnownDispatched, pkgmodel.FailureReasonMalformedDispatch},
		"unknown operation":    {func(c *command.DeployTask) { c.Operation = "seed_everything" }, KnownDispatched, pkgmodel.FailureReasonMalformedDispatch},
	} {
		cmd := validDispatch()
		c.edit(&cmd)
		reason, message := DispatchProblem(cmd, c.known)
		assert.Equal(t, c.want, reason, name)
		assert.NotEmpty(t, message, name)
	}
}

// A rebuilt record holds a slot and is starting; a refused one has ended
// failed with its reason, so a repeated dispatch is answered from it.
func TestNewAdoptedAndRefusedDeployments(t *testing.T) {
	now := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	adopted := NewAdoptedDeployment(validDispatch(), nil, now)
	assert.Equal(t, StatusStarting, adopted.Status())
	assert.True(t, adopted.Status().InFlight())
	assert.Equal(t, &now, adopted.DeployedAt())

	refused := NewRefusedDeployment(validDispatch(), nil, pkgmodel.FailureReasonAttemptLost, "lost", now)
	assert.Equal(t, StatusFailed, refused.Status())
	assert.Equal(t, "failed", refused.Outcome())
	assert.Equal(t, pkgmodel.FailureReasonAttemptLost, refused.Facts().Reason)
	r := refused.ReportOf(ReportFailed, "lost")
	assert.Equal(t, pkgmodel.FailureReasonAttemptLost, r.FailureReason)
}
