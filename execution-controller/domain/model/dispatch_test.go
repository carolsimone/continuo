package model

import (
	"strings"
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
		known            pkgmodel.AttemptStatus
		want             DispatchAction
	}{
		{"its record is held", true, false, false, "", pkgmodel.AttemptStatusRunning, ResendReport},
		{"its record is held, whatever else", true, true, true, pkgmodel.FailureReasonMalformedDispatch, pkgmodel.AttemptStatusDispatched, ResendReport},
		{"a later attempt is held", false, true, false, "", pkgmodel.AttemptStatusDispatched, IgnoreOlder},
		{"the dispatch cannot run", false, false, false, pkgmodel.FailureReasonInvalidNodeType, pkgmodel.AttemptStatusDispatched, RefuseInvalid},
		{"its Job exists", false, false, true, "", pkgmodel.AttemptStatusRunning, AdoptJob},
		{"a new dispatch", false, false, false, "", pkgmodel.AttemptStatusDispatched, CreateAttempt},
		{"a question about a queued attempt it lost", false, false, false, "", pkgmodel.AttemptStatusQueued, ReportLost},
		{"a question about a running attempt it lost", false, false, false, "", pkgmodel.AttemptStatusRunning, ReportLost},
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
	reason, _ := DispatchProblem(validDispatch(), pkgmodel.AttemptStatusDispatched)
	assert.Empty(t, reason, "a valid dispatch")
	for name, c := range map[string]struct {
		edit  func(*command.DeployTask)
		known pkgmodel.AttemptStatus
		want  pkgmodel.FailureReason
	}{
		"undeclared node type": {func(c *command.DeployTask) { c.NodeType = "dbt-macro" }, pkgmodel.AttemptStatusDispatched, pkgmodel.FailureReasonInvalidNodeType},
		"unknown known status": {func(*command.DeployTask) {}, "lost", pkgmodel.FailureReasonMalformedDispatch},
		"no job name":          {func(c *command.DeployTask) { c.JobName = "" }, pkgmodel.AttemptStatusDispatched, pkgmodel.FailureReasonMalformedDispatch},
		"a job name too long":  {func(c *command.DeployTask) { c.JobName = string(make([]byte, 64)) }, pkgmodel.AttemptStatusDispatched, pkgmodel.FailureReasonMalformedDispatch},
		"no image":             {func(c *command.DeployTask) { c.ImageTag = "" }, pkgmodel.AttemptStatusDispatched, pkgmodel.FailureReasonMalformedDispatch},
		"no timeout":           {func(c *command.DeployTask) { c.TimeoutSeconds = 0 }, pkgmodel.AttemptStatusDispatched, pkgmodel.FailureReasonMalformedDispatch},
		"unknown operation":    {func(c *command.DeployTask) { c.Operation = "seed_everything" }, pkgmodel.AttemptStatusDispatched, pkgmodel.FailureReasonMalformedDispatch},
	} {
		cmd := validDispatch()
		c.edit(&cmd)
		reason, message := DispatchProblem(cmd, c.known)
		assert.Equal(t, c.want, reason, name)
		assert.NotEmpty(t, message, name)
	}
}

// A job_name the Kubernetes API cannot read as a Job name (and so as a label
// selector value) is refused up front: every live read of it would fail, and
// the attempt would never end. The reason names the field.
func TestDispatchProblemRefusesAJobNameKubernetesCannotRead(t *testing.T) {
	for name, jobName := range map[string]string{
		"upper case":          "Svc-sch-tbl-7f3e1a2b",
		"underscore":          "svc_sch-tbl-7f3e1a2b",
		"slash":               "svc/sch-tbl-7f3e1a2b",
		"percent sign":        "svc%sch-tbl-7f3e1a2b",
		"dot":                 "svc.sch-tbl-7f3e1a2b",
		"space":               "svc sch-tbl-7f3e1a2b",
		"leading dash":        "-svc-sch-tbl-7f3e1a2b",
		"trailing dash":       "svc-sch-tbl-7f3e1a2b-",
		"a dash alone":        "-",
		"non-ASCII letter":    "svc-sch-tbl-7f3e1a2é",
		"64 valid characters": strings.Repeat("a", 64),
	} {
		cmd := validDispatch()
		cmd.JobName = jobName
		reason, message := DispatchProblem(cmd, pkgmodel.AttemptStatusDispatched)
		assert.Equal(t, pkgmodel.FailureReasonMalformedDispatch, reason, name)
		assert.Contains(t, message, "job_name", name)
	}

	cmd := validDispatch()
	cmd.JobName = ""
	reason, message := DispatchProblem(cmd, pkgmodel.AttemptStatusDispatched)
	assert.Equal(t, pkgmodel.FailureReasonMalformedDispatch, reason, "no job name")
	assert.Contains(t, message, "job_name", "no job name")
}

// Every name state gives an attempt (pkg/domain.ComputeJobName: a DNS-1123
// label of at most 63 characters, with the -r{attempt} suffix after the
// first attempt) is accepted. The names are those the function returns for
// service-1 / analytics / int_customer_orders_aggregated_daily and
// service-1 / analytics / orders in run 3f2a9c1e-..., at the attempts shown.
func TestDispatchProblemAcceptsTheJobNamesStateComputes(t *testing.T) {
	for name, jobName := range map[string]string{
		"a short name":                             "service-1-analytics-orders-3f2a9c1e",
		"a short retry":                            "service-1-analytics-orders-3f2a9c1e-r1",
		"a retry from attempt 10":                  "service-1-analytics-orders-3f2a9c1e-r10",
		"the longest first attempt, 63 characters": "service-1-analytics-int-customer-orders-aggregated-dai-3f2a9c1e",
		"a cut retry, 63 characters":               "service-1-analytics-int-customer-orders-aggr-3c1270-3f2a9c1e-r1",
		"a cut retry from attempt 10":              "service-1-analytics-int-customer-orders-agg-3c1270-3f2a9c1e-r10",
		"a single character":                       "a",
		"63 characters":                            strings.Repeat("a", 63),
	} {
		cmd := validDispatch()
		cmd.JobName = jobName
		reason, message := DispatchProblem(cmd, pkgmodel.AttemptStatusDispatched)
		assert.Empty(t, reason, "%s: %s", name, message)
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
	r := refused.ReportOf(pkgmodel.AttemptStatusFailed, "lost")
	assert.Equal(t, pkgmodel.FailureReasonAttemptLost, r.FailureReason)
}
