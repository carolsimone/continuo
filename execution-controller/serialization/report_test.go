package serialization

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/stretchr/testify/assert"
)

// A report is emitted, and kept, as its task.status.updated:v2 payload; a
// failure names its reason's class, and the payload reads back as the report.
func TestReport_IsEmittedAsItsPayloadAndReadsBack(t *testing.T) {
	queued := time.Date(2026, 10, 20, 23, 1, 20, 0, time.UTC)
	code, secs := int32(1), 12.5
	cmd := command.DeployTask{RunID: "run-1", TaskID: "task-1", Attempt: 2, JobName: "svc-sch-tbl-7f3e1a2b-r2"}
	r := model.Report{Status: pkgmodel.AttemptStatusFailed, QueuedAt: &queued, ExitCode: &code, ExecutionSeconds: &secs,
		FailureReason: pkgmodel.FailureReasonExitCode, ErrorMessage: "boom", LogKey: "logs/k"}

	p := ReportFromDomain(cmd, r)

	assert.Equal(t, pkgevents.TaskStatusReport{RunID: "run-1", TaskID: "task-1", Attempt: 2, JobName: "svc-sch-tbl-7f3e1a2b-r2",
		Status: "failed", QueuedAt: &queued, ExitCode: &code, ExecutionSeconds: &secs,
		FailureClass: "task", FailureReason: "exit_code", ErrorMessage: "boom", LogS3Key: "logs/k"}, p)
	assert.Equal(t, r, ReportToDomain(p))
}

// The findings of a seed's preflight ride on the report as its seed_preflight
// and read back as the same findings.
func TestReport_CarriesTheSeedPreflightFindings(t *testing.T) {
	cmd := command.DeployTask{RunID: "run-1", TaskID: "task-1", JobName: "svc-sch-tbl-7f3e1a2b-r0"}
	findings := &seedreload.Findings{DropSemantics: "cascade", Completeness: "exact", Checked: true,
		Dependents: []seedreload.Dependent{{Schema: "bi", Name: "fx_rates_report", Kind: "view", Owner: "bi", Depth: 1, Allowed: true}}}
	r := model.Report{Status: pkgmodel.AttemptStatusSucceeded, SeedPreflight: findings}

	p := ReportFromDomain(cmd, r)

	assert.Equal(t, pkgevents.SeedPreflightFindingsOf(findings), p.SeedPreflight)
	assert.Equal(t, r, ReportToDomain(p))
	assert.Nil(t, ReportFromDomain(cmd, model.Report{Status: pkgmodel.AttemptStatusQueued}).SeedPreflight)
}
