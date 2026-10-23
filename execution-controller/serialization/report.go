package serialization

import (
	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/num"
)

// ReportFromDomain renders the report of the attempt cmd runs as its
// task.status.updated:v2 payload, which is also what deployments.last_report
// keeps. A failure names its reason's class.
func ReportFromDomain(cmd command.DeployTask, r model.Report) pkgevents.TaskStatusReport {
	p := pkgevents.TaskStatusReport{
		RunID: cmd.ScheduleID, TaskID: cmd.TaskID, Attempt: num.ClampInt32(cmd.Attempt), JobName: cmd.JobName,
		Status: r.Status, QueuedAt: r.QueuedAt, StartedAt: r.StartedAt, FinishedAt: r.FinishedAt,
		ExecutionSeconds: r.ExecutionSeconds, ExitCode: r.ExitCode,
		ErrorMessage: r.ErrorMessage, LogS3Key: r.LogKey, RunResultsURI: r.RunResultsKey,
		ParseCache: r.ParseCache, ParseCacheReason: r.ParseCacheReason,
	}
	if r.FailureReason != "" {
		p.FailureReason, p.FailureClass = string(r.FailureReason), string(r.FailureReason.Class())
	}
	return p
}

// ReportToDomain reads a stored report back.
func ReportToDomain(p pkgevents.TaskStatusReport) model.Report {
	return model.Report{
		Status: p.Status, QueuedAt: p.QueuedAt, StartedAt: p.StartedAt, FinishedAt: p.FinishedAt,
		ExecutionSeconds: p.ExecutionSeconds, ExitCode: p.ExitCode, FailureReason: pkgmodel.FailureReason(p.FailureReason),
		ErrorMessage: p.ErrorMessage, LogKey: p.LogS3Key, RunResultsKey: p.RunResultsURI,
		ParseCache: p.ParseCache, ParseCacheReason: p.ParseCacheReason,
	}
}
