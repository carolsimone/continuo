package observer

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/outcomes"
	"github.com/carolsimone/continuo/execution-controller/service/reports"
	"github.com/carolsimone/continuo/execution-controller/service/uow"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
)

// settle records how an attempt ended on its deployment, which releases the
// slot. A release leg's node settles through outcomes.Recorder. A production
// attempt is reported succeeded, stopped (it was asked to stop because its
// run was cancelled, and did not succeed first) or failed, with every fact
// seen of it; run-controller decides what follows. A stopped attempt is counted once
// the transaction commits.
func (o *Observer) settle(ctx context.Context, u uow.UnitOfWork, d *model.Deployment, v model.Verdict, now time.Time) error {
	if d.Mode() != model.ModeProduction {
		return o.settleCandidate(ctx, u, d, v)
	}
	outcome, status, message := "ok", pkgmodel.AttemptStatusSucceeded, ""
	switch {
	case v.Stopped:
		outcome, status = model.OutcomeStopped, pkgmodel.AttemptStatusStopped
		u.AfterCommit(o.deps.StopMetrics.AttemptStopped)
	case !v.Succeeded:
		outcome, status, message = "failed", pkgmodel.AttemptStatusFailed, o.errorMessage(d.Facts())
	}
	if err := d.Finish(outcome, now); err != nil {
		return fmt.Errorf("finish deployment %s: %w", d.ID(), err)
	}
	if err := reports.Write(ctx, u.OutboxRepo(), d, status, message); err != nil {
		return err
	}
	cmd := d.Command()
	o.deps.Logger.Info("Attempt ended", "task_id", cmd.TaskID, "attempt", cmd.Attempt, "job_name", cmd.JobName,
		"status", status, "failure_class", d.Facts().Class(), "failure_reason", d.Facts().Reason)
	return u.DeploymentsRepo().Save(ctx, d)
}

// settleCandidate records a release leg's node outcome. The deployment's
// facts are saved first, so outcomes.Recorder, which loads the row afresh
// within this transaction, keeps them.
func (o *Observer) settleCandidate(ctx context.Context, u uow.UnitOfWork, d *model.Deployment, v model.Verdict) error {
	if err := u.DeploymentsRepo().Save(ctx, d); err != nil {
		return err
	}
	outcome := "failed"
	if v.Succeeded {
		outcome = "ok"
	}
	vc, f := d.ValidationCommand(), d.Facts()
	in := outcomes.NodeOutcome{Mode: d.Mode(), ReleaseID: vc.ReleaseID, NodeID: vc.NodeID, Outcome: outcome,
		DBTLogURI: f.LogKey, RunResultsURI: f.RunResultsKey}
	if d.Mode() == model.ModeCompile {
		in.FailedContainer = f.FailedContainer
	}
	return o.deps.Outcomes.Record(ctx, u, in)
}

// reportRunning reports that a production attempt's main container started.
// The caller saves d.
func (o *Observer) reportRunning(ctx context.Context, u uow.UnitOfWork, d *model.Deployment) error {
	if d.Mode() != model.ModeProduction {
		return nil
	}
	return reports.Write(ctx, u.OutboxRepo(), d, pkgmodel.AttemptStatusRunning, "")
}

// errorMessage is the message a failed attempt reports: the log's (its result
// block's message, else its tail), else what Kubernetes said.
func (o *Observer) errorMessage(f model.AttemptFacts) string {
	if f.LogMessage != "" {
		return f.LogMessage
	}
	return o.truncate(f.Message)
}

// truncate cuts msg to ErrorMessageMaxLen.
func (o *Observer) truncate(msg string) string {
	return reports.TruncateMessage(msg, o.cfg.ErrorMessageMaxLen)
}
