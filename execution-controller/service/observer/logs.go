package observer

import (
	"context"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/validationresult"
)

// nothingToDo is what dbt prints when its selector matched no node. Only the
// log of a node that is not Python is read for it: a Python task may print the
// same words as ordinary output.
const nothingToDo = "Nothing to do"

// readLogs reads the log of d's pod and uploads it, with the structured result
// block it printed when there is one, under keys named after the deployment;
// it returns the facts the log shows. A seed attempt or seed-build leg whose
// command carries a dependents check also has the dependents-check container's log
// read, which adds the dependents check's findings and, for a production attempt in
// check mode, the reason it stopped the load. The
// dbt container's result is read only from the pod's main log, never from the
// dependents check's. It runs outside any transaction, under LogIOTimeout, and
// soft-fails: a log that cannot be read or uploaded leaves its own facts
// empty, and the dependents check's log is read even when the pod's log cannot be.
func (o *Observer) readLogs(ctx context.Context, d *model.Deployment) model.AttemptFacts {
	ioCtx, cancel := context.WithTimeout(ctx, o.cfg.LogIOTimeout)
	defer cancel()

	fullLog, tail, err := o.deps.Logs.GetPodLogs(ioCtx, d.JobName(), o.cfg.LogTailLines)
	if err != nil {
		o.deps.Logger.Warn("Failed to read the pod log", "job_name", d.JobName(), "error", err)
	}
	// The result block, printed by validation pods and python-family
	// containers, is cut out of the text log. The full log and the tail are
	// read separately and each can fail on its own, so a block missing from an
	// empty full log is looked for in the tail, for the message only.
	cleanLog, structured := validationresult.Split(fullLog)
	block := structured
	if block == "" {
		_, block = validationresult.Split(tail)
	}
	var blockMessage string
	var blockReason pkgmodel.FailureReason
	if block != "" {
		// A "success" block describes a run that completed before an
		// unrelated crash; its message and reason are not the failure's.
		if r, err := validationresult.Parse([]byte(block)); err == nil && r.Status != "success" {
			blockMessage = strings.TrimSpace(r.Message)
			blockReason = model.ReasonFromResultBlock(r.Reason)
		}
	}
	f := model.AttemptFacts{NothingToDo: !d.NodeType().IsPython() && strings.Contains(tail, nothingToDo), LogMessage: o.truncate(blockMessage), Reason: blockReason}
	if p := o.readDependentsCheck(ioCtx, d); p != nil {
		f.DependentsCheck = p.findings
		if p.reason != "" {
			f.Reason, f.LogMessage = p.reason, o.truncate(p.message)
		}
	}
	if f.LogMessage == "" {
		f.LogMessage = o.truncate(tail)
	}

	path := artifactPath(d)
	if cleanLog != "" {
		key := fmt.Sprintf("logs/task-executions/%s/%s.log", path, d.ID())
		if err := o.deps.Uploader.UploadLog(ioCtx, key, cleanLog); err != nil {
			o.deps.Logger.Warn("Failed to upload the pod log", "job_name", d.JobName(), "key", key, "error", err)
		} else {
			f.LogKey = key
		}
	}
	if structured != "" {
		key := fmt.Sprintf("run-results/task-executions/%s/%s.json", path, d.ID())
		if err := o.deps.Uploader.UploadLog(ioCtx, key, structured); err != nil {
			o.deps.Logger.Warn("Failed to upload the run results", "job_name", d.JobName(), "key", key, "error", err)
		} else {
			f.RunResultsKey = key
		}
	}
	return f
}

// dependentsCheckResult is what a seed attempt's dependents check container printed.
type dependentsCheckResult struct {
	findings *seedreload.Findings
	// reason is why the dependents check stopped the load, decided only for a
	// check-mode block that did not succeed and names a reason; message is
	// that block's message.
	reason  pkgmodel.FailureReason
	message string
}

// dependentsCheckOf is the dependents check d's pod runs: a production attempt's seed
// dispatch asks for one, and so may a seed-build leg's request. Any other
// deployment runs none.
func dependentsCheckOf(d *model.Deployment) *seedreload.DependentsCheck {
	switch d.Mode() {
	case model.ModeProduction:
		return d.Command().DependentsCheck
	case model.ModeSeedBuild:
		return d.ValidationCommand().DependentsCheck
	default:
		return nil
	}
}

// readDependentsCheck reads the log of d's dependents-check container and parses
// its result block. It returns nil when d's command carries no dependents check, the
// log cannot be read or it holds no dependents check block, which leaves the
// attempt to the usual classification of its exit code. The mode is the one in
// d's command, not the block's: a report-mode block never decides a reason, and
// a seed-build leg's pod always runs in report mode, so its findings are
// recorded and never a reason.
func (o *Observer) readDependentsCheck(ctx context.Context, d *model.Deployment) *dependentsCheckResult {
	dependentsCheck := dependentsCheckOf(d)
	if dependentsCheck == nil {
		return nil
	}
	log, err := o.deps.Logs.GetContainerLog(ctx, d.JobName(), model.DependentsCheckContainer)
	if err != nil {
		o.deps.Logger.Warn("Failed to read the dependents check log", "job_name", d.JobName(), "error", err)
		return nil
	}
	_, raw := validationresult.Split(log)
	if raw == "" {
		return nil
	}
	p, err := validationresult.ParseWarehouseRead([]byte(raw))
	if err != nil || p.Op != validationresult.OpDependentsCheck {
		return nil
	}
	res := &dependentsCheckResult{findings: findingsOf(p)}
	if d.Mode() == model.ModeProduction && dependentsCheck.Mode == seedreload.CheckMode && p.Status != "success" {
		res.reason, res.message = model.ReasonFromResultBlock(p.Reason), p.Message
	}
	return res
}

// findingsOf is what the dependents check block p found in the warehouse.
func findingsOf(p *validationresult.WarehouseRead) *seedreload.Findings {
	f := &seedreload.Findings{DropSemantics: p.DropSemantics, Completeness: p.Completeness, Checked: p.Checked}
	for _, d := range p.Dependents {
		f.Dependents = append(f.Dependents, seedreload.Dependent(d))
	}
	return f
}

// artifactPath files a Job's log and run results under the node it ran,
// <project>/<schema>/<table>, or under <project>/compile for a release's
// compile leg, which runs no node: an object key may hold no empty segment.
func artifactPath(d *model.Deployment) string {
	if d.Mode() == model.ModeProduction {
		c := d.Command()
		return fmt.Sprintf("%s/%s/%s", c.ProjectName, c.SchemaName, c.TableName)
	}
	vc := d.ValidationCommand()
	if d.Mode() == model.ModeCompile {
		return vc.ProjectName + "/compile"
	}
	return fmt.Sprintf("%s/%s/%s", vc.ProjectName, vc.SchemaName, vc.TableName)
}
