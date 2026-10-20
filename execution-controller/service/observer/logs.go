package observer

import (
	"context"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/pkg/validationresult"
)

// nothingToDo is what dbt prints when its selector matched no node.
const nothingToDo = "Nothing to do"

// readLogs reads the log of d's pod and uploads it, with the structured result
// block it printed when there is one, under keys named after the deployment;
// it returns the facts the log shows. It runs outside any transaction, under
// LogIOTimeout, and soft-fails: a log that cannot be read or uploaded leaves
// its facts empty.
func (o *Observer) readLogs(ctx context.Context, d *model.Deployment) model.AttemptFacts {
	ioCtx, cancel := context.WithTimeout(ctx, o.cfg.LogIOTimeout)
	defer cancel()

	fullLog, tail, err := o.deps.Logs.GetPodLogs(ioCtx, d.JobName(), o.cfg.LogTailLines)
	if err != nil {
		o.deps.Logger.Warn("Failed to read the pod log", "job_name", d.JobName(), "error", err)
		return model.AttemptFacts{}
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
	if block != "" {
		// A "success" block describes a run that completed before an
		// unrelated crash; its message is not the failure's.
		if r, err := validationresult.Parse([]byte(block)); err == nil && r.Status != "success" {
			blockMessage = strings.TrimSpace(r.Message)
		}
	}
	f := model.AttemptFacts{NothingToDo: strings.Contains(tail, nothingToDo), LogMessage: o.truncate(blockMessage)}
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

// artifactPath files a Job's log and run results under the node it ran,
// <service>/<schema>/<table>, or under <service>/compile for a release's
// compile leg, which runs no node: an object key may hold no empty segment.
func artifactPath(d *model.Deployment) string {
	if d.Mode() == model.ModeProduction {
		c := d.Command()
		return fmt.Sprintf("%s/%s/%s", c.ServiceName, c.SchemaName, c.TableName)
	}
	vc := d.ValidationCommand()
	if d.Mode() == model.ModeCompile {
		return vc.ServiceName + "/compile"
	}
	return fmt.Sprintf("%s/%s/%s", vc.ServiceName, vc.SchemaName, vc.TableName)
}
