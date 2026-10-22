// Package reports writes execution-controller's task.status.updated:v2
// reports of production attempts to the outbox, in the caller's transaction.
package reports

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/serialization"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// Write reports d's attempt with status, from everything observed of it and,
// for a failure, errorMessage: it keeps the report as d's last report, which
// the caller saves with d, and writes it to the outbox.
func Write(ctx context.Context, repo outbox.Repository, d *model.Deployment, status pkgmodel.AttemptStatus, errorMessage string) error {
	r := d.ReportOf(status, errorMessage)
	d.RecordReport(r)
	return write(ctx, repo, d, r)
}

// Resend writes d's last report to the outbox again, as the answer to a
// repeated dispatch of its attempt. An attempt with none yet is reported
// queued.
func Resend(ctx context.Context, repo outbox.Repository, d *model.Deployment) error {
	if last := d.LastReport(); last != nil {
		return write(ctx, repo, d, *last)
	}
	return Write(ctx, repo, d, pkgmodel.AttemptStatusQueued, "")
}

func write(ctx context.Context, repo outbox.Repository, d *model.Deployment, r model.Report) error {
	cmd := d.Command()
	taskID, err := uuid.Parse(cmd.TaskID)
	if err != nil {
		return fmt.Errorf("task_id %q: %w", cmd.TaskID, err)
	}
	payload, err := json.Marshal(serialization.ReportFromDomain(cmd, r))
	if err != nil {
		return fmt.Errorf("marshal the %s report of task %s: %w", r.Status, cmd.TaskID, err)
	}
	if err := repo.Create(ctx, &outbox.Entry{MessageProcessingID: d.MessageProcessingID(), AggregateType: "task",
		AggregateID: taskID, EventType: event.EventTypeTaskStatusReport, Payload: payload,
		StreamName: streams.TaskStatusUpdatedV2}); err != nil {
		return fmt.Errorf("write the %s report of attempt %d of task %s: %w", r.Status, cmd.Attempt, cmd.TaskID, err)
	}
	return nil
}
