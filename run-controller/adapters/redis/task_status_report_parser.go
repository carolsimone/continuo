package redis

import (
	"fmt"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// ParseTaskStatusReport decodes one task.status.updated:v2 entry, envelope
// fields plus a typed payload, into the report run-controller applies, with the tenant
// its envelope names. A decode failure (no envelope, an unknown
// schema_version, a run or task id that is not a UUID, a missing or negative
// attempt, an unknown status, a failure without a declared reason or with
// another class than its reason's) or an envelope without a tenant_id is
// permanent: the consumer dead-letters the entry. A fact the report leaves
// empty is not reported, so the attempt keeps what it holds.
func ParseTaskStatusReport(msg goredis.XMessage) (events.AttemptReported, error) {
	env, p, err := pkgevents.DecodeTaskStatusReport(stringFields(msg.Values))
	if err != nil {
		return events.AttemptReported{}, fmt.Errorf("%w: decode task status report (message %s): %v",
			pkgevents.ErrPermanent, msg.ID, err)
	}
	if env.TenantID == "" {
		return events.AttemptReported{}, fmt.Errorf("%w: task status report (message %s) carries no tenant_id",
			pkgevents.ErrPermanent, msg.ID)
	}
	runID, err := uuid.Parse(p.RunID)
	if err != nil {
		return events.AttemptReported{}, fmt.Errorf("%w: run_id %q (message %s): %v", pkgevents.ErrPermanent, p.RunID, msg.ID, err)
	}
	taskID, err := uuid.Parse(p.TaskID)
	if err != nil {
		return events.AttemptReported{}, fmt.Errorf("%w: task_id %q (message %s): %v", pkgevents.ErrPermanent, p.TaskID, msg.ID, err)
	}
	return events.AttemptReported{TenantID: env.TenantID, RunID: runID, Report: run.AttemptReport{
		TaskID:           taskID,
		Attempt:          p.Attempt,
		JobName:          p.JobName,
		Status:           run.AttemptStatus(p.Status),
		QueuedAt:         p.QueuedAt,
		StartedAt:        p.StartedAt,
		FinishedAt:       p.FinishedAt,
		ExecutionSeconds: p.ExecutionSeconds,
		ExitCode:         p.ExitCode,
		FailureReason:    model.FailureReason(p.FailureReason),
		ErrorMessage:     reported(p.ErrorMessage),
		LogS3Key:         reported(p.LogS3Key),
		RunResultsURI:    reported(p.RunResultsURI),
		ParseCache:       reported(p.ParseCache),
		ParseCacheReason: reported(p.ParseCacheReason),
	}}, nil
}

// reported is s as a reported fact: nil when the report leaves it empty.
func reported(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
