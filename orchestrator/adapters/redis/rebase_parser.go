package redis

import (
	"github.com/carolsimone/continuo/orchestrator/domain/model"
	goredis "github.com/redis/go-redis/v9"
)

// ParseRebase translates a trigger.rebase:v1 XMessage into a RebaseInput. All
// three identifiers are required, non-empty strings; a missing one yields an
// events.ErrPermanent-wrapped error so the consumer ACKs the poison message.
// RunID is the new run's schedule_id; SourceRunID is the failed/cancelled
// source run whose :EXECUTES set the RebasePartition selector reads.
func ParseRebase(msg goredis.XMessage) (model.RebaseInput, error) {
	scheduleID, err := requireUUIDField(msg, "schedule_id")
	if err != nil {
		return model.RebaseInput{}, err
	}
	scheduleName, err := requireString(msg, "schedule_name")
	if err != nil {
		return model.RebaseInput{}, err
	}
	sourceRunID, err := requireUUIDField(msg, "source_run_id")
	if err != nil {
		return model.RebaseInput{}, err
	}
	return model.RebaseInput{
		RunID:        scheduleID,
		ScheduleName: scheduleName,
		SourceRunID:  sourceRunID,
	}, nil
}
