package redis

import (
	"github.com/carolsimone/continuo/orchestrator/domain/model"
	goredis "github.com/redis/go-redis/v9"
)

// ParseRerun translates a trigger.rerun:v1 XMessage into a RerunInput. All
// three identifiers are required, non-empty strings; a missing one yields an
// events.ErrPermanent-wrapped error so the consumer ACKs the poison message.
// RunID is the schedule_id of the new run (the Snapshot target); SourceRunID is
// the source run's schedule_id read by the SourcePinnedDAG selector.
func ParseRerun(msg goredis.XMessage) (model.RerunInput, error) {
	scheduleID, err := requireUUIDField(msg, "schedule_id")
	if err != nil {
		return model.RerunInput{}, err
	}
	scheduleName, err := requireString(msg, "schedule_name")
	if err != nil {
		return model.RerunInput{}, err
	}
	sourceRunID, err := requireUUIDField(msg, "source_run_id")
	if err != nil {
		return model.RerunInput{}, err
	}
	return model.RerunInput{
		RunID:        scheduleID,
		ScheduleName: scheduleName,
		SourceRunID:  sourceRunID,
		InitiatedBy:  optionalInitiatedBy(msg),
	}, nil
}
