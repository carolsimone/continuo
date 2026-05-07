package redis

import (
	"fmt"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/google/uuid"
)

// ParseSchedulerStartedEvent extracts a domain.SchedulerStarted event from a
// Redis stream message's Values map. Missing kind defaults to "cron"; missing
// or empty source_run_id yields nil. Backward-compat with pre-PR0 state
// messages that don't carry the kind/source_run_id fields.
//
// Unit tests live in scheduler_started_parser_test.go.
func ParseSchedulerStartedEvent(values map[string]interface{}) (domain.SchedulerStarted, error) {
	runnerID, ok := values["runner_id"].(string)
	if !ok || runnerID == "" {
		return domain.SchedulerStarted{}, fmt.Errorf("missing or invalid runner_id")
	}
	schedulerID, err := uuid.Parse(runnerID)
	if err != nil {
		return domain.SchedulerStarted{}, fmt.Errorf("invalid runner_id UUID %q: %w", runnerID, err)
	}
	scheduleName, ok2 := values["schedule_name"].(string)
	if !ok2 || scheduleName == "" {
		return domain.SchedulerStarted{}, fmt.Errorf("missing or invalid schedule_name")
	}

	kind, _ := values["kind"].(string)
	if kind == "" {
		kind = "cron"
	}

	var sourceRunID *uuid.UUID
	if raw, ok := values["source_run_id"].(string); ok && raw != "" {
		parsed, err := uuid.Parse(raw)
		if err != nil {
			return domain.SchedulerStarted{}, fmt.Errorf("invalid source_run_id UUID %q: %w", raw, err)
		}
		sourceRunID = &parsed
	}

	return domain.SchedulerStarted{
		ScheduleID:   schedulerID,
		ScheduleName: scheduleName,
		Kind:         kind,
		SourceRunID:  sourceRunID,
	}, nil
}
