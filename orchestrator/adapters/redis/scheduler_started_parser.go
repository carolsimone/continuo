package redis

import (
	"fmt"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/google/uuid"
)

// ParseSchedulerStartedEvent extracts a domain.SchedulerStarted event from a
// Redis stream message's Values map. Missing kind defaults to "cron"; missing
// or empty source_run_id yields nil. Parse failures are events.ErrPermanent so
// the consumer ACKs the poison message instead of retrying it.
//
// Unit tests live in scheduler_started_parser_test.go.
func ParseSchedulerStartedEvent(values map[string]interface{}) (domain.SchedulerStarted, error) {
	runnerID, ok := values["runner_id"].(string)
	if !ok || runnerID == "" {
		return domain.SchedulerStarted{}, fmt.Errorf("%w: missing or invalid runner_id", events.ErrPermanent)
	}
	schedulerID, err := uuid.Parse(runnerID)
	if err != nil {
		return domain.SchedulerStarted{}, fmt.Errorf("%w: invalid runner_id UUID %q: %v", events.ErrPermanent, runnerID, err)
	}
	scheduleName, ok2 := values["schedule_name"].(string)
	if !ok2 || scheduleName == "" {
		return domain.SchedulerStarted{}, fmt.Errorf("%w: missing or invalid schedule_name", events.ErrPermanent)
	}

	kind, _ := values["kind"].(string)
	if kind == "" {
		kind = "cron"
	}

	var sourceRunID *uuid.UUID
	if raw, ok := values["source_run_id"].(string); ok && raw != "" {
		parsed, err := uuid.Parse(raw)
		if err != nil {
			return domain.SchedulerStarted{}, fmt.Errorf("%w: invalid source_run_id UUID %q: %v", events.ErrPermanent, raw, err)
		}
		sourceRunID = &parsed
	}

	rawInitiatedBy, _ := values["initiated_by"].(string)
	initiatedBy := identity.OrSystem(rawInitiatedBy)

	return domain.SchedulerStarted{
		ScheduleID:   schedulerID,
		ScheduleName: scheduleName,
		Kind:         kind,
		SourceRunID:  sourceRunID,
		InitiatedBy:  initiatedBy,
	}, nil
}
