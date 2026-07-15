package redis

import (
	"encoding/json"
	"fmt"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// ParseRunEntriesDispatchFailed translates a run.entries.dispatch_failed:v1
// XMessage into a typed domain event. All errors are parse-permanent — the
// binding wraps with events.ErrPermanent.
func ParseRunEntriesDispatchFailed(msg goredis.XMessage) (events.RunEntriesDispatchFailed, error) {
	payloadStr, _ := msg.Values["payload"].(string)
	if payloadStr == "" {
		return events.RunEntriesDispatchFailed{}, fmt.Errorf("missing payload field")
	}
	var wire pkgevents.RunEntriesDispatchFailed
	if err := json.Unmarshal([]byte(payloadStr), &wire); err != nil {
		return events.RunEntriesDispatchFailed{}, fmt.Errorf("invalid JSON: %w", err)
	}
	scheduleID, err := uuid.Parse(wire.ScheduleID)
	if err != nil {
		return events.RunEntriesDispatchFailed{}, fmt.Errorf("invalid schedule_id: %w", err)
	}
	return events.RunEntriesDispatchFailed{
		ScheduleID:   scheduleID,
		ScheduleName: wire.ScheduleName,
		Reason:       wire.Reason,
		Benign:       wire.Reason == pkgevents.DispatchFailedReasonNoTests,
	}, nil
}
