package redis

import (
	"fmt"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// ParseScheduleCancelled translates a schedule.cancelled:v1 XMessage
// into a typed domain event. All errors are permanent.
func ParseScheduleCancelled(msg goredis.XMessage) (events.ScheduleCancelled, error) {
	idStr := stringField(msg.Values, "schedule_id")
	if idStr == "" {
		return events.ScheduleCancelled{}, fmt.Errorf("missing schedule_id")
	}
	id, err := uuid.Parse(idStr)
	if err != nil {
		return events.ScheduleCancelled{}, fmt.Errorf("invalid schedule_id: %w", err)
	}
	return events.ScheduleCancelled{ScheduleID: id}, nil
}
