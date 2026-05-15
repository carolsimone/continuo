package redis

import (
	"fmt"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// ParseScheduleCancelled translates a schedule.cancelled:v1 XMessage into a
// typed domain event. Errors are parse-permanent — the binding returns nil
// (ACK + drop) on these.
func ParseScheduleCancelled(msg goredis.XMessage) (domain.ScheduleCancelled, error) {
	idStr, _ := msg.Values["schedule_id"].(string)
	if idStr == "" {
		return domain.ScheduleCancelled{}, fmt.Errorf("missing schedule_id")
	}
	id, err := uuid.Parse(idStr)
	if err != nil {
		return domain.ScheduleCancelled{}, fmt.Errorf("invalid schedule_id %q: %w", idStr, err)
	}
	return domain.ScheduleCancelled{ScheduleID: id}, nil
}
