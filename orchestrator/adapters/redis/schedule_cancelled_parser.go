package redis

import (
	"fmt"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// ParseScheduleCancelled translates a schedule.cancelled:v1 XMessage into a
// typed domain event. Parse failures are events.ErrPermanent — the consumer
// ACKs + drops the poison message on these.
func ParseScheduleCancelled(msg goredis.XMessage) (domain.ScheduleCancelled, error) {
	idStr, _ := msg.Values["schedule_id"].(string)
	if idStr == "" {
		return domain.ScheduleCancelled{}, fmt.Errorf("%w: missing schedule_id", events.ErrPermanent)
	}
	id, err := uuid.Parse(idStr)
	if err != nil {
		return domain.ScheduleCancelled{}, fmt.Errorf("%w: invalid schedule_id %q: %v", events.ErrPermanent, idStr, err)
	}
	return domain.ScheduleCancelled{ScheduleID: id, CancelledBy: optionalUserField(msg, "cancelled_by")}, nil
}
