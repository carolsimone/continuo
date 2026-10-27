package redis

import (
	"fmt"

	"github.com/carolsimone/continuo/execution-controller/domain/events"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// ParseRunCancelled decodes one run.cancelled:v1 entry. An entry that cannot
// be read never becomes readable, so every error is permanent and the
// consumer dead-letters the entry.
func ParseRunCancelled(msg goredis.XMessage) (events.RunCancelled, error) {
	env, p, err := pkgevents.DecodeRunCancelled(stringFields(msg.Values))
	if err != nil {
		return events.RunCancelled{}, fmt.Errorf("%w: decode run cancel (message %s): %v", pkgevents.ErrPermanent, msg.ID, err)
	}
	if env.TenantID == "" {
		return events.RunCancelled{}, fmt.Errorf("%w: run cancel (message %s) names no tenant", pkgevents.ErrPermanent, msg.ID)
	}
	attempts := make([]events.AttemptRef, len(p.Attempts))
	for i, a := range p.Attempts {
		attempts[i] = events.AttemptRef{TaskID: a.TaskID, Attempt: int(a.Attempt), JobName: a.JobName}
	}
	return events.RunCancelled{TenantID: env.TenantID, RunID: uuid.MustParse(p.RunID), CancelledBy: p.CancelledBy, Reason: p.Reason,
		Attempts: attempts}, nil
}
