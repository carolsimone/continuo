package redis

import (
	"fmt"

	"github.com/carolsimone/continuo/orchestrator/domain"
	goredis "github.com/redis/go-redis/v9"
)

// ParseRunFinalized translates a run.finalized:v1 XMessage into a typed
// domain event. ScheduleID and Status are kept as strings since the
// Neo4j FinalizeRun call signature uses strings.
func ParseRunFinalized(msg goredis.XMessage) (domain.RunFinalized, error) {
	scheduleID, _ := msg.Values["schedule_id"].(string)
	status, _ := msg.Values["status"].(string)
	if scheduleID == "" || status == "" {
		return domain.RunFinalized{},
			fmt.Errorf("missing schedule_id or status (schedule_id=%q status=%q)", scheduleID, status)
	}
	return domain.RunFinalized{ScheduleID: scheduleID, Status: status}, nil
}
