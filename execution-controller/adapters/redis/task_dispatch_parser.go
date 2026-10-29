package redis

import (
	"fmt"

	"github.com/carolsimone/continuo/execution-controller/domain/events"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// ParseTaskDispatch decodes one task.dispatch:v1 entry, envelope fields plus a
// typed payload. An entry whose run, task or attempt cannot be read never
// becomes valid, so the error is permanent and the consumer dead-letters it; so
// is an entry that names no tenant, whose attempt's reports could not be
// published under one. The other fields are returned as run-controller wrote them; the
// handler records an attempt whose fields cannot run as failed.
func ParseTaskDispatch(msg goredis.XMessage) (events.TaskDispatch, error) {
	env, p, err := pkgevents.DecodeTaskDispatch(stringFields(msg.Values))
	if err != nil {
		return events.TaskDispatch{}, fmt.Errorf("%w: decode task dispatch (message %s): %v", pkgevents.ErrPermanent, msg.ID, err)
	}
	if env.TenantID == "" {
		return events.TaskDispatch{}, fmt.Errorf("%w: task dispatch (message %s) names no tenant", pkgevents.ErrPermanent, msg.ID)
	}
	return events.TaskDispatch{
		TenantID: env.TenantID, RunID: uuid.MustParse(p.RunID), TaskID: uuid.MustParse(p.TaskID), Attempt: int(p.Attempt),
		KnownStatus: p.KnownStatus, ScheduleName: p.ScheduleName,
		ServiceName: p.ServiceName, SchemaName: p.SchemaName, TableName: p.TableName,
		NodeType: p.NodeType, Operation: p.Operation, ImageTag: p.ImageTag, SecretRef: p.SecretRef,
		JobName: p.JobName, TimeoutSeconds: p.TimeoutSeconds,
	}, nil
}
