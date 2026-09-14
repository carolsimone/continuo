package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/execution-controller/adapters/delayqueue"
	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/serialization"
	"github.com/carolsimone/continuo/execution-controller/service/validation"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

// OutboxPublisher implements pkg/outbox.Publisher. Each execution_outbox row
// publishes exactly one event to entry.StreamName; the typed payload depends on
// entry.EventType. Multi-effect operations are modelled as several rows written
// in one transaction at the call site, never as fan-out here.
type OutboxPublisher struct {
	redis  *goredis.Client
	logger *slog.Logger
}

var _ outbox.Publisher = (*OutboxPublisher)(nil)

// NewOutboxPublisher creates an OutboxPublisher wired to the given Redis client.
func NewOutboxPublisher(r *goredis.Client, l *slog.Logger) *OutboxPublisher {
	return &OutboxPublisher{redis: r, logger: l}
}

// Publish routes an outbox row to Redis. Every event type XADDs a field map to
// entry.StreamName, capped at streams.StreamMaxLen with approximate trimming,
// except check_delayed: that row is written to the delay queue (ticket + due
// time) so a not-yet-due check waits off the stream until the promoter moves it.
func (p *OutboxPublisher) Publish(ctx context.Context, entry *outbox.Entry) error {
	if entry.EventType == event.EventTypeCheckDelayed {
		return p.scheduleDelayedCheck(ctx, entry)
	}
	values, err := p.toValues(entry)
	if err != nil {
		return err
	}
	// outbox_entry_id rides every XADD so consumer-side DedupWithOutboxEntryID
	// catches a row republished with a fresh Redis message id.
	values["outbox_entry_id"] = entry.ID.String()
	if _, err := p.redis.XAdd(ctx, &goredis.XAddArgs{
		Stream: entry.StreamName,
		MaxLen: streams.StreamMaxLen,
		Approx: true,
		Values: values,
	}).Result(); err != nil {
		return fmt.Errorf("xadd to %s: %w", entry.StreamName, err)
	}
	return nil
}

// scheduleDelayedCheck converts a check_delayed row into the typed CheckK8s
// payload and enqueues it in the delay queue keyed by job name.
func (p *OutboxPublisher) scheduleDelayedCheck(ctx context.Context, entry *outbox.Entry) error {
	var dto serialization.JobCheckRequestDTO
	if err := json.Unmarshal(entry.Payload, &dto); err != nil {
		return fmt.Errorf("%w: unmarshal check_delayed: %v", pkgevents.ErrPermanent, err)
	}
	e := dto.ToDomain()
	retryCount, err := num.Int32(e.RetryCount, "retry_count")
	if err != nil {
		return fmt.Errorf("%w: check.k8s payload: %v", pkgevents.ErrPermanent, err)
	}
	maxRetries, err := num.Int32(e.MaxRetries, "max_retries")
	if err != nil {
		return fmt.Errorf("%w: check.k8s payload: %v", pkgevents.ErrPermanent, err)
	}
	payload, err := json.Marshal(pkgevents.CheckK8s{
		TaskID: e.TaskID, ScheduleID: e.ScheduleID, ScheduleName: e.ScheduleName,
		ServiceName: e.ServiceName, SchemaName: e.SchemaName, TableName: e.TableName,
		JobName: e.JobName, NodeType: e.NodeType, ImageTag: e.ImageTag, Operation: e.Operation,
		RetryCount: retryCount, MaxRetries: maxRetries, RunningAnnounced: e.RunningAnnounced,
	})
	if err != nil {
		return fmt.Errorf("marshal check.k8s payload: %w", err)
	}
	return delayqueue.Schedule(ctx, p.redis, e.JobName, entry.ID.String(), string(payload), e.CheckAfter)
}

// toValues routes entry.EventType to its typed struct and returns the field map
// for XADD. An unknown event type is a retryable error, not a dead-letter:
// during a rolling upgrade an older replica can dequeue a row a newer replica
// will publish once it cycles back.
func (p *OutboxPublisher) toValues(entry *outbox.Entry) (map[string]interface{}, error) {
	switch entry.EventType {
	case outbox.DeadLetterEventType:
		values, err := outbox.DeadLetterValues(entry)
		if err != nil {
			return nil, fmt.Errorf("%w: dead-letter values: %v", pkgevents.ErrPermanent, err)
		}
		return values, nil

	case event.EventTypeTaskStatusUpdated:
		var e pkgevents.TaskStatusUpdated
		if err := json.Unmarshal(entry.Payload, &e); err != nil {
			return nil, fmt.Errorf("%w: unmarshal task_status_updated: %v", pkgevents.ErrPermanent, err)
		}
		return e.ToMap(), nil

	case event.EventTypeTaskExecutionRecorded:
		var e pkgevents.TaskExecutionRecorded
		if err := json.Unmarshal(entry.Payload, &e); err != nil {
			return nil, fmt.Errorf("%w: unmarshal task_execution_recorded: %v", pkgevents.ErrPermanent, err)
		}
		return e.ToMap(), nil

	case event.EventTypeNodeUpdated:
		var dto serialization.NodeUpdatedDTO
		if err := json.Unmarshal(entry.Payload, &dto); err != nil {
			return nil, fmt.Errorf("%w: unmarshal node_updated: %v", pkgevents.ErrPermanent, err)
		}
		return dto.ToDomain().ToMap(), nil

	case event.EventTypeTaskRetry:
		var dto serialization.TaskRetryDTO
		if err := json.Unmarshal(entry.Payload, &dto); err != nil {
			return nil, fmt.Errorf("%w: unmarshal task_retry: %v", pkgevents.ErrPermanent, err)
		}
		return dto.ToDomain().ToMap(), nil

	case event.EventTypeTaskFailed:
		var dto serialization.TaskFailedDTO
		if err := json.Unmarshal(entry.Payload, &dto); err != nil {
			return nil, fmt.Errorf("%w: unmarshal task_failed: %v", pkgevents.ErrPermanent, err)
		}
		return dto.ToDomain().ToMap(), nil

	case event.EventTypeValidationNodeCompleted, event.EventTypeSeedBuildNodeCompleted, event.EventTypeCompileNodeCompleted,
		validation.EventTypeValidationCompleted, validation.EventTypeSeedBuildCompleted, validation.EventTypeCompileCompleted,
		validation.EventTypeValidationNodeResult:
		// Candidate-leg events carry their body as a single JSON "payload" field;
		// the stored payload is already that body.
		return map[string]interface{}{"payload": string(entry.Payload)}, nil

	default:
		return nil, fmt.Errorf("execution publisher: unknown event_type %q (retryable: a newer replica may handle it during a rolling upgrade)", entry.EventType)
	}
}
