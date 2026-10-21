package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/service/validation"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
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

var (
	_ outbox.Publisher = (*OutboxPublisher)(nil)
	_ outbox.Renderer  = (*OutboxPublisher)(nil)
)

// NewOutboxPublisher creates an OutboxPublisher wired to the given Redis client.
func NewOutboxPublisher(r *goredis.Client, l *slog.Logger) *OutboxPublisher {
	return &OutboxPublisher{redis: r, logger: l}
}

// Publish routes an outbox row to Redis. Every event type XADDs a field map to
// entry.StreamName without a length cap (the dead-letter-controller's trim loop
// bounds every stream), except check_delayed: a status-check ticket a previous
// version wrote, which is published nowhere and so marked processed, since the
// observer follows every Job without it.
func (p *OutboxPublisher) Publish(ctx context.Context, entry *outbox.Entry) error {
	if entry.EventType == event.EventTypeCheckDelayed {
		return nil
	}
	values, err := p.Render(entry)
	if err != nil {
		return err
	}
	if _, err := p.redis.XAdd(ctx, p.xaddArgs(entry, values)).Result(); err != nil {
		return fmt.Errorf("xadd to %s: %w", entry.StreamName, err)
	}
	return nil
}

// xaddArgs builds the XADD for entry from its rendered fields. outbox_entry_id
// rides every XADD so consumer-side DedupWithOutboxEntryID catches a row
// republished with a fresh Redis message id. It sets no length cap.
func (p *OutboxPublisher) xaddArgs(entry *outbox.Entry, values map[string]any) *goredis.XAddArgs {
	values["outbox_entry_id"] = entry.ID.String()
	return &goredis.XAddArgs{
		Stream: entry.StreamName,
		Values: values,
	}
}

// Render returns the field map Publish XADDs for entry, without
// outbox_entry_id. A check_delayed row is published nowhere, so it has no
// fields to render and returns an error. Every call returns a fresh map.
func (p *OutboxPublisher) Render(entry *outbox.Entry) (map[string]any, error) {
	if entry.EventType == event.EventTypeCheckDelayed {
		return nil, fmt.Errorf("%s rows are published nowhere", event.EventTypeCheckDelayed)
	}
	values, err := p.toValues(entry)
	if err != nil {
		return nil, err
	}
	delete(values, "outbox_entry_id")
	return values, nil
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

	case validation.EventTypeValidationCompleted, validation.EventTypeSeedBuildCompleted, validation.EventTypeCompileCompleted,
		validation.EventTypeValidationNodeResult:
		// Candidate-leg events carry their body as a single JSON "payload" field;
		// the stored payload is already that body.
		return map[string]interface{}{"payload": string(entry.Payload)}, nil

	default:
		return nil, fmt.Errorf("execution publisher: unknown event_type %q (retryable: a newer replica may handle it during a rolling upgrade)", entry.EventType)
	}
}
