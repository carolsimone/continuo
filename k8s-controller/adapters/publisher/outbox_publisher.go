package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/carolsimone/continuo/k8s-controller/domain/event"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/pkg/outbox"
	goredis "github.com/redis/go-redis/v9"
)

// OutboxPublisher implements pkg/outbox.Publisher for k8s-controller. Each row
// publishes exactly one event to entry.StreamName; the typed payload depends
// on entry.EventType. Multi-effect operations (e.g., a task completion that
// publishes status, execution record, and node-status-updated) are modeled as
// multiple canonical rows written in one transaction at the call site (D1),
// not as fanout inside this Publisher.
type OutboxPublisher struct {
	redis  *goredis.Client
	logger *slog.Logger
}

// NewOutboxPublisher creates an OutboxPublisher wired to the given Redis client.
func NewOutboxPublisher(r *goredis.Client, l *slog.Logger) *OutboxPublisher {
	return &OutboxPublisher{redis: r, logger: l}
}

// Publish unmarshals entry.Payload into the typed event struct for
// entry.EventType and XADDs the resulting field map to entry.StreamName.
func (p *OutboxPublisher) Publish(ctx context.Context, entry *outbox.Entry) error {
	values, err := p.toValues(entry)
	if err != nil {
		return err
	}
	// Inject outbox_entry_id on every XADD so consumer-side
	// DedupWithOutboxEntryID can catch Processor-crash redeliveries (same
	// outbox row republished with a fresh Redis msg_id).
	values["outbox_entry_id"] = entry.ID.String()
	if _, err := p.redis.XAdd(ctx, &goredis.XAddArgs{
		Stream: entry.StreamName,
		Values: values,
	}).Result(); err != nil {
		return fmt.Errorf("xadd to %s: %w", entry.StreamName, err)
	}
	return nil
}

// toValues routes entry.EventType to the matching typed struct, unmarshals
// entry.Payload, and returns the flat field map for XADD.
func (p *OutboxPublisher) toValues(entry *outbox.Entry) (map[string]interface{}, error) {
	switch entry.EventType {
	case "task_status_updated":
		var e pkgevents.TaskStatusUpdated
		if err := json.Unmarshal(entry.Payload, &e); err != nil {
			return nil, fmt.Errorf("unmarshal task_status_updated: %w", err)
		}
		return e.ToMap(), nil

	case "task_execution_recorded":
		var e pkgevents.TaskExecutionRecorded
		if err := json.Unmarshal(entry.Payload, &e); err != nil {
			return nil, fmt.Errorf("unmarshal task_execution_recorded: %w", err)
		}
		return e.ToMap(), nil

	case "task_retry":
		var e event.TaskRetry
		if err := json.Unmarshal(entry.Payload, &e); err != nil {
			return nil, fmt.Errorf("unmarshal task_retry: %w", err)
		}
		return e.ToMap(), nil

	case "task_failed":
		var e event.TaskFailed
		if err := json.Unmarshal(entry.Payload, &e); err != nil {
			return nil, fmt.Errorf("unmarshal task_failed: %w", err)
		}
		return e.ToMap(), nil

	case "check_delayed":
		var e event.JobCheckRequest
		if err := json.Unmarshal(entry.Payload, &e); err != nil {
			return nil, fmt.Errorf("unmarshal check_delayed: %w", err)
		}
		// The typed event travels in the JSON payload; check_after stays a flat
		// field so the binding's delay gate can read it without decoding the
		// payload, and so re-circulated copies preserve the schedule.
		retryCount, err := num.Int32(e.RetryCount, "retry_count")
		if err != nil {
			return nil, fmt.Errorf("check.k8s payload: %w", err)
		}
		maxRetries, err := num.Int32(e.MaxRetries, "max_retries")
		if err != nil {
			return nil, fmt.Errorf("check.k8s payload: %w", err)
		}
		payload, err := json.Marshal(pkgevents.CheckK8s{
			TaskID:           e.TaskID,
			ScheduleID:       e.ScheduleID,
			ScheduleName:     e.ScheduleName,
			ServiceName:      e.ServiceName,
			SchemaName:       e.SchemaName,
			TableName:        e.TableName,
			JobName:          e.JobName,
			NodeType:         e.NodeType,
			ImageTag:         e.ImageTag,
			Operation:        e.Operation,
			RetryCount:       retryCount,
			MaxRetries:       maxRetries,
			RunningAnnounced: e.RunningAnnounced,
		})
		if err != nil {
			return nil, fmt.Errorf("marshal check.k8s payload: %w", err)
		}
		return map[string]interface{}{
			"payload":     string(payload),
			"check_after": strconv.FormatInt(e.CheckAfter, 10),
		}, nil

	case "node_status_updated":
		var e event.NodeStatusUpdated
		if err := json.Unmarshal(entry.Payload, &e); err != nil {
			return nil, fmt.Errorf("unmarshal node_status_updated: %w", err)
		}
		return e.ToMap(), nil

	case event.EventTypeValidationNodeCompleted, event.EventTypeSeedBuildNodeCompleted, event.EventTypeCompileNodeCompleted:
		// The three candidate-leg node-completed events
		// (validation.node.completed:v1 / seed.build.node.completed:v1 /
		// compile.node.completed:v1) each carry the per-node result as a single
		// JSON "payload" field (release_id, node_id, outcome, ...) — the shape
		// executor-controller's Parse*NodeCompleted decoders expect. The stored
		// payload is already that body; re-emit it verbatim.
		return map[string]interface{}{"payload": string(entry.Payload)}, nil

	default:
		return nil, fmt.Errorf("k8s publisher: unknown event_type %q", entry.EventType)
	}
}
