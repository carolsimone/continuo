package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	goredis "github.com/redis/go-redis/v9"
)

// OutboxPublisher publishes orchestrator outbox entries to Redis streams.
// Each entry represents one event: its Payload is unmarshaled into a per-event
// field map and written via XADD to the stream named by entry.StreamName.
type OutboxPublisher struct {
	redis  *goredis.Client
	logger *slog.Logger
}

// NewOutboxPublisher constructs a publisher backed by a real Redis client.
func NewOutboxPublisher(r *goredis.Client, l *slog.Logger) *OutboxPublisher {
	return &OutboxPublisher{redis: r, logger: l}
}

// Publish dispatches on event type to build the Redis field map, then XADDs it
// to the stream stored in entry.StreamName.
func (p *OutboxPublisher) Publish(ctx context.Context, entry *outbox.Entry) error {
	values, err := p.payloadToValues(entry)
	if err != nil {
		return err
	}
	_, err = p.redis.XAdd(ctx, &goredis.XAddArgs{
		Stream: entry.StreamName,
		MaxLen: 10000,
		Approx: true,
		Values: values,
	}).Result()
	if err != nil {
		return fmt.Errorf("xadd to %s: %w", entry.StreamName, err)
	}
	return nil
}

// PayloadToValuesForTest is a test-only accessor that exposes payloadToValues
// for white-box unit tests without requiring a live Redis client.
func (p *OutboxPublisher) PayloadToValuesForTest(entry *outbox.Entry) (map[string]interface{}, error) {
	return p.payloadToValues(entry)
}

// payloadToValues builds the Redis stream field map for each known event type.
// The logic is identical to the former orchestrator outbox processor's switch,
// moved here without behavioral change.
func (p *OutboxPublisher) payloadToValues(entry *outbox.Entry) (map[string]interface{}, error) {
	switch entry.EventType {
	case "node_ready_for_execution":
		var evt domain.NodeReadyForExecution
		if err := json.Unmarshal(entry.Payload, &evt); err != nil {
			return nil, fmt.Errorf("unmarshal node_ready_for_execution: %w", err)
		}
		return map[string]interface{}{
			"outbox_entry_id":  entry.ID.String(),
			"schedule_id":      evt.ScheduleID,
			"schedule_name":    evt.ScheduleName,
			"service_name":     evt.ServiceName,
			"schema_name":      evt.SchemaName,
			"table_name":       evt.TableName,
			"task_id":          evt.TaskID,
			"job_name":         evt.JobName,
			"node_type":        evt.NodeType,
			"image_tag":        evt.ImageTag,
			"manifest_version": evt.ManifestVersion,
		}, nil

	case "cascade_task_skipped":
		var evt domain.CascadeTaskSkipped
		if err := json.Unmarshal(entry.Payload, &evt); err != nil {
			return nil, fmt.Errorf("unmarshal cascade_task_skipped: %w", err)
		}
		// A cascade-skipped node is reported on task.status.updated:v1 as a
		// terminal "skipped" task status. Serialize through the shared
		// TaskStatusUpdated.ToMap so every producer of this stream emits an
		// identical wire shape.
		values := pkgevents.TaskStatusUpdated{
			TaskID:     evt.TaskID,
			ScheduleID: evt.ScheduleID,
			Status:     "skipped",
			RetryCount: 0,
		}.ToMap()
		values["outbox_entry_id"] = entry.ID.String()
		return values, nil

	case "run_entries_dispatched", "run_entries_dispatch_failed",
		"release_promoted":
		// These event types carry a self-contained JSON payload that downstream
		// consumers decode directly from the "payload" field.
		return map[string]interface{}{
			"outbox_entry_id": entry.ID.String(),
			"payload":         string(entry.Payload),
		}, nil

	default:
		return nil, fmt.Errorf("orchestrator publisher: unknown event_type %q", entry.EventType)
	}
}
