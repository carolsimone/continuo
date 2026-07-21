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

// streamMaxLen caps each published stream at roughly this many entries
// (Approx/~ trimming). It bounds Redis memory for streams a consumer group may
// lag on. Trimming can drop the oldest entries before a lagging group reads
// them; 10000 is the accepted bound across producers.
const streamMaxLen = 10000

// Publish dispatches on event type to build the Redis field map, then XADDs it
// to the stream stored in entry.StreamName.
func (p *OutboxPublisher) Publish(ctx context.Context, entry *outbox.Entry) error {
	args, err := p.xaddArgs(entry)
	if err != nil {
		return err
	}
	if _, err := p.redis.XAdd(ctx, args).Result(); err != nil {
		return fmt.Errorf("xadd to %s: %w", entry.StreamName, err)
	}
	return nil
}

// PublishBatch pipelines one XADD per entry over a single Redis round trip and
// returns one error per entry, positionally aligned with entries. Commands are
// queued in slice order on a single connection, so per-aggregate FIFO (imposed
// by the outbox SELECT order) is preserved. A payload that fails to encode is
// reported in its slot without aborting the rest of the batch.
func (p *OutboxPublisher) PublishBatch(ctx context.Context, entries []*outbox.Entry) []error {
	errs := make([]error, len(entries))
	pipe := p.redis.Pipeline()
	cmds := make([]*goredis.StringCmd, len(entries))
	for i, entry := range entries {
		args, err := p.xaddArgs(entry)
		if err != nil {
			errs[i] = err
			continue
		}
		cmds[i] = pipe.XAdd(ctx, args)
	}
	// Exec flushes every queued command. A transport-level error is surfaced
	// per command below; we ignore Exec's aggregate error and read each result.
	_, _ = pipe.Exec(ctx)
	for i, cmd := range cmds {
		if cmd == nil {
			continue // encode failure already recorded
		}
		if err := cmd.Err(); err != nil {
			errs[i] = fmt.Errorf("xadd to %s: %w", entries[i].StreamName, err)
		}
	}
	return errs
}

// xaddArgs builds the XADD arguments for one entry, including the shared MaxLen
// cap so single and batched publishes trim identically.
func (p *OutboxPublisher) xaddArgs(entry *outbox.Entry) (*goredis.XAddArgs, error) {
	values, err := p.payloadToValues(entry)
	if err != nil {
		return nil, err
	}
	return &goredis.XAddArgs{
		Stream: entry.StreamName,
		MaxLen: streamMaxLen,
		Approx: true,
		Values: values,
	}, nil
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
	case outbox.DeadLetterEventType:
		// Dead-letter rows publish generically: their payload is already a flat
		// scalar map (outbox.DeadLetterPayload), expanded via DeadLetterValues
		// rather than a bespoke case below.
		values, err := outbox.DeadLetterValues(entry)
		if err != nil {
			// Our own payload; a decode failure here is deterministic, never transient.
			return nil, fmt.Errorf("%w: dead-letter values: %v", pkgevents.ErrPermanent, err)
		}
		return values, nil

	case "node_ready_for_execution":
		var evt domain.NodeReadyForExecution
		if err := json.Unmarshal(entry.Payload, &evt); err != nil {
			return nil, fmt.Errorf("%w: unmarshal node_ready_for_execution: %v", pkgevents.ErrPermanent, err)
		}
		values := map[string]interface{}{
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
		}
		// Mode is carried only for non-production dispatches (promote_seed): the
		// executor stamps it as a Job label so k8s-controller suppresses the
		// production task lifecycle. Omitted for normal production jobs so their
		// wire shape is unchanged.
		if evt.Mode != "" {
			values["mode"] = evt.Mode
		}
		// Operation is carried only for non-default dispatches (e.g. "test"): the
		// executor uses it to pick the dbt verb instead of the NodeType default.
		// Omitted for normal run dispatches so their wire shape is unchanged.
		if evt.Operation != "" {
			values["operation"] = evt.Operation
		}
		return values, nil

	case "cascade_task_skipped":
		var evt domain.CascadeTaskSkipped
		if err := json.Unmarshal(entry.Payload, &evt); err != nil {
			return nil, fmt.Errorf("%w: unmarshal cascade_task_skipped: %v", pkgevents.ErrPermanent, err)
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
		// Unknown event_type is retried, not dead-lettered: during a rolling
		// deployment an old replica can dequeue a row for a newly-introduced
		// event_type that a newer replica (already deployed) will publish
		// moments later once this row cycles back to it.
		return nil, fmt.Errorf("orchestrator publisher: unknown event_type %q (retryable — a newer replica may handle it during a rolling upgrade)", entry.EventType)
	}
}
