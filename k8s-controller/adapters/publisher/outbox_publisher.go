package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/k8s-controller/adapters/delayqueue"
	"github.com/carolsimone/continuo/k8s-controller/domain/event"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
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

var _ outbox.Publisher = (*OutboxPublisher)(nil)

// NewOutboxPublisher creates an OutboxPublisher wired to the given Redis client.
func NewOutboxPublisher(r *goredis.Client, l *slog.Logger) *OutboxPublisher {
	return &OutboxPublisher{redis: r, logger: l}
}

// streamMaxLen caps each published stream (Approx/~ trimming); it is the shared
// streams.StreamMaxLen so the app-side publisher and the delay-queue promoter
// bound the stream identically.
const streamMaxLen = streams.StreamMaxLen

// Publish routes an outbox row to Redis. Most event types XADD their typed
// field map to entry.StreamName. The check.k8s scheduling event (check_delayed)
// is the exception: it is written to the delay queue (HSET ticket + ZADD due
// time) instead of the stream, so a not-yet-due check waits off the stream. The
// promoter later moves it onto the stream when due.
func (p *OutboxPublisher) Publish(ctx context.Context, entry *outbox.Entry) error {
	if entry.EventType == "check_delayed" {
		return p.scheduleDelayedCheck(ctx, entry)
	}
	args, err := p.xaddArgs(entry)
	if err != nil {
		return err
	}
	if _, err := p.redis.XAdd(ctx, args).Result(); err != nil {
		return fmt.Errorf("xadd to %s: %w", entry.StreamName, err)
	}
	return nil
}

// scheduleDelayedCheck converts a check_delayed outbox row into the typed
// CheckK8s payload and enqueues it in the delay queue keyed by JobName.
func (p *OutboxPublisher) scheduleDelayedCheck(ctx context.Context, entry *outbox.Entry) error {
	var e event.JobCheckRequest
	if err := json.Unmarshal(entry.Payload, &e); err != nil {
		return fmt.Errorf("%w: unmarshal check_delayed: %v", pkgevents.ErrPermanent, err)
	}
	retryCount, err := num.Int32(e.RetryCount, "retry_count")
	if err != nil {
		return fmt.Errorf("check.k8s payload: %w", err)
	}
	maxRetries, err := num.Int32(e.MaxRetries, "max_retries")
	if err != nil {
		return fmt.Errorf("check.k8s payload: %w", err)
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
		return fmt.Errorf("marshal check.k8s payload: %w", err)
	}
	return delayqueue.Schedule(ctx, p.redis, e.JobName, entry.ID.String(), string(payload), e.CheckAfter)
}

// xaddArgs builds the XADD arguments for an outbox entry, including the shared
// MaxLen cap so the stream cannot grow unbounded.
func (p *OutboxPublisher) xaddArgs(entry *outbox.Entry) (*goredis.XAddArgs, error) {
	values, err := p.toValues(entry)
	if err != nil {
		return nil, err
	}
	// Inject outbox_entry_id on every XADD so consumer-side
	// DedupWithOutboxEntryID can catch Processor-crash redeliveries (same
	// outbox row republished with a fresh Redis msg_id).
	values["outbox_entry_id"] = entry.ID.String()
	return &goredis.XAddArgs{
		Stream: entry.StreamName,
		MaxLen: streamMaxLen,
		Approx: true,
		Values: values,
	}, nil
}

// toValues routes entry.EventType to the matching typed struct, unmarshals
// entry.Payload, and returns the flat field map for XADD.
func (p *OutboxPublisher) toValues(entry *outbox.Entry) (map[string]interface{}, error) {
	switch entry.EventType {
	case outbox.DeadLetterEventType:
		// Dead-letter rows publish generically: their payload is already a flat
		// scalar map (outbox.DeadLetterPayload), expanded via DeadLetterValues
		// instead of falling through to the generic default case below (which
		// would wrap it opaquely under a single "payload" field).
		values, err := outbox.DeadLetterValues(entry)
		if err != nil {
			// Our own payload; a decode failure here is deterministic, never transient.
			return nil, fmt.Errorf("%w: dead-letter values: %v", pkgevents.ErrPermanent, err)
		}
		return values, nil

	case "task_status_updated":
		var e pkgevents.TaskStatusUpdated
		if err := json.Unmarshal(entry.Payload, &e); err != nil {
			return nil, fmt.Errorf("%w: unmarshal task_status_updated: %v", pkgevents.ErrPermanent, err)
		}
		return e.ToMap(), nil

	case "task_execution_recorded":
		var e pkgevents.TaskExecutionRecorded
		if err := json.Unmarshal(entry.Payload, &e); err != nil {
			return nil, fmt.Errorf("%w: unmarshal task_execution_recorded: %v", pkgevents.ErrPermanent, err)
		}
		return e.ToMap(), nil

	case "task_retry":
		var e event.TaskRetry
		if err := json.Unmarshal(entry.Payload, &e); err != nil {
			return nil, fmt.Errorf("%w: unmarshal task_retry: %v", pkgevents.ErrPermanent, err)
		}
		return e.ToMap(), nil

	case "task_failed":
		var e event.TaskFailed
		if err := json.Unmarshal(entry.Payload, &e); err != nil {
			return nil, fmt.Errorf("%w: unmarshal task_failed: %v", pkgevents.ErrPermanent, err)
		}
		return e.ToMap(), nil

	case "node_status_updated":
		var e event.NodeStatusUpdated
		if err := json.Unmarshal(entry.Payload, &e); err != nil {
			return nil, fmt.Errorf("%w: unmarshal node_status_updated: %v", pkgevents.ErrPermanent, err)
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
		return nil, fmt.Errorf("%w: k8s publisher: unknown event_type %q", pkgevents.ErrPermanent, entry.EventType)
	}
}
