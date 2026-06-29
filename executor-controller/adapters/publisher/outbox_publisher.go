package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/domain/event"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	goredis "github.com/redis/go-redis/v9"
)

// OutboxPublisher implements pkg/outbox.Publisher for executor-controller. Each
// row publishes exactly one event to entry.StreamName; the typed payload depends
// on entry.EventType. The K8s deploy is no longer a publish concern — it is a
// command effect handled by deployer.Dispatcher, which writes these canonical
// rows only after a deploy resolves. This makes executor's publisher identical
// in shape to state, orchestrator, and k8s-controller.
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
	// outbox_entry_id is injected on every XADD so consumer-side
	// DedupWithOutboxEntryID can catch Processor-crash redeliveries.
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
// entry.Payload, and returns the field map for XADD.
func (p *OutboxPublisher) toValues(entry *outbox.Entry) (map[string]interface{}, error) {
	switch entry.EventType {
	case "task_status_updated":
		var e pkgevents.TaskStatusUpdated
		if err := json.Unmarshal(entry.Payload, &e); err != nil {
			return nil, fmt.Errorf("unmarshal task_status_updated: %w", err)
		}
		return e.ToMap(), nil

	case "node_deployed":
		var e event.JobDeployed
		if err := json.Unmarshal(entry.Payload, &e); err != nil {
			return nil, fmt.Errorf("unmarshal node_deployed: %w", err)
		}
		// node.deployed:v1 carries a typed JSON payload (pkg/events.NodeDeployed);
		// outbox_entry_id is added as a flat sibling by Publish for dedup.
		payload, err := json.Marshal(pkgevents.NodeDeployed{
			TaskID:         e.TaskID,
			ScheduleID:     e.ScheduleID,
			ScheduleName:   e.ScheduleName,
			ServiceName:    e.ServiceName,
			SchemaName:     e.SchemaName,
			TableName:      e.TableName,
			JobName:        e.JobName,
			NodeType:       e.NodeType,
			ImageTag:       e.ImageTag,
			TaskRetryCount: int32(e.TaskRetryCount),
			MaxRetries:     int32(e.MaxRetries),
		})
		if err != nil {
			return nil, fmt.Errorf("marshal node.deployed payload: %w", err)
		}
		return map[string]interface{}{"payload": string(payload)}, nil

	case "node_updated":
		var e event.NodeUpdated
		if err := json.Unmarshal(entry.Payload, &e); err != nil {
			return nil, fmt.Errorf("unmarshal node_updated: %w", err)
		}
		return e.ToMap(), nil

	case "validation_completed", "seed_build_completed", "compile_completed":
		// The three candidate-leg aggregate-completion events
		// (validation.completed:v1 / seed.build.completed:v1 / compile.completed:v1)
		// each carry the aggregate as a single JSON "payload" field that
		// release-controller's HandleValidationResult / HandleSeedBuildResult /
		// HandleCompileResult decodes. The stored payload is already that body;
		// re-emit it verbatim.
		return map[string]interface{}{"payload": string(entry.Payload)}, nil

	default:
		return nil, fmt.Errorf("executor publisher: unknown event_type %q", entry.EventType)
	}
}
