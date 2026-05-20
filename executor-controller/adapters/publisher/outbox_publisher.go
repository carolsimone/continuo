package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/adapters/k8s"
	"github.com/carolsimone/continuo/executor-controller/domain/event"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

// K8sDeployer matches executor-controller/service/handlers.K8sDeployer.
type K8sDeployer interface {
	CreateQueryJob(ctx context.Context, params k8s.JobParams) error
}

// OutboxPublisher implements pkg/outbox.Publisher for executor's
// "deploy-then-announce" workflow: every row triggers a K8s Job create
// followed by two Redis XADDs. K8s create is idempotent (job name dedup);
// failures retry the whole sequence safely (the two publishes are deduped
// consumer-side by pkg/messageprocessing).
type OutboxPublisher struct {
	k8sClient    K8sDeployer
	redis        *goredis.Client
	k8sNamespace string
	logger       *slog.Logger
}

// NewOutboxPublisher creates an OutboxPublisher wired to the given K8s deployer,
// Redis client, and namespace.
func NewOutboxPublisher(k8sClient K8sDeployer, redis *goredis.Client, namespace string, logger *slog.Logger) *OutboxPublisher {
	return &OutboxPublisher{
		k8sClient:    k8sClient,
		redis:        redis,
		k8sNamespace: namespace,
		logger:       logger,
	}
}

// Publish processes a single executor outbox entry. It expects event_type
// "deploy_task" and performs three ordered side effects:
//  1. K8s Job create (idempotent by JobName — safe to retry).
//  2. XADD task.status.updated:v1 with status=RUNNING so the state service
//     advances the task tracker.
//  3. XADD node.deployed:v1 for k8s-controller so it can watch the pod.
//
// Any step failure returns an error; pkg/outbox.Processor will retry the whole
// sequence. Steps 2 and 3 are deduped consumer-side, so re-delivery is safe.
func (p *OutboxPublisher) Publish(ctx context.Context, entry *outbox.Entry) error {
	if entry.EventType != "deploy_task" {
		return fmt.Errorf("executor publisher: unknown event_type %q", entry.EventType)
	}

	var d event.DeployTask
	if err := json.Unmarshal(entry.Payload, &d); err != nil {
		return fmt.Errorf("unmarshal deploy_task payload: %w", err)
	}

	params, err := d.ToJobParams(p.k8sNamespace)
	if err != nil {
		// Payload was validated at write time; retrying will never succeed.
		// Wrap with ErrPermanent so the Processor exhausts the retry budget
		// immediately and invokes the TerminalFailureHook.
		return fmt.Errorf("invalid deploy_task payload: %w", errors.Join(err, pkgevents.ErrPermanent))
	}

	// Step 1: K8s deploy (idempotent by JobName).
	if err := p.k8sClient.CreateQueryJob(ctx, params); err != nil {
		return fmt.Errorf("k8s deployment failed: %w", err)
	}

	// Step 2: announce RUNNING to state's task tracker.
	status := pkgevents.TaskStatusUpdated{
		TaskID:     d.TaskID,
		ScheduleID: d.ScheduleID,
		Status:     "RUNNING",
		RetryCount: int32(d.TaskRetryCount),
	}
	statusMap := status.ToMap()
	// outbox_entry_id is injected on every XADD so consumer-side
	// DedupWithOutboxEntryID can catch Processor-crash redeliveries.
	statusMap["outbox_entry_id"] = entry.ID.String()
	if err := p.xadd(ctx, streams.TaskStatusUpdatedV1, statusMap); err != nil {
		return err
	}

	// Step 3: announce node.deployed:v1 for k8s-controller. The typed event
	// travels in the JSON payload field; outbox_entry_id stays a flat sibling
	// for consumer-side DedupWithOutboxEntryID.
	deployedPayload, err := json.Marshal(d.ToNodeDeployedEvent())
	if err != nil {
		return fmt.Errorf("marshal node.deployed payload: %w", err)
	}
	if err := p.xadd(ctx, streams.NodeDeployedV1, map[string]interface{}{
		"payload":         string(deployedPayload),
		"outbox_entry_id": entry.ID.String(),
	}); err != nil {
		return err
	}

	p.logger.Info("Outbox entry published",
		"entry_id", entry.ID,
		"task_id", d.TaskID,
		"job_name", d.JobName,
	)
	return nil
}

func (p *OutboxPublisher) xadd(ctx context.Context, stream string, values map[string]interface{}) error {
	if _, err := p.redis.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: values}).Result(); err != nil {
		return fmt.Errorf("xadd to %s: %w", stream, err)
	}
	return nil
}

// NewTerminalFailureHook returns a hook that runs when executor's retry budget
// for a deploy_task row exhausts. It publishes:
//   - task.status.updated:v1 with status=FAILED so state's task tracker marks
//     the task FAILED.
//   - node.updated:v1 with status=FAILED so orchestrator's HandleNodeCompleted
//     advances the schedule (CheckScheduleCompletion fires).
//
// Best-effort: errors are logged; the hook always returns nil so
// pkg/outbox.Processor still proceeds to MarkFailed afterward.
func NewTerminalFailureHook(redis *goredis.Client, logger *slog.Logger) outbox.TerminalFailureHook {
	return func(ctx context.Context, entry *outbox.Entry, cause error) error {
		var d event.DeployTask
		if err := json.Unmarshal(entry.Payload, &d); err != nil {
			logger.Error("Terminal hook: cannot unmarshal payload — skipping FAILED announcements",
				"entry_id", entry.ID, "error", err)
			return nil
		}

		// Publish task.status.updated:v1 FAILED so state's task tracker advances.
		failed := pkgevents.TaskStatusUpdated{
			TaskID:     d.TaskID,
			ScheduleID: d.ScheduleID,
			Status:     "FAILED",
			RetryCount: int32(d.TaskRetryCount),
		}
		if _, err := redis.XAdd(ctx, &goredis.XAddArgs{
			Stream: streams.TaskStatusUpdatedV1,
			Values: failed.ToMap(),
		}).Result(); err != nil {
			logger.Error("Terminal hook: failed to publish task.status.updated FAILED",
				"entry_id", entry.ID, "task_id", d.TaskID, "error", err)
		}

		// Publish node.updated:v1 FAILED so orchestrator's HandleNodeCompleted
		// fires CheckScheduleCompletion and unblocks the schedule.
		nodeEvt := map[string]interface{}{
			"task_id":       d.TaskID,
			"schedule_id":   d.ScheduleID,
			"schedule_name": d.ScheduleName,
			"service_name":  d.ServiceName,
			"schema_name":   d.SchemaName,
			"table_name":    d.TableName,
			"status":        "FAILED",
		}
		if _, err := redis.XAdd(ctx, &goredis.XAddArgs{
			Stream: streams.NodeUpdatedV1,
			Values: nodeEvt,
		}).Result(); err != nil {
			logger.Error("Terminal hook: failed to publish node.updated FAILED",
				"entry_id", entry.ID, "task_id", d.TaskID, "error", err)
		}

		return nil
	}
}
