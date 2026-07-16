// Package tasklifecycle writes the announcements a production task owes the
// rest of the system as it reaches a terminal state.
package tasklifecycle

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/executor-controller/domain/event"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// Fanout emits a production task's lifecycle announcements onto the outbox. It
// holds no state: every call takes the outbox repository bound to the caller's
// transaction, so the announcements commit with the state change that caused
// them.
type Fanout struct{}

// DispatchRejected announces a production task the executor will never run.
// Both rows go onto the caller's transaction: task.status.updated:v1 FAILED
// settles the task for the UI, and node.updated:v1 FAILED lets the orchestrator
// advance the schedule. Without them the run would wait forever on a node that
// has no dispatch to report.
func (Fanout) DispatchRejected(
	ctx context.Context,
	repo outbox.Repository,
	dep *model.Deployment,
	reason string,
) error {
	cmd := dep.Command()
	retryCount, err := num.Int32(cmd.TaskRetryCount, "task_retry_count")
	if err != nil {
		return fmt.Errorf("announce rejected task %s (%s): %w", cmd.TaskID, reason, err)
	}
	announcements := []struct {
		eventType string
		stream    string
		payload   interface{}
	}{
		{"task_status_updated", streams.TaskStatusUpdatedV1, pkgevents.TaskStatusUpdated{
			TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, Status: "FAILED", RetryCount: retryCount,
		}},
		{"node_updated", streams.NodeUpdatedV1, event.NodeUpdated{
			TaskID: cmd.TaskID, ScheduleID: cmd.ScheduleID, ScheduleName: cmd.ScheduleName,
			ServiceName: cmd.ServiceName, SchemaName: cmd.SchemaName, TableName: cmd.TableName,
			Status: "FAILED",
		}},
	}
	for _, a := range announcements {
		if err := create(ctx, repo, dep, a.eventType, a.stream, a.payload); err != nil {
			return fmt.Errorf("announce rejected task %s (%s): %w", cmd.TaskID, reason, err)
		}
	}
	return nil
}

// create writes one outbox row for dep, keyed on the task aggregate so a task's
// announcements publish in the order they were written.
func create(
	ctx context.Context,
	repo outbox.Repository,
	dep *model.Deployment,
	eventType, stream string,
	payload interface{},
) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s payload: %w", eventType, err)
	}
	aggregateID, err := uuid.Parse(dep.Command().TaskID)
	if err != nil {
		return fmt.Errorf("invalid task_id %q on deployment %s: %w", dep.Command().TaskID, dep.ID(), err)
	}
	return repo.Create(ctx, &outbox.Entry{
		MessageProcessingID: dep.MessageProcessingID(),
		AggregateType:       "task",
		AggregateID:         aggregateID,
		EventType:           eventType,
		Payload:             body,
		StreamName:          stream,
		MaxRetries:          3,
	})
}
