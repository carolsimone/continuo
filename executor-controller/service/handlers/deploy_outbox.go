// executor-controller/service/handlers/deploy_outbox.go
package handlers

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/executor-controller/domain/event"
	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// createDeploymentOutboxEntry writes a single executor_outbox row from
// a QueryModel-shaped event plus retry-field overrides. Both
// QueryModelHandler and RetryTaskHandler call this helper; the only
// difference between them is which retry values get passed in.
//
// msgProcID is the binding-layer dedup row's UUID (from message_processing);
// it is stored as message_processing_id in the outbox row for provenance.
// Pass uuid.Nil when no inbound trigger applies.
//
// MaxRetries <= 0 falls back to the service default of 2.
func createDeploymentOutboxEntry(
	ctx context.Context,
	u uow.UnitOfWork,
	base events.QueryModel,
	msgProcID uuid.UUID,
	taskRetryCount, maxRetries int,
) error {
	if maxRetries <= 0 {
		maxRetries = 2
	}

	payload, err := json.Marshal(event.DeployTask{
		TaskID:         base.TaskID.String(),
		ScheduleID:     base.ScheduleID.String(),
		ScheduleName:   base.ScheduleName,
		ServiceName:    base.ServiceName,
		SchemaName:     base.SchemaName,
		TableName:      base.TableName,
		JobName:        base.JobName,
		NodeType:       string(base.NodeType),
		ImageTag:       base.ImageTag,
		TaskRetryCount: taskRetryCount,
		TaskMaxRetries: maxRetries,
	})
	if err != nil {
		return fmt.Errorf("marshal deploy_task payload: %w", err)
	}

	taskID := base.TaskID

	var procID *uuid.UUID
	if msgProcID != uuid.Nil {
		id := msgProcID
		procID = &id
	}

	entry := &pkgoutbox.Entry{
		MessageProcessingID: procID,
		AggregateType:       "task",
		AggregateID:         taskID,
		EventType:           "deploy_task",
		Payload:             payload,
		StreamName:          streams.NodeDeployedV1,
		MaxRetries:          3,
	}

	if err := u.OutboxRepo().Create(ctx, entry); err != nil {
		return fmt.Errorf("create executor outbox entry: %w", err)
	}
	return nil
}
