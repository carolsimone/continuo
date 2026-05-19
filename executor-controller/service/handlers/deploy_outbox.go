// executor-controller/service/handlers/deploy_outbox.go
package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/google/uuid"
)

// createDeploymentOutboxEntry writes a single deployment_outbox row from
// a QueryModel-shaped event plus retry-fields override. Both
// QueryModelHandler and RetryTaskHandler call this helper; the only
// difference between them is which retry values get passed in.
//
// base.OutboxEntryID is copied through to deployment_outbox.outbox_entry_id
// as the application-level idempotency key. When the publisher does not
// carry one (uuid.Nil), the column is written as NULL and dedup
// degrades to the shared (msg.ID, stream_name) layer in
// message_processing.
//
// The repository's INSERT uses ON CONFLICT (outbox_entry_id) DO NOTHING
// against the partial unique index, so a publisher retry that produces
// a new Redis msg.ID for the same outbox_entry_id is a silent no-op:
// the binding's dedup row commits and the message ACKs.
//
// MaxRetries <= 0 falls back to the service default of 2.
func createDeploymentOutboxEntry(
	ctx context.Context,
	u uow.UnitOfWork,
	base events.QueryModel,
	taskRetryCount, maxRetries int,
) error {
	if maxRetries <= 0 {
		maxRetries = 2
	}
	var outboxEntryID *uuid.UUID
	if base.OutboxEntryID != uuid.Nil {
		id := base.OutboxEntryID
		outboxEntryID = &id
	}
	entry := &model.DeploymentOutboxEntry{
		ID:               uuid.New(),
		OutboxEntryID:    outboxEntryID,
		TaskID:           base.TaskID,
		ScheduleID:       base.ScheduleID,
		ScheduleName:     base.ScheduleName,
		ServiceName:      base.ServiceName,
		SchemaName:       base.SchemaName,
		TableName:        base.TableName,
		JobName:          base.JobName,
		NodeType:         string(base.NodeType),
		ImageTag:         base.ImageTag,
		TaskRetryCount:   taskRetryCount,
		TaskMaxRetries:   maxRetries,
		Status:           string(model.OutboxStatusPending),
		CreatedAt:        time.Now(),
		OutboxRetryCount: 0,
		OutboxMaxRetries: 3,
	}
	if err := u.OutboxRepo().Create(ctx, entry); err != nil {
		return fmt.Errorf("create deployment outbox entry: %w", err)
	}
	return nil
}
