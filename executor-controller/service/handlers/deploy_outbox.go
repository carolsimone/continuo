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
// MaxRetries <= 0 falls back to the service default of 2 (preserving
// the prior DeployHandler.Handle behavior).
func createDeploymentOutboxEntry(
	ctx context.Context,
	u uow.UnitOfWork,
	base events.QueryModel,
	taskRetryCount, maxRetries int,
) error {
	if maxRetries <= 0 {
		maxRetries = 2
	}
	entry := &model.DeploymentOutboxEntry{
		ID:               uuid.New(),
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
