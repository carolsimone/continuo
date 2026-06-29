package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/google/uuid"
)

// createDeployment enqueues a new pending Deployment aggregate from a
// QueryModel-shaped event plus task-retry overrides. Both QueryModelHandler and
// RetryTaskHandler call this helper; the only difference is which task retry
// values get passed in.
//
// msgProcID is the binding-layer dedup row's UUID (from message_processing);
// it is stored for provenance. Pass uuid.Nil when no inbound trigger applies.
//
// taskMaxRetries <= 0 falls back to the service default of 2 — the task-level
// retry budget carried in the command for the eventual RUNNING/FAILED
// announcements, distinct from the deploy-attempt budget the aggregate owns.
func createDeployment(
	ctx context.Context,
	u uow.UnitOfWork,
	base events.QueryModel,
	msgProcID uuid.UUID,
	taskRetryCount, taskMaxRetries int,
) error {
	if taskMaxRetries <= 0 {
		taskMaxRetries = 2
	}

	cmd := command.DeployTask{
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
		TaskMaxRetries: taskMaxRetries,
		Mode:           base.Mode,
	}

	var procID *uuid.UUID
	if msgProcID != uuid.Nil {
		id := msgProcID
		procID = &id
	}

	if err := u.DeploymentsRepo().Add(ctx, model.NewDeployment(cmd, procID, time.Now())); err != nil {
		return fmt.Errorf("add executor deployment: %w", err)
	}
	return nil
}
