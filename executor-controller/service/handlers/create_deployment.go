package handlers

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/deployer"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/google/uuid"
)

// createDeployment writes a single pending executor_deployments row from a
// QueryModel-shaped event plus task-retry overrides. Both QueryModelHandler
// and RetryTaskHandler call this helper; the only difference is which task
// retry values get passed in.
//
// msgProcID is the binding-layer dedup row's UUID (from message_processing);
// it is stored as message_processing_id for provenance. Pass uuid.Nil when no
// inbound trigger applies.
//
// taskMaxRetries <= 0 falls back to the service default of 2. This is the
// task-level retry budget carried in job_params for the eventual RUNNING/FAILED
// announcements — distinct from the deployment row's own K8s-deploy retry
// budget (Deployment.MaxRetries, defaulted to 3 by the repository).
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

	jobParams, err := json.Marshal(deployer.DeployJob{
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
	})
	if err != nil {
		return fmt.Errorf("marshal deploy job params: %w", err)
	}

	var procID *uuid.UUID
	if msgProcID != uuid.Nil {
		id := msgProcID
		procID = &id
	}

	d := &deployer.Deployment{
		MessageProcessingID: procID,
		TaskID:              base.TaskID,
		ScheduleID:          base.ScheduleID,
		JobParams:           jobParams,
		Status:              "pending",
	}
	if err := u.DeploymentsRepo().Create(ctx, d); err != nil {
		return fmt.Errorf("create executor deployment: %w", err)
	}
	return nil
}
