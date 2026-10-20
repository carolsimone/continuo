package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/events"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
)

// createDeployment enqueues the pending Deployment of a task's first attempt
// from a query.model:v1 dispatch.
//
// msgProcID is the binding-layer dedup row's UUID (from message_processing);
// it is stored for provenance. Pass uuid.Nil when no inbound trigger applies.
//
// taskMaxRetries is the task-level retry budget carried in the command for the
// observer's retry decision and the RUNNING/FAILED announcements, distinct
// from the deploy-attempt budget the aggregate owns; taskMaxRetries <= 0 falls
// back to pkgevents.DefaultTaskMaxRetries.
func createDeployment(
	ctx context.Context,
	u uow.UnitOfWork,
	base events.QueryModel,
	msgProcID uuid.UUID,
	taskMaxRetries int,
) error {
	if taskMaxRetries <= 0 {
		taskMaxRetries = int(pkgevents.DefaultTaskMaxRetries)
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
		SecretRef:      base.SecretRef,
		TaskMaxRetries: taskMaxRetries,
		Operation:      string(base.Operation),
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
