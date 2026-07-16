package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/routing"
	"github.com/carolsimone/continuo/executor-controller/service/tasklifecycle"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// defaultTaskMaxRetries is the task-level retry budget for a record that carries
// none. It is the budget the RUNNING/FAILED announcements report, distinct from
// the deploy-attempt budget the Deployment aggregate owns.
const defaultTaskMaxRetries = 2

// incompleteRefReason is the rejection recorded on a node that is routed to
// workers but carries no usable runtime manifest reference.
const incompleteRefReason = "runtime manifest reference is incomplete"

// createDeployment enqueues a new Deployment from a QueryModel-shaped event plus
// task-retry overrides, on the path policy selects for it. Both QueryModelHandler
// and RetryTaskHandler call this helper; the only difference is which task retry
// values get passed in.
//
// msgProcID is the binding-layer dedup row's UUID (from message_processing); it
// is stored for provenance. Pass uuid.Nil when no inbound trigger applies.
//
// A record routed to workers without a usable runtime manifest reference cannot
// run, and has no full-project parse to fall back to. It is enqueued as a
// rejected audit row and announced FAILED in this same transaction, then
// createDeployment returns nil so the binding commits and ACKs: a permanent
// defect must not be redelivered forever, and run state must not hang on it.
func createDeployment(
	ctx context.Context,
	u uow.UnitOfWork,
	policy routing.Policy,
	logger *slog.Logger,
	base events.QueryModel,
	msgProcID uuid.UUID,
	taskRetryCount, taskMaxRetries int,
) error {
	if taskMaxRetries <= 0 {
		taskMaxRetries = defaultTaskMaxRetries
	}

	cmd := command.DeployTask{
		TaskID:             base.TaskID.String(),
		ScheduleID:         base.ScheduleID.String(),
		ScheduleName:       base.ScheduleName,
		ServiceName:        base.ServiceName,
		SchemaName:         base.SchemaName,
		TableName:          base.TableName,
		JobName:            base.JobName,
		NodeType:           string(base.NodeType),
		ImageTag:           base.ImageTag,
		TaskRetryCount:     taskRetryCount,
		TaskMaxRetries:     taskMaxRetries,
		Operation:          string(base.Operation),
		Mode:               base.Mode,
		DBTUniqueID:        base.DBTUniqueID,
		RuntimeManifestRef: base.RuntimeManifestRef,
	}

	now := time.Now()
	if policy.Resolve(base.ServiceName, base.Mode, base.DBTUniqueID, base.RuntimeManifestRef) == model.ExecutionModeWorkers {
		if err := policy.Validate(base.ServiceName, base.Mode, base.DBTUniqueID, base.RuntimeManifestRef); err != nil {
			return rejectDeployment(ctx, u, logger, cmd, msgProcID, err, now)
		}
		poolKey := pkg_model.WorkerPoolKey(base.ServiceName, base.ImageTag, base.RuntimeManifestSHA256)
		if err := u.DeploymentsRepo().Add(ctx, model.NewWorkerDeployment(cmd, msgProcID, poolKey, now)); err != nil {
			return fmt.Errorf("add executor worker deployment: %w", err)
		}
		return nil
	}

	if err := u.DeploymentsRepo().Add(ctx, model.NewDeployment(cmd, optionalID(msgProcID), now)); err != nil {
		return fmt.Errorf("add executor deployment: %w", err)
	}
	return nil
}

// rejectDeployment records a record the executor will never run and announces it
// FAILED, so the run advances rather than waiting on a node that has no dispatch
// to report.
func rejectDeployment(
	ctx context.Context,
	u uow.UnitOfWork,
	logger *slog.Logger,
	cmd command.DeployTask,
	msgProcID uuid.UUID,
	cause error,
	now time.Time,
) error {
	logger.Error("rejecting dispatch — node is routed to workers but carries no usable runtime manifest",
		"task_id", cmd.TaskID, "service_name", cmd.ServiceName,
		"dbt_unique_id", cmd.DBTUniqueID, "error", cause)

	dep := model.NewDeployment(cmd, optionalID(msgProcID), now)
	if err := dep.RejectBeforeExecution(incompleteRefReason, now); err != nil {
		return fmt.Errorf("reject executor deployment: %w", err)
	}
	if err := u.DeploymentsRepo().Add(ctx, dep); err != nil {
		return fmt.Errorf("add rejected executor deployment: %w", err)
	}
	if err := (tasklifecycle.Fanout{}).DispatchRejected(ctx, u.OutboxRepo(), dep, incompleteRefReason); err != nil {
		return fmt.Errorf("announce rejected executor deployment: %w", err)
	}
	return nil
}

// optionalID narrows a possibly-absent message_processing ID to the nullable
// form the aggregate stores.
func optionalID(id uuid.UUID) *uuid.UUID {
	if id == uuid.Nil {
		return nil
	}
	return &id
}
