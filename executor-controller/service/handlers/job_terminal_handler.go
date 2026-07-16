package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/google/uuid"
)

// JobTerminalHandler returns the execution slot held by a deployment whose
// Kubernetes Job has settled.
//
// This is the one release with no aggregate transition behind it. A Job's
// lifecycle ends outside the executor: the row sits in 'deployed' while
// Kubernetes runs the work, and nothing in the aggregate observes it finishing.
// Worker tasks report their own outcome and release their slot inside the
// transition that records it, so they never come through here.
//
// The handler writes no task or node lifecycle events. The Job's business
// outcome travels on its own streams and is already handled there; this event
// exists solely so the capacity the Job occupied is returned to the budget that
// Jobs and workers share.
type JobTerminalHandler struct {
	logger *slog.Logger
}

func NewJobTerminalHandler(logger *slog.Logger) *JobTerminalHandler {
	return &JobTerminalHandler{logger: logger}
}

// Handle releases the deployment's execution slot. The binding owns the
// transaction lifecycle and has already run dedup; msgProcID is accepted for
// signature parity with the standardized handler shape and is currently unused.
//
// A release that frees nothing is not an error: the slot may already have been
// returned by a duplicate delivery of this event, or by the schedule being
// cancelled while the Job ran. Both mean the capacity is already back in the
// budget, so the message is ACKed.
func (h *JobTerminalHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.ExecutorJobTerminal,
	_ uuid.UUID,
) error {
	// Free the slot as of when the Job actually settled. A Job that reported no
	// completion instant still has to release, so fall back to now.
	releasedAt := evt.CompletedAt
	if releasedAt.IsZero() {
		releasedAt = time.Now()
	}

	released, err := u.DeploymentsRepo().ReleaseSlot(ctx, evt.ExecutorDeploymentID, releasedAt)
	if err != nil {
		return fmt.Errorf("release execution slot for deployment %s: %w", evt.ExecutorDeploymentID, err)
	}
	if !released {
		h.logger.Debug("Execution slot already released — nothing to do",
			"deployment_id", evt.ExecutorDeploymentID, "job_name", evt.JobName)
		return nil
	}

	h.logger.Info("Released the execution slot of a settled Job",
		"deployment_id", evt.ExecutorDeploymentID,
		"job_name", evt.JobName,
		"terminal_status", evt.TerminalStatus)
	return nil
}
