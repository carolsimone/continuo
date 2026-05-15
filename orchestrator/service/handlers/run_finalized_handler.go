package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain"
)

// RunFinalizer projects a run's terminal status onto Neo4j. The orchestrator
// merely projects the outcome state has already decided — see comment on the
// run.finalized:v1 stream for why this is necessary.
type RunFinalizer interface {
	FinalizeRun(ctx context.Context, runID, terminalStatus string) error
}

// RunFinalizedHandler stamps completed_at and terminal_status on the
// :Run node when state's scheduler_tracker reaches a terminal state.
type RunFinalizedHandler struct {
	finalizer RunFinalizer
	logger    *slog.Logger
}

// NewRunFinalizedHandler constructs the handler.
func NewRunFinalizedHandler(finalizer RunFinalizer, logger *slog.Logger) *RunFinalizedHandler {
	return &RunFinalizedHandler{finalizer: finalizer, logger: logger}
}

// Handle projects the terminal status onto the Neo4j :Run node.
func (h *RunFinalizedHandler) Handle(ctx context.Context, evt domain.RunFinalized) error {
	if err := h.finalizer.FinalizeRun(ctx, evt.ScheduleID, evt.Status); err != nil {
		return fmt.Errorf("finalize run %s: %w", evt.ScheduleID, err)
	}
	h.logger.Info("Run finalized in Neo4j", "schedule_id", evt.ScheduleID, "status", evt.Status)
	return nil
}
