package redis

import (
	"context"
	"fmt"
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// RunFinalizer projects a run's terminal status from state's run.finalized:v1
// stream onto the orchestrator's Neo4j :Run node. It exists because some runs
// (notably full-inherited rebases) never produce node.updated:v1 traffic, so
// the Run aggregate's internal finalization path is not exercised; state
// remains the authority for terminal_task_count == total_task_count and the
// orchestrator merely projects that outcome back into Neo4j.
type RunFinalizer interface {
	FinalizeRun(ctx context.Context, runID, terminalStatus string) error
}

// NewRunFinalizedHandler returns a MessageHandler that stamps completed_at and
// terminal_status on the :Run node when state's scheduler_tracker reaches a
// terminal state.
func NewRunFinalizedHandler(
	repo RunFinalizer,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		scheduleID, _ := msg.Values["schedule_id"].(string)
		status, _ := msg.Values["status"].(string)

		if scheduleID == "" || status == "" {
			logger.Error("run.finalized: missing schedule_id or status — discarding",
				"msg_id", msg.ID,
				"schedule_id", scheduleID,
				"status", status,
			)
			return nil
		}

		if err := repo.FinalizeRun(ctx, scheduleID, status); err != nil {
			return fmt.Errorf("finalize run %s: %w", scheduleID, err)
		}

		logger.Info("Run finalized in Neo4j", "schedule_id", scheduleID, "status", status)
		return nil
	}
}
