package redis

import (
	"context"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"
)

// RunFinalizer is the narrow interface the handler needs from the run repository.
type RunFinalizer interface {
	FinalizeRun(ctx context.Context, runID, terminalStatus string) error
}

// NewRunFinalizedHandler returns a MessageHandler that marks a Run node in Neo4j
// as complete by setting completed_at and terminal_status.
func NewRunFinalizedHandler(
	repo RunFinalizer,
	logger *slog.Logger,
) MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		scheduleID, _ := msg.Values["schedule_id"].(string)
		status, _ := msg.Values["status"].(string)

		if scheduleID == "" || status == "" {
			logger.Error("run.finalized: missing schedule_id or status — discarding",
				"msg_id", msg.ID,
				"schedule_id", scheduleID,
				"status", status,
			)
			return nil // permanent error: ack by returning nil
		}

		if err := repo.FinalizeRun(ctx, scheduleID, status); err != nil {
			return fmt.Errorf("finalize run %s: %w", scheduleID, err)
		}

		logger.Info("Run finalized in Neo4j", "schedule_id", scheduleID, "status", status)
		return nil
	}
}
