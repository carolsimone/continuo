package redis

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// NewRebaseBinding wires ParseRebase into the shared DerivedRunHandler. Parse
// failures are permanent (events.ErrPermanent): the binding logs and returns
// the error so the consumer ACKs and drops the poison message.
func NewRebaseBinding(
	handler *handlers.DerivedRunHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		cmd, err := ParseRebase(msg)
		if err != nil {
			logger.Error("trigger.rebase: parse failure — discarding",
				"message_id", msg.ID, "error", err)
			return err
		}
		return handler.Handle(ctx, model.DerivedRunInput{
			RunID:        cmd.RunID,
			ScheduleName: cmd.ScheduleName,
			SourceRunID:  cmd.SourceRunID,
			InitiatedBy:  cmd.InitiatedBy,
		}, msg.ID, messageprocessing.ExtractOutboxEntryID(msg.Values))
	}
}
