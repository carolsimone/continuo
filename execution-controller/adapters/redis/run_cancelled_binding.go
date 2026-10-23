package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/execution-controller/service/handlers"
	"github.com/carolsimone/continuo/execution-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

// NewRunCancelledBinding returns a pkg/redis.MessageHandler that parses each
// run.cancelled:v1 message, runs dedup, and invokes RunCancelledHandler inside
// one Unit-of-Work transaction; once it commits, the Jobs of the rows it moved
// to terminating are deleted. A message that cannot be read is permanent
// (dead-lettered); a handler error leaves the message pending for retry; a
// duplicate commits an empty transaction, deletes nothing and is
// acknowledged.
func NewRunCancelledBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.RunCancelledHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseRunCancelled(msg)
		if err != nil {
			logger.Error("run.cancelled: parse failure", "message_id", msg.ID, "error", err)
			return err
		}
		payload, mErr := json.Marshal(msg.Values)
		if mErr != nil {
			payload = []byte("{}")
		}

		u := uowFactory()
		if err := u.Begin(ctx); err != nil {
			return fmt.Errorf("begin uow: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				if rbErr := u.Rollback(); rbErr != nil {
					logger.Error("run.cancelled: rollback failed", "message_id", msg.ID, "error", rbErr)
				}
			}
		}()

		msgProcID, dup, err := messageprocessing.DedupWithOutboxEntryID(
			ctx, u.MessageProcessingRepo(), logger,
			msg.ID, streams.RunCancelledV1, payload,
			messageprocessing.ExtractOutboxEntryID(msg.Values),
		)
		if err != nil {
			return err
		}
		if dup {
			if err := u.Commit(); err != nil {
				return fmt.Errorf("commit dedup tx: %w", err)
			}
			committed = true
			return nil
		}
		deleteJobs, err := handler.Handle(ctx, u, evt, msgProcID)
		if err != nil {
			logger.Error("run.cancelled: handler error", "message_id", msg.ID, "run_id", evt.RunID,
				"permanent", errors.Is(err, pkgevents.ErrPermanent), "error", err)
			return err
		}
		if err := u.MessageProcessingRepo().UpdateState(ctx, msgProcID, messageprocessing.StateCompleted); err != nil {
			return fmt.Errorf("mark completed: %w", err)
		}
		if err := u.Commit(); err != nil {
			return fmt.Errorf("commit tx: %w", err)
		}
		committed = true
		handler.DeleteJobs(ctx, deleteJobs)
		return nil
	}
}
