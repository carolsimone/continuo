package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	goredis "github.com/redis/go-redis/v9"
)

// taskStatusUpdatedStreamName is the wire-stable name of the Redis stream
// whose messages this binding handles. It is also the value stored in the
// message_processing.stream_name column for dedup rows.
const taskStatusUpdatedStreamName = "task.status.updated:v1"

// NewTaskStatusUpdatedBinding returns a pkg/redis.MessageHandler that turns
// each task.status.updated:v1 message into a typed event, runs dedup, and
// invokes TaskStatusUpdatedHandler inside a single Unit-of-Work transaction.
//
// task.status.updated:v1 uses flat msg.Values fields rather than a single
// JSON payload, so the binding serializes the full Values map and stores it
// as the message_processing.payload (JSONB NOT NULL). The dedup key remains
// (message_id, stream_name); the payload is observability-only. Marshal
// errors collapse to "{}" rather than aborting the message.
//
// The dedup row ID returned by Dedup is threaded to the handler as msgProcID
// so the run.finalized:v1 outbox entry emitted on the finalize branch carries
// provenance back to the originating message.
//
// Errors are surfaced to the StreamConsumer so it can pick the right ACK
// policy: parse failures are wrapped with events.ErrPermanent (NACK and
// drop), while handler/repository failures propagate as-is (NACK and let
// the message stay pending for retry). On a duplicate the transaction is
// committed (empty txn) and nil is returned so the consumer ACKs.
//
// The transient task-not-found path returns a plain error (NOT wrapped with
// ErrPermanent): the binding rolls back and the StreamConsumer's reclaim
// loop redelivers the message after run.entries.dispatched:v1 has caught up.
func NewTaskStatusUpdatedBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.TaskStatusUpdatedHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseTaskStatusUpdated(msg)
		if err != nil {
			logger.Error("task.status.updated: parse failure", "message_id", msg.ID, "error", err)
			return fmt.Errorf("%w: %v", pkgevents.ErrPermanent, err)
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
					logger.Error("task.status.updated: rollback failed",
						"message_id", msg.ID, "error", rbErr)
				}
			}
		}()

		msgProcID, dup, err := messageprocessing.Dedup(
			ctx, u.MessageProcessingRepo(), logger,
			msg.ID, taskStatusUpdatedStreamName, payload,
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

		if err := handler.Handle(ctx, u, evt, msgProcID); err != nil {
			if errors.Is(err, pkgevents.ErrPermanent) {
				logger.Error("task.status.updated: permanent handler error",
					"message_id", msg.ID, "error", err)
			} else {
				logger.Error("task.status.updated: transient handler error",
					"message_id", msg.ID, "error", err)
			}
			return err
		}
		if err := u.Commit(); err != nil {
			return fmt.Errorf("commit tx: %w", err)
		}
		committed = true
		return nil
	}
}
