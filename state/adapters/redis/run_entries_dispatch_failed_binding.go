package redis

import (
	"context"
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

// runEntriesDispatchFailedStreamName is the wire-stable name of the Redis
// stream whose messages this binding handles. It is also the value stored in
// the message_processing.stream_name column for dedup rows.
const runEntriesDispatchFailedStreamName = "run.entries.dispatch_failed:v1"

// NewRunEntriesDispatchFailedBinding returns a pkg/redis.MessageHandler that
// turns each run.entries.dispatch_failed:v1 message into a typed event, runs
// dedup, and invokes RunEntriesDispatchFailedHandler inside a single
// Unit-of-Work transaction. The dedup row ID returned by Dedup is threaded
// to the handler as msgProcID so the outbox entry it writes carries
// provenance back to the originating message.
//
// Errors are surfaced to the StreamConsumer so it can pick the right ACK
// policy: parse failures are wrapped with events.ErrPermanent (NACK and
// drop), while handler/repository failures propagate as-is (NACK and let
// the message stay pending for retry). On a duplicate the transaction is
// committed (empty txn) and nil is returned so the consumer ACKs.
func NewRunEntriesDispatchFailedBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.RunEntriesDispatchFailedHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseRunEntriesDispatchFailed(msg)
		if err != nil {
			logger.Error("run.entries.dispatch_failed: parse failure", "message_id", msg.ID, "error", err)
			return fmt.Errorf("%w: %v", pkgevents.ErrPermanent, err)
		}

		payload, _ := msg.Values["payload"].(string)
		u := uowFactory()
		if err := u.Begin(ctx); err != nil {
			return fmt.Errorf("begin uow: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				if rbErr := u.Rollback(); rbErr != nil {
					logger.Error("run.entries.dispatch_failed: rollback failed",
						"message_id", msg.ID, "error", rbErr)
				}
			}
		}()

		msgProcID, dup, err := messageprocessing.DedupWithOutboxEntryID(
			ctx, u.MessageProcessingRepo(), logger,
			msg.ID, runEntriesDispatchFailedStreamName, []byte(payload),
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

		if err := handler.Handle(ctx, u, evt, msgProcID); err != nil {
			if errors.Is(err, pkgevents.ErrPermanent) {
				logger.Error("run.entries.dispatch_failed: permanent handler error",
					"message_id", msg.ID, "error", err)
			} else {
				logger.Error("run.entries.dispatch_failed: transient handler error",
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
