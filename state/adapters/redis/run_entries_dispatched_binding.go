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

// runEntriesDispatchedStreamName is the wire-stable name of the Redis stream
// whose messages this binding handles. It is also the value stored in the
// message_processing.stream_name column for dedup rows.
const runEntriesDispatchedStreamName = "run.entries.dispatched:v1"

// NewRunEntriesDispatchedBinding returns a pkg/redis.MessageHandler that
// parses each run.entries.dispatched:v1 message, runs dedup, and invokes
// RunEntriesDispatchedHandler inside a single Unit-of-Work transaction. The
// dedup row ID returned by Dedup is threaded to the handler as msgProcID so
// the run.finalized:v1 outbox entry emitted on the auto-rollup branch carries
// provenance back to the originating message.
//
// Errors are surfaced to the StreamConsumer so it can pick the right ACK
// policy: parse failures are wrapped with events.ErrPermanent (NACK and
// drop), while handler/repository failures propagate as-is (NACK and let
// the message stay pending for retry). On a duplicate the transaction is
// committed (empty txn) and nil is returned so the consumer ACKs.
func NewRunEntriesDispatchedBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.RunEntriesDispatchedHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseRunEntriesDispatched(msg)
		if err != nil {
			logger.Error("run.entries.dispatched: parse failure", "message_id", msg.ID, "error", err)
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
					logger.Error("run.entries.dispatched: rollback failed",
						"message_id", msg.ID, "error", rbErr)
				}
			}
		}()

		msgProcID, dup, err := messageprocessing.Dedup(
			ctx, u.MessageProcessingRepo(), logger,
			msg.ID, runEntriesDispatchedStreamName, []byte(payload),
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
				logger.Error("run.entries.dispatched: permanent handler error",
					"message_id", msg.ID, "error", err)
			} else {
				logger.Error("run.entries.dispatched: transient handler error",
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
