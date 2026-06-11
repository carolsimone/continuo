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
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// taskExecutionRecordedStreamName is the wire-stable name of the Redis stream
// whose messages this binding handles. It is also the value stored in the
// message_processing.stream_name column for dedup rows.
const taskExecutionRecordedStreamName = streams.TaskExecutionRecordedV1

// NewTaskExecutionRecordedBinding returns a pkg/redis.MessageHandler that
// turns each task.execution.recorded:v1 message into a typed event, runs
// dedup, and invokes TaskExecutionRecordedHandler inside a single
// Unit-of-Work transaction.
//
// task.execution.recorded:v1 uses flat msg.Values fields rather than a
// single JSON payload, so the binding serializes the full Values map and
// stores it as the message_processing.payload. The dedup key remains
// (message_id, stream_name); the payload is observability-only.
//
// The handler writes no outbox entries, so msgProcID is unused — we pass
// uuid.Nil to satisfy the uniform Handle signature.
//
// Errors are surfaced to the StreamConsumer so it can pick the right ACK
// policy: parse failures are wrapped with events.ErrPermanent (NACK and
// drop), while handler/repository failures propagate as-is (NACK and let
// the message stay pending for retry). On a duplicate the transaction is
// committed (empty txn) and nil is returned so the consumer ACKs.
func NewTaskExecutionRecordedBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.TaskExecutionRecordedHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseTaskExecutionRecorded(msg)
		if err != nil {
			logger.Error("task.execution.recorded: parse failure", "message_id", msg.ID, "error", err)
			return fmt.Errorf("%w: %v", pkgevents.ErrPermanent, err)
		}

		// Flat-field stream — serialize the whole Values map for dedup-row
		// observability. message_processing.payload is JSONB NOT NULL, so a
		// nil/zero payload would fail the insert. Marshal errors collapse to
		// "{}" rather than aborting the message: the payload is forensic
		// only, dedup correctness rides on message_id.
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
					logger.Error("task.execution.recorded: rollback failed",
						"message_id", msg.ID, "error", rbErr)
				}
			}
		}()

		_, dup, err := messageprocessing.DedupWithOutboxEntryID(
			ctx, u.MessageProcessingRepo(), logger,
			msg.ID, taskExecutionRecordedStreamName, payload,
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

		if err := handler.Handle(ctx, u, evt, uuid.Nil); err != nil {
			if errors.Is(err, pkgevents.ErrPermanent) {
				logger.Error("task.execution.recorded: permanent handler error",
					"message_id", msg.ID, "error", err)
			} else {
				logger.Error("task.execution.recorded: transient handler error",
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
