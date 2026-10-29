package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// streamBinding describes one Redis-stream binding. Every run-controller consumer runs
// the identical parse → load (when set) → begin → rollback-defer → dedup →
// handle → mark completed → commit pipeline (bindStreamHandler below); the only
// things that vary per stream are captured here.
//
// E is the parsed event type produced by Parse.
type streamBinding[E any] struct {
	// label is the stream's short name used in log lines
	// (e.g. "task.status.updated").
	label string
	// streamName is the wire-stable Redis stream name, also stored in the
	// message_processing.stream_name column for dedup rows.
	streamName string
	// parse turns a raw Redis message into the typed event. A parse failure is
	// permanent: bindStreamHandler wraps it with events.ErrPermanent so the
	// consumer dead-letters the poison message.
	parse func(msg goredis.XMessage) (E, error)
	// load, when set, completes the parsed event with input read from outside
	// the database, such as an object-storage read, before any transaction
	// opens. Its error is returned as it is: a permanent one is dead-lettered,
	// an outage pauses the consumer, and any other is retried.
	load func(ctx context.Context, evt E) (E, error)
	// payload derives the bytes stored in the dedup row's payload column:
	// defaultPayload reads the explicit "payload" stream field.
	payload func(msg goredis.XMessage) []byte
	// handle invokes the domain handler. msgProcID is the dedup row id; handlers
	// that emit no follow-on outbox entry ignore it (the binding passes uuid.Nil
	// shape through the handler closure instead).
	handle func(ctx context.Context, u uow.UnitOfWork, evt E, msgProcID uuid.UUID) error
}

// defaultPayload reads the explicit "payload" stream field (the shape used by
// streams that publish a single JSONB blob). A missing field yields an empty
// slice; the dedup insert tolerates it because correctness rides on message_id.
func defaultPayload(msg goredis.XMessage) []byte {
	payload, _ := msg.Values["payload"].(string)
	return []byte(payload)
}

// bindStreamHandler builds the pkg/redis.MessageHandler for a streamBinding.
//
// Errors are surfaced to the StreamConsumer so it can pick the right ACK
// policy: parse failures are wrapped with events.ErrPermanent (the consumer
// dead-letters the message), while load, handler and repository failures
// propagate as they are and the consumer classifies them. On a duplicate the
// transaction is committed (empty txn) and nil is returned so the consumer
// ACKs. A handled message's dedup row is marked completed in the handler's
// transaction, just before it commits.
func bindStreamHandler[E any](
	uowFactory func() uow.UnitOfWork,
	logger *slog.Logger,
	b streamBinding[E],
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := b.parse(msg)
		if err != nil {
			logger.Error(b.label+": parse failure", "message_id", msg.ID, "error", err)
			return fmt.Errorf("%w: %v", pkgevents.ErrPermanent, err)
		}

		if b.load != nil {
			evt, err = b.load(ctx, evt)
			if err != nil {
				logger.Error(b.label+": load failure", "message_id", msg.ID,
					"permanent", errors.Is(err, pkgevents.ErrPermanent), "error", err)
				return err
			}
		}

		u := uowFactory()
		if err := u.Begin(ctx); err != nil {
			return fmt.Errorf("begin uow: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				if rbErr := u.Rollback(); rbErr != nil {
					logger.Error(b.label+": rollback failed", "message_id", msg.ID, "error", rbErr)
				}
			}
		}()

		msgProcID, dup, err := messageprocessing.DedupWithOutboxEntryID(
			ctx, u.MessageProcessingRepo(), logger,
			msg.ID, b.streamName, b.payload(msg),
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

		if err := b.handle(ctx, u, evt, msgProcID); err != nil {
			if errors.Is(err, pkgevents.ErrPermanent) {
				logger.Error(b.label+": permanent handler error", "message_id", msg.ID, "error", err)
			} else {
				logger.Error(b.label+": transient handler error", "message_id", msg.ID, "error", err)
			}
			return err
		}
		if err := u.MessageProcessingRepo().UpdateState(ctx, msgProcID, messageprocessing.StateCompleted); err != nil {
			return fmt.Errorf("mark completed: %w", err)
		}
		if err := u.Commit(); err != nil {
			return fmt.Errorf("commit tx: %w", err)
		}
		committed = true
		return nil
	}
}
