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
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// streamBinding describes one Redis-stream binding. Every state consumer runs
// the identical parse → begin → rollback-defer → dedup → handle → commit
// pipeline (bindStreamHandler below); the only things that vary per stream are
// captured here.
//
// E is the parsed event type produced by Parse.
type streamBinding[E any] struct {
	// label is the stream's short name used in log lines
	// (e.g. "run.entries.dispatched").
	label string
	// streamName is the wire-stable Redis stream name, also stored in the
	// message_processing.stream_name column for dedup rows.
	streamName string
	// parse turns a raw Redis message into the typed event. A parse failure is
	// permanent: bindStreamHandler wraps it with events.ErrPermanent so the
	// consumer ACKs and drops the poison message.
	parse func(msg goredis.XMessage) (E, error)
	// payload derives the bytes stored in the dedup row's payload column. Two
	// shapes exist: the explicit "payload" stream field, and a JSON encoding of
	// the whole Values map for flat-field streams. defaultPayload / valuesPayload
	// cover both.
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

// valuesPayload JSON-encodes the whole Values map, used by flat-field streams
// that carry no single "payload" blob. message_processing.payload is JSONB NOT
// NULL, so a marshal failure collapses to "{}" rather than aborting the message:
// the payload is forensic only.
func valuesPayload(msg goredis.XMessage) []byte {
	payload, err := json.Marshal(msg.Values)
	if err != nil {
		return []byte("{}")
	}
	return payload
}

// bindStreamHandler builds the pkg/redis.MessageHandler for a streamBinding.
//
// Errors are surfaced to the StreamConsumer so it can pick the right ACK
// policy: parse failures are wrapped with events.ErrPermanent (NACK and drop),
// while handler/repository failures propagate as-is (NACK and leave the message
// pending for retry). On a duplicate the transaction is committed (empty txn)
// and nil is returned so the consumer ACKs.
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
		if err := u.Commit(); err != nil {
			return fmt.Errorf("commit tx: %w", err)
		}
		committed = true
		return nil
	}
}
