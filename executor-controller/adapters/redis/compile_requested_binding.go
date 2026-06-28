// executor-controller/adapters/redis/compile_requested_binding.go
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// compileDedupNamespace derives a deterministic outbox_entry_id keying
// compile.requested:v1 dedup on the release rather than (message_id,
// stream_name). DO NOT CHANGE — a different value would re-dedup every
// previously processed release as fresh.
var compileDedupNamespace = uuid.MustParse("a7c3e1f9-2b4d-6e8a-0c2e-4f6a8b0c2e4f")

// compileDedupKey maps a release_id to the deterministic UUID used as the
// outbox_entry_id override in message_processing. One compile.requested:v1
// message carries one service per release, so dedup is per-release: a
// redelivery (even with a fresh Redis message_id) collides on this key and is
// skipped, so the compile deployment is enqueued exactly once.
func compileDedupKey(releaseID string) uuid.UUID {
	return uuid.NewSHA1(compileDedupNamespace, []byte("compile-requested:"+releaseID))
}

// NewCompileRequestedBinding returns a pkg/redis.MessageHandler that parses
// each compile.requested:v1 message, runs per-release dedup, and invokes
// CompileRequestedHandler inside a single Unit-of-Work transaction. The
// wire-stable stream name is sourced from pkg/streams
// (streams.CompileRequestedV1); no stream literal lives in this file.
//
// Errors are surfaced to the StreamConsumer so it can pick the right ACK
// policy: parse failures are wrapped with events.ErrPermanent (NACK and drop),
// while handler/repository failures propagate as-is (NACK and let the message
// stay pending for retry). On a duplicate the transaction is committed (empty
// txn) and nil is returned so the consumer ACKs.
//
// No EnsureCandidateSchema is needed for compile — the compile job writes
// to S3, not Postgres.
func NewCompileRequestedBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.CompileRequestedHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseCompileRequested(msg)
		if err != nil {
			logger.Error("compile.requested: parse failure", "message_id", msg.ID, "error", err)
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
					logger.Error("compile.requested: rollback failed",
						"message_id", msg.ID, "error", rbErr)
				}
			}
		}()

		dedupKey := compileDedupKey(evt.ReleaseID)
		msgProcID, dup, err := messageprocessing.DedupWithOutboxEntryID(
			ctx, u.MessageProcessingRepo(), logger,
			msg.ID, streams.CompileRequestedV1, payload,
			&dedupKey,
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
				logger.Error("compile.requested: permanent handler error",
					"message_id", msg.ID, "error", err)
			} else {
				logger.Error("compile.requested: transient handler error",
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
