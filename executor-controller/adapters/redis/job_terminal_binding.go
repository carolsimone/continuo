// executor-controller/adapters/redis/job_terminal_binding.go
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
	goredis "github.com/redis/go-redis/v9"
)

// NewJobTerminalBinding returns a pkg/redis.MessageHandler that parses each
// executor.job.terminal:v1 message, runs standard dedup, and invokes
// JobTerminalHandler inside a single Unit-of-Work transaction. The wire-stable
// stream name is sourced from pkg/streams (streams.ExecutorJobTerminalV1); no
// stream literal lives in this file.
//
// This stream carries a normal upstream outbox_entry_id from k8s-controller, so
// dedup is the STANDARD (msg.ID, stream_name) layer with an outbox_entry_id
// fallback. The release itself is idempotent besides, so a delivery that slips
// past dedup frees nothing twice.
//
// Errors are surfaced to the StreamConsumer so it can pick the right ACK policy:
// parse failures are wrapped with events.ErrPermanent (NACK and drop), while
// handler/repository failures propagate as-is (NACK and let the message stay
// pending for retry) — a slot must never be lost to a transient failure. On a
// duplicate the transaction is committed (empty txn) and nil is returned so the
// consumer ACKs.
func NewJobTerminalBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.JobTerminalHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseExecutorJobTerminal(msg)
		if err != nil {
			logger.Error("executor.job.terminal: parse failure", "message_id", msg.ID, "error", err)
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
					logger.Error("executor.job.terminal: rollback failed",
						"message_id", msg.ID, "error", rbErr)
				}
			}
		}()

		msgProcID, dup, err := messageprocessing.DedupWithOutboxEntryID(
			ctx, u.MessageProcessingRepo(), logger,
			msg.ID, streams.ExecutorJobTerminalV1, payload,
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
				logger.Error("executor.job.terminal: permanent handler error",
					"message_id", msg.ID, "error", err)
			} else {
				logger.Error("executor.job.terminal: transient handler error",
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
