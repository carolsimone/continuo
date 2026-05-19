// executor-controller/adapters/redis/query_model_binding.go
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

// NewQueryModelBinding returns a pkg/redis.MessageHandler that parses each
// query.model:v1 message, runs dedup, and invokes QueryModelHandler inside
// a single Unit-of-Work transaction. The wire-stable stream name is sourced
// from pkg/streams (streams.QueryModelV1); no stream literal lives in this
// file.
//
// Errors are surfaced to the StreamConsumer so it can pick the right ACK
// policy: parse failures are wrapped with events.ErrPermanent (NACK and
// drop), while handler/repository failures propagate as-is (NACK and let
// the message stay pending for retry). On a duplicate the transaction is
// committed (empty txn) and nil is returned so the consumer ACKs.
func NewQueryModelBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.QueryModelHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseQueryModel(msg)
		if err != nil {
			logger.Error("query.model: parse failure", "message_id", msg.ID, "error", err)
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
					logger.Error("query.model: rollback failed",
						"message_id", msg.ID, "error", rbErr)
				}
			}
		}()

		msgProcID, dup, err := messageprocessing.Dedup(
			ctx, u.MessageProcessingRepo(), logger,
			msg.ID, streams.QueryModelV1, payload,
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
				logger.Error("query.model: permanent handler error",
					"message_id", msg.ID, "error", err)
			} else {
				logger.Error("query.model: transient handler error",
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
