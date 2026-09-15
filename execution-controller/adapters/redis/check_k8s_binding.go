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
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// NewCheckK8sBinding returns a pkg/redis.MessageHandler for check.k8s:v1. Every
// message it receives is genuinely due — the delay-queue promoter only XADDs due
// tickets — so there is no not-due/recirculation gate. Parse failures are
// permanent (ACK + drop); dedup, handler and repository work all run inside one
// UnitOfWork transaction, so a duplicate message is ACKed without invoking the
// handler and a handler/repository failure propagates so the message stays
// pending for retry.
func NewCheckK8sBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.JobStatusHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		cmd, err := ParseCheckK8s(msg, 0)
		if err != nil {
			logger.Error("check_k8s: parse failure", "message_id", msg.ID, "error", err)
			return fmt.Errorf("%w: %v", pkgevents.ErrPermanent, err)
		}

		// Best-effort dedup payload; marshalling a map of Redis string values does not realistically fail.
		payload, _ := json.Marshal(msg.Values)

		u := uowFactory()
		if err := u.Begin(ctx); err != nil {
			return fmt.Errorf("begin uow: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				if rbErr := u.Rollback(); rbErr != nil {
					logger.Error("check_k8s: rollback failed", "message_id", msg.ID, "error", rbErr)
				}
			}
		}()

		_, dup, err := messageprocessing.DedupWithOutboxEntryID(
			ctx, u.MessageProcessingRepo(), logger,
			msg.ID, streams.CheckK8sV1, payload,
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

		if err := handler.Handle(ctx, u, cmd, uuid.Nil); err != nil {
			if errors.Is(err, pkgevents.ErrPermanent) {
				logger.Error("check_k8s: permanent handler error", "message_id", msg.ID, "error", err)
			} else {
				logger.Error("check_k8s: transient handler error", "message_id", msg.ID, "error", err)
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
