package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/k8s-controller/domain/command"
	"github.com/carolsimone/continuo/k8s-controller/service/handlers"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// NewNodeDeployedBinding returns a pkg/redis.MessageHandler for node.deployed:v1.
// It parses the message, runs dedup, and invokes CheckStatusHandler inside one
// UnitOfWork transaction. Parse failures are permanent (ACK + drop); handler and
// repository failures propagate so the message stays pending for retry.
func NewNodeDeployedBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.CheckStatusHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		cmd, err := ParseNodeDeployed(msg, 0)
		if err != nil {
			logger.Error("node_deployed: parse failure", "message_id", msg.ID, "error", err)
			return fmt.Errorf("%w: %v", pkgevents.ErrPermanent, err)
		}
		return runCheckJobBinding(ctx, uowFactory, handler, logger, msg, cmd, streams.NodeDeployedV1)
	}
}

// runCheckJobBinding is the shared dedup + UoW + handler flow for the two
// full-pattern check-job streams.
func runCheckJobBinding(
	ctx context.Context,
	uowFactory func() uow.UnitOfWork,
	handler *handlers.CheckStatusHandler,
	logger *slog.Logger,
	msg goredis.XMessage,
	cmd command.CheckJobStatus,
	streamName string,
) error {
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
				logger.Error("check_job: rollback failed", "message_id", msg.ID, "error", rbErr)
			}
		}
	}()

	_, dup, err := messageprocessing.DedupWithOutboxEntryID(
		ctx, u.MessageProcessingRepo(), logger,
		msg.ID, streamName, payload,
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
			logger.Error("check_job: permanent handler error", "message_id", msg.ID, "error", err)
		} else {
			logger.Error("check_job: transient handler error", "message_id", msg.ID, "error", err)
		}
		return err
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	committed = true
	return nil
}
