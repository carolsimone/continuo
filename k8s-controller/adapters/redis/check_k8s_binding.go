package redis

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/k8s-controller/service/handlers"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

// NewCheckK8sBinding returns a pkg/redis.MessageHandler for check.k8s:v1. Every
// message it receives is genuinely due — the delay-queue promoter only XADDs due
// tickets — so there is no not-due/recirculation gate. Parse failures are
// permanent (ACK + drop); handler/repository failures propagate so the message
// stays pending for retry.
func NewCheckK8sBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.CheckStatusHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		cmd, err := ParseCheckK8s(msg, 0)
		if err != nil {
			logger.Error("check_k8s: parse failure", "message_id", msg.ID, "error", err)
			return fmt.Errorf("%w: %v", pkgevents.ErrPermanent, err)
		}
		return runCheckJobBinding(ctx, uowFactory, handler, logger, msg, cmd, streams.CheckK8sV1)
	}
}
