package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/service/handlers"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

// checkStreamMaxLen bounds check.k8s:v1 on every recirculation write, matching
// the project-wide streamMaxLen convention. Phase-1 backstop for #282; Phase 2
// removes the recirculation path entirely.
const checkStreamMaxLen = 10000

// recirculateArgs builds the capped XADD for re-circulating a not-yet-due
// check ticket.
func recirculateArgs(values map[string]interface{}) *goredis.XAddArgs {
	return &goredis.XAddArgs{
		Stream: streams.CheckK8sV1,
		MaxLen: checkStreamMaxLen,
		Approx: true,
		Values: values,
	}
}

// checkAfterElapsed reports whether a check.k8s:v1 message is due for
// processing. A message carrying a "check_after" Unix-second timestamp in the
// future is not yet due and should be re-circulated. A missing or unparseable
// timestamp is treated as ready (process now).
func checkAfterElapsed(msg goredis.XMessage, now time.Time) bool {
	s, ok := msg.Values["check_after"].(string)
	if !ok || s == "" {
		return true
	}
	checkAfter, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return true
	}
	return now.Unix() >= checkAfter
}

// NewCheckK8sBinding returns a pkg/redis.MessageHandler for check.k8s:v1.
// Before processing it gates on check_after: a message whose check_after is in
// the future is re-circulated (XAdd a fresh copy) and ACKed, deferring the
// re-check. Due messages run the shared dedup + UoW + handler flow.
func NewCheckK8sBinding(
	client *goredis.Client,
	uowFactory func() uow.UnitOfWork,
	handler *handlers.CheckStatusHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		if !checkAfterElapsed(msg, time.Now()) {
			if err := client.XAdd(ctx, recirculateArgs(msg.Values)).Err(); err != nil {
				return fmt.Errorf("re-circulate check.k8s message: %w", err)
			}
			return nil // ACK the original; the fresh copy carries the re-check
		}
		cmd, err := ParseCheckK8s(msg, 0)
		if err != nil {
			logger.Error("check_k8s: parse failure", "message_id", msg.ID, "error", err)
			return fmt.Errorf("%w: %v", pkgevents.ErrPermanent, err)
		}
		return runCheckJobBinding(ctx, uowFactory, handler, logger, msg, cmd, streams.CheckK8sV1)
	}
}
