package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// NewTaskStatusReportBinding returns a pkg/redis.MessageHandler that turns
// each task.status.updated:v2 entry into a report, runs dedup, and invokes
// AttemptReportHandler inside a single Unit-of-Work transaction. The dedup
// row's payload is the entry's payload field, and its id is threaded to the
// handler so the task.dispatch:v1 rows the report writes carry provenance back
// to it.
//
// A report for a task its run does not hold, or for an attempt state never
// dispatched, is permanent (permanentWhenNeverValid); every other handler
// error keeps its class. See bindStreamHandler for the shared pipeline and
// ACK-policy semantics.
func NewTaskStatusReportBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.AttemptReportHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return bindStreamHandler(uowFactory, logger, streamBinding[events.AttemptReported]{
		label:      "task.status.updated",
		streamName: streams.TaskStatusUpdatedV2,
		parse:      ParseTaskStatusReport,
		payload:    defaultPayload,
		handle:     permanentWhenNeverValid(handler.Handle),
	})
}

// permanentWhenNeverValid wraps handle so that a report for a task no run
// holds (run.ErrTaskNotHeld) or for an attempt the task was never dispatched
// (run.ErrAttemptNotIssued) is permanent: a run's tasks are stored in the
// transaction that creates it, and each attempt's row in the transaction that
// dispatches it, so a redelivery can never find either, and the consumer
// dead-letters the message at once.
func permanentWhenNeverValid[E any](
	handle func(context.Context, uow.UnitOfWork, E, uuid.UUID) error,
) func(context.Context, uow.UnitOfWork, E, uuid.UUID) error {
	return func(ctx context.Context, u uow.UnitOfWork, evt E, msgProcID uuid.UUID) error {
		err := handle(ctx, u, evt, msgProcID)
		if errors.Is(err, run.ErrTaskNotHeld) || errors.Is(err, run.ErrAttemptNotIssued) {
			return fmt.Errorf("%w: %w", pkgevents.ErrPermanent, err)
		}
		return err
	}
}
