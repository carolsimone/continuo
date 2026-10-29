package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/events"
	"github.com/carolsimone/continuo/run-controller/service/handlers"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/google/uuid"
)

// NewTaskStatusReportBinding returns a pkg/redis.MessageHandler that turns
// each task.status.updated:v2 entry into a report carrying its envelope's
// tenant, runs dedup, and invokes AttemptReportHandler inside a single
// Unit-of-Work transaction. The dedup row's payload is the entry's payload
// field, and its id is threaded to the handler so the task.dispatch:v1 rows
// the report writes carry provenance back to it.
//
// A report for a run state never created, for a run of another tenant than
// its envelope names, for a task its run does not hold, for an attempt state
// never dispatched, or a stopped report of an attempt state never asked to
// stop, is permanent (permanentWhenNeverValid); every other handler error
// keeps its class. See bindStreamHandler for the shared pipeline and
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

// permanentWhenNeverValid wraps handle so that a report for a run with no row
// under the tenant its envelope names (postgres.ErrNotFound, which the run's
// lock returns), for a task its run does not hold (run.ErrTaskNotHeld), for
// an attempt the task was never dispatched (run.ErrAttemptNotIssued) or a
// stopped report of an open attempt state never asked to stop
// (run.ErrStopNotRequested) is permanent: a run's row and tasks are stored,
// under the run's tenant, in the transaction that creates it, before any task
// is dispatched, each attempt's row in the transaction that dispatches it,
// and a stop is requested in the transaction that cancels the run, so a
// redelivery can never find any of them, and the consumer dead-letters the
// message at once.
func permanentWhenNeverValid[E any](
	handle func(context.Context, uow.UnitOfWork, E, uuid.UUID) error,
) func(context.Context, uow.UnitOfWork, E, uuid.UUID) error {
	return func(ctx context.Context, u uow.UnitOfWork, evt E, msgProcID uuid.UUID) error {
		err := handle(ctx, u, evt, msgProcID)
		if errors.Is(err, postgres.ErrNotFound) || errors.Is(err, run.ErrTaskNotHeld) || errors.Is(err, run.ErrAttemptNotIssued) ||
			errors.Is(err, run.ErrStopNotRequested) {
			return fmt.Errorf("%w: %w", pkgevents.ErrPermanent, err)
		}
		return err
	}
}
