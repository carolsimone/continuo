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

// taskStatusUpdatedStreamName is the wire-stable name of the Redis stream
// whose messages this binding handles. It is also the value stored in the
// message_processing.stream_name column for dedup rows.
const taskStatusUpdatedStreamName = streams.TaskStatusUpdatedV1

// NewTaskStatusUpdatedBinding returns a pkg/redis.MessageHandler that turns
// each task.status.updated:v1 message into a typed event, runs dedup, and
// invokes TaskStatusUpdatedHandler inside a single Unit-of-Work transaction.
//
// task.status.updated:v1 uses flat msg.Values fields rather than a single JSON
// payload, so the dedup row's payload is the JSON-encoded Values map
// (valuesPayload); the payload is observability-only and dedup correctness
// rides on message_id. The dedup row ID is threaded to the handler as msgProcID
// so the query.model:v1 outbox entries a success writes carry provenance back
// to the originating message.
//
// A status for a task its run does not hold is permanent
// (permanentWhenTaskNotHeld); every other handler error keeps its class. See
// bindStreamHandler for the shared pipeline and ACK-policy semantics.
func NewTaskStatusUpdatedBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.TaskStatusUpdatedHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return bindStreamHandler(uowFactory, logger, streamBinding[events.TaskStatusUpdated]{
		label:      "task.status.updated",
		streamName: taskStatusUpdatedStreamName,
		parse:      ParseTaskStatusUpdated,
		payload:    valuesPayload,
		handle:     permanentWhenTaskNotHeld(handler.Handle),
	})
}

// permanentWhenTaskNotHeld wraps handle so that a status for a task its run
// does not hold (run.ErrTaskNotHeld) is permanent: a run's tasks are stored in
// the transaction that creates it, before any is dispatched, so a redelivery
// can never find the task, and the consumer dead-letters the message at once.
func permanentWhenTaskNotHeld(
	handle func(context.Context, uow.UnitOfWork, events.TaskStatusUpdated, uuid.UUID) error,
) func(context.Context, uow.UnitOfWork, events.TaskStatusUpdated, uuid.UUID) error {
	return func(ctx context.Context, u uow.UnitOfWork, evt events.TaskStatusUpdated, msgProcID uuid.UUID) error {
		err := handle(ctx, u, evt, msgProcID)
		if errors.Is(err, run.ErrTaskNotHeld) {
			return fmt.Errorf("%w: %w", pkgevents.ErrPermanent, err)
		}
		return err
	}
}
