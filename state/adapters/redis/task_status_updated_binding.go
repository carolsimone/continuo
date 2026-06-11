package redis

import (
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
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
// so the run.finalized:v1 outbox entry emitted on the finalize branch carries
// provenance back to the originating message.
//
// The transient task-not-found path inside the handler returns a plain error
// (NOT wrapped with ErrPermanent): bindStreamHandler rolls back and the
// StreamConsumer's reclaim loop redelivers the message after
// run.entries.dispatched:v1 has caught up. See bindStreamHandler for the shared
// pipeline and ACK-policy semantics.
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
		handle:     handler.Handle,
	})
}
