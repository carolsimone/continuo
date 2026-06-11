package redis

import (
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
)

// taskExecutionRecordedStreamName is the wire-stable name of the Redis stream
// whose messages this binding handles. It is also the value stored in the
// message_processing.stream_name column for dedup rows.
const taskExecutionRecordedStreamName = streams.TaskExecutionRecordedV1

// NewTaskExecutionRecordedBinding returns a pkg/redis.MessageHandler that
// turns each task.execution.recorded:v1 message into a typed event, runs
// dedup, and invokes TaskExecutionRecordedHandler inside a single Unit-of-Work
// transaction.
//
// task.execution.recorded:v1 uses flat msg.Values fields rather than a single
// JSON payload, so the dedup row's payload is the JSON-encoded Values map
// (valuesPayload); the payload is observability-only and dedup correctness
// rides on message_id. The handler writes no outbox entry, so it ignores the
// dedup row ID. See bindStreamHandler for the shared pipeline and ACK-policy
// semantics.
func NewTaskExecutionRecordedBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.TaskExecutionRecordedHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return bindStreamHandler(uowFactory, logger, streamBinding[events.TaskExecutionRecorded]{
		label:      "task.execution.recorded",
		streamName: taskExecutionRecordedStreamName,
		parse:      ParseTaskExecutionRecorded,
		payload:    valuesPayload,
		handle:     handler.Handle,
	})
}
