package redis

import (
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
)

// runEntriesDispatchFailedStreamName is the wire-stable name of the Redis
// stream whose messages this binding handles. It is also the value stored in
// the message_processing.stream_name column for dedup rows.
const runEntriesDispatchFailedStreamName = streams.RunEntriesDispatchFailedV1

// NewRunEntriesDispatchFailedBinding returns a pkg/redis.MessageHandler that
// turns each run.entries.dispatch_failed:v1 message into a typed event, runs
// dedup, and invokes RunEntriesDispatchFailedHandler inside a single
// Unit-of-Work transaction. The dedup row ID is threaded to the handler as
// msgProcID so the outbox entry it writes carries provenance back to the
// originating message. See bindStreamHandler for the shared pipeline and
// ACK-policy semantics.
func NewRunEntriesDispatchFailedBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.RunEntriesDispatchFailedHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return bindStreamHandler(uowFactory, logger, streamBinding[events.RunEntriesDispatchFailed]{
		label:      "run.entries.dispatch_failed",
		streamName: runEntriesDispatchFailedStreamName,
		parse:      ParseRunEntriesDispatchFailed,
		payload:    defaultPayload,
		handle:     handler.Handle,
	})
}
