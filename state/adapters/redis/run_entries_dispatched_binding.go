package redis

import (
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
)

// runEntriesDispatchedStreamName is the wire-stable name of the Redis stream
// whose messages this binding handles. It is also the value stored in the
// message_processing.stream_name column for dedup rows.
const runEntriesDispatchedStreamName = streams.RunEntriesDispatchedV1

// NewRunEntriesDispatchedBinding returns a pkg/redis.MessageHandler that
// parses each run.entries.dispatched:v1 message, runs dedup, and invokes
// RunEntriesDispatchedHandler inside a single Unit-of-Work transaction. The
// dedup row ID is threaded to the handler as msgProcID so the run.finalized:v1
// outbox entry emitted on the auto-rollup branch carries provenance back to
// the originating message. See bindStreamHandler for the shared pipeline and
// ACK-policy semantics.
func NewRunEntriesDispatchedBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.RunEntriesDispatchedHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return bindStreamHandler(uowFactory, logger, streamBinding[events.RunEntriesDispatched]{
		label:      "run.entries.dispatched",
		streamName: runEntriesDispatchedStreamName,
		parse:      ParseRunEntriesDispatched,
		payload:    defaultPayload,
		handle:     handler.Handle,
	})
}
