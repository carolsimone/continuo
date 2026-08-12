package redis

import (
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
)

// releaseSeedsPendingStreamName is the wire-stable name of the Redis stream whose
// messages this binding handles. It is also the value stored in the
// message_processing.stream_name column for dedup rows.
//
// state is the only consumer of this stream, so its dedup rows are scoped by
// the stream name rather than by a group name.
const releaseSeedsPendingStreamName = streams.ReleaseSeedsPendingV1

// NewReleaseSeedsPendingBinding returns a pkg/redis.MessageHandler that parses each
// release.promoted:v1 message, runs dedup, and invokes PromotedSeedsHandler
// inside a single Unit-of-Work transaction. See bindStreamHandler for the shared
// pipeline and ACK-policy semantics.
func NewReleaseSeedsPendingBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.PromotedSeedsHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return bindStreamHandler(uowFactory, logger, streamBinding[events.ReleaseSeedsPending]{
		label:      "release.seeds.pending",
		streamName: releaseSeedsPendingStreamName,
		parse:      ParseReleaseSeedsPending,
		payload:    defaultPayload,
		handle:     handler.Handle,
	})
}
