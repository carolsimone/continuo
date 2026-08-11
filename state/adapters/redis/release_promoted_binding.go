package redis

import (
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
)

// releasePromotedStreamName is the wire-stable name of the Redis stream whose
// messages this binding handles. It is also the value stored in the
// message_processing.stream_name column for dedup rows.
//
// release.promoted:v1 is read by several consumer groups, but state has exactly
// one of them, so its dedup rows are scoped by the stream name rather than by
// the group name.
const releasePromotedStreamName = streams.ReleasePromotedV1

// NewReleasePromotedBinding returns a pkg/redis.MessageHandler that parses each
// release.promoted:v1 message, runs dedup, and invokes PromotedSeedsHandler
// inside a single Unit-of-Work transaction. See bindStreamHandler for the shared
// pipeline and ACK-policy semantics.
func NewReleasePromotedBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.PromotedSeedsHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return bindStreamHandler(uowFactory, logger, streamBinding[events.ReleasePromoted]{
		label:      "release.promoted",
		streamName: releasePromotedStreamName,
		parse:      ParseReleasePromoted,
		payload:    defaultPayload,
		handle:     handler.Handle,
	})
}
