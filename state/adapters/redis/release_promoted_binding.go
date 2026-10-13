package redis

import (
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	goredis "github.com/redis/go-redis/v9"
)

// releasePromotedStreamName is the wire-stable name of the Redis stream whose
// messages this binding handles. It is also the value stored in the
// message_processing.stream_name column for dedup rows.
//
// state has one consumer group on this stream, so its dedup rows are scoped by
// the stream name rather than by a group name.
const releasePromotedStreamName = streams.ReleasePromotedV2

// NewReleasePromotedBinding returns a pkg/redis.MessageHandler that parses each
// release.promoted:v2 entry, reads the release's topology artifact through
// handler.Load before any transaction opens, then runs dedup and handler.Handle
// in one Unit-of-Work transaction. See bindStreamHandler for the shared
// pipeline and ACK-policy semantics.
func NewReleasePromotedBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.ReleasePromotedHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return bindStreamHandler(uowFactory, logger, streamBinding[handlers.ReleasePromotedInput]{
		label:      "release.promoted",
		streamName: releasePromotedStreamName,
		parse: func(msg goredis.XMessage) (handlers.ReleasePromotedInput, error) {
			p, err := ParseReleasePromoted(msg)
			return handlers.ReleasePromotedInput{Promotion: p}, err
		},
		load:    handler.Load,
		payload: defaultPayload,
		handle:  handler.Handle,
	})
}
