package redis

import (
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
)

// scheduleCatalogStreamName is the wire-stable name of the Redis stream
// whose messages this binding handles. It is also the value stored in the
// message_processing.stream_name column for dedup rows.
const scheduleCatalogStreamName = streams.SchedulesLoadedV1

// NewScheduleCatalogBinding returns a pkg/redis.MessageHandler that turns
// each schedules.loaded:v1 message into a typed event, runs dedup, and
// invokes ScheduleCatalogHandler inside a single Unit-of-Work transaction.
// The handler writes no provenance-bearing outbox entry, so it ignores the
// dedup row ID. See bindStreamHandler for the shared pipeline and ACK-policy
// semantics.
func NewScheduleCatalogBinding(
	uowFactory func() uow.UnitOfWork,
	handler *handlers.ScheduleCatalogHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return bindStreamHandler(uowFactory, logger, streamBinding[events.ScheduleCatalogLoaded]{
		label:      "schedule_catalog",
		streamName: scheduleCatalogStreamName,
		parse:      ParseScheduleCatalogLoaded,
		payload:    defaultPayload,
		handle:     handler.Handle,
	})
}
