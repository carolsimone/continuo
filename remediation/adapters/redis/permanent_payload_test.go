package redis

import (
	"context"
	"log/slog"
	"testing"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/remediation/service/handlers"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// A payload no redelivery can decode must reach the consumer as a permanent
// error, so it is dead-lettered instead of silently acknowledged. The
// release.rejected:v1 handler and the remediation.retry_requested:v2 handler
// both read a payload field.
func TestHandlers_UndecodablePayloadIsPermanent(t *testing.T) {
	messages := map[string]goredis.XMessage{
		"missing payload": {ID: "1-0", Values: map[string]any{}},
		"undecodable":     {ID: "2-0", Values: map[string]any{"payload": "{not json"}},
	}
	logger := slog.New(slog.DiscardHandler)
	handlersByStream := map[string]func(context.Context, goredis.XMessage) error{
		streams.ReleaseRejectedV1:           newRejectionHandler(streams.ReleaseRejectedV1, handlers.Deps{}, logger),
		streams.RemediationRetryRequestedV2: newRetryHandler(handlers.Deps{}, logger),
	}
	for stream, h := range handlersByStream {
		for mname, m := range messages {
			err := h(context.Background(), m)
			assert.ErrorIs(t, err, pkgevents.ErrPermanent, "%s / %s", stream, mname)
		}
	}
}
