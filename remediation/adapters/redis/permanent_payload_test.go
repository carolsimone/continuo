package redis

import (
	"context"
	"io"
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
// release.rejected:v1 handler and its retry replay share one constructor, so
// both streams are covered.
func TestHandlers_UndecodablePayloadIsPermanent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	messages := map[string]goredis.XMessage{
		"missing payload": {ID: "1-0", Values: map[string]any{}},
		"undecodable":     {ID: "2-0", Values: map[string]any{"payload": "{not json"}},
	}
	for _, stream := range []string{streams.ReleaseRejectedV1, streams.RemediationRetryRequestedV1} {
		h := newRejectionHandler(stream, handlers.Deps{}, logger)
		for mname, m := range messages {
			err := h(context.Background(), m)
			assert.ErrorIs(t, err, pkgevents.ErrPermanent, "%s / %s", stream, mname)
		}
	}
}
