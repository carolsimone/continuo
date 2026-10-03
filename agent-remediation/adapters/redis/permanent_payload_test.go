package redis

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/service/handlers"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// A payload no redelivery can decode must reach the consumer as a permanent
// error, so it is dead-lettered instead of silently acknowledged.
func TestHandlers_UndecodablePayloadIsPermanent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newRemediationRequestedHandler(handlers.Deps{}, logger)
	messages := map[string]goredis.XMessage{
		"missing payload": {ID: "1-0", Values: map[string]any{}},
		"undecodable":     {ID: "2-0", Values: map[string]any{"payload": "{not json"}},
	}
	for mname, m := range messages {
		err := h(context.Background(), m)
		assert.ErrorIs(t, err, pkgevents.ErrPermanent, mname)
	}
}
