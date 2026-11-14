package redis

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/service/handlers"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// A payload no redelivery can decode must reach the consumer as a permanent
// error, so it is dead-lettered instead of silently acknowledged.
func TestHandlers_UndecodablePayloadIsPermanent(t *testing.T) {
	t.Run("legacy", func(t *testing.T) {
		requireUndecodablePayloadPermanent(t, newLegacyRemediationRequestedHandler(handlers.Deps{}))
	})
	t.Run("runner", func(t *testing.T) {
		requireUndecodablePayloadPermanent(t, newRunnerRemediationRequestedHandler(func(context.Context, runner.Accepted) error {
			t.Fatal("a payload that cannot be decoded must not be accepted")
			return nil
		}))
	})
}

func requireUndecodablePayloadPermanent(t *testing.T, h pkgredis.MessageHandler) {
	messages := map[string]goredis.XMessage{
		"missing payload": {ID: "1-0", Values: map[string]any{}},
		"undecodable":     {ID: "2-0", Values: map[string]any{"payload": "{not json"}},
	}
	for mname, m := range messages {
		err := h(context.Background(), m)
		assert.ErrorIs(t, err, pkgevents.ErrPermanent, mname)
	}
}
