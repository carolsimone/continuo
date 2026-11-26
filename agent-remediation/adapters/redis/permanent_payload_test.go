package redis

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// A payload no redelivery can decode must reach the consumer as a permanent
// error, so it is dead-lettered instead of silently acknowledged.
func TestHandlers_UndecodablePayloadIsPermanent(t *testing.T) {
	h := newHealTriggeredHandler(func(context.Context, runner.Accepted) error {
		t.Fatal("a payload that cannot be decoded must not be accepted")
		return nil
	})
	valid, _ := healTriggeredMessageOf(t, "3-0", "acme", healTriggeredFixture(), nil)
	withField := func(field string, value any) goredis.XMessage {
		values := map[string]any{}
		for k, v := range valid.Values {
			values[k] = v
		}
		values[field] = value
		return goredis.XMessage{ID: "4-0", Values: values}
	}
	messages := map[string]goredis.XMessage{
		"missing payload":            {ID: "1-0", Values: map[string]any{}},
		"undecodable":                {ID: "2-0", Values: map[string]any{"payload": "{not json"}},
		"an envelope without a time": withField("occurred_at", ""),
		"another schema version":     withField("schema_version", "2"),
		"a payload that is not JSON": withField("payload", "{not json"),
		"a payload with no release":  withField("payload", `{"source":"validation","remediation_round":1,"nodes":[]}`),
		"a payload with no round":    withField("payload", `{"source":"validation","release_id":"r","nodes":[]}`),
	}
	for mname, m := range messages {
		err := h(context.Background(), m)
		assert.ErrorIs(t, err, pkgevents.ErrPermanent, mname)
	}
}
