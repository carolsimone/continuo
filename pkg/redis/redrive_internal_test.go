package redis

import (
	"context"
	"errors"
	"testing"

	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestProcessOne_RedriveForOtherGroupIsAckedNotHandled(t *testing.T) {
	rec := &recorder{}
	handled := 0
	c, _ := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error { handled++; return nil }, rec)
	c.processOne(context.Background(), goredis.XMessage{ID: "1-0", Values: map[string]any{"k": "v", RedriveGroupField: "another-group"}})
	assert.Equal(t, 0, handled, "another group's redrive must not reach the handler")
	assert.Equal(t, []string{"ack:1-0"}, rec.calls)
}

func TestProcessOne_RedriveFieldsStrippedBeforeHandler(t *testing.T) {
	rec := &recorder{}
	var seen map[string]any
	c, _ := reliabilityConsumer(t, func(_ context.Context, m goredis.XMessage) error { seen = m.Values; return nil }, rec)
	c.processOne(context.Background(), goredis.XMessage{ID: "2-0", Values: map[string]any{
		"k": "v", RedriveGroupField: "svc-orders", RedrivenFromField: "dl-1",
	}})
	assert.Equal(t, map[string]any{"k": "v"}, seen)
	assert.Equal(t, []string{"ack:2-0"}, rec.calls)
}

func TestProcessOne_RedriveWithoutGroupGoesToEveryGroup(t *testing.T) {
	rec := &recorder{}
	handled := 0
	c, _ := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error { handled++; return nil }, rec)
	c.processOne(context.Background(), goredis.XMessage{ID: "3-0", Values: map[string]any{"k": "v", RedrivenFromField: "dl-2"}})
	assert.Equal(t, 1, handled, "an outbox redrive names no group: every group handles it")
}

func TestSettleReclaimed_EmptyEntryIsAckedNotHandled(t *testing.T) {
	rec := &recorder{}
	handled := 0
	c, _ := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error { handled++; return nil }, rec)
	c.settleReclaimed(context.Background(), goredis.XMessage{ID: "4-0"})
	assert.Equal(t, 0, handled)
	assert.Equal(t, []string{"ack:4-0"}, rec.calls)
}

func TestProcessOne_WithoutDeadLettersKeepsPermanentPending(t *testing.T) {
	rec := &recorder{}
	c, _ := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error { return events.ErrPermanent }, rec)
	WithoutDeadLetters()(c)
	c.processOne(context.Background(), msg("5-0"))
	assert.Empty(t, rec.deadLetters)
	assert.NotContains(t, rec.calls, "ack:5-0")
}

func TestSettleReclaimed_WithoutDeadLettersKeepsExhaustedPending(t *testing.T) {
	rec := &recorder{deliveries: 9}
	c, _ := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error { return errors.New("still failing") }, rec)
	WithoutDeadLetters()(c)
	c.settleReclaimed(context.Background(), msg("6-0"))
	assert.Empty(t, rec.deadLetters)
	assert.NotContains(t, rec.calls, "ack:6-0")
}
