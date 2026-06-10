package redis

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"

	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testConsumer(handler MessageHandler) *StreamConsumer {
	return &StreamConsumer{
		streamName: "test-stream",
		logger:     slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		handler:    handler,
	}
}

// safeInvoke must convert a handler panic into a non-permanent error so a single
// poison message cannot crash the consumer process; the message then stays in
// the PEL for the next sweep (transient path), it is not ACK-dropped.
func TestSafeInvoke_RecoversPanic(t *testing.T) {
	c := testConsumer(func(context.Context, goredis.XMessage) error {
		panic("boom")
	})
	err := c.safeInvoke(context.Background(), goredis.XMessage{ID: "1-0"})
	require.Error(t, err)
	assert.False(t, errors.Is(err, events.ErrPermanent), "recovered panic must be transient (stays in PEL)")
	assert.Contains(t, err.Error(), "panic")
}

func TestSafeInvoke_PassesThroughNilAndError(t *testing.T) {
	c := testConsumer(func(context.Context, goredis.XMessage) error { return nil })
	require.NoError(t, c.safeInvoke(context.Background(), goredis.XMessage{ID: "1-0"}))

	sentinel := errors.New("handler error")
	c = testConsumer(func(context.Context, goredis.XMessage) error { return sentinel })
	assert.ErrorIs(t, c.safeInvoke(context.Background(), goredis.XMessage{ID: "1-0"}), sentinel)
}

// invokeWithRetry must not let a handler panic escape: the panic is recovered as
// a transient error, retried, and the loop continues. Here the handler panics on
// its first attempt and succeeds on the second, proving the consumer survives.
func TestInvokeWithRetry_RecoversPanicThenRetries(t *testing.T) {
	var calls atomic.Int32
	c := testConsumer(func(context.Context, goredis.XMessage) error {
		if calls.Add(1) == 1 {
			panic("boom on first attempt")
		}
		return nil
	})
	err := c.invokeWithRetry(context.Background(), goredis.XMessage{ID: "1-0"})
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load())
}
