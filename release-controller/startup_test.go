package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func refused() error { return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED} }

func TestRetryWhileUnreachable_RetriesAnOutageUntilItClears(t *testing.T) {
	calls := 0
	step := func(context.Context) error {
		calls++
		if calls < 3 {
			return refused()
		}
		return nil
	}
	require.NoError(t, retryWhileUnreachable(context.Background(), discardLogger(), "test", step, time.Millisecond, 2*time.Millisecond))
	assert.Equal(t, 3, calls)
}

func TestRetryWhileUnreachable_ReturnsAnyOtherErrorAtOnce(t *testing.T) {
	calls := 0
	boom := errors.New("decode legacy candidate_topology of r1: invalid character")
	err := retryWhileUnreachable(context.Background(), discardLogger(), "test", func(context.Context) error {
		calls++
		return boom
	}, time.Millisecond, time.Millisecond)
	assert.ErrorIs(t, err, boom)
	assert.Equal(t, 1, calls)
}

func TestRetryWhileUnreachable_StopsWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := retryWhileUnreachable(ctx, discardLogger(), "test", func(context.Context) error { return refused() }, time.Hour, time.Hour)
	assert.ErrorIs(t, err, context.Canceled)
}
