package redis

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dropTestLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestFailInFlightOnDrop_FailsRowForDroppedTrigger verifies the recovery wired
// onto the consumer's drop seam: when a remediation.requested trigger is
// abandoned, the release id is decoded from its payload and the in-flight row is
// failed with a reason that carries the cause, so the release stops reporting a
// fix as forever generating.
func TestFailInFlightOnDrop_FailsRowForDroppedTrigger(t *testing.T) {
	var gotRelease, gotReason string
	calls := 0
	drop := failInFlightOnDrop(dropTestLogger(), func(_ context.Context, releaseID, reason string) (int, error) {
		calls++
		gotRelease = releaseID
		gotReason = reason
		return 1, nil
	})

	raw, err := json.Marshal(requestedPayloadFixture)
	require.NoError(t, err)
	drop(context.Background(),
		goredis.XMessage{ID: "9-0", Values: map[string]any{"payload": string(raw)}},
		errors.New("connect: connection refused"))

	require.Equal(t, 1, calls, "the recovery must run for a decodable dropped trigger")
	assert.Equal(t, "rel-456", gotRelease)
	assert.Contains(t, gotReason, "connect: connection refused", "the reason must carry the drop cause")
}

// TestFailInFlightOnDrop_SkipsMessageWithoutPayload verifies a message carrying
// no payload field is ignored: no in-flight row was ever created for it, so
// there is nothing to fail.
func TestFailInFlightOnDrop_SkipsMessageWithoutPayload(t *testing.T) {
	calls := 0
	drop := failInFlightOnDrop(dropTestLogger(), func(context.Context, string, string) (int, error) {
		calls++
		return 0, nil
	})

	drop(context.Background(), goredis.XMessage{ID: "9-0", Values: map[string]any{}}, errors.New("x"))

	require.Zero(t, calls, "a message with no payload names no release to recover")
}

// TestFailInFlightOnDrop_SkipsUndecodablePayload verifies a malformed payload is
// ignored: markGenerating runs only after a successful decode, so a payload that
// cannot be decoded never created an in-flight row.
func TestFailInFlightOnDrop_SkipsUndecodablePayload(t *testing.T) {
	calls := 0
	drop := failInFlightOnDrop(dropTestLogger(), func(context.Context, string, string) (int, error) {
		calls++
		return 0, nil
	})

	drop(context.Background(), goredis.XMessage{ID: "9-0", Values: map[string]any{"payload": "{not json"}}, errors.New("x"))

	require.Zero(t, calls, "an undecodable payload names no release to recover")
}
