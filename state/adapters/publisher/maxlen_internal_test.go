package publisher

import (
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestXAddArgs_SetsMaxLenApprox asserts state's publisher caps every stream with
// MaxLen/Approx, matching the orchestrator's publisher so state streams cannot
// grow unbounded.
func TestXAddArgs_SetsMaxLenApprox(t *testing.T) {
	p := NewOutboxPublisher(nil, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))

	entry := &outbox.Entry{
		ID:         uuid.New(),
		Payload:    []byte(`{"schedule_id":"s1"}`),
		StreamName: "schedules.loaded:v1",
	}

	args, err := p.xaddArgs(entry)
	require.NoError(t, err)
	assert.Equal(t, int64(streamMaxLen), args.MaxLen, "MaxLen cap must be applied")
	assert.True(t, args.Approx, "trimming must be approximate (~)")
	assert.Equal(t, "schedules.loaded:v1", args.Stream)
	assert.Equal(t, entry.ID.String(), args.Values.(map[string]interface{})["outbox_entry_id"], "outbox_entry_id still injected")
}
