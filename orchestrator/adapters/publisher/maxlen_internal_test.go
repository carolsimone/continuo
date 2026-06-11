package publisher

import (
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestXAddArgs_SetsMaxLenApprox asserts every published stream entry carries the
// MaxLen/Approx cap, bounding Redis memory for streams a consumer group may lag
// on.
func TestXAddArgs_SetsMaxLenApprox(t *testing.T) {
	p := NewOutboxPublisher(nil, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))

	payload, err := json.Marshal(domain.NodeReadyForExecution{ScheduleID: "s1", TableName: "t"})
	require.NoError(t, err)
	entry := &outbox.Entry{ID: uuid.New(), EventType: "node_ready_for_execution", Payload: payload, StreamName: "node.ready:v1"}

	args, err := p.xaddArgs(entry)
	require.NoError(t, err)
	assert.Equal(t, int64(streamMaxLen), args.MaxLen, "MaxLen cap must be applied")
	assert.True(t, args.Approx, "trimming must be approximate (~) to avoid per-XADD scan cost")
	assert.Equal(t, "node.ready:v1", args.Stream)
}
