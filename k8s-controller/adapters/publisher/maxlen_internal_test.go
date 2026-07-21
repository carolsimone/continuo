package publisher

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/k8s-controller/domain/event"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestXAddArgs_SetsMaxLenApprox asserts k8s-controller's publisher caps every
// stream with MaxLen/Approx, matching state's and the orchestrator's publishers
// so k8s-controller streams cannot grow unbounded.
func TestXAddArgs_SetsMaxLenApprox(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	entry := &outbox.Entry{
		ID:         uuid.New(),
		StreamName: streams.NodeUpdatedV1, // any handled stream
		EventType:  "node_status_updated",
		Payload:    mustJSON(t, event.NodeStatusUpdated{}),
	}

	args, err := p.xaddArgs(entry)

	require.NoError(t, err)
	assert.Equal(t, int64(streamMaxLen), args.MaxLen, "MaxLen cap must be applied")
	assert.True(t, args.Approx, "cap must use approximate (~) trimming")
	assert.Equal(t, int64(10000), args.MaxLen, "cap must match the project-wide convention")
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}
