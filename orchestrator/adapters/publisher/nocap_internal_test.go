package publisher

import (
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
)

// TestXAddArgs_DoNotTrim asserts the publisher never trims a stream: the
// dead-letter-controller's trim loop is the only thing that bounds them.
func TestXAddArgs_DoNotTrim(t *testing.T) {
	p := NewOutboxPublisher(nil, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))

	payload, err := json.Marshal(domain.NodeReadyForExecution{ScheduleID: "s1", TableName: "t"})
	if err != nil {
		t.Fatal(err)
	}
	args, err := p.xaddArgs(&outbox.Entry{ID: uuid.New(), EventType: "node_ready_for_execution", Payload: payload, StreamName: "node.ready:v1"})
	if err != nil {
		t.Fatal(err)
	}
	if args.MaxLen != 0 || args.MinID != "" || args.Approx {
		t.Fatalf("publisher trims: MaxLen=%d MinID=%q Approx=%v", args.MaxLen, args.MinID, args.Approx)
	}
}
