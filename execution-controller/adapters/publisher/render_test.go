package publisher

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/service/validation"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/alicebob/miniredis/v2"
)

var _ outbox.Renderer = (*OutboxPublisher)(nil)

func TestRender_CandidateLegEventIsPayloadField(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	values, err := p.Render(&outbox.Entry{ID: uuid.New(), EventType: validation.EventTypeValidationCompleted, Payload: []byte(`{"release_id":"r1"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if values["payload"] != `{"release_id":"r1"}` {
		t.Fatalf("values = %v", values)
	}
}

func TestRender_DeadLetterRowOmitsOutboxEntryID(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	values, err := p.Render(&outbox.Entry{
		ID: uuid.New(), EventType: outbox.DeadLetterEventType, StreamName: streams.OutboxDeadLetterV1,
		Payload: []byte(`{"failure_kind":"permanent","original_event_type":"node_updated"}`),
	})
	require.NoError(t, err)
	require.Equal(t, "permanent", values["failure_kind"])
	require.NotContains(t, values, "outbox_entry_id")
}

func TestRender_TypedEventsAreAcceptedByStringifyFields(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	for _, entry := range []*outbox.Entry{
		{ID: uuid.New(), EventType: event.EventTypeTaskStatusReport, Payload: []byte(`{"run_id":"s1","task_id":"t1","attempt":0,"status":"running"}`)},
	} {
		values, err := p.Render(entry)
		require.NoError(t, err)
		require.NotContains(t, values, "outbox_entry_id")
		_, err = outbox.StringifyFields(values)
		require.NoError(t, err)
	}
}

func TestRender_ReturnsAFreshMapEachCall(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	entry := &outbox.Entry{ID: uuid.New(), EventType: event.EventTypeTaskStatusReport, Payload: []byte(`{"run_id":"s1","task_id":"t1","status":"queued"}`)}
	first, err := p.Render(entry)
	require.NoError(t, err)
	first["event_id"] = "mutated"
	second, err := p.Render(entry)
	require.NoError(t, err)
	require.Equal(t, entry.ID.String(), second["event_id"])
}

// TestPublish_XAddsExactlyRenderPlusOutboxEntryID guards against Publish
// recomputing its fields apart from Render.
func TestPublish_XAddsExactlyRenderPlusOutboxEntryID(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	p := NewOutboxPublisher(rdb, nil)
	entry := &outbox.Entry{
		ID: uuid.New(), EventType: event.EventTypeTaskStatusReport, StreamName: streams.TaskStatusUpdatedV2,
		Payload: []byte(`{"run_id":"s1","task_id":"t1","attempt":2,"status":"running"}`),
	}
	rendered, err := p.Render(entry)
	require.NoError(t, err)
	want, err := outbox.StringifyFields(rendered)
	require.NoError(t, err)
	want["outbox_entry_id"] = entry.ID.String()

	require.NoError(t, p.Publish(context.Background(), entry))
	res, err := rdb.XRange(context.Background(), streams.TaskStatusUpdatedV2, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, res, 1)
	got := map[string]string{}
	for k, v := range res[0].Values {
		got[k] = v.(string)
	}
	require.Equal(t, want, got)
}

// A report row renders as the golden task.status.updated:v2 entry: the row's
// id is the event id, its creation the event's time.
func TestRender_ATaskStatusReportIsTheGoldenEntry(t *testing.T) {
	for _, name := range []string{"queued", "running", "succeeded", "failed"} {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("..", "..", "..", "pkg", "events", "testdata", "task_status_updated_v2_"+name+".json")) //nolint:gosec // G304: a fixed fixture path
			require.NoError(t, err)
			var fx struct {
				Input struct {
					EventID    string          `json:"event_id"`
					OccurredAt time.Time       `json:"occurred_at"`
					Payload    json.RawMessage `json:"payload"`
				} `json:"input"`
				Fields map[string]json.RawMessage `json:"fields"`
			}
			require.NoError(t, json.Unmarshal(raw, &fx))

			got, err := NewOutboxPublisher(nil, nil).Render(&outbox.Entry{ID: uuid.MustParse(fx.Input.EventID),
				EventType: event.EventTypeTaskStatusReport, Payload: fx.Input.Payload, CreatedAt: fx.Input.OccurredAt})
			require.NoError(t, err)

			require.Len(t, got, len(fx.Fields))
			for key, want := range fx.Fields {
				if key == "payload" {
					var w, g any
					require.NoError(t, json.Unmarshal(want, &w))
					require.NoError(t, json.Unmarshal([]byte(got[key].(string)), &g))
					require.Equal(t, w, g)
					continue
				}
				var w string
				require.NoError(t, json.Unmarshal(want, &w))
				require.Equal(t, w, got[key], key)
			}
		})
	}
}
