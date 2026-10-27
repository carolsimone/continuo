package publisher

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ outbox.Renderer = (*OutboxPublisher)(nil)

func TestRender_ReturnsPublishedFieldsWithoutOutboxEntryID(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	body, _ := json.Marshal(map[string]any{"schedule_id": "s1", "nested": map[string]any{"a": 1}})
	values, err := p.Render(&outbox.Entry{ID: uuid.New(), EventType: "compile_requested", StreamName: streams.CompileRequestedV1, Payload: body})
	if err != nil {
		t.Fatal(err)
	}
	if values["schedule_id"] != "s1" {
		t.Fatalf("schedule_id = %v", values["schedule_id"])
	}
	if values["nested"] != `{"a":1}` {
		t.Fatalf("nested = %v, want JSON string", values["nested"])
	}
	if _, ok := values["outbox_entry_id"]; ok {
		t.Fatal("Render must not include outbox_entry_id")
	}
}

func TestRender_DeadLetterRowOmitsOutboxEntryID(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	entry := &outbox.Entry{
		ID:         uuid.New(),
		EventType:  outbox.DeadLetterEventType,
		StreamName: streams.OutboxDeadLetterV1,
		Payload:    deadLetterPayload(t, model.DeadLetterKindPermanent, "compile_requested"),
	}
	values, err := p.Render(entry)
	require.NoError(t, err)
	require.Equal(t, string(model.DeadLetterKindPermanent), values["failure_kind"])
	require.NotContains(t, values, "outbox_entry_id")
}

func TestRender_ReturnsAFreshMapEachCall(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	entry := &outbox.Entry{ID: uuid.New(), EventType: "compile_requested", StreamName: streams.CompileRequestedV1, Payload: []byte(`{"schedule_id":"s1"}`)}
	first, err := p.Render(entry)
	require.NoError(t, err)
	first["schedule_id"] = "mutated"
	second, err := p.Render(entry)
	require.NoError(t, err)
	require.Equal(t, "s1", second["schedule_id"])
}

func TestRender_ValuesAreAcceptedByStringifyFields(t *testing.T) {
	p := NewOutboxPublisher(nil, nil)
	values, err := p.Render(&outbox.Entry{ID: uuid.New(), EventType: "compile_requested", StreamName: streams.CompileRequestedV1, Payload: []byte(`{"n":3,"ok":true,"gone":null,"nested":[1,2]}`)})
	require.NoError(t, err)
	_, err = outbox.StringifyFields(values)
	require.NoError(t, err)
}

// TestPublish_XAddsExactlyRenderPlusOutboxEntryID guards against Publish
// recomputing its fields apart from Render.
func TestPublish_XAddsExactlyRenderPlusOutboxEntryID(t *testing.T) {
	rdb, cleanup := newTestRedis(t)
	defer cleanup()
	p := NewOutboxPublisher(rdb, newTestLogger())
	entry := &outbox.Entry{ID: uuid.New(), EventType: "compile_requested", StreamName: streams.CompileRequestedV1, Payload: []byte(`{"schedule_id":"s1","nested":{"a":1}}`)}

	rendered, err := p.Render(entry)
	require.NoError(t, err)
	want, err := outbox.StringifyFields(rendered)
	require.NoError(t, err)
	want["outbox_entry_id"] = entry.ID.String()

	require.NoError(t, p.Publish(context.Background(), entry))
	res, err := rdb.XRange(context.Background(), streams.CompileRequestedV1, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, res, 1)
	got := map[string]string{}
	for k, v := range res[0].Values {
		got[k] = v.(string)
	}
	require.Equal(t, want, got)
}

// A task.dispatch:v1 row whose payload does not decode as a dispatch can never
// publish: the error is permanent, so the relay dead-letters the row.
func TestRender_ADispatchThatDoesNotDecodeIsPermanent(t *testing.T) {
	for name, payload := range map[string]string{
		"not an object":           `"not a dispatch"`,
		"an attempt not a number": `{"run_id":"r1","task_id":"t1","attempt":"zero"}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewOutboxPublisher(nil, nil).Render(&outbox.Entry{ID: uuid.New(), EventType: streams.TaskDispatchV1,
				StreamName: streams.TaskDispatchV1, Payload: wrapped(t, "default", []byte(payload))})
			require.ErrorIs(t, err, pkgevents.ErrPermanent)
		})
	}
}

// A run.cancelled:v1 row renders as the golden entry: its payload, the cancel,
// under an envelope that names the row as the event, state as its producer and
// the row's creation as when it occurred.
func TestRender_RunCancelledMatchesTheGoldenFixture(t *testing.T) {
	for _, name := range []string{"run_cancelled_v1.json", "run_cancelled_v1_attempts.json"} {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("..", "..", "..", "pkg", "events", "testdata", name)) //nolint:gosec // G304: a fixed fixture path
			require.NoError(t, err)
			var fixture struct {
				Input struct {
					EventID    string          `json:"event_id"`
					OccurredAt time.Time       `json:"occurred_at"`
					Payload    json.RawMessage `json:"payload"`
				} `json:"input"`
				Fields map[string]json.RawMessage `json:"fields"`
			}
			require.NoError(t, json.Unmarshal(raw, &fixture))

			rendered, err := NewOutboxPublisher(nil, nil).Render(&outbox.Entry{
				ID: uuid.MustParse(fixture.Input.EventID), CreatedAt: fixture.Input.OccurredAt,
				EventType: streams.RunCancelledV1, StreamName: streams.RunCancelledV1,
				Payload: wrapped(t, "default", fixture.Input.Payload),
			})

			require.NoError(t, err)
			require.Len(t, rendered, len(fixture.Fields), "the entry carries exactly the fixture's fields")
			for key, want := range fixture.Fields {
				if key == "payload" {
					var w, g any
					require.NoError(t, json.Unmarshal(want, &w))
					payload, ok := rendered[key].(string)
					require.True(t, ok, "the payload is one JSON field")
					require.NoError(t, json.Unmarshal([]byte(payload), &g))
					assert.Equal(t, w, g)
					continue
				}
				var w string
				require.NoError(t, json.Unmarshal(want, &w))
				assert.Equal(t, w, rendered[key], key)
			}
		})
	}
}

// A run.cancelled:v1 row whose payload does not decode as a cancel can never
// publish: the error is permanent, so the relay dead-letters the row.
func TestRender_ACancelThatDoesNotDecodeIsPermanent(t *testing.T) {
	_, err := NewOutboxPublisher(nil, nil).Render(&outbox.Entry{ID: uuid.New(), EventType: streams.RunCancelledV1,
		StreamName: streams.RunCancelledV1, Payload: wrapped(t, "default", []byte(`{"run_id":"r1","attempts":"none"}`))})
	require.ErrorIs(t, err, pkgevents.ErrPermanent)
}

// wrapped stores payload as a state outbox row does: under its tenant.
func wrapped(t *testing.T, tenantID string, payload []byte) []byte {
	t.Helper()
	stored, err := outbox.WrapTenant(tenantID, payload)
	require.NoError(t, err)
	return stored
}

// goldenInput reads the fixture's input: the event id, when it occurred and
// the payload a row of that stream stores.
func goldenInput(t *testing.T, name string) (uuid.UUID, time.Time, json.RawMessage) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "pkg", "events", "testdata", name)) //nolint:gosec // G304: a fixed fixture path
	require.NoError(t, err)
	var fixture struct {
		Input struct {
			EventID    string          `json:"event_id"`
			OccurredAt time.Time       `json:"occurred_at"`
			Payload    json.RawMessage `json:"payload"`
		} `json:"input"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	return uuid.MustParse(fixture.Input.EventID), fixture.Input.OccurredAt, fixture.Input.Payload
}

// The envelope of a task.dispatch:v1 row names the tenant the row stores, and
// the rest of the entry is the golden fixture's.
func TestRender_TaskDispatchCarriesTheRowsTenant(t *testing.T) {
	id, at, payload := goldenInput(t, "task_dispatch_v1.json")

	rendered, err := NewOutboxPublisher(nil, nil).Render(&outbox.Entry{ID: id, CreatedAt: at,
		EventType: streams.TaskDispatchV1, StreamName: streams.TaskDispatchV1, Payload: wrapped(t, "acme", payload)})
	require.NoError(t, err)

	assert.Equal(t, "acme", rendered["tenant_id"])
	assert.Equal(t, id.String(), rendered["event_id"])
	assert.Equal(t, producer, rendered["producer"])
	var got, want any
	require.NoError(t, json.Unmarshal([]byte(rendered["payload"].(string)), &got))
	require.NoError(t, json.Unmarshal(payload, &want))
	assert.Equal(t, want, got, "the wire payload is the dispatch, without the tenant")
}

// The envelope of a run.cancelled:v1 row names the tenant the row stores.
func TestRender_RunCancelledCarriesTheRowsTenant(t *testing.T) {
	id, at, payload := goldenInput(t, "run_cancelled_v1_attempts.json")

	rendered, err := NewOutboxPublisher(nil, nil).Render(&outbox.Entry{ID: id, CreatedAt: at,
		EventType: streams.RunCancelledV1, StreamName: streams.RunCancelledV1, Payload: wrapped(t, "acme", payload)})
	require.NoError(t, err)

	assert.Equal(t, "acme", rendered["tenant_id"])
	var got, want any
	require.NoError(t, json.Unmarshal([]byte(rendered["payload"].(string)), &got))
	require.NoError(t, json.Unmarshal(payload, &want))
	assert.Equal(t, want, got, "the wire payload is the cancel, without the tenant")
}

// A dispatch or cancel row that stores no tenant can never publish: the error
// is permanent, so the relay dead-letters the row.
func TestRender_ARowWithoutATenantIsPermanent(t *testing.T) {
	for _, stream := range []string{streams.TaskDispatchV1, streams.RunCancelledV1} {
		t.Run(stream, func(t *testing.T) {
			_, err := NewOutboxPublisher(nil, nil).Render(&outbox.Entry{ID: uuid.New(), EventType: stream,
				StreamName: stream, Payload: []byte(`{"run_id":"r1"}`)})
			require.ErrorIs(t, err, pkgevents.ErrPermanent)
			require.ErrorIs(t, err, outbox.ErrNoTenant)
		})
	}
}
