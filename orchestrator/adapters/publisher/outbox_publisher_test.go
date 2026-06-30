package publisher_test

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/adapters/publisher"
	"github.com/carolsimone/continuo/orchestrator/domain"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// payloadToValuesTestHelper exposes the payloadToValues logic for unit testing
// by constructing an entry and inspecting the fields the publisher would emit.
// We test by creating a publisher with a nil Redis client and calling an
// exported test-seam method. Since payloadToValues is unexported, we test it
// indirectly by round-tripping via the entry struct.

func mustMarshal(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func makeEntry(eventType string, payload []byte) *outbox.Entry {
	return &outbox.Entry{
		ID:            uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		AggregateType: "orchestrator",
		AggregateID:   uuid.New(),
		EventType:     eventType,
		Payload:       payload,
	}
}

// payloadToValuesFor calls the publisher's internal dispatch via a test-local
// helper that mirrors the switch. This lets us validate every case without
// requiring a live Redis connection.
func payloadToValuesFor(t *testing.T, entry *outbox.Entry) map[string]interface{} {
	t.Helper()
	p := publisher.NewOutboxPublisher(nil, nil)
	vals, err := p.PayloadToValuesForTest(entry)
	require.NoError(t, err)
	return vals
}

func TestOutboxPublisher_NodeReadyForExecution(t *testing.T) {
	evt := domain.NodeReadyForExecution{
		ScheduleID:      "sched-1",
		ScheduleName:    "daily",
		ServiceName:     "svc",
		SchemaName:      "public",
		TableName:       "orders",
		TaskID:          "task-1",
		JobName:         "job-1",
		NodeType:        "dbt-model",
		ImageTag:        "v1",
		ManifestVersion: "m1",
	}
	entry := makeEntry("node_ready_for_execution", mustMarshal(t, evt))
	vals := payloadToValuesFor(t, entry)

	assert.Equal(t, entry.ID.String(), vals["outbox_entry_id"])
	assert.Equal(t, "sched-1", vals["schedule_id"])
	assert.Equal(t, "daily", vals["schedule_name"])
	assert.Equal(t, "svc", vals["service_name"])
	assert.Equal(t, "public", vals["schema_name"])
	assert.Equal(t, "orders", vals["table_name"])
	assert.Equal(t, "task-1", vals["task_id"])
	assert.Equal(t, "job-1", vals["job_name"])
	assert.Equal(t, "dbt-model", vals["node_type"])
	assert.Equal(t, "v1", vals["image_tag"])
	assert.Equal(t, "m1", vals["manifest_version"])
	// Production dispatches carry no mode (wire shape unchanged).
	_, hasMode := vals["mode"]
	assert.False(t, hasMode, "production node_ready_for_execution must not carry a mode field")
}

func TestOutboxPublisher_NodeReadyForExecution_CarriesPromoteSeedMode(t *testing.T) {
	// Regression: the mode field must reach the wire for promote-seed jobs, else
	// the executor can't stamp the mode label and k8s-controller emits production
	// task lifecycle events for a synthetic schedule with no state run.
	evt := domain.NodeReadyForExecution{
		ScheduleID: "sched-1", ScheduleName: "promote-seed", ServiceName: "svc",
		SchemaName: "public", TableName: "seed_x", TaskID: "task-1", JobName: "job-1",
		NodeType: "dbt-seed", ImageTag: "v1", Mode: pkgevents.ModePromoteSeed,
	}
	entry := makeEntry("node_ready_for_execution", mustMarshal(t, evt))
	vals := payloadToValuesFor(t, entry)
	assert.Equal(t, pkgevents.ModePromoteSeed, vals["mode"])
}

func TestOutboxPublisher_CascadeTaskSkipped(t *testing.T) {
	evt := domain.CascadeTaskSkipped{
		TaskID:     "task-x",
		ScheduleID: "sched-2",
	}
	entry := makeEntry("cascade_task_skipped", mustMarshal(t, evt))
	vals := payloadToValuesFor(t, entry)

	assert.Equal(t, entry.ID.String(), vals["outbox_entry_id"])
	assert.Equal(t, "task-x", vals["task_id"])
	assert.Equal(t, "sched-2", vals["schedule_id"])
	assert.Equal(t, "skipped", vals["status"])
	// retry_count comes from the shared TaskStatusUpdated.ToMap as int32; Redis
	// serializes it to "0" on the wire, identical to the executor/k8s producers.
	assert.Equal(t, int32(0), vals["retry_count"])
}

func TestOutboxPublisher_GenericPayloadCases(t *testing.T) {
	for _, evtType := range []string{
		"run_entries_dispatched",
		"run_entries_dispatch_failed",
		"release_promoted",
	} {
		t.Run(evtType, func(t *testing.T) {
			raw := []byte(`{"key":"value"}`)
			entry := makeEntry(evtType, raw)
			vals := payloadToValuesFor(t, entry)

			assert.Equal(t, entry.ID.String(), vals["outbox_entry_id"])
			assert.Equal(t, string(raw), vals["payload"])
		})
	}
}

func TestOutboxPublisher_UnknownEventType_ReturnsError(t *testing.T) {
	entry := makeEntry("unknown_event", []byte(`{}`))
	p := publisher.NewOutboxPublisher(nil, nil)
	_, err := p.PayloadToValuesForTest(entry)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown_event")
}
