package events_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type deadLetterFixture struct {
	Input struct {
		TenantID          string            `json:"tenant_id"`
		Producer          string            `json:"producer"`
		OccurredAt        time.Time         `json:"occurred_at"`
		OriginalStream    string            `json:"original_stream"`
		OriginalGroup     string            `json:"original_group"`
		OriginalMessageID string            `json:"original_message_id"`
		Fields            map[string]string `json:"fields"`
		FailureKind       string            `json:"failure_kind"`
		Error             string            `json:"error"`
		DeliveryCount     int64             `json:"delivery_count"`
	} `json:"input"`
	Fields map[string]json.RawMessage `json:"fields"`
}

func TestConsumerDeadLetterFields_MatchGoldenFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/consumer_dead_letter_v1.json")
	require.NoError(t, err)
	var fx deadLetterFixture
	require.NoError(t, json.Unmarshal(raw, &fx))

	got, err := events.ConsumerDeadLetterFields(events.DeadLetteredMessage{
		TenantID:      fx.Input.TenantID,
		Producer:      fx.Input.Producer,
		OccurredAt:    fx.Input.OccurredAt,
		Stream:        fx.Input.OriginalStream,
		Group:         fx.Input.OriginalGroup,
		MessageID:     fx.Input.OriginalMessageID,
		Fields:        fx.Input.Fields,
		FailureKind:   model.DeadLetterKind(fx.Input.FailureKind),
		Error:         fx.Input.Error,
		DeliveryCount: fx.Input.DeliveryCount,
	})
	require.NoError(t, err)

	require.Len(t, got, len(fx.Fields), "the entry must carry exactly the fixture's fields")
	for _, key := range []string{"event_id", "tenant_id", "occurred_at", "producer", "schema_version"} {
		var want string
		require.NoError(t, json.Unmarshal(fx.Fields[key], &want), key)
		assert.Equal(t, want, got[key], key)
	}
	var wantPayload, gotPayload any
	require.NoError(t, json.Unmarshal(fx.Fields["payload"], &wantPayload))
	require.NoError(t, json.Unmarshal([]byte(got["payload"].(string)), &gotPayload))
	assert.Equal(t, wantPayload, gotPayload)
}

// The Python consumer's tests read their own copy of the fixture; both
// producers are pinned to the same entry only while the copies are identical.
func TestConsumerDeadLetterFixture_PythonCopyIsIdentical(t *testing.T) {
	goCopy, err := os.ReadFile("testdata/consumer_dead_letter_v1.json")
	require.NoError(t, err)
	pyCopy, err := os.ReadFile("../../topology-controller/tests/fixtures/consumer_dead_letter_v1.json")
	require.NoError(t, err)
	assert.Equal(t, string(goCopy), string(pyCopy))
}

func TestConsumerDeadLetterEventID_IsStablePerMessage(t *testing.T) {
	a := events.ConsumerDeadLetterEventID("default", "g", "s", "1-0")
	assert.Equal(t, a, events.ConsumerDeadLetterEventID("default", "g", "s", "1-0"))
	assert.NotEqual(t, a, events.ConsumerDeadLetterEventID("default", "g", "s", "2-0"))
	assert.NotEqual(t, a, events.ConsumerDeadLetterEventID("acme", "g", "s", "1-0"), "the tenant is part of the natural key")
}

func TestEnvelopeFields_OccurredAtIsUTCWithMicroseconds(t *testing.T) {
	cest := time.FixedZone("CEST", 2*60*60)
	f, err := events.Envelope{OccurredAt: time.Date(2026, 10, 3, 14, 0, 0, 1500, cest), SchemaVersion: 2}.Fields(struct{}{})
	require.NoError(t, err)
	assert.Equal(t, "2026-10-03T12:00:00.000001Z", f["occurred_at"])
	assert.Equal(t, "2", f["schema_version"])
	assert.Equal(t, "{}", f["payload"])
}

func TestConsumerDeadLetterFields_NilFieldsBecomeAnEmptyObject(t *testing.T) {
	got, err := events.ConsumerDeadLetterFields(events.DeadLetteredMessage{TenantID: "default", FailureKind: model.DeadLetterKindPermanent})
	require.NoError(t, err)
	assert.Contains(t, got["payload"], `"fields":{}`)
}
