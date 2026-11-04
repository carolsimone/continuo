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
		OriginalKey       string            `json:"original_key"`
		Fields            map[string]string `json:"fields"`
		FailureKind       string            `json:"failure_kind"`
		Error             string            `json:"error"`
		DeliveryCount     int64             `json:"delivery_count"`
	} `json:"input"`
	Fields map[string]json.RawMessage `json:"fields"`
}

// The fixtures are a dead letter of an entry read from the stream's legacy key
// and one of an entry read from its prefixed key.
var deadLetterFixtures = []string{
	"testdata/consumer_dead_letter_v1.json",
	"testdata/consumer_dead_letter_v1_prefixed_key.json",
}

func TestConsumerDeadLetterFields_MatchGoldenFixture(t *testing.T) {
	for _, path := range deadLetterFixtures {
		t.Run(path, func(t *testing.T) {
			raw, err := os.ReadFile(path) //nolint:gosec // G304: a fixed fixture path
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
				Key:           fx.Input.OriginalKey,
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
		})
	}
}

func TestConsumerDeadLetter_DedupMessageIDQualifiesTheIDByAnyKeyButTheLegacyOne(t *testing.T) {
	cases := []struct {
		name, key, want string
	}{
		{"a producer that names no key reads only the legacy key", "", "1-0"},
		{"the legacy key", "orders:v1", "1-0"},
		{"the prefixed key", "continuo:orders:v1", "1-0@continuo:orders:v1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := events.ConsumerDeadLetter{OriginalStream: "orders:v1", OriginalMessageID: "1-0", OriginalKey: tc.key}
			assert.Equal(t, tc.want, p.DedupMessageID())
		})
	}
}

func TestConsumerDeadLetterFields_EventIDFollowsTheDedupIdentity(t *testing.T) {
	render := func(key string) (map[string]any, events.ConsumerDeadLetter) {
		values, err := events.ConsumerDeadLetterFields(events.DeadLetteredMessage{
			TenantID: "default", Stream: "orders:v1", Group: "g", MessageID: "1-0", Key: key,
			FailureKind: model.DeadLetterKindPermanent,
		})
		require.NoError(t, err)
		var p events.ConsumerDeadLetter
		require.NoError(t, json.Unmarshal([]byte(values["payload"].(string)), &p))
		return values, p
	}
	legacy, lp := render("orders:v1")
	prefixed, pp := render("continuo:orders:v1")

	assert.Equal(t, "orders:v1", lp.OriginalKey, "the payload names the key the message was read from")
	assert.Equal(t, "continuo:orders:v1", pp.OriginalKey)
	assert.Equal(t, events.ConsumerDeadLetterEventID("default", "g", "orders:v1", "1-0"), legacy["event_id"],
		"a legacy entry's dead letter keeps the event id earlier releases gave it")
	assert.Equal(t, events.ConsumerDeadLetterEventID("default", "g", "orders:v1", "1-0@continuo:orders:v1"), prefixed["event_id"])
	assert.NotEqual(t, legacy["event_id"], prefixed["event_id"], "two entries that share an ID on two keys are two dead letters")
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
