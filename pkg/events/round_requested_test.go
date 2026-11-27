package events_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/events"
)

type roundRequestedFixture struct {
	Input struct {
		EventID    string                `json:"event_id"`
		TenantID   string                `json:"tenant_id"`
		Producer   string                `json:"producer"`
		OccurredAt time.Time             `json:"occurred_at"`
		Payload    events.RoundRequested `json:"payload"`
	} `json:"input"`
	Fields map[string]json.RawMessage `json:"fields"`
}

func loadRoundRequestedFixture(t *testing.T) roundRequestedFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/round_requested_v1.json")
	require.NoError(t, err)
	var fx roundRequestedFixture
	require.NoError(t, json.Unmarshal(raw, &fx))
	return fx
}

func roundRequestedFields(t *testing.T, p events.RoundRequested) map[string]string {
	t.Helper()
	fields, err := events.RoundRequestedFields("e", events.DefaultTenantID, "release-controller", time.Date(2026, 11, 12, 14, 5, 33, 0, time.UTC), p)
	require.NoError(t, err)
	return stringFields(fields)
}

func TestRoundRequestedFields_MatchGoldenFixture(t *testing.T) {
	fx := loadRoundRequestedFixture(t)
	got, err := events.RoundRequestedFields(fx.Input.EventID, fx.Input.TenantID, fx.Input.Producer, fx.Input.OccurredAt, fx.Input.Payload)
	require.NoError(t, err)
	assertFieldsMatchFixture(t, fx.Fields, got)
}

func TestDecodeRoundRequested_ReadsGoldenFixture(t *testing.T) {
	fx := loadRoundRequestedFixture(t)
	env, p, err := events.DecodeRoundRequested(fixtureFields(t, fx.Fields))
	require.NoError(t, err)
	assert.Equal(t, fx.Input.Payload.ReleaseID, p.ReleaseID)
	assert.Equal(t, fx.Input.Payload.Round, p.Round)
	assert.JSONEq(t, string(fx.Input.Payload.Rejection), string(p.Rejection), "the stored rejection travels unchanged")
	assert.Equal(t, fx.Input.EventID, env.EventID)
	assert.Equal(t, fx.Input.TenantID, env.TenantID)
	assert.Equal(t, fx.Input.Producer, env.Producer)
	assert.Equal(t, events.RoundRequestedSchemaVersion, env.SchemaVersion)
}

func TestDecodeRoundRequested_RefusesWhatTheContractForbids(t *testing.T) {
	cases := map[string]func(p *events.RoundRequested){
		"no release id":      func(p *events.RoundRequested) { p.ReleaseID = "" },
		"round 0":            func(p *events.RoundRequested) { p.Round = 0 },
		"round 1":            func(p *events.RoundRequested) { p.Round = 1 },
		"negative round":     func(p *events.RoundRequested) { p.Round = -3 },
		"no rejection":       func(p *events.RoundRequested) { p.Rejection = nil },
		"rejection null":     func(p *events.RoundRequested) { p.Rejection = json.RawMessage(`null`) },
		"rejection an array": func(p *events.RoundRequested) { p.Rejection = json.RawMessage(`[]`) },
		"rejection a string": func(p *events.RoundRequested) { p.Rejection = json.RawMessage(`"rejected"`) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := events.RoundRequested{ReleaseID: "rel_1", Round: 2, Rejection: json.RawMessage(`{"release_id":"rel_1"}`)}
			mutate(&p)
			fields := roundRequestedFields(t, p)
			if p.Rejection == nil {
				fields["payload"] = `{"release_id":"rel_1","round":2}`
			}
			_, _, err := events.DecodeRoundRequested(fields)
			require.ErrorIs(t, err, events.ErrMalformedEnvelope)
		})
	}
}

func TestDecodeRoundRequested_RefusesAnUnreadableEntry(t *testing.T) {
	valid := func() map[string]string {
		return roundRequestedFields(t, events.RoundRequested{ReleaseID: "rel_1", Round: 2, Rejection: json.RawMessage(`{"release_id":"rel_1"}`)})
	}
	cases := map[string]map[string]string{
		"other version":    withField(valid(), "schema_version", "2"),
		"payload not json": withPayload(valid(), `{`),
		"no payload":       withPayload(valid(), ``),
	}
	for name, fields := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := events.DecodeRoundRequested(fields)
			require.ErrorIs(t, err, events.ErrMalformedEnvelope)
		})
	}
}

func TestDecodeRoundRequested_AcceptsALaterRound(t *testing.T) {
	_, p, err := events.DecodeRoundRequested(roundRequestedFields(t,
		events.RoundRequested{ReleaseID: "rel_1", Round: 7, Rejection: json.RawMessage(`{}`)}))
	require.NoError(t, err)
	assert.Equal(t, 7, p.Round)
}
