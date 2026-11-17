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

type remediationRetryRequestedFixture struct {
	Input struct {
		EventID    string                           `json:"event_id"`
		TenantID   string                           `json:"tenant_id"`
		Producer   string                           `json:"producer"`
		OccurredAt time.Time                        `json:"occurred_at"`
		Payload    events.RemediationRetryRequested `json:"payload"`
	} `json:"input"`
	Fields map[string]json.RawMessage `json:"fields"`
}

func loadRemediationRetryRequestedFixture(t *testing.T) remediationRetryRequestedFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/remediation_retry_requested_v2.json")
	require.NoError(t, err)
	var fx remediationRetryRequestedFixture
	require.NoError(t, json.Unmarshal(raw, &fx))
	return fx
}

func remediationRetryRequestedFields(t *testing.T, p events.RemediationRetryRequested) map[string]string {
	t.Helper()
	fields, err := events.RemediationRetryRequestedFields("e", events.DefaultTenantID, "release-controller", time.Date(2026, 11, 12, 14, 5, 33, 0, time.UTC), p)
	require.NoError(t, err)
	return stringFields(fields)
}

func TestRemediationRetryRequestedFields_MatchGoldenFixture(t *testing.T) {
	fx := loadRemediationRetryRequestedFixture(t)
	got, err := events.RemediationRetryRequestedFields(fx.Input.EventID, fx.Input.TenantID, fx.Input.Producer, fx.Input.OccurredAt, fx.Input.Payload)
	require.NoError(t, err)
	assertFieldsMatchFixture(t, fx.Fields, got)
}

func TestDecodeRemediationRetryRequested_ReadsGoldenFixture(t *testing.T) {
	fx := loadRemediationRetryRequestedFixture(t)
	env, p, err := events.DecodeRemediationRetryRequested(fixtureFields(t, fx.Fields))
	require.NoError(t, err)
	assert.Equal(t, fx.Input.Payload.ReleaseID, p.ReleaseID)
	assert.Equal(t, fx.Input.Payload.RemediationRound, p.RemediationRound)
	assert.JSONEq(t, string(fx.Input.Payload.Rejection), string(p.Rejection), "the stored rejection travels unchanged")
	assert.Equal(t, fx.Input.EventID, env.EventID)
	assert.Equal(t, fx.Input.TenantID, env.TenantID)
	assert.Equal(t, fx.Input.Producer, env.Producer)
	assert.Equal(t, events.RemediationRetryRequestedSchemaVersion, env.SchemaVersion)
}

func TestDecodeRemediationRetryRequested_RefusesWhatTheContractForbids(t *testing.T) {
	cases := map[string]func(p *events.RemediationRetryRequested){
		"no release id":      func(p *events.RemediationRetryRequested) { p.ReleaseID = "" },
		"round 0":            func(p *events.RemediationRetryRequested) { p.RemediationRound = 0 },
		"round 1":            func(p *events.RemediationRetryRequested) { p.RemediationRound = 1 },
		"negative round":     func(p *events.RemediationRetryRequested) { p.RemediationRound = -3 },
		"no rejection":       func(p *events.RemediationRetryRequested) { p.Rejection = nil },
		"rejection null":     func(p *events.RemediationRetryRequested) { p.Rejection = json.RawMessage(`null`) },
		"rejection an array": func(p *events.RemediationRetryRequested) { p.Rejection = json.RawMessage(`[]`) },
		"rejection a string": func(p *events.RemediationRetryRequested) { p.Rejection = json.RawMessage(`"rejected"`) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := events.RemediationRetryRequested{ReleaseID: "rel_1", RemediationRound: 2, Rejection: json.RawMessage(`{"release_id":"rel_1"}`)}
			mutate(&p)
			fields := remediationRetryRequestedFields(t, p)
			if p.Rejection == nil {
				fields["payload"] = `{"release_id":"rel_1","remediation_round":2}`
			}
			_, _, err := events.DecodeRemediationRetryRequested(fields)
			require.ErrorIs(t, err, events.ErrMalformedEnvelope)
		})
	}
}

func TestDecodeRemediationRetryRequested_RefusesAnUnreadableEntry(t *testing.T) {
	valid := func() map[string]string {
		return remediationRetryRequestedFields(t, events.RemediationRetryRequested{ReleaseID: "rel_1", RemediationRound: 2, Rejection: json.RawMessage(`{"release_id":"rel_1"}`)})
	}
	cases := map[string]map[string]string{
		"other version":    withField(valid(), "schema_version", "2"),
		"payload not json": withPayload(valid(), `{`),
		"no payload":       withPayload(valid(), ``),
	}
	for name, fields := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := events.DecodeRemediationRetryRequested(fields)
			require.ErrorIs(t, err, events.ErrMalformedEnvelope)
		})
	}
}

func TestDecodeRemediationRetryRequested_AcceptsALaterRound(t *testing.T) {
	_, p, err := events.DecodeRemediationRetryRequested(remediationRetryRequestedFields(t,
		events.RemediationRetryRequested{ReleaseID: "rel_1", RemediationRound: 7, Rejection: json.RawMessage(`{}`)}))
	require.NoError(t, err)
	assert.Equal(t, 7, p.RemediationRound)
}
