package events_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
)

type remediationHandedOverFixture struct {
	Input struct {
		EventID    string                       `json:"event_id"`
		TenantID   string                       `json:"tenant_id"`
		Producer   string                       `json:"producer"`
		OccurredAt time.Time                    `json:"occurred_at"`
		Payload    events.RemediationHandedOver `json:"payload"`
	} `json:"input"`
	Fields map[string]json.RawMessage `json:"fields"`
}

func loadRemediationHandedOverFixture(t *testing.T) remediationHandedOverFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/remediation_handed_over_v1.json")
	require.NoError(t, err)
	var fx remediationHandedOverFixture
	require.NoError(t, json.Unmarshal(raw, &fx))
	return fx
}

func validRemediationHandedOver() events.RemediationHandedOver {
	return events.RemediationHandedOver{
		RemediationID:    "3f9d2b8e-6c15-4a07-9e43-1b5a7c0d8f26",
		ReleaseID:        "rel_20261112_0042",
		RemediationRound: 1,
		Flow:             "rejected-release",
		Tries:            3,
		Nodes: []events.HandedOverNode{
			{NodeID: "analytics.orders_enriched", Reason: string(model.HandoverReasonTriesSpent), Message: "m"},
		},
	}
}

func remediationHandedOverFields(t *testing.T, p events.RemediationHandedOver) map[string]string {
	t.Helper()
	fields, err := events.RemediationHandedOverFields("e", events.DefaultTenantID, "agent-remediation", time.Date(2026, 11, 12, 10, 47, 19, 0, time.UTC), p)
	require.NoError(t, err)
	return stringFields(fields)
}

func TestRemediationHandedOverFields_MatchGoldenFixture(t *testing.T) {
	fx := loadRemediationHandedOverFixture(t)
	got, err := events.RemediationHandedOverFields(fx.Input.EventID, fx.Input.TenantID, fx.Input.Producer, fx.Input.OccurredAt, fx.Input.Payload)
	require.NoError(t, err)
	assertFieldsMatchFixture(t, fx.Fields, got)
}

func TestRemediationHandedOverFields_AlwaysCarryANodesArray(t *testing.T) {
	p := validRemediationHandedOver()
	p.Nodes = nil
	fields := remediationHandedOverFields(t, p)
	assert.JSONEq(t, `[]`, string(mustRawField(t, fields["payload"], "nodes")))
}

func TestDecodeRemediationHandedOver_ReadsGoldenFixture(t *testing.T) {
	fx := loadRemediationHandedOverFixture(t)
	env, p, err := events.DecodeRemediationHandedOver(fixtureFields(t, fx.Fields))
	require.NoError(t, err)
	assert.Equal(t, fx.Input.Payload, p)
	assert.Equal(t, fx.Input.EventID, env.EventID)
	assert.Equal(t, fx.Input.TenantID, env.TenantID)
	assert.Equal(t, fx.Input.Producer, env.Producer)
	assert.Equal(t, events.RemediationHandedOverSchemaVersion, env.SchemaVersion)
}

func TestDecodeRemediationHandedOver_AcceptsEveryHandoverReasonAndNoTries(t *testing.T) {
	for _, reason := range model.HandoverReasons() {
		t.Run(string(reason), func(t *testing.T) {
			p := validRemediationHandedOver()
			p.Tries = 0
			p.Nodes[0].Reason = string(reason)
			_, got, err := events.DecodeRemediationHandedOver(remediationHandedOverFields(t, p))
			require.NoError(t, err)
			assert.Equal(t, string(reason), got.Nodes[0].Reason)
		})
	}
}

func TestDecodeRemediationHandedOver_RefusesWhatTheContractForbids(t *testing.T) {
	cases := map[string]func(p *events.RemediationHandedOver){
		"no remediation id":             func(p *events.RemediationHandedOver) { p.RemediationID = "" },
		"no release id":                 func(p *events.RemediationHandedOver) { p.ReleaseID = "" },
		"no flow":                       func(p *events.RemediationHandedOver) { p.Flow = "" },
		"round 0":                       func(p *events.RemediationHandedOver) { p.RemediationRound = 0 },
		"negative tries":                func(p *events.RemediationHandedOver) { p.Tries = -1 },
		"a node without a node id":      func(p *events.RemediationHandedOver) { p.Nodes[0].NodeID = "" },
		"a node without a reason":       func(p *events.RemediationHandedOver) { p.Nodes[0].Reason = "" },
		"a node with an unknown reason": func(p *events.RemediationHandedOver) { p.Nodes[0].Reason = "gave_up" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := validRemediationHandedOver()
			mutate(&p)
			_, _, err := events.DecodeRemediationHandedOver(remediationHandedOverFields(t, p))
			require.ErrorIs(t, err, events.ErrMalformedEnvelope)
		})
	}
}

func TestDecodeRemediationHandedOver_RefusesAnUnreadableEntry(t *testing.T) {
	valid := func() map[string]string { return remediationHandedOverFields(t, validRemediationHandedOver()) }
	cases := map[string]map[string]string{
		"other version":    withField(valid(), "schema_version", "2"),
		"payload not json": withPayload(valid(), `{`),
		"no payload":       withPayload(valid(), ``),
	}
	for name, fields := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := events.DecodeRemediationHandedOver(fields)
			require.ErrorIs(t, err, events.ErrMalformedEnvelope)
		})
	}
}
