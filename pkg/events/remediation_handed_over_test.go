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

// readHandedOver reads rendered remediation.handed_over:v1 fields back as a
// reader of the stream would: the envelope through ParseEnvelope and the
// payload as JSON.
func readHandedOver(t *testing.T, fields map[string]string) (events.Envelope, events.RemediationHandedOver) {
	t.Helper()
	env, payload, err := events.ParseEnvelope(fields)
	require.NoError(t, err)
	var p events.RemediationHandedOver
	require.NoError(t, json.Unmarshal(payload, &p))
	return env, p
}

func TestRemediationHandedOverFields_MatchGoldenFixture(t *testing.T) {
	fx := loadRemediationHandedOverFixture(t)
	got, err := events.RemediationHandedOverFields(fx.Input.EventID, fx.Input.TenantID, fx.Input.Producer, fx.Input.OccurredAt, fx.Input.Payload)
	require.NoError(t, err)
	assertFieldsMatchFixture(t, fx.Fields, got)

	env, p := readHandedOver(t, fixtureFields(t, fx.Fields))
	assert.Equal(t, fx.Input.Payload, p)
	assert.Equal(t, fx.Input.EventID, env.EventID)
	assert.Equal(t, fx.Input.TenantID, env.TenantID)
	assert.Equal(t, fx.Input.Producer, env.Producer)
	assert.True(t, fx.Input.OccurredAt.Equal(env.OccurredAt))
	assert.Equal(t, events.RemediationHandedOverSchemaVersion, env.SchemaVersion)
}

func TestRemediationHandedOverFields_AlwaysCarryANodesArray(t *testing.T) {
	p := validRemediationHandedOver()
	p.Nodes = nil
	fields := remediationHandedOverFields(t, p)
	assert.JSONEq(t, `[]`, string(mustRawField(t, fields["payload"], "nodes")))
}

func TestRemediationHandedOverFields_ANodeNamesItsOwnerOnlyWhenItHasOne(t *testing.T) {
	p := validRemediationHandedOver()
	p.Nodes = append(p.Nodes, events.HandedOverNode{
		NodeID: "analytics.payments_current", Reason: string(model.HandoverReasonNoType), Message: "m", Owner: "finance-data",
	})

	fields := remediationHandedOverFields(t, p)

	assert.JSONEq(t, `[
		{"node_id": "analytics.orders_enriched", "reason": "tries_spent", "message": "m"},
		{"node_id": "analytics.payments_current", "reason": "no_type", "message": "m", "owner": "finance-data"}
	]`, string(mustRawField(t, fields["payload"], "nodes")))
	_, got := readHandedOver(t, fields)
	assert.Equal(t, p, got)
}

func TestRemediationHandedOverFields_AcceptEveryHandoverReasonAndNoTries(t *testing.T) {
	for _, reason := range model.HandoverReasons() {
		t.Run(string(reason), func(t *testing.T) {
			p := validRemediationHandedOver()
			p.Tries = 0
			p.Nodes[0].Reason = string(reason)
			_, got := readHandedOver(t, remediationHandedOverFields(t, p))
			assert.Equal(t, p, got)
		})
	}
}

func TestRemediationHandedOverFields_RefuseWhatTheContractForbids(t *testing.T) {
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
			fields, err := events.RemediationHandedOverFields("e", events.DefaultTenantID, "agent-remediation", time.Date(2026, 11, 12, 10, 47, 19, 0, time.UTC), p)
			require.ErrorIs(t, err, events.ErrMalformedEnvelope)
			assert.Nil(t, fields)
		})
	}
}
