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

type healTriggeredFixture struct {
	Input struct {
		EventID    string               `json:"event_id"`
		TenantID   string               `json:"tenant_id"`
		Producer   string               `json:"producer"`
		OccurredAt time.Time            `json:"occurred_at"`
		Payload    events.HealTriggered `json:"payload"`
	} `json:"input"`
	Fields map[string]json.RawMessage `json:"fields"`
}

const (
	healTriggeredFixturePath = "testdata/heal_triggered_v1.json"
)

func loadHealTriggeredFixture(t *testing.T) healTriggeredFixture {
	t.Helper()
	raw, err := os.ReadFile(healTriggeredFixturePath)
	require.NoError(t, err)
	var fx healTriggeredFixture
	require.NoError(t, json.Unmarshal(raw, &fx))
	return fx
}

func validHealTriggered() events.HealTriggered {
	return events.HealTriggered{
		Source:       string(model.RemediationSourceValidation),
		ReleaseID:    "rel_20261112_0042",
		Round:        1,
		Repo:         "acme/analytics-dbt",
		CommitSHA:    "9f1c2e7a4b6d8031c5e7f9a2b4d6081a3c5e7f90",
		ClassifiedAt: "2026-11-12T10:31:07Z",
		Nodes: []events.HealTriggeredNode{
			{NodeID: "analytics.orders_enriched", Category: "code_defect", Reason: "validation_failed"},
		},
	}
}

func healTriggeredFields(t *testing.T, p events.HealTriggered) map[string]string {
	t.Helper()
	fields, err := events.HealTriggeredFields("e", events.DefaultTenantID, "remediation", time.Date(2026, 11, 12, 10, 31, 7, 0, time.UTC), p)
	require.NoError(t, err)
	return stringFields(fields)
}

func TestHealTriggeredFields_MatchGoldenFixture(t *testing.T) {
	fx := loadHealTriggeredFixture(t)
	got, err := events.HealTriggeredFields(fx.Input.EventID, fx.Input.TenantID, fx.Input.Producer, fx.Input.OccurredAt, fx.Input.Payload)
	require.NoError(t, err)
	assertFieldsMatchFixture(t, fx.Fields, got)
}

func TestHealTriggeredFields_AlwaysCarryANodesArray(t *testing.T) {
	p := validHealTriggered()
	p.Nodes = nil
	fields := healTriggeredFields(t, p)
	assert.JSONEq(t, `[]`, string(mustRawField(t, fields["payload"], "nodes")))
}

func mustRawField(t *testing.T, payload, key string) json.RawMessage {
	t.Helper()
	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(payload), &m))
	raw, ok := m[key]
	require.True(t, ok, "payload has no %s", key)
	return raw
}

func TestDecodeHealTriggered_ReadsGoldenFixture(t *testing.T) {
	fx := loadHealTriggeredFixture(t)
	env, p, err := events.DecodeHealTriggered(fixtureFields(t, fx.Fields))
	require.NoError(t, err)
	assert.Equal(t, fx.Input.Payload, p)
	assert.Equal(t, fx.Input.EventID, env.EventID)
	assert.Equal(t, fx.Input.TenantID, env.TenantID)
	assert.Equal(t, fx.Input.Producer, env.Producer)
	assert.Equal(t, fx.Input.OccurredAt.UTC(), env.OccurredAt)
	assert.Equal(t, events.HealTriggeredSchemaVersion, env.SchemaVersion)
}

func TestDecodeHealTriggered_ReadsTheUnfixableMarkAndTheChangedAncestors(t *testing.T) {
	_, p, err := events.DecodeHealTriggered(fixtureFields(t, loadHealTriggeredFixture(t).Fields))
	require.NoError(t, err)
	require.Len(t, p.Nodes, 3)

	assert.Nil(t, p.Nodes[0].Unfixable)
	assert.Equal(t, []events.HealTriggeredAncestor{
		{NodeID: "analytics.customers_clean", FilePath: "models/analytics/customers_clean.sql", Project: "service-1", Depth: 1},
		{NodeID: "staging.stg_customers", FilePath: "models/staging/stg_customers.sql", Project: "service-1", Depth: 2},
	}, p.Nodes[0].ChangedAncestors)

	assert.Equal(t, &events.Unfixable{
		Reason: string(model.HandoverReasonClassifierInfra),
		Detail: "the warehouse connection was reset, a new release run clears it",
	}, p.Nodes[2].Unfixable)
}

// A store holds the heal.triggered:v1 payload as the stream carried it.
func TestDecodeHealTriggeredPayload_ReadsAStoredPayload(t *testing.T) {
	fx := loadHealTriggeredFixture(t)
	raw := fx.Fields["payload"]
	p, err := events.DecodeHealTriggeredPayload(raw)
	require.NoError(t, err)
	assert.Equal(t, fx.Input.Payload, p)
}

// A trigger with no node is the consumers' to judge, not the decoder's.
func TestDecodeHealTriggered_AcceptsATriggerWithNoNodes(t *testing.T) {
	p := validHealTriggered()
	p.Nodes = nil
	_, got, err := events.DecodeHealTriggered(healTriggeredFields(t, p))
	require.NoError(t, err)
	assert.Empty(t, got.Nodes)
}

func TestDecodeHealTriggered_AcceptsEverySourceAndBothClassifierReasons(t *testing.T) {
	for _, source := range model.RemediationSources() {
		t.Run(string(source), func(t *testing.T) {
			p := validHealTriggered()
			p.Source = string(source)
			_, _, err := events.DecodeHealTriggered(healTriggeredFields(t, p))
			require.NoError(t, err)
		})
	}
	for _, reason := range []model.HandoverReason{model.HandoverReasonClassifierInfra, model.HandoverReasonClassifierUnhealableParse} {
		t.Run(string(reason), func(t *testing.T) {
			p := validHealTriggered()
			p.Nodes[0].Unfixable = &events.Unfixable{Reason: string(reason), Detail: "d"}
			_, got, err := events.DecodeHealTriggered(healTriggeredFields(t, p))
			require.NoError(t, err)
			assert.Equal(t, string(reason), got.Nodes[0].Unfixable.Reason)
		})
	}
}

func TestDecodeHealTriggered_RefusesAnUnknownSource(t *testing.T) {
	p := validHealTriggered()
	p.Source = "runtime"
	_, _, err := events.DecodeHealTriggered(healTriggeredFields(t, p))
	require.ErrorIs(t, err, events.ErrMalformedEnvelope)
	assert.Contains(t, err.Error(), "source")
}

func TestDecodeHealTriggered_RefusesAnUnknownUnfixableReason(t *testing.T) {
	p := validHealTriggered()
	p.Nodes[0].Unfixable = &events.Unfixable{Reason: "cosmic_rays", Detail: "d"}
	_, _, err := events.DecodeHealTriggered(healTriggeredFields(t, p))
	require.ErrorIs(t, err, events.ErrMalformedEnvelope)
	assert.Contains(t, err.Error(), "unfixable")
}

// Only the classifier marks a node unfixable: a handover reason that another
// stage records is refused on the trigger even though the vocabulary knows it.
func TestDecodeHealTriggered_RefusesAHandoverReasonThatIsNotTheClassifiers(t *testing.T) {
	for _, reason := range []model.HandoverReason{
		model.HandoverReasonTriesSpent, model.HandoverReasonDraftsSpent, model.HandoverReasonDeclined,
		model.HandoverReasonNoType, model.HandoverReasonNoRepository, model.HandoverReasonUnverifiableEdits,
	} {
		t.Run(string(reason), func(t *testing.T) {
			p := validHealTriggered()
			p.Nodes[0].Unfixable = &events.Unfixable{Reason: string(reason), Detail: "d"}
			_, _, err := events.DecodeHealTriggered(healTriggeredFields(t, p))
			require.ErrorIs(t, err, events.ErrMalformedEnvelope)
		})
	}
}

// Every rule applies to a stream entry and to a stored payload alike.
func TestHealTriggeredDecoders_RefuseWhatTheContractForbids(t *testing.T) {
	cases := map[string]func(p *events.HealTriggered){
		"no release id":               func(p *events.HealTriggered) { p.ReleaseID = "" },
		"negative round":              func(p *events.HealTriggered) { p.Round = -1 },
		"no source":                   func(p *events.HealTriggered) { p.Source = "" },
		"unknown source":              func(p *events.HealTriggered) { p.Source = "plan" },
		"a node without a node id":    func(p *events.HealTriggered) { p.Nodes[0].NodeID = "" },
		"an unfixable with no reason": func(p *events.HealTriggered) { p.Nodes[0].Unfixable = &events.Unfixable{Detail: "d"} },
		"an unknown unfixable reason": func(p *events.HealTriggered) {
			p.Nodes[0].Unfixable = &events.Unfixable{Reason: "x", Detail: "d"}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := validHealTriggered()
			mutate(&p)
			fields := healTriggeredFields(t, p)

			_, _, err := events.DecodeHealTriggered(fields)
			require.ErrorIs(t, err, events.ErrMalformedEnvelope, "stream decoder")

			_, err = events.DecodeHealTriggeredPayload([]byte(fields["payload"]))
			require.ErrorIs(t, err, events.ErrMalformedEnvelope, "stored payload decoder")
		})
	}
}

// A heal.triggered:v1 entry always carries its round: the stream decoder refuses a 0 and a
// missing round.
func TestDecodeHealTriggered_RefusesRoundZeroAndAMissingRound(t *testing.T) {
	zero := validHealTriggered()
	zero.Round = 0
	_, _, err := events.DecodeHealTriggered(healTriggeredFields(t, zero))
	require.ErrorIs(t, err, events.ErrMalformedEnvelope, "round 0")

	missing := withPayload(healTriggeredFields(t, validHealTriggered()),
		`{"source":"validation","release_id":"rel_1","nodes":[{"node_id":"a.b"}]}`)
	_, _, err = events.DecodeHealTriggered(missing)
	require.ErrorIs(t, err, events.ErrMalformedEnvelope, "no round")
}

// A stored payload carries its round as the stream did: the stored-payload
// decoder refuses a 0 and a missing round, and keeps a later round.
func TestDecodeHealTriggeredPayload_RefusesRoundZeroAndAMissingRound(t *testing.T) {
	zero := validHealTriggered()
	zero.Round = 0
	_, err := events.DecodeHealTriggeredPayload([]byte(healTriggeredFields(t, zero)["payload"]))
	require.ErrorIs(t, err, events.ErrMalformedEnvelope, "round 0")

	_, err = events.DecodeHealTriggeredPayload([]byte(`{"source":"validation","release_id":"rel_1","nodes":[{"node_id":"a.b"}]}`))
	require.ErrorIs(t, err, events.ErrMalformedEnvelope, "no round")

	later := validHealTriggered()
	later.Round = 3
	got, err := events.DecodeHealTriggeredPayload([]byte(healTriggeredFields(t, later)["payload"]))
	require.NoError(t, err)
	assert.Equal(t, 3, got.Round)
}

func TestDecodeHealTriggered_RefusesAnUnreadableEntry(t *testing.T) {
	valid := func() map[string]string { return healTriggeredFields(t, validHealTriggered()) }
	cases := map[string]map[string]string{
		"other version":    withField(valid(), "schema_version", "2"),
		"payload not json": withPayload(valid(), `{`),
		"no payload":       withPayload(valid(), ``),
		"nodes not a list": withPayload(valid(), `{"source":"validation","release_id":"r","round":1,"nodes":{}}`),
		"no occurred_at":   withField(valid(), "occurred_at", ""),
	}
	for name, fields := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := events.DecodeHealTriggered(fields)
			require.ErrorIs(t, err, events.ErrMalformedEnvelope)
		})
	}
	_, err := events.DecodeHealTriggeredPayload([]byte(`{`))
	require.ErrorIs(t, err, events.ErrMalformedEnvelope)
}
