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

type remediationRequestedFixture struct {
	Input struct {
		EventID    string                      `json:"event_id"`
		TenantID   string                      `json:"tenant_id"`
		Producer   string                      `json:"producer"`
		OccurredAt time.Time                   `json:"occurred_at"`
		Payload    events.RemediationRequested `json:"payload"`
	} `json:"input"`
	Fields map[string]json.RawMessage `json:"fields"`
}

const (
	remediationRequestedV3Fixture       = "testdata/remediation_requested_v3.json"
	remediationRequestedV2StoredFixture = "testdata/remediation_requested_v2_stored.json"
)

func loadRemediationRequestedFixture(t *testing.T) remediationRequestedFixture {
	t.Helper()
	raw, err := os.ReadFile(remediationRequestedV3Fixture)
	require.NoError(t, err)
	var fx remediationRequestedFixture
	require.NoError(t, json.Unmarshal(raw, &fx))
	return fx
}

func validRemediationRequested() events.RemediationRequested {
	return events.RemediationRequested{
		Source:           string(model.RemediationSourceValidation),
		ReleaseID:        "rel_20261112_0042",
		RemediationRound: 1,
		Repo:             "acme/analytics-dbt",
		CommitSHA:        "9f1c2e7a4b6d8031c5e7f9a2b4d6081a3c5e7f90",
		ClassifiedAt:     "2026-11-12T10:31:07Z",
		Nodes: []events.RemediationRequestedNode{
			{NodeID: "analytics.orders_enriched", Category: "code_defect", Reason: "validation_failed"},
		},
	}
}

func remediationRequestedFields(t *testing.T, p events.RemediationRequested) map[string]string {
	t.Helper()
	fields, err := events.RemediationRequestedFields("e", events.DefaultTenantID, "remediation", time.Date(2026, 11, 12, 10, 31, 7, 0, time.UTC), p)
	require.NoError(t, err)
	return stringFields(fields)
}

func TestRemediationRequestedFields_MatchGoldenFixture(t *testing.T) {
	fx := loadRemediationRequestedFixture(t)
	got, err := events.RemediationRequestedFields(fx.Input.EventID, fx.Input.TenantID, fx.Input.Producer, fx.Input.OccurredAt, fx.Input.Payload)
	require.NoError(t, err)
	assertFieldsMatchFixture(t, fx.Fields, got)
}

func TestRemediationRequestedFields_AlwaysCarryANodesArray(t *testing.T) {
	p := validRemediationRequested()
	p.Nodes = nil
	fields := remediationRequestedFields(t, p)
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

func TestDecodeRemediationRequested_ReadsGoldenFixture(t *testing.T) {
	fx := loadRemediationRequestedFixture(t)
	env, p, err := events.DecodeRemediationRequested(fixtureFields(t, fx.Fields))
	require.NoError(t, err)
	assert.Equal(t, fx.Input.Payload, p)
	assert.Equal(t, fx.Input.EventID, env.EventID)
	assert.Equal(t, fx.Input.TenantID, env.TenantID)
	assert.Equal(t, fx.Input.Producer, env.Producer)
	assert.Equal(t, fx.Input.OccurredAt.UTC(), env.OccurredAt)
	assert.Equal(t, events.RemediationRequestedSchemaVersion, env.SchemaVersion)
}

func TestDecodeRemediationRequested_ReadsTheUnfixableMarkAndTheChangedAncestors(t *testing.T) {
	_, p, err := events.DecodeRemediationRequested(fixtureFields(t, loadRemediationRequestedFixture(t).Fields))
	require.NoError(t, err)
	require.Len(t, p.Nodes, 3)

	assert.Nil(t, p.Nodes[0].Unfixable)
	assert.Equal(t, []events.RemediationRequestedAncestor{
		{NodeID: "analytics.customers_clean", FilePath: "models/analytics/customers_clean.sql", Service: "service-1", Depth: 1},
		{NodeID: "staging.stg_customers", FilePath: "models/staging/stg_customers.sql", Service: "service-1", Depth: 2},
	}, p.Nodes[0].ChangedAncestors)

	assert.Equal(t, &events.Unfixable{
		Reason: string(model.HandoverReasonClassifierInfra),
		Detail: "the warehouse connection was reset, a new release run clears it",
	}, p.Nodes[2].Unfixable)
}

// A payload remediation.requested:v2 stored (proposal.trigger_payload,
// remediation.trigger_payload) carries the event id inside and marks no node
// unfixable.
func TestDecodeRemediationRequestedPayload_ReadsAStoredV2Payload(t *testing.T) {
	raw, err := os.ReadFile(remediationRequestedV2StoredFixture)
	require.NoError(t, err)

	p, err := events.DecodeRemediationRequestedPayload(raw)
	require.NoError(t, err)

	assert.Equal(t, "validation", p.Source)
	assert.Equal(t, "rel_20261112_0042", p.ReleaseID)
	assert.Equal(t, 1, p.RemediationRound)
	assert.Equal(t, "acme/analytics-dbt", p.Repo)
	assert.Equal(t, "2026-11-12T09:58:41Z", p.ClassifiedAt)
	assert.Equal(t, "s3://continuo/tenants/default/releases/rel_20261112_0042/code-bundle.json.gz", p.CodeBundleURI)
	require.Len(t, p.Nodes, 2)
	for _, n := range p.Nodes {
		assert.Nil(t, n.Unfixable, "a v2 node is always fixable: %s", n.NodeID)
	}
	assert.Equal(t, "analytics.orders_enriched", p.Nodes[0].NodeID)
	assert.Equal(t, "column \"customer_segment\" does not exist", p.Nodes[0].ErrorExcerpt)
	assert.Equal(t, "dbt-model", p.Nodes[0].NodeType)
	require.Len(t, p.Nodes[0].ChangedAncestors, 2)
	assert.Equal(t, events.RemediationRequestedAncestor{NodeID: "staging.stg_customers", FilePath: "models/staging/stg_customers.sql", Service: "service-1", Depth: 2}, p.Nodes[0].ChangedAncestors[1])
	assert.Empty(t, p.Nodes[1].ChangedAncestors)
}

func TestDecodeRemediationRequestedPayload_ReadsAV3Payload(t *testing.T) {
	fx := loadRemediationRequestedFixture(t)
	raw := fx.Fields["payload"]
	p, err := events.DecodeRemediationRequestedPayload(raw)
	require.NoError(t, err)
	assert.Equal(t, fx.Input.Payload, p)
}

// A trigger with no node is the consumers' to judge, not the decoder's.
func TestDecodeRemediationRequested_AcceptsATriggerWithNoNodes(t *testing.T) {
	p := validRemediationRequested()
	p.Nodes = nil
	_, got, err := events.DecodeRemediationRequested(remediationRequestedFields(t, p))
	require.NoError(t, err)
	assert.Empty(t, got.Nodes)
}

func TestDecodeRemediationRequested_AcceptsEverySourceAndBothClassifierReasons(t *testing.T) {
	for _, source := range model.RemediationSources() {
		t.Run(string(source), func(t *testing.T) {
			p := validRemediationRequested()
			p.Source = string(source)
			_, _, err := events.DecodeRemediationRequested(remediationRequestedFields(t, p))
			require.NoError(t, err)
		})
	}
	for _, reason := range []model.HandoverReason{model.HandoverReasonClassifierInfra, model.HandoverReasonClassifierUnhealableParse} {
		t.Run(string(reason), func(t *testing.T) {
			p := validRemediationRequested()
			p.Nodes[0].Unfixable = &events.Unfixable{Reason: string(reason), Detail: "d"}
			_, got, err := events.DecodeRemediationRequested(remediationRequestedFields(t, p))
			require.NoError(t, err)
			assert.Equal(t, string(reason), got.Nodes[0].Unfixable.Reason)
		})
	}
}

func TestDecodeRemediationRequested_RefusesAnUnknownSource(t *testing.T) {
	p := validRemediationRequested()
	p.Source = "runtime"
	_, _, err := events.DecodeRemediationRequested(remediationRequestedFields(t, p))
	require.ErrorIs(t, err, events.ErrMalformedEnvelope)
	assert.Contains(t, err.Error(), "source")
}

func TestDecodeRemediationRequested_RefusesAnUnknownUnfixableReason(t *testing.T) {
	p := validRemediationRequested()
	p.Nodes[0].Unfixable = &events.Unfixable{Reason: "cosmic_rays", Detail: "d"}
	_, _, err := events.DecodeRemediationRequested(remediationRequestedFields(t, p))
	require.ErrorIs(t, err, events.ErrMalformedEnvelope)
	assert.Contains(t, err.Error(), "unfixable")
}

// Only the classifier marks a node unfixable: a handover reason that another
// stage records is refused on the trigger even though the vocabulary knows it.
func TestDecodeRemediationRequested_RefusesAHandoverReasonThatIsNotTheClassifiers(t *testing.T) {
	for _, reason := range []model.HandoverReason{
		model.HandoverReasonTriesSpent, model.HandoverReasonPreflightRoundsSpent, model.HandoverReasonDeclined,
		model.HandoverReasonNoType, model.HandoverReasonNoRepository, model.HandoverReasonUnverifiableEdits,
	} {
		t.Run(string(reason), func(t *testing.T) {
			p := validRemediationRequested()
			p.Nodes[0].Unfixable = &events.Unfixable{Reason: string(reason), Detail: "d"}
			_, _, err := events.DecodeRemediationRequested(remediationRequestedFields(t, p))
			require.ErrorIs(t, err, events.ErrMalformedEnvelope)
		})
	}
}

// Every rule applies to a stream entry and to a stored payload alike.
func TestRemediationRequestedDecoders_RefuseWhatTheContractForbids(t *testing.T) {
	cases := map[string]func(p *events.RemediationRequested){
		"no release id":               func(p *events.RemediationRequested) { p.ReleaseID = "" },
		"round 0":                     func(p *events.RemediationRequested) { p.RemediationRound = 0 },
		"negative round":              func(p *events.RemediationRequested) { p.RemediationRound = -1 },
		"no source":                   func(p *events.RemediationRequested) { p.Source = "" },
		"unknown source":              func(p *events.RemediationRequested) { p.Source = "plan" },
		"a node without a node id":    func(p *events.RemediationRequested) { p.Nodes[0].NodeID = "" },
		"an unfixable with no reason": func(p *events.RemediationRequested) { p.Nodes[0].Unfixable = &events.Unfixable{Detail: "d"} },
		"an unknown unfixable reason": func(p *events.RemediationRequested) {
			p.Nodes[0].Unfixable = &events.Unfixable{Reason: "x", Detail: "d"}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := validRemediationRequested()
			mutate(&p)
			fields := remediationRequestedFields(t, p)

			_, _, err := events.DecodeRemediationRequested(fields)
			require.ErrorIs(t, err, events.ErrMalformedEnvelope, "stream decoder")

			_, err = events.DecodeRemediationRequestedPayload([]byte(fields["payload"]))
			require.ErrorIs(t, err, events.ErrMalformedEnvelope, "stored payload decoder")
		})
	}
}

func TestDecodeRemediationRequested_RefusesAnUnreadableEntry(t *testing.T) {
	valid := func() map[string]string { return remediationRequestedFields(t, validRemediationRequested()) }
	cases := map[string]map[string]string{
		"other version":    withField(valid(), "schema_version", "2"),
		"payload not json": withPayload(valid(), `{`),
		"no payload":       withPayload(valid(), ``),
		"nodes not a list": withPayload(valid(), `{"source":"validation","release_id":"r","remediation_round":1,"nodes":{}}`),
		"no occurred_at":   withField(valid(), "occurred_at", ""),
	}
	for name, fields := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := events.DecodeRemediationRequested(fields)
			require.ErrorIs(t, err, events.ErrMalformedEnvelope)
		})
	}
	_, err := events.DecodeRemediationRequestedPayload([]byte(`{`))
	require.ErrorIs(t, err, events.ErrMalformedEnvelope)
}
