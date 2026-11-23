package redis

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/handlers"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
)

func TestTriggerFromPayload_DecodesBatch(t *testing.T) {
	raw := []byte(`{"event_id":"e","source":"validation","release_id":"r","remediation_round":1,"repo":"o/r","commit_sha":"sha","code_bundle_uri":"s3://b/bundle.json",
	  "nodes":[{"node_id":"s.a","error_signature":"sig","category":"logic","reason":"logic:missing_object","dbt_log_uri":"s3://l/a","candidate_artifact_uri":"s3://c/a","file_path":"models/a.sql","service":"svc","node_type":"dbt-model","changed_ancestors":[{"node_id":"s.u","file_path":"models/u.sql","service":"svc","depth":1}]},
	           {"node_id":"s.b","error_signature":"sig","category":"logic","reason":"logic:missing_object","dbt_log_uri":"s3://l/b"}]}`)
	tr, err := TriggerFromPayload(raw)
	require.NoError(t, err)
	assert.Equal(t, "r", tr.ReleaseID)
	assert.Equal(t, "s3://b/bundle.json", tr.CodeBundleURI)
	require.Len(t, tr.Nodes, 2)
	assert.Equal(t, []handlers.ChangedAncestor{{NodeID: "s.u", FilePath: "models/u.sql", Service: "svc", Depth: 1}},
		tr.Nodes[0].ChangedAncestors)
	assert.Equal(t, "svc", tr.Nodes[0].Service)
	assert.Equal(t, raw, tr.RawPayload)
}

// storedPayloadFixture is a heal trigger payload as a store holds one published
// with an event_id of its own (remediation.requested:v2), every release-level
// and node-level field populated.
var storedPayloadFixture = map[string]any{
	"event_id":          "evt-abc-123",
	"source":            "validation",
	"release_id":        "rel-456",
	"remediation_round": 2,
	"repo":              "acme/dbt-project",
	"commit_sha":        "deadbeef1234",
	"code_bundle_uri":   "s3://bucket/code-bundles/rel-456/bundle.json",
	"classified_at":     "2026-08-28T00:00:00Z",
	"nodes": []any{
		map[string]any{
			"node_id":                "orders.model.orders_daily",
			"relation_id":            "analytics.orders_daily",
			"category":               "sql_syntax_error",
			"error_signature":        "column \"foo\" does not exist",
			"reason":                 "logic:missing_column",
			"error_excerpt":          "column \"foo\" does not exist",
			"dbt_log_uri":            "s3://bucket/logs/orders_daily.log",
			"candidate_artifact_uri": "s3://bucket/sql/orders_daily.sql",
			"file_path":              "models/orders_daily.sql",
			"service":                "svc-orders",
			"node_type":              "dbt-model",
			"other_service":          "svc-finance",
			"other_file_path":        "models/orders_legacy.sql",
			"changed_ancestors": []any{map[string]any{
				"node_id": "orders.model.orders_base", "file_path": "models/orders_base.sql", "service": "svc-orders"}},
		},
	},
}

// requestedFixture is a remediation.requested:v3 payload with every
// release-level and node-level field populated, and a second node the
// classifier marked unfixable.
func requestedFixture() pkgevents.RemediationRequested {
	return pkgevents.RemediationRequested{
		Source:           "validation",
		ReleaseID:        "rel-456",
		RemediationRound: 2,
		Repo:             "acme/dbt-project",
		CommitSHA:        "deadbeef1234",
		CodeBundleURI:    "s3://bucket/code-bundles/rel-456/bundle.json",
		ClassifiedAt:     "2026-08-28T00:00:00Z",
		Nodes: []pkgevents.RemediationRequestedNode{
			{
				NodeID:               "orders.model.orders_daily",
				RelationID:           "analytics.orders_daily",
				Category:             "sql_syntax_error",
				ErrorSignature:       "column \"foo\" does not exist",
				Reason:               "logic:missing_column",
				ErrorExcerpt:         "column \"foo\" does not exist",
				DBTLogURI:            "s3://bucket/logs/orders_daily.log",
				CandidateArtifactURI: "s3://bucket/sql/orders_daily.sql",
				FilePath:             "models/orders_daily.sql",
				Service:              "svc-orders",
				NodeType:             "dbt-model",
				OtherService:         "svc-finance",
				OtherFilePath:        "models/orders_legacy.sql",
				ChangedAncestors: []pkgevents.RemediationRequestedAncestor{
					{NodeID: "orders.model.orders_base", FilePath: "models/orders_base.sql", Service: "svc-orders"}},
			},
			{
				NodeID:         "orders.model.orders_weekly",
				Category:       "infra",
				ErrorSignature: "connection reset",
				Reason:         "infra:connection_reset",
				DBTLogURI:      "s3://bucket/logs/orders_weekly.log",
				Service:        "svc-orders",
				NodeType:       "dbt-model",
				Unfixable:      &pkgevents.Unfixable{Reason: string(model.HandoverReasonClassifierInfra), Detail: "the warehouse reset the connection"},
			},
		},
	}
}

// requestedMessageOf is the remediation.requested:v3 stream message carrying p
// for tenantID, with the payload bytes it carries.
func requestedMessageOf(t *testing.T, id, tenantID string, p pkgevents.RemediationRequested, extra map[string]any) (goredis.XMessage, []byte) {
	t.Helper()
	fields, err := pkgevents.RemediationRequestedFields("evt-1", tenantID, "remediation", time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC), p)
	require.NoError(t, err)
	for k, v := range extra {
		fields[k] = v
	}
	return goredis.XMessage{ID: id, Values: fields}, []byte(fields["payload"].(string))
}

// TestTriggerFromPayload_DropsUnfixableNodes verifies the decode leaves out a
// node the classifier marked unfixable.
func TestTriggerFromPayload_DropsUnfixableNodes(t *testing.T) {
	raw, err := json.Marshal(requestedFixture())
	require.NoError(t, err)

	trigger, err := TriggerFromPayload(raw)
	require.NoError(t, err)

	require.Len(t, trigger.Nodes, 1)
	assert.Equal(t, "orders.model.orders_daily", trigger.Nodes[0].NodeID)
	assert.Equal(t, raw, trigger.RawPayload, "the replayed trigger carries the bytes it was decoded from")
}

// TestTriggerFromPayload_ReadsAStoredV2Payload verifies a payload stored from
// remediation.requested:v2, with an event_id of its own and no unfixable node,
// decodes to every node it names.
func TestTriggerFromPayload_ReadsAStoredV2Payload(t *testing.T) {
	raw, err := json.Marshal(storedPayloadFixture)
	require.NoError(t, err)

	trigger, err := TriggerFromPayload(raw)
	require.NoError(t, err)

	assert.Equal(t, "rel-456", trigger.ReleaseID)
	assert.Equal(t, 2, trigger.RemediationRound)
	require.Len(t, trigger.Nodes, 1)
	assert.Equal(t, "orders.model.orders_daily", trigger.Nodes[0].NodeID)
	assert.Equal(t, "svc-finance", trigger.Nodes[0].OtherService)
}

// TestTriggerFromPayload_RefusesAPayloadThatIsNotATrigger verifies the decode
// errors on bytes that are not a heal trigger.
func TestTriggerFromPayload_RefusesAPayloadThatIsNotATrigger(t *testing.T) {
	for name, raw := range map[string]string{
		"not JSON":    "not-json",
		"no release":  `{"source":"validation","nodes":[]}`,
		"bad source":  `{"source":"nope","release_id":"r","nodes":[]}`,
		"empty bytes": "",
	} {
		_, err := TriggerFromPayload([]byte(raw))
		assert.ErrorIs(t, err, pkgevents.ErrMalformedEnvelope, name)
	}
}

// TestTriggerFromPayload_LeavesTheDedupIdentityToTheCaller pins the split
// between the payload and the message that delivered it: decoding the payload
// alone yields every trigger field but no dedup identity, so a caller
// replaying stored bytes — the verification reconciler starting the attempt
// that follows a failed verification — supplies an identity of its own rather
// than inheriting the identity of the message the first attempt consumed.
func TestTriggerFromPayload_LeavesTheDedupIdentityToTheCaller(t *testing.T) {
	raw, err := json.Marshal(storedPayloadFixture)
	require.NoError(t, err)

	trigger, err := TriggerFromPayload(raw)
	require.NoError(t, err)

	require.Equal(t, "validation", trigger.Source)
	require.Equal(t, "rel-456", trigger.ReleaseID)
	require.Len(t, trigger.Nodes, 1)
	require.Equal(t, "dbt-model", trigger.Nodes[0].NodeType)
	require.Equal(t, raw, trigger.RawPayload)
	require.Empty(t, trigger.MessageID)
	require.Nil(t, trigger.OutboxEntryID)
}

// TestBinding_AcceptsTheTrigger verifies that a message is decoded into the
// trigger it carries and handed to accept with the envelope's tenant, its
// message id, the producer's outbox entry id, the payload bytes and the
// stream's fields.
func TestBinding_AcceptsTheTrigger(t *testing.T) {
	entryID := uuid.New()
	msg, payload := requestedMessageOf(t, "7-0", "acme", requestedFixture(), map[string]any{"outbox_entry_id": entryID.String()})

	var got []runner.Accepted
	h := newRemediationRequestedHandler(func(_ context.Context, a runner.Accepted) error {
		got = append(got, a)
		return nil
	})
	require.NoError(t, h(context.Background(), msg))

	require.Len(t, got, 1)
	a := got[0]
	assert.Equal(t, "acme", a.Trigger.TenantID)
	assert.Equal(t, "rel-456", a.Trigger.ReleaseID)
	assert.Equal(t, 2, a.Trigger.Round)
	require.Len(t, a.Trigger.Nodes, 2)
	assert.Equal(t, "orders.model.orders_daily", a.Trigger.Nodes[0].NodeID)
	assert.Equal(t, payload, a.Payload)
	assert.Equal(t, "7-0", a.MessageID)
	require.NotNil(t, a.OutboxEntryID)
	assert.Equal(t, entryID, *a.OutboxEntryID)
	assert.Equal(t, map[string]string{
		"event_id":        "evt-1",
		"tenant_id":       "acme",
		"occurred_at":     "2026-11-14T09:00:00.000000Z",
		"producer":        "remediation",
		"schema_version":  "1",
		"outbox_entry_id": entryID.String(),
	}, a.Fields, "the stream's fields travel with the trigger, without the payload it already carries")
}

// TestBinding_KeepsUnfixableNodes verifies that a node the classifier marked
// unfixable stays on the trigger, with its reason and detail: the runner hands
// it over itself.
func TestBinding_KeepsUnfixableNodes(t *testing.T) {
	msg, _ := requestedMessageOf(t, "7-0", "acme", requestedFixture(), nil)
	var got runner.Accepted
	h := newRemediationRequestedHandler(func(_ context.Context, a runner.Accepted) error {
		got = a
		return nil
	})

	require.NoError(t, h(context.Background(), msg))

	require.Len(t, got.Trigger.Nodes, 2)
	assert.Nil(t, got.Trigger.Nodes[0].Unfixable)
	assert.Equal(t, "orders.model.orders_weekly", got.Trigger.Nodes[1].NodeID)
	assert.Equal(t, &remediation.Unfixable{
		Reason: model.HandoverReasonClassifierInfra, Detail: "the warehouse reset the connection"}, got.Trigger.Nodes[1].Unfixable)
}

// TestBinding_LeavesAnAbsentTenantToAccept verifies that a message
// whose envelope names no tenant reaches accept with an empty tenant: the
// runner's Accept is the one place that gives it the default tenant.
func TestBinding_LeavesAnAbsentTenantToAccept(t *testing.T) {
	msg, _ := requestedMessageOf(t, "8-0", "", requestedFixture(), nil)
	var got runner.Accepted
	h := newRemediationRequestedHandler(func(_ context.Context, a runner.Accepted) error {
		got = a
		return nil
	})
	require.NoError(t, h(context.Background(), msg))
	assert.Empty(t, got.Trigger.TenantID)
	assert.Equal(t, "rel-456", got.Trigger.ReleaseID)
}

// TestBinding_RedeliversAnAcceptError verifies that an error from
// accept reaches the consumer as it is, so the message is delivered again.
func TestBinding_RedeliversAnAcceptError(t *testing.T) {
	msg, _ := requestedMessageOf(t, "9-0", "acme", requestedFixture(), nil)
	boom := errors.New("database down")
	h := newRemediationRequestedHandler(func(context.Context, runner.Accepted) error { return boom })
	err := h(context.Background(), msg)
	require.ErrorIs(t, err, boom)
	assert.NotErrorIs(t, err, pkgevents.ErrPermanent)
}
