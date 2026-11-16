package redis

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/handlers"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
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

// proposeRecorder is a propose function that records the trigger of each call.
type proposeRecorder struct{ got []handlers.Trigger }

func (r *proposeRecorder) propose(_ context.Context, t handlers.Trigger) error {
	r.got = append(r.got, t)
	return nil
}

// TestBinding_LegacyEngineMapsEveryFieldOfTheTrigger verifies that with the
// legacy engine a message is decoded into the trigger it carries, with the
// message's dedup identity and its payload bytes.
func TestBinding_LegacyEngineMapsEveryFieldOfTheTrigger(t *testing.T) {
	msg, payload := requestedMessageOf(t, "1-0", "default", requestedFixture(), nil)
	rec := &proposeRecorder{}

	require.NoError(t, legacyRemediationRequestedHandler(rec.propose)(context.Background(), msg))

	require.Len(t, rec.got, 1)
	trigger := rec.got[0]
	require.Equal(t, "validation", trigger.Source)
	require.Equal(t, "rel-456", trigger.ReleaseID)
	require.Equal(t, 2, trigger.RemediationRound)
	require.Equal(t, "acme/dbt-project", trigger.Repo)
	require.Equal(t, "deadbeef1234", trigger.CommitSHA)
	require.Equal(t, "s3://bucket/code-bundles/rel-456/bundle.json", trigger.CodeBundleURI)
	require.Equal(t, "1-0", trigger.MessageID)
	require.Nil(t, trigger.OutboxEntryID) // no outbox_entry_id in Values
	require.Equal(t, payload, trigger.RawPayload)

	require.Len(t, trigger.Nodes, 1)
	n := trigger.Nodes[0]
	require.Equal(t, "orders.model.orders_daily", n.NodeID)
	require.Equal(t, "analytics.orders_daily", n.RelationID)
	require.Equal(t, "sql_syntax_error", n.Category)
	require.Equal(t, "column \"foo\" does not exist", n.ErrorSignature)
	require.Equal(t, "logic:missing_column", n.Reason)
	require.Equal(t, "column \"foo\" does not exist", n.ErrorExcerpt)
	require.Equal(t, "s3://bucket/logs/orders_daily.log", n.DBTLogURI)
	require.Equal(t, "s3://bucket/sql/orders_daily.sql", n.CandidateArtifactURI)
	require.Equal(t, "models/orders_daily.sql", n.FilePath)
	require.Equal(t, "svc-orders", n.Service)
	require.Equal(t, "dbt-model", n.NodeType)
	require.Equal(t, "svc-finance", n.OtherService,
		"other_service must decode — the only datum identifying the competing claimant when both share one service")
	require.Equal(t, "models/orders_legacy.sql", n.OtherFilePath,
		"other_file_path must decode — deleting its mapping would leave this assertion, and every other assertion here, green")
	require.Equal(t, []handlers.ChangedAncestor{
		{NodeID: "orders.model.orders_base", FilePath: "models/orders_base.sql", Service: "svc-orders"}}, n.ChangedAncestors,
		"changed_ancestors must decode with their candidate location — without the ids every failure is fixed in its own source instead of the ancestor they share, and without the location the fix edits wherever the PROMOTED graph still places a renamed ancestor")
}

// TestBinding_LegacyEngineTakesTheOutboxEntryID verifies the producer's outbox
// entry id reaches the trigger's second dedup axis.
func TestBinding_LegacyEngineTakesTheOutboxEntryID(t *testing.T) {
	entryID := uuid.New()
	msg, _ := requestedMessageOf(t, "3-0", "default", requestedFixture(), map[string]any{"outbox_entry_id": entryID.String()})
	rec := &proposeRecorder{}

	require.NoError(t, legacyRemediationRequestedHandler(rec.propose)(context.Background(), msg))

	require.Len(t, rec.got, 1)
	require.NotNil(t, rec.got[0].OutboxEntryID)
	assert.Equal(t, entryID, *rec.got[0].OutboxEntryID)
}

// TestBinding_LegacyEngineDropsUnfixableNodes verifies that the legacy engine
// proposes a fix for the failing nodes a fix can reach alone: a node the
// classifier marked unfixable never reaches the driver, while the payload bytes
// the trigger carries stay whole.
func TestBinding_LegacyEngineDropsUnfixableNodes(t *testing.T) {
	msg, payload := requestedMessageOf(t, "1-0", "default", requestedFixture(), nil)
	rec := &proposeRecorder{}

	require.NoError(t, legacyRemediationRequestedHandler(rec.propose)(context.Background(), msg))

	require.Len(t, rec.got, 1)
	require.Len(t, rec.got[0].Nodes, 1)
	assert.Equal(t, "orders.model.orders_daily", rec.got[0].Nodes[0].NodeID)
	assert.Equal(t, payload, rec.got[0].RawPayload)
}

// TestBinding_LegacyEngineAcknowledgesATriggerOfUnfixableNodesAlone verifies
// that a trigger whose every node is unfixable is acknowledged by the legacy
// engine's own driver with nothing written: no unit of work is opened.
func TestBinding_LegacyEngineAcknowledgesATriggerOfUnfixableNodesAlone(t *testing.T) {
	p := requestedFixture()
	p.Nodes = p.Nodes[1:]
	msg, _ := requestedMessageOf(t, "2-0", "default", p, nil)
	deps := handlers.Deps{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		NewUoW: func() uow.UnitOfWork {
			t.Fatal("a trigger with no node a fix can reach must write nothing")
			return nil
		},
	}

	require.NoError(t, newLegacyRemediationRequestedHandler(deps)(context.Background(), msg))
}

// TestTriggerFromPayload_DropsUnfixableNodes verifies the decode every legacy
// reader shares (the binding, the drop handler and the verification
// reconciler) leaves out a node the classifier marked unfixable.
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

// TestBinding_LegacyEngineReadsASeedBuildNode verifies that when a node carries
// the service field (set for seed_build failures from the candidate topology),
// it is decoded onto the node so the fixer can skip the NodeLocator lookup.
func TestBinding_LegacyEngineReadsASeedBuildNode(t *testing.T) {
	p := pkgevents.RemediationRequested{
		Source: "seed_build", ReleaseID: "rel-seed-1", RemediationRound: 1, Repo: "o/r", CommitSHA: "sha1",
		Nodes: []pkgevents.RemediationRequestedNode{{
			NodeID: "seed.svc.customers", Category: "logic", ErrorSignature: "extra column",
			DBTLogURI: "s3://b/seed.log", FilePath: "seeds/customers.csv", Service: "svc-data",
		}},
	}
	msg, _ := requestedMessageOf(t, "2-0", "default", p, nil)
	rec := &proposeRecorder{}

	require.NoError(t, legacyRemediationRequestedHandler(rec.propose)(context.Background(), msg))

	require.Len(t, rec.got, 1)
	trigger := rec.got[0]
	require.Equal(t, "seed_build", trigger.Source)
	require.Len(t, trigger.Nodes, 1)
	require.Equal(t, "seeds/customers.csv", trigger.Nodes[0].FilePath)
	require.Equal(t, "svc-data", trigger.Nodes[0].Service,
		"service must be decoded so the source fix can skip the NodeLocator lookup")
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

// TestBinding_RunnerEngineAcceptsTheTrigger verifies that with the runner
// engine a message is decoded into the trigger it carries and handed to accept
// with the envelope's tenant, its message id, the producer's outbox entry id,
// the payload bytes and the stream's fields, and that today's proposer is never
// called.
func TestBinding_RunnerEngineAcceptsTheTrigger(t *testing.T) {
	entryID := uuid.New()
	msg, payload := requestedMessageOf(t, "7-0", "acme", requestedFixture(), map[string]any{"outbox_entry_id": entryID.String()})

	var got []runner.Accepted
	h := newRunnerRemediationRequestedHandler(func(_ context.Context, a runner.Accepted) error {
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

// TestBinding_RunnerEngineKeepsUnfixableNodes verifies that with the runner
// engine a node the classifier marked unfixable stays on the trigger, with its
// reason and detail: the runner hands it over itself.
func TestBinding_RunnerEngineKeepsUnfixableNodes(t *testing.T) {
	msg, _ := requestedMessageOf(t, "7-0", "acme", requestedFixture(), nil)
	var got runner.Accepted
	h := newRunnerRemediationRequestedHandler(func(_ context.Context, a runner.Accepted) error {
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

// TestBinding_RunnerEngineLeavesAnAbsentTenantToAccept verifies that a message
// whose envelope names no tenant reaches accept with an empty tenant: the
// runner's Accept is the one place that gives it the default tenant.
func TestBinding_RunnerEngineLeavesAnAbsentTenantToAccept(t *testing.T) {
	msg, _ := requestedMessageOf(t, "8-0", "", requestedFixture(), nil)
	var got runner.Accepted
	h := newRunnerRemediationRequestedHandler(func(_ context.Context, a runner.Accepted) error {
		got = a
		return nil
	})
	require.NoError(t, h(context.Background(), msg))
	assert.Empty(t, got.Trigger.TenantID)
	assert.Equal(t, "rel-456", got.Trigger.ReleaseID)
}

// TestBinding_RunnerEngineRedeliversAnAcceptError verifies that an error from
// accept reaches the consumer as it is, so the message is delivered again.
func TestBinding_RunnerEngineRedeliversAnAcceptError(t *testing.T) {
	msg, _ := requestedMessageOf(t, "9-0", "acme", requestedFixture(), nil)
	boom := errors.New("database down")
	h := newRunnerRemediationRequestedHandler(func(context.Context, runner.Accepted) error { return boom })
	err := h(context.Background(), msg)
	require.ErrorIs(t, err, boom)
	assert.NotErrorIs(t, err, pkgevents.ErrPermanent)
}
