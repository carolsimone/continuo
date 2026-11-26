package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
)

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
				Project:              "svc-orders",
				NodeType:             "dbt-model",
				OtherProject:         "svc-finance",
				OtherFilePath:        "models/orders_legacy.sql",
				ChangedAncestors: []pkgevents.RemediationRequestedAncestor{
					{NodeID: "orders.model.orders_base", FilePath: "models/orders_base.sql", Project: "svc-orders"}},
			},
			{
				NodeID:         "orders.model.orders_weekly",
				Category:       "infra",
				ErrorSignature: "connection reset",
				Reason:         "infra:connection_reset",
				DBTLogURI:      "s3://bucket/logs/orders_weekly.log",
				Project:        "svc-orders",
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
