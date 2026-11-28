package redis

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// healTriggeredPayload is the trigger the remediation classifier
// publishes for one rejected release: a fixable node and one it marked
// unfixable.
func healTriggeredPayload() events.HealTriggered {
	return events.HealTriggered{
		Source:        "validation",
		ReleaseID:     "rel-1",
		Round:         1,
		Repo:          "acme/analytics-dbt",
		CommitSHA:     "9f1c2e7a4b6d8031c5e7f9a2b4d6081a3c5e7f90",
		CodeBundleURI: "s3://b/code-bundles/rel-1/bundle.json",
		ClassifiedAt:  "2026-08-12T09:00:00Z",
		Nodes: []events.HealTriggeredNode{
			{
				NodeID:         "analytics.revenue",
				Category:       "sql_syntax_error",
				ErrorSignature: "sig-1",
				Reason:         `column "foo" does not exist`,
				ErrorExcerpt:   `ERROR: column "foo" does not exist`,
				DBTLogURI:      "s3://b/logs/rel-1/analytics.revenue.log",
			},
			{
				NodeID:         "analytics.orders",
				Category:       "infra_transient",
				ErrorSignature: "sig-2",
				Reason:         "infra:connection_refused",
				DBTLogURI:      "s3://b/logs/rel-1/analytics.orders.log",
				Unfixable:      &events.Unfixable{Reason: "classifier_infra", Detail: "could not connect to server: connection refused"},
			},
		},
	}
}

// healTriggeredMessage is the entry the remediation component publishes
// for tenantID, with the outbox row id its publisher stamps.
func healTriggeredMessage(t *testing.T, tenantID string, p events.HealTriggered) goredis.XMessage {
	t.Helper()
	fields, err := events.HealTriggeredFields("evt-1", tenantID, "remediation", time.Now().UTC(), p)
	require.NoError(t, err)
	fields["outbox_entry_id"] = "6e1d0a4c-6b0e-4d55-8c55-3f7d1f2e9a10"
	return goredis.XMessage{ID: "1-0", Values: fields}
}

func TestParseHealTriggered_HappyPath(t *testing.T) {
	tenantID, evt, err := ParseHealTriggered(healTriggeredMessage(t, "tenant-7", healTriggeredPayload()))
	require.NoError(t, err)
	assert.Equal(t, "tenant-7", tenantID, "the tenant is the envelope's")
	assert.Equal(t, "validation", evt.Source)
	assert.Equal(t, "rel-1", evt.ReleaseID)
	assert.Equal(t, "s3://b/code-bundles/rel-1/bundle.json", evt.CodeBundleURI)
	assert.Equal(t, "2026-08-12T09:00:00Z", evt.ClassifiedAt)
	require.Len(t, evt.Nodes, 2)
	assert.Equal(t, "analytics.revenue", evt.Nodes[0].NodeID)
	assert.Equal(t, "sql_syntax_error", evt.Nodes[0].Category)
	assert.Equal(t, "sig-1", evt.Nodes[0].ErrorSignature)
	assert.Equal(t, `column "foo" does not exist`, evt.Nodes[0].Reason)
	assert.Equal(t, `ERROR: column "foo" does not exist`, evt.Nodes[0].ErrorExcerpt)
	assert.Equal(t, "s3://b/logs/rel-1/analytics.revenue.log", evt.Nodes[0].DBTLogURI)
	assert.False(t, evt.Nodes[0].Unfixable)
}

// A node the classifier marked unfixable reaches the handler marked, whatever
// its reason; a node with no mark is fixable.
func TestParseHealTriggered_CarriesTheUnfixableMark(t *testing.T) {
	_, evt, err := ParseHealTriggered(healTriggeredMessage(t, "tenant-7", healTriggeredPayload()))
	require.NoError(t, err)
	require.Len(t, evt.Nodes, 2)
	assert.False(t, evt.Nodes[0].Unfixable, "analytics.revenue carries no unfixable mark")
	assert.True(t, evt.Nodes[1].Unfixable, "analytics.orders is marked unfixable")
	assert.Equal(t, "analytics.orders", evt.Nodes[1].NodeID)
	assert.Equal(t, "sig-2", evt.Nodes[1].ErrorSignature)

	p := healTriggeredPayload()
	p.Nodes[1].Unfixable = &events.Unfixable{Reason: "classifier_unhealable_parse", Detail: "parse kind no source change fixes"}
	_, evt, err = ParseHealTriggered(healTriggeredMessage(t, "tenant-7", p))
	require.NoError(t, err)
	assert.True(t, evt.Nodes[1].Unfixable, "either classifier reason marks the node")
}

// An entry its producer published without a tenant has nowhere to be
// written: it is dead-lettered, never written under no tenant.
func TestParseHealTriggered_NoTenantIsPermanent(t *testing.T) {
	withoutTenant := healTriggeredMessage(t, "", healTriggeredPayload())
	delete(withoutTenant.Values, "tenant_id")
	for name, msg := range map[string]goredis.XMessage{
		"empty tenant_id field": healTriggeredMessage(t, "", healTriggeredPayload()),
		"no tenant_id field":    withoutTenant,
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := ParseHealTriggered(msg)
			require.Error(t, err)
			assert.ErrorIs(t, err, events.ErrPermanent)
		})
	}
}

func TestParseHealTriggered_MissingPayloadField(t *testing.T) {
	msg := healTriggeredMessage(t, "default", healTriggeredPayload())
	delete(msg.Values, "payload")
	_, _, err := ParseHealTriggered(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParseHealTriggered_EmptyPayloadField(t *testing.T) {
	msg := healTriggeredMessage(t, "default", healTriggeredPayload())
	msg.Values["payload"] = ""
	_, _, err := ParseHealTriggered(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParseHealTriggered_BadJSON(t *testing.T) {
	msg := healTriggeredMessage(t, "default", healTriggeredPayload())
	msg.Values["payload"] = "not-json"
	_, _, err := ParseHealTriggered(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

// An entry of another schema_version is not a v3 trigger: it is dead-lettered
// rather than read as one.
func TestParseHealTriggered_UnknownSchemaVersionIsPermanent(t *testing.T) {
	msg := healTriggeredMessage(t, "default", healTriggeredPayload())
	msg.Values["schema_version"] = "99"
	_, _, err := ParseHealTriggered(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

// The entry's tenant reaches the failure precedents group's marker, with the entry's
// outbox entry id; an entry without a tenant never reaches the handler.
func TestHealTriggeredBinding_PassesTheTenant(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	u := &processedUnitOfWork{}
	binding := NewHealTriggeredBinding(handlers.NewHealTriggeredFailurePrecedentsHandler(u, nil, logger), logger)

	require.NoError(t, binding(context.Background(), healTriggeredMessage(t, "tenant-7", healTriggeredPayload())))
	require.Len(t, u.lookups, 1)
	assert.Equal(t, "tenant-7", u.lookups[0].TenantID)
	assert.Equal(t, streams.CatalogHealTriggeredFailurePrecedents, u.lookups[0].Group)
	require.NotNil(t, u.lookups[0].OutboxEntryID)
	assert.Equal(t, "6e1d0a4c-6b0e-4d55-8c55-3f7d1f2e9a10", u.lookups[0].OutboxEntryID.String())

	err := binding(context.Background(), healTriggeredMessage(t, "", healTriggeredPayload()))
	assert.ErrorIs(t, err, events.ErrPermanent)
	assert.Len(t, u.lookups, 1, "the handler never sees an entry without a tenant")
}
