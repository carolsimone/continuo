package redis

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const remediationRequestedPayload = `{
	"event_id": "evt-1",
	"source": "validation",
	"release_id": "rel-1",
	"code_bundle_uri": "s3://b/code-bundles/rel-1/bundle.json",
	"classified_at": "2026-08-12T09:00:00Z",
	"nodes": [{
		"node_id": "analytics.revenue",
		"category": "sql_syntax_error",
		"error_signature": "sig-1",
		"reason": "column \"foo\" does not exist",
		"error_excerpt": "ERROR: column \"foo\" does not exist",
		"dbt_log_uri": "s3://b/logs/rel-1/analytics.revenue.log"
	}]
}`

func TestParseRemediationRequested_HappyPath(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"payload": remediationRequestedPayload, "tenant_id": "tenant-7",
	}}

	tenantID, evt, err := ParseRemediationRequested(msg)
	require.NoError(t, err)
	assert.Equal(t, "tenant-7", tenantID, "the tenant is the entry's tenant_id field")
	assert.Equal(t, "evt-1", evt.EventID)
	assert.Equal(t, "validation", evt.Source)
	assert.Equal(t, "rel-1", evt.ReleaseID)
	assert.Equal(t, "s3://b/code-bundles/rel-1/bundle.json", evt.CodeBundleURI)
	assert.Equal(t, "2026-08-12T09:00:00Z", evt.ClassifiedAt)
	require.Len(t, evt.Nodes, 1)
	assert.Equal(t, "analytics.revenue", evt.Nodes[0].NodeID)
	assert.Equal(t, "sql_syntax_error", evt.Nodes[0].Category)
	assert.Equal(t, "sig-1", evt.Nodes[0].ErrorSignature)
	assert.Equal(t, `column "foo" does not exist`, evt.Nodes[0].Reason)
	assert.Equal(t, `ERROR: column "foo" does not exist`, evt.Nodes[0].ErrorExcerpt)
	assert.Equal(t, "s3://b/logs/rel-1/analytics.revenue.log", evt.Nodes[0].DBTLogURI)
}

// An entry its producer published without a tenant has nowhere to be
// written: it is dead-lettered, never written under no tenant.
func TestParseRemediationRequested_NoTenantIsPermanent(t *testing.T) {
	for name, values := range map[string]map[string]interface{}{
		"no tenant_id field":    {"payload": remediationRequestedPayload},
		"empty tenant_id field": {"payload": remediationRequestedPayload, "tenant_id": ""},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := ParseRemediationRequested(goredis.XMessage{ID: "1-0", Values: values})
			require.Error(t, err)
			assert.ErrorIs(t, err, events.ErrPermanent)
		})
	}
}

func TestParseRemediationRequested_MissingPayloadField(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"tenant_id": "default"}}
	_, _, err := ParseRemediationRequested(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParseRemediationRequested_EmptyPayloadField(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": "", "tenant_id": "default"}}
	_, _, err := ParseRemediationRequested(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParseRemediationRequested_BadJSON(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": "not-json", "tenant_id": "default"}}
	_, _, err := ParseRemediationRequested(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

// The entry's tenant reaches the rejections group's marker, with the entry's
// outbox entry id; an entry without a tenant never reaches the handler.
func TestRemediationRequestedBinding_PassesTheTenant(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	u := &processedUnitOfWork{}
	binding := NewRemediationRequestedBinding(handlers.NewRemediationRequestedRejectionsHandler(u, nil, logger), logger)

	require.NoError(t, binding(context.Background(), goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"payload": remediationRequestedPayload, "tenant_id": "tenant-7",
		"outbox_entry_id": "6e1d0a4c-6b0e-4d55-8c55-3f7d1f2e9a10",
	}}))
	require.Len(t, u.lookups, 1)
	assert.Equal(t, "tenant-7", u.lookups[0].TenantID)
	assert.Equal(t, streams.CatalogRemediationRequestedRejections, u.lookups[0].Group)
	require.NotNil(t, u.lookups[0].OutboxEntryID)
	assert.Equal(t, "6e1d0a4c-6b0e-4d55-8c55-3f7d1f2e9a10", u.lookups[0].OutboxEntryID.String())

	err := binding(context.Background(), goredis.XMessage{ID: "2-0", Values: map[string]interface{}{
		"payload": remediationRequestedPayload,
	}})
	assert.ErrorIs(t, err, events.ErrPermanent)
	assert.Len(t, u.lookups, 1, "the handler never sees an entry without a tenant")
}
