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

const prClosedPayload = `{
	"proposal_id": "prop-1",
	"release_id": "rel-1",
	"node_id": "analytics.revenue",
	"resolved_node_ids": ["analytics.revenue", "analytics.margin"],
	"service": "core",
	"pr_url": "https://github.com/org/repo/pull/42",
	"pr_number": 42,
	"outcome": "merged",
	"closed_at": "2026-08-14T11:30:00Z",
	"edits": [
		{"path": "models/revenue.sql", "target_node_id": "analytics.revenue", "amended": true, "diff": "@@ -1 +1 @@"},
		{"path": "models/margin.sql", "target_node_id": "analytics.margin", "amended": false}
	]
}`

func TestParsePRClosed_HappyPath(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"payload": prClosedPayload, "tenant_id": "tenant-7",
	}}

	tenantID, evt, err := ParsePRClosed(msg)
	require.NoError(t, err)
	assert.Equal(t, "tenant-7", tenantID, "the tenant is the entry's tenant_id field")
	assert.Equal(t, "prop-1", evt.ProposalID)
	assert.Equal(t, "rel-1", evt.ReleaseID)
	assert.Equal(t, "analytics.revenue", evt.NodeID)
	assert.Equal(t, []string{"analytics.revenue", "analytics.margin"}, evt.ResolvedNodeIDs)
	assert.Equal(t, "core", evt.Service)
	assert.Equal(t, "https://github.com/org/repo/pull/42", evt.PrURL)
	assert.Equal(t, 42, evt.PrNumber)
	assert.Equal(t, "merged", evt.Outcome)
	assert.Equal(t, "2026-08-14T11:30:00Z", evt.ClosedAt)
	require.Len(t, evt.Edits, 2)
	assert.Equal(t, "models/revenue.sql", evt.Edits[0].Path)
	assert.Equal(t, "analytics.revenue", evt.Edits[0].TargetNodeID)
	assert.True(t, evt.Edits[0].Amended)
	assert.Equal(t, "@@ -1 +1 @@", evt.Edits[0].Diff)
	assert.False(t, evt.Edits[1].Amended)
}

// An entry its producer published without a tenant has nowhere to be
// written: it is dead-lettered, never written under no tenant.
func TestParsePRClosed_NoTenantIsPermanent(t *testing.T) {
	for name, values := range map[string]map[string]interface{}{
		"no tenant_id field":    {"payload": prClosedPayload},
		"empty tenant_id field": {"payload": prClosedPayload, "tenant_id": ""},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := ParsePRClosed(goredis.XMessage{ID: "1-0", Values: values})
			require.Error(t, err)
			assert.ErrorIs(t, err, events.ErrPermanent)
		})
	}
}

func TestParsePRClosed_MissingPayloadField(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"tenant_id": "default"}}
	_, _, err := ParsePRClosed(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParsePRClosed_EmptyPayloadField(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": "", "tenant_id": "default"}}
	_, _, err := ParsePRClosed(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParsePRClosed_BadJSON(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": "not-json", "tenant_id": "default"}}
	_, _, err := ParsePRClosed(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

// The entry's tenant reaches the provenance group's marker, with the entry's
// outbox entry id; an entry without a tenant never reaches the handler.
func TestPrClosedBinding_PassesTheTenant(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	u := &processedUnitOfWork{}
	binding := NewPrClosedBinding(handlers.NewPrClosedProvenanceHandler(u, logger), logger)

	require.NoError(t, binding(context.Background(), goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"payload": prClosedPayload, "tenant_id": "tenant-7",
		"outbox_entry_id": "6e1d0a4c-6b0e-4d55-8c55-3f7d1f2e9a10",
	}}))
	require.Len(t, u.lookups, 1)
	assert.Equal(t, "tenant-7", u.lookups[0].TenantID)
	assert.Equal(t, streams.CatalogRemediationPrClosedProvenance, u.lookups[0].Group)
	require.NotNil(t, u.lookups[0].OutboxEntryID)
	assert.Equal(t, "6e1d0a4c-6b0e-4d55-8c55-3f7d1f2e9a10", u.lookups[0].OutboxEntryID.String())

	err := binding(context.Background(), goredis.XMessage{ID: "2-0", Values: map[string]interface{}{
		"payload": prClosedPayload,
	}})
	assert.ErrorIs(t, err, events.ErrPermanent)
	assert.Len(t, u.lookups, 1, "the handler never sees an entry without a tenant")
}
