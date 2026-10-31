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

const prOpenedPayload = `{
	"proposal_id": "prop-1",
	"release_id": "rel-1",
	"node_id": "analytics.revenue",
	"pr_url": "https://github.com/org/repo/pull/42",
	"pr_number": 42,
	"opened_by": "agent-remediation",
	"opened_at": "2026-08-12T09:05:00Z",
	"service": "core"
}`

func TestParsePROpened_HappyPath(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"payload": prOpenedPayload, "tenant_id": "tenant-7",
	}}

	tenantID, evt, err := ParsePROpened(msg)
	require.NoError(t, err)
	assert.Equal(t, "tenant-7", tenantID, "the tenant is the entry's tenant_id field")
	assert.Equal(t, "prop-1", evt.ProposalID)
	assert.Equal(t, "rel-1", evt.ReleaseID)
	assert.Equal(t, "analytics.revenue", evt.NodeID)
	assert.Equal(t, "https://github.com/org/repo/pull/42", evt.PrURL)
	assert.Equal(t, 42, evt.PrNumber)
	assert.Equal(t, "agent-remediation", evt.OpenedBy)
	assert.Equal(t, "2026-08-12T09:05:00Z", evt.OpenedAt)
	assert.Equal(t, "core", evt.Service)
}

// An entry its producer published without a tenant has nowhere to be
// written: it is dead-lettered, never written under no tenant.
func TestParsePROpened_NoTenantIsPermanent(t *testing.T) {
	for name, values := range map[string]map[string]interface{}{
		"no tenant_id field":    {"payload": prOpenedPayload},
		"empty tenant_id field": {"payload": prOpenedPayload, "tenant_id": ""},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := ParsePROpened(goredis.XMessage{ID: "1-0", Values: values})
			require.Error(t, err)
			assert.ErrorIs(t, err, events.ErrPermanent)
		})
	}
}

func TestParsePROpened_MissingPayloadField(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"tenant_id": "default"}}
	_, _, err := ParsePROpened(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParsePROpened_EmptyPayloadField(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": "", "tenant_id": "default"}}
	_, _, err := ParsePROpened(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

func TestParsePROpened_BadJSON(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": "not-json", "tenant_id": "default"}}
	_, _, err := ParsePROpened(msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
}

// The entry's tenant reaches the proposals group's marker, with the entry's
// outbox entry id; an entry without a tenant never reaches the handler.
func TestPrOpenedBinding_PassesTheTenant(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	u := &processedUnitOfWork{}
	binding := NewPrOpenedBinding(handlers.NewPrOpenedProposalsHandler(u, logger), logger)

	require.NoError(t, binding(context.Background(), goredis.XMessage{ID: "1-0", Values: map[string]interface{}{
		"payload": prOpenedPayload, "tenant_id": "tenant-7",
		"outbox_entry_id": "6e1d0a4c-6b0e-4d55-8c55-3f7d1f2e9a10",
	}}))
	require.Len(t, u.lookups, 1)
	assert.Equal(t, "tenant-7", u.lookups[0].TenantID)
	assert.Equal(t, streams.CatalogRemediationPrOpenedProposals, u.lookups[0].Group)
	require.NotNil(t, u.lookups[0].OutboxEntryID)
	assert.Equal(t, "6e1d0a4c-6b0e-4d55-8c55-3f7d1f2e9a10", u.lookups[0].OutboxEntryID.String())

	err := binding(context.Background(), goredis.XMessage{ID: "2-0", Values: map[string]interface{}{
		"payload": prOpenedPayload,
	}})
	assert.ErrorIs(t, err, events.ErrPermanent)
	assert.Len(t, u.lookups, 1, "the handler never sees an entry without a tenant")
}
