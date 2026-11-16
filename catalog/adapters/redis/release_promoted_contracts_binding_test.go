package redis

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// contractsBinding is the contracts group's binding over a handler on u.
func contractsBinding(u uow.UnitOfWork) pkgredis.MessageHandler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewReleasePromotedContractsBinding(handlers.NewReleasePromotedContractsHandler(u, nil, nil, nil, time.Minute, logger), logger)
}

// The contracts group parses release.promoted:v2 with the topology group's
// parser: a promotion whose envelope names no tenant is dead-lettered before
// the handler sees it.
func TestParseReleasePromotedContracts_NoTenantIsPermanent(t *testing.T) {
	u := &processedUnitOfWork{}
	msg := releasePromotedV2MessageOf(t, "", events.ReleasePromoted{
		ReleaseID: "rel-7", PromotionSeq: 7, TopologyURI: "s3://b/k", TopologySHA256: "ab",
	})

	err := contractsBinding(u)(context.Background(), msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
	assert.Empty(t, u.lookups, "the handler never sees a message without a tenant")
}

// The envelope's tenant reaches the contracts group's marker, with the
// message's outbox entry id.
func TestReleasePromotedContractsBinding_PassesTheTenant(t *testing.T) {
	u := &processedUnitOfWork{}
	msg := releasePromotedV2MessageOf(t, "tenant-7", events.ReleasePromoted{
		ReleaseID: "rel-7", PromotionSeq: 7, TopologyURI: "s3://b/k", TopologySHA256: "ab",
	})

	require.NoError(t, contractsBinding(u)(context.Background(), msg))
	require.Len(t, u.lookups, 1)
	assert.Equal(t, "tenant-7", u.lookups[0].TenantID)
	assert.Equal(t, streams.CatalogReleaseContracts, u.lookups[0].Group)
	require.NotNil(t, u.lookups[0].OutboxEntryID)
	assert.Equal(t, "6e1d0a4c-6b0e-4d55-8c55-3f7d1f2e9a10", u.lookups[0].OutboxEntryID.String())
}
