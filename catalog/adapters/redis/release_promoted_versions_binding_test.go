package redis

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// processedUnitOfWork records every marker the handler looks up and answers
// that the message is processed, so the handler stops before it reads object
// storage or writes.
type processedUnitOfWork struct {
	lookups []uow.Marker
}

func (p *processedUnitOfWork) Processed(_ context.Context, m uow.Marker) (bool, error) {
	p.lookups = append(p.lookups, m)
	return true, nil
}

func (p *processedUnitOfWork) WritesPaused(context.Context, string) (bool, error) {
	panic("a processed message checks nothing")
}

func (p *processedUnitOfWork) Write(context.Context, uow.Marker, bool, func(context.Context, uow.Tx) error) error {
	panic("a processed message writes nothing")
}

// versionsBinding is the versions group's binding over a handler on u.
func versionsBinding(u uow.UnitOfWork) pkgredis.MessageHandler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewReleasePromotedVersionsBinding(handlers.NewReleasePromotedVersionsHandler(u, nil, nil, logger), logger)
}

// The versions group parses release.promoted:v2 with the topology group's
// parser: a promotion whose envelope names no tenant is dead-lettered before
// the handler sees it.
func TestParseReleasePromotedVersions_NoTenantIsPermanent(t *testing.T) {
	u := &processedUnitOfWork{}
	msg := releasePromotedV2MessageOf(t, "", events.ReleasePromoted{
		ReleaseID: "rel-7", PromotionSeq: 7, TopologyURI: "s3://b/k", TopologySHA256: "ab",
	})

	err := versionsBinding(u)(context.Background(), msg)
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrPermanent)
	assert.Empty(t, u.lookups, "the handler never sees a message without a tenant")
}

// The envelope's tenant reaches the versions group's marker, with the
// message's outbox entry id.
func TestReleasePromotedVersionsBinding_PassesTheTenant(t *testing.T) {
	u := &processedUnitOfWork{}
	msg := releasePromotedV2MessageOf(t, "tenant-7", events.ReleasePromoted{
		ReleaseID: "rel-7", PromotionSeq: 7, TopologyURI: "s3://b/k", TopologySHA256: "ab",
	})

	require.NoError(t, versionsBinding(u)(context.Background(), msg))
	require.Len(t, u.lookups, 1)
	assert.Equal(t, "tenant-7", u.lookups[0].TenantID)
	assert.Equal(t, streams.CatalogReleaseVersions, u.lookups[0].Group)
	require.NotNil(t, u.lookups[0].OutboxEntryID)
	assert.Equal(t, "6e1d0a4c-6b0e-4d55-8c55-3f7d1f2e9a10", u.lookups[0].OutboxEntryID.String())
}
