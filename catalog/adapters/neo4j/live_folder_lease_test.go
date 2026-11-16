//go:build integration

package neo4jinfra

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// liveNode returns the tenant's live-folder node's properties, or nil when
// the tenant has none.
func (f *uowFixture) liveNode() map[string]any {
	f.t.Helper()
	ctx := context.Background()
	s := f.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = s.Close(ctx) }()
	res, err := s.Run(ctx, `MATCH (m:Meta {tenant_id: $tenant_id, key: 'contracts-live'}) RETURN properties(m) AS p`,
		map[string]any{"tenant_id": f.tenant})
	require.NoError(f.t, err)
	recs, err := res.Collect(ctx)
	require.NoError(f.t, err)
	require.LessOrEqual(f.t, len(recs), 1, "one live-folder node per tenant")
	if len(recs) == 0 {
		return nil
	}
	p, _ := recs[0].Get("p")
	return p.(map[string]any)
}

// recordLiveFolder runs transaction B's live-folder write in a final write of
// messageID.
func (f *uowFixture) recordLiveFolder(messageID string, seq int64, settings string) error {
	return NewUnitOfWork(f.client, quietLogger()).Write(context.Background(), f.marker(messageID, nil), true,
		func(ctx context.Context, tx uow.Tx) error {
			return tx.Contracts().RecordLiveFolder(ctx, f.tenant, seq, settings)
		})
}

// The live-folder lease lives on its own node: an export holding the tenant's
// lease does not stop a rewrite from taking the live-folder lease, and a
// rewrite holding it pauses no graph write and stops no export.
func TestLiveFolderLease_IsNotTheExportLease(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	exportLease := NewExportLease(f.client, quietLogger())
	live := NewLiveFolderLease(f.client, quietLogger())

	require.NoError(t, exportLease.Acquire(ctx, f.tenant, ports.LeaseExport, "exporter-1", time.Minute))
	acquired, err := live.Acquire(ctx, f.tenant, "rewrite-1", time.Minute)
	require.NoError(t, err)
	assert.True(t, acquired, "an export does not hold the live folder")
	require.NoError(t, exportLease.Release(ctx, f.tenant, "exporter-1"))

	require.NoError(t, NewUnitOfWork(f.client, quietLogger()).Write(ctx, f.marker("1762077700000-0", nil), true,
		func(ctx context.Context, tx uow.Tx) error { return f.writeProbe(ctx, tx, "during-rewrite") }),
		"a graph write goes ahead while a rewrite holds the live folder")
	require.NoError(t, exportLease.Acquire(ctx, f.tenant, ports.LeaseExport, "exporter-2", time.Minute),
		"an export starts while a rewrite holds the live folder")

	node := f.liveNode()
	assert.Equal(t, "rewrite-1", node["holder"])
	assert.NotContains(t, node, "_lock")
	assert.Equal(t, "exporter-2", f.leaseNode()["holder"])
}

func TestLiveFolderLease_ASecondHolderIsRefusedUntilRelease(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	live := NewLiveFolderLease(f.client, quietLogger())

	acquired, err := live.Acquire(ctx, f.tenant, "rewrite-1", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
	acquired, err = live.Acquire(ctx, f.tenant, "rewrite-2", time.Minute)
	require.NoError(t, err)
	assert.False(t, acquired, "another live lease is held")
	assert.NotContains(t, f.liveNode(), "_lock", "a refused Acquire leaves no lock property")

	require.NoError(t, live.Release(ctx, f.tenant, "rewrite-2"), "a release by another holder is no error")
	assert.Equal(t, "rewrite-1", f.liveNode()["holder"], "and leaves the lease as it is")
	require.NoError(t, live.Release(ctx, f.tenant, "rewrite-1"))
	acquired, err = live.Acquire(ctx, f.tenant, "rewrite-2", time.Minute)
	require.NoError(t, err)
	assert.True(t, acquired)
}

func TestLiveFolderLease_RenewKeepsTheLeaseAndALapsedOneIsLost(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	live := NewLiveFolderLease(f.client, quietLogger())

	acquired, err := live.Acquire(ctx, f.tenant, "rewrite-1", 200*time.Millisecond)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, live.Renew(ctx, f.tenant, "rewrite-1", time.Minute))
	assert.ErrorIs(t, live.Renew(ctx, f.tenant, "rewrite-2", time.Minute), ports.ErrLeaseLost)

	require.NoError(t, live.Release(ctx, f.tenant, "rewrite-1"))
	acquired, err = live.Acquire(ctx, f.tenant, "rewrite-3", time.Millisecond)
	require.NoError(t, err)
	require.True(t, acquired)
	time.Sleep(50 * time.Millisecond)
	assert.ErrorIs(t, live.Renew(ctx, f.tenant, "rewrite-3", time.Minute), ports.ErrLeaseLost, "a lapsed lease is lost")
	acquired, err = live.Acquire(ctx, f.tenant, "rewrite-4", time.Minute)
	require.NoError(t, err)
	assert.True(t, acquired, "a lapsed lease is taken over")
}

// Transaction B stores the folder's watermark, which never goes back, and the
// settings fingerprint; a rewrite of the whole folder stores only the
// fingerprint.
func TestLiveFolderState_ReadsWhatTransactionBAndRecordSettingsStored(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	state := NewLiveFolderLease(f.client, quietLogger())

	seq, settings, err := state.Read(ctx, f.tenant)
	require.NoError(t, err)
	assert.Equal(t, int64(0), seq)
	assert.Empty(t, settings, "a folder never written has no state")

	require.NoError(t, f.recordLiveFolder("1762077800000-0", 52, "sha-a"))
	seq, settings, err = state.Read(ctx, f.tenant)
	require.NoError(t, err)
	assert.Equal(t, int64(52), seq)
	assert.Equal(t, "sha-a", settings)

	require.NoError(t, f.recordLiveFolder("1762077800001-0", 40, "sha-b"))
	seq, settings, err = state.Read(ctx, f.tenant)
	require.NoError(t, err)
	assert.Equal(t, int64(52), seq, "an older promotion never lowers the watermark")
	assert.Equal(t, "sha-b", settings)

	require.NoError(t, state.RecordSettings(ctx, f.tenant, "sha-c"))
	seq, settings, err = state.Read(ctx, f.tenant)
	require.NoError(t, err)
	assert.Equal(t, int64(52), seq)
	assert.Equal(t, "sha-c", settings)
	assert.NotContains(t, f.liveNode(), "_lock")
	f.assertGateUnlocked()
}

// RecordSettings writes the graph, so it backs off like every write while an
// export holds the tenant's lease.
func TestLiveFolderState_RecordSettingsBacksOffWhileAnExportHoldsTheLease(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	exportLease := NewExportLease(f.client, quietLogger())
	state := NewLiveFolderLease(f.client, quietLogger())

	require.NoError(t, exportLease.Acquire(ctx, f.tenant, ports.LeaseExport, "exporter-1", time.Minute))

	assert.ErrorIs(t, state.RecordSettings(ctx, f.tenant, "sha-c"), uow.ErrWritesPaused)
	assert.Nil(t, f.liveNode(), "nothing was written")
}
