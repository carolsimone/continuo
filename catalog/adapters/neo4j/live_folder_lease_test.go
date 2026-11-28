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
func (f *uowFixture) recordLiveFolder(messageID string, seq int64, release, settings string) error {
	return NewUnitOfWork(f.client, quietLogger()).Write(context.Background(), f.marker(messageID, nil), true,
		func(ctx context.Context, tx uow.Tx) error {
			return tx.Contracts().RecordLiveFolder(ctx, f.tenant, seq, release, settings)
		})
}

// The live-folder lease lives on its own node, and taking it is a graph write:
// while a backup or a restore holds the tenant's lease a rewrite cannot take
// it (and creates no live-folder node a restore's import would meet), while a
// rewrite holding it pauses no graph write and stops no backup.
func TestLiveFolderLease_IsNotTheBackupLease(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	backupLease := NewBackupLease(f.client, quietLogger())
	live := NewLiveFolderLease(f.client, quietLogger())

	for _, kind := range []ports.LeaseKind{ports.LeaseBackup, ports.LeaseRestore} {
		require.NoError(t, backupLease.Acquire(ctx, f.tenant, kind, "taker-1", time.Minute))
		acquired, err := live.Acquire(ctx, f.tenant, "rewrite-1", time.Minute)
		require.NoError(t, err)
		assert.False(t, acquired, "an %s holds the tenant's writes, the live-folder lease among them", kind)
		assert.Nil(t, f.liveNode(), "a refused take creates no live-folder node")
		require.NoError(t, backupLease.Release(ctx, f.tenant, "taker-1"))
	}

	acquired, err := live.Acquire(ctx, f.tenant, "rewrite-1", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired, "the lease is taken once the backup lease is released")
	assert.NotContains(t, f.leaseNode(), "_lock", "the backup gate's lock never persists")

	require.NoError(t, NewUnitOfWork(f.client, quietLogger()).Write(ctx, f.marker("1762077700000-0", nil), true,
		func(ctx context.Context, tx uow.Tx) error { return f.writeProbe(ctx, tx, "during-rewrite") }),
		"a graph write goes ahead while a rewrite holds the live folder")
	require.NoError(t, backupLease.Acquire(ctx, f.tenant, ports.LeaseBackup, "taker-2", time.Minute),
		"a backup starts while a rewrite holds the live folder")

	node := f.liveNode()
	assert.Equal(t, "rewrite-1", node["holder"])
	assert.NotContains(t, node, "_lock")
	assert.Equal(t, "taker-2", f.leaseNode()["holder"])
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

// Transaction B stores the folder's watermark, which never goes back and
// takes its release with it, and the settings fingerprint unless it is empty;
// a rewrite of the whole folder stores only the fingerprint.
func TestLiveFolderState_ReadsWhatTransactionBAndRecordSettingsStored(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	state := NewLiveFolderLease(f.client, quietLogger())
	read := func() ports.LiveFolderMark {
		t.Helper()
		mark, err := state.Read(ctx, f.tenant)
		require.NoError(t, err)
		return mark
	}

	assert.Equal(t, ports.LiveFolderMark{}, read(), "a folder never written has no state")

	require.NoError(t, f.recordLiveFolder("1762077800000-0", 52, "rel-52", "sha-a"))
	assert.Equal(t, ports.LiveFolderMark{PromotionSeq: 52, ReleaseID: "rel-52", SettingsSHA256: "sha-a"}, read())

	require.NoError(t, f.recordLiveFolder("1762077800001-0", 40, "rel-40", "sha-b"))
	assert.Equal(t, ports.LiveFolderMark{PromotionSeq: 52, ReleaseID: "rel-52", SettingsSHA256: "sha-b"}, read(),
		"an older promotion lowers neither the watermark nor its release")

	require.NoError(t, f.recordLiveFolder("1762077800002-0", 53, "rel-53", ""))
	assert.Equal(t, ports.LiveFolderMark{PromotionSeq: 53, ReleaseID: "rel-53", SettingsSHA256: "sha-b"}, read(),
		"a promotion that wrote no file leaves the fingerprint as it is")

	require.NoError(t, state.RecordSettings(ctx, f.tenant, "sha-c"))
	assert.Equal(t, ports.LiveFolderMark{PromotionSeq: 53, ReleaseID: "rel-53", SettingsSHA256: "sha-c"}, read())
	assert.NotContains(t, f.liveNode(), "_lock")
	f.assertGateUnlocked()
}

// RecordSettings writes the graph, so it backs off like every write while a
// backup holds the tenant's lease.
func TestLiveFolderState_RecordSettingsBacksOffWhileAnBackupHoldsTheLease(t *testing.T) {
	ctx := context.Background()
	f := newUoWFixture(t)
	backupLease := NewBackupLease(f.client, quietLogger())
	state := NewLiveFolderLease(f.client, quietLogger())

	require.NoError(t, backupLease.Acquire(ctx, f.tenant, ports.LeaseBackup, "taker-1", time.Minute))

	assert.ErrorIs(t, state.RecordSettings(ctx, f.tenant, "sha-c"), uow.ErrWritesPaused)
	assert.Nil(t, f.liveNode(), "nothing was written")
}
