package e2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedReconcileNodeKey is seed_table_1's key in state's node_materializations:
// "<schema>.<table>", lowercased.
const seedReconcileNodeKey = seedSchemaName + ".seed_table_1"

// TestSeedReconcile_LoadsAChangedSeedOnce announces the e2e DAG with
// seed_table_1 carrying a content hash state has never loaded. state plans one
// promote-seed run for it in the transaction that applies the announcement,
// orchestrator projects the run once its graph has applied the same
// announcement, the seed loads with service-1's image, and state records the
// hash it loaded. Announcing the same content again under a new release loads
// nothing. The default e2e DAG, whose seeds carry no content hash, is restored
// at the end.
func TestSeedReconcile_LoadsAChangedSeedOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	clients := setupClients(t, ctx)
	defer clients.close(ctx)
	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	// state records the seeds its first promotion applies as already loaded.
	// Applying the default DAG first makes the announcement below an ordinary
	// one, even on a fresh stack.
	base := seedTopology(t, ctx, clients)
	waitForStateLiveRelease(t, ctx, clients, base)
	defer restoreDefaultTopology(t, ctx, clients)

	nodes, schedules := e2eTopology(t, ctx, clients)
	hash := "sha256:e2e-seed-reconcile-" + uuid.NewString()
	var seedImageTag string
	for i := range nodes {
		if nodes[i].TableName == "seed_table_1" {
			nodes[i].ContentHash = hash
			seedImageTag = nodes[i].ImageTag
		}
	}
	require.NotEmpty(t, seedImageTag, "seed_table_1 is pinned to its service's image")

	first := announceTopology(t, ctx, "e2e-seedrec-"+uuid.NewString()[:8], nodes)
	waitForAnnouncedTopology(t, ctx, clients, first, schedules)
	waitForStateLiveRelease(t, ctx, clients, first)

	runs := promoteSeedRuns(t, ctx, clients, first.ReleaseID)
	require.Len(t, runs, 1, "one promote-seed run, for the one seed whose content changed")
	waitForAllTasksSucceeded(t, ctx, clients, runs[0], []string{"seed_table_1"})
	verifySchedulerSucceeded(t, ctx, clients, runs[0])
	// A reconcile pass that ran while the first Job was in flight would have
	// dispatched the seed again; the run list is read again now that it is done.
	require.Equal(t, runs, promoteSeedRuns(t, ctx, clients, first.ReleaseID), "the seed is dispatched once while its run is in flight")

	loaded := readLoadedSeed(t, ctx, clients)
	assert.Equal(t, hash, loaded.ContentHash)
	assert.Equal(t, seedImageTag, loaded.ImageTag, "the seed is built with the image its node is pinned to")
	assert.Equal(t, first.ReleaseID, loaded.ReleaseID)
	assert.Equal(t, uuid.NullUUID{UUID: runs[0], Valid: true}, loaded.RunID)
	require.NotNil(t, loaded.Source)
	assert.Equal(t, "build", *loaded.Source)

	second := announceTopology(t, ctx, "e2e-seedrec-"+uuid.NewString()[:8], nodes)
	waitForAnnouncedTopology(t, ctx, clients, second, schedules)
	waitForStateLiveRelease(t, ctx, clients, second)

	assert.Empty(t, promoteSeedRuns(t, ctx, clients, second.ReleaseID), "the same content announced again loads nothing")
	assert.Equal(t, loaded, readLoadedSeed(t, ctx, clients), "the recorded version is unchanged")
	assert.Equal(t, runs, promoteSeedRuns(t, ctx, clients, first.ReleaseID), "no later reconcile pass dispatched the first release's seed again")
}

// waitForStateLiveRelease waits until state's live release for the default
// tenant is res's promotion or a newer one. state moves it in the transaction
// that plans the release's seed loads, so the plan is visible once it returns.
func waitForStateLiveRelease(t *testing.T, ctx context.Context, clients *testClients, res announceResult) {
	t.Helper()
	pollUntil(t, ctx, 60*time.Second, time.Second, func() (bool, error) {
		var seq int64
		err := clients.stateDB.QueryRowContext(ctx,
			`SELECT promotion_seq FROM live_release WHERE tenant_id = $1`, events.DefaultTenantID).Scan(&seq)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return seq >= res.PromotionSeq, nil
	}, fmt.Sprintf("timeout waiting for state's live release to reach %s (promotion seq %d)", res.ReleaseID, res.PromotionSeq))
}

// promoteSeedRuns lists the promote-seed runs state created for releaseID.
func promoteSeedRuns(t *testing.T, ctx context.Context, clients *testClients, releaseID string) []uuid.UUID {
	t.Helper()
	var ids []uuid.UUID
	require.NoError(t, clients.stateDB.SelectContext(ctx, &ids,
		`SELECT schedule_id FROM scheduler_tracker WHERE kind = 'promote_seed' AND release_id = $1 ORDER BY created_at`, releaseID))
	return ids
}

// loadedSeed is seed_table_1's row in state's node_materializations.
type loadedSeed struct {
	ContentHash string        `db:"content_hash"`
	ImageTag    string        `db:"image_tag"`
	ReleaseID   string        `db:"release_id"`
	RunID       uuid.NullUUID `db:"run_id"`
	Source      *string       `db:"source"`
}

func readLoadedSeed(t *testing.T, ctx context.Context, clients *testClients) loadedSeed {
	t.Helper()
	var row loadedSeed
	require.NoError(t, clients.stateDB.GetContext(ctx, &row, `
		SELECT content_hash, image_tag, release_id, run_id, source FROM node_materializations
		WHERE tenant_id = $1 AND node_key = $2`, events.DefaultTenantID, seedReconcileNodeKey))
	return row
}

// restoreDefaultTopology re-announces the default e2e DAG, whose seeds carry no
// content hash, and deletes the version this test recorded for seed_table_1,
// so later tests start from the topology they expect.
func restoreDefaultTopology(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()
	restored := seedTopology(t, ctx, clients)
	waitForStateLiveRelease(t, ctx, clients, restored)
	_, err := clients.stateDB.ExecContext(ctx,
		`DELETE FROM node_materializations WHERE tenant_id = $1 AND node_key = $2`, events.DefaultTenantID, seedReconcileNodeKey)
	require.NoError(t, err)
}
