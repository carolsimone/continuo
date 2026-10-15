package e2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTopologyVersioning_MidRunIsolation validates that a topology announced
// between Run 1's creation and its completion does NOT affect the in-flight run:
// its promotion_seq must remain pinned to the seq live when it was created.
// Run 2 (triggered after the announcement) must pick up the new seq.
func TestTopologyVersioning_MidRunIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	const scheduleName = "seed"
	tables := []string{"seed_table_1", "seed_table_2", "seed_table_3"}

	defer cleanupTestData(t, ctx, clients, scheduleName)

	// Step 1: Verify all services and Kubernetes are reachable.
	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	// Step 2: Clean any leftover data from previous runs.
	cleanupTestData(t, ctx, clients, scheduleName)

	// Step 3: Announce the topology and wait for the orchestrator to apply it,
	// establishing promotion seq G1.
	t.Log("=== Step 3: seedTopology — establishing promotion seq G1 ===")
	g1 := seedTopology(t, ctx, clients).PromotionSeq

	// Step 4: The live pointer carries G1.
	require.Equal(t, g1, readLivePointer(ctx, clients).promotionSeq, "the live pointer must carry G1")
	t.Logf("G1 = %d", g1)
	waitForStateLiveSeq(t, ctx, clients, g1)

	// Step 5: Trigger Run 1 via the ui HTTP endpoint.
	t.Log("=== Step 5: triggerScheduleHTTP — starting Run 1 (S1) ===")
	scheduleID1Str := triggerScheduleHTTP(t, clients.uiBase, scheduleName)
	t.Logf("Run 1 created: schedule_id=%s", scheduleID1Str)
	s1, err := uuid.Parse(scheduleID1Str)
	require.NoError(t, err, "schedule_id for Run 1 is not a valid UUID")

	// Step 7: S1 carries G1.
	t.Log("=== Step 7: verifying S1 promotion_seq in state == G1 ===")
	s1Seq := readRunPromotionSeq(t, ctx, clients, s1)
	assert.Equal(t, g1, s1Seq, "Run 1's promotion_seq must equal G1=%d, got %d", g1, s1Seq)

	// Step 8: Announce the topology again while S1 is still in flight (fresh
	// release_id, same DAG): the announcement takes G2 > G1.
	t.Log("=== Step 8: seedTopology mid-run (fresh release_id, same DAG) — announcing G2 ===")
	g2 := seedTopology(t, ctx, clients).PromotionSeq

	// Step 9: G2 is later than G1, and the live pointer carries it.
	t.Logf("G2 = %d", g2)
	require.Greater(t, g2, g1, "every announcement takes a larger promotion seq: G1=%d G2=%d", g1, g2)
	require.Equal(t, g2, readLivePointer(ctx, clients).promotionSeq, "the live pointer must carry G2")
	waitForStateLiveSeq(t, ctx, clients, g2)

	// Step 10: S1 still carries G1. This is the core isolation invariant: an
	// in-flight run is not affected by a topology announced after its
	// creation.
	t.Log("=== Step 10: verifying S1 promotion_seq in state still == G1 ===")
	assert.Equal(t, g1, readRunPromotionSeq(t, ctx, clients, s1),
		"Run 1 must remain pinned to G1=%d after the mid-run announcement", g1)

	// Step 11: Wait for S1 to complete successfully.
	t.Log("=== Step 11: waiting for Run 1 to complete ===")
	waitForAllTasksSucceeded(t, ctx, clients, s1, tables)
	verifySchedulerSucceeded(t, ctx, clients, s1)
	t.Log("Run 1 completed successfully")

	// Step 12: Clean up Run 1 data before triggering Run 2.
	t.Log("=== Step 12: cleaning up Run 1 data ===")
	cleanupTestData(t, ctx, clients, scheduleName)

	// Step 13: Trigger Run 2, which must pick up G2.
	t.Log("=== Step 13: triggerScheduleHTTP — starting Run 2 (S2) ===")
	scheduleID2Str := triggerScheduleHTTP(t, clients.uiBase, scheduleName)
	t.Logf("Run 2 created: schedule_id=%s", scheduleID2Str)
	s2, err := uuid.Parse(scheduleID2Str)
	require.NoError(t, err, "schedule_id for Run 2 is not a valid UUID")

	// Step 15: S2 carries G2.
	t.Log("=== Step 15: verifying S2 promotion_seq in state == G2 ===")
	assert.Equal(t, g2, readRunPromotionSeq(t, ctx, clients, s2), "Run 2's promotion_seq must equal G2=%d", g2)

	// Step 16: Wait for S2 to complete successfully.
	t.Log("=== Step 16: waiting for Run 2 to complete ===")
	waitForAllTasksSucceeded(t, ctx, clients, s2, tables)
	verifySchedulerSucceeded(t, ctx, clients, s2)
	t.Log("TestTopologyVersioning_MidRunIsolation PASSED")
}

// waitForStateLiveSeq polls until state's live release reaches promotion seq
// seq. A promoted release carries the seq, so a run triggered after this
// returns is stamped with it.
func waitForStateLiveSeq(t *testing.T, ctx context.Context, clients *testClients, seq int64) {
	t.Helper()
	pollUntil(t, ctx, 2*time.Minute, 500*time.Millisecond, func() (bool, error) {
		var live int64
		err := clients.stateDB.GetContext(ctx, &live, `SELECT promotion_seq FROM live_release WHERE tenant_id = 'default'`)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return live >= seq, err
	}, fmt.Sprintf("Timeout waiting for state's live release to reach promotion seq %d", seq))
}

// readRunPromotionSeq returns the promotion seq state stamped on the run that
// scheduleID created.
func readRunPromotionSeq(t *testing.T, ctx context.Context, clients *testClients, scheduleID uuid.UUID) int64 {
	t.Helper()
	var seq sql.NullInt64
	require.NoError(t, clients.stateDB.GetContext(ctx, &seq,
		`SELECT promotion_seq FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID),
		"failed to read promotion_seq for run %s", scheduleID)
	require.True(t, seq.Valid, "run %s names no release", scheduleID)
	return seq.Int64
}
