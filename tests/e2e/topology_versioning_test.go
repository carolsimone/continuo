package e2e

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTopologyVersioning_MidRunIsolation validates that a topology announced
// between Run 1's SnapshotGraph and its completion does NOT affect the in-flight
// run: its promotion_seq must remain pinned to the seq captured at SnapshotGraph
// time. Run 2 (triggered after the announcement) must pick up the new seq.
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

	// Tap run.entries.dispatched:v1 before either run is triggered: state consumes
	// it and the trim loop removes consumed entries, so steps 6 and 15 read what
	// the tap recorded rather than the stream's history.
	dispatchedTap := startStreamTap(t, ctx, clients.redisClient, streams.RunEntriesDispatchedV1)

	// Step 5: Trigger Run 1 via the ui HTTP endpoint.
	t.Log("=== Step 5: triggerScheduleHTTP — starting Run 1 (S1) ===")
	scheduleID1Str := triggerScheduleHTTP(t, clients.uiBase, scheduleName)
	t.Logf("Run 1 created: schedule_id=%s", scheduleID1Str)
	s1, err := uuid.Parse(scheduleID1Str)
	require.NoError(t, err, "schedule_id for Run 1 is not a valid UUID")

	// Step 6: Wait for run.entries.dispatched:v1 for S1. This event is published
	// after SnapshotGraph commits, proving S1's promotion_seq has been stamped on
	// its Run node in Neo4j.
	t.Log("=== Step 6: waiting for run.entries.dispatched:v1 for S1 ===")
	waitForDispatched(t, ctx, dispatchedTap, s1)

	// Step 7: S1 carries G1.
	t.Log("=== Step 7: verifying S1 promotion_seq in Neo4j == G1 ===")
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

	// Step 10: S1 still carries G1. This is the core isolation invariant: an
	// in-flight run is not affected by a topology announced after its
	// SnapshotGraph.
	t.Log("=== Step 10: verifying S1 promotion_seq in Neo4j still == G1 ===")
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

	// Step 14: Wait for run.entries.dispatched:v1 for S2.
	t.Log("=== Step 14: waiting for run.entries.dispatched:v1 for S2 ===")
	waitForDispatched(t, ctx, dispatchedTap, s2)

	// Step 15: S2 carries G2.
	t.Log("=== Step 15: verifying S2 promotion_seq in Neo4j == G2 ===")
	assert.Equal(t, g2, readRunPromotionSeq(t, ctx, clients, s2), "Run 2's promotion_seq must equal G2=%d", g2)

	// Step 16: Wait for S2 to complete successfully.
	t.Log("=== Step 16: waiting for Run 2 to complete ===")
	waitForAllTasksSucceeded(t, ctx, clients, s2, tables)
	verifySchedulerSucceeded(t, ctx, clients, s2)
	t.Log("TestTopologyVersioning_MidRunIsolation PASSED")
}

// waitForDispatched waits until the tap has recorded run.entries.dispatched:v1
// for scheduleID, which is published after the run's SnapshotGraph commits.
func waitForDispatched(t *testing.T, ctx context.Context, tap *streamTap, scheduleID uuid.UUID) {
	t.Helper()
	pollUntil(t, ctx, 2*time.Minute, time.Second, func() (bool, error) {
		for _, msg := range tap.Entries() {
			payloadStr, _ := msg.Values["payload"].(string)
			if payloadStr == "" {
				continue
			}
			var p map[string]interface{}
			if json.Unmarshal([]byte(payloadStr), &p) != nil {
				continue
			}
			if p["schedule_id"] == scheduleID.String() {
				return true, nil
			}
		}
		return false, nil
	}, "Timeout waiting for run.entries.dispatched:v1 for schedule "+scheduleID.String())
}

// readRunPromotionSeq returns the promotion_seq the Neo4j Run node of
// scheduleID was snapshotted under.
func readRunPromotionSeq(t *testing.T, ctx context.Context, clients *testClients, scheduleID uuid.UUID) int64 {
	t.Helper()

	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode: neo4jdriver.AccessModeRead,
	})
	defer session.Close(ctx)

	result, err := session.Run(ctx,
		`MATCH (r:Run {run_id: $run_id}) RETURN r.promotion_seq AS seq`,
		map[string]interface{}{"run_id": scheduleID.String()},
	)
	require.NoError(t, err, "failed to query promotion_seq for run_id=%s", scheduleID)
	require.True(t, result.Next(ctx), "no Run node found in Neo4j for run_id=%s", scheduleID)

	seqVal, ok := result.Record().Get("seq")
	require.True(t, ok, "promotion_seq missing from Run node run_id=%s", scheduleID)
	require.NotNil(t, seqVal, "promotion_seq is nil on Run node run_id=%s", scheduleID)

	seq, ok := seqVal.(int64)
	require.True(t, ok, "promotion_seq is not int64 (got %T) for run_id=%s", seqVal, scheduleID)
	return seq
}
