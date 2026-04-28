package e2e

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTopologyVersioning_MidRunIsolation validates that a new manifest.loaded:v1
// arriving between Run 1's SnapshotGraph and its completion does NOT affect the
// in-flight run: its topology_generation must remain pinned to the value captured
// at SnapshotGraph time. Run 2 (triggered after the reload) must pick up the
// incremented generation.
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

	// Step 3: Load the graph for the first time and wait for schedules.loaded:v1,
	// which confirms orchestrator committed IngestTopology (generation G1).
	t.Log("=== Step 3: triggerGraphLoad — establishing generation G1 ===")
	triggerGraphLoad(t, ctx, clients)

	// Step 4: Read G1 from orchestrator_db.
	var g1 int64
	err := clients.orchestratorDB.QueryRowContext(ctx,
		`SELECT topology_generation FROM topology_state WHERE id = TRUE`,
	).Scan(&g1)
	require.NoError(t, err, "failed to read topology_generation (G1) from topology_state")
	t.Logf("G1 = %d", g1)

	// Step 5: Trigger Run 1 via the ui-service HTTP endpoint.
	t.Log("=== Step 5: triggerScheduleHTTP — starting Run 1 (S1) ===")
	scheduleID1Str := triggerScheduleHTTP(t, clients.uiBase, scheduleName)
	t.Logf("Run 1 created: schedule_id=%s", scheduleID1Str)
	s1, err := uuid.Parse(scheduleID1Str)
	require.NoError(t, err, "schedule_id for Run 1 is not a valid UUID")

	// Step 6: Wait for run.entries.dispatched:v1 for S1.
	// This event is published after SnapshotGraph commits, proving that S1's
	// topology_generation has been stamped on its Run node in Neo4j.
	t.Log("=== Step 6: waiting for run.entries.dispatched:v1 for S1 ===")

	// Record the current last message ID so we only look at new messages.
	dispatchedEntries, err := clients.redisClient.XRevRangeN(ctx, "run.entries.dispatched:v1", "+", "-", 1).Result()
	lastDispatchedID := "0-0"
	if err == nil && len(dispatchedEntries) > 0 {
		lastDispatchedID = dispatchedEntries[0].ID
	}

	pollUntil(t, ctx, 2*time.Minute, time.Second, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, "run.entries.dispatched:v1", "("+lastDispatchedID, "+").Result()
		if err != nil {
			return false, nil
		}
		for _, msg := range msgs {
			payloadStr, _ := msg.Values["payload"].(string)
			if payloadStr == "" {
				continue
			}
			var p map[string]interface{}
			if json.Unmarshal([]byte(payloadStr), &p) != nil {
				continue
			}
			if p["schedule_id"] == s1.String() {
				return true, nil
			}
		}
		return false, nil
	}, "Timeout waiting for run.entries.dispatched:v1 for schedule "+s1.String())
	t.Log("run.entries.dispatched:v1 received for S1 — SnapshotGraph has committed")

	// Step 7: Read S1's topology_generation from Neo4j and assert it equals G1.
	t.Log("=== Step 7: verifying S1 topology_generation in Neo4j == G1 ===")
	s1Gen := readRunTopologyGeneration(t, ctx, clients, s1)
	assert.Equal(t, g1, s1Gen,
		"Run 1's topology_generation must equal G1=%d, got %d", g1, s1Gen)
	t.Logf("S1 topology_generation = %d (expected G1=%d) ✓", s1Gen, g1)

	// Step 8: Trigger a second graph load while S1 is still in-flight.
	// This increments the generation to G2 = G1+1.
	t.Log("=== Step 8: triggerGraphLoad mid-run — incrementing to G2 ===")
	triggerGraphLoad(t, ctx, clients)

	// Step 9: Read G2 from orchestrator_db and assert G2 == G1+1.
	var g2 int64
	err = clients.orchestratorDB.QueryRowContext(ctx,
		`SELECT topology_generation FROM topology_state WHERE id = TRUE`,
	).Scan(&g2)
	require.NoError(t, err, "failed to read topology_generation (G2) from topology_state")
	t.Logf("G2 = %d", g2)
	require.Equal(t, g1+1, g2, "G2 must equal G1+1: G1=%d G2=%d", g1, g2)

	// Step 10: Re-read S1's topology_generation from Neo4j and assert it still == G1.
	// This is the core isolation invariant: an in-flight run must not be affected
	// by a topology reload that happened after its SnapshotGraph.
	t.Log("=== Step 10: verifying S1 topology_generation in Neo4j still == G1 ===")
	s1GenAfterReload := readRunTopologyGeneration(t, ctx, clients, s1)
	assert.Equal(t, g1, s1GenAfterReload,
		"Run 1 must remain pinned to G1=%d after mid-run reload; got %d", g1, s1GenAfterReload)
	t.Logf("S1 topology_generation after reload = %d (expected G1=%d) ✓", s1GenAfterReload, g1)

	// Step 11: Wait for S1 to complete successfully.
	t.Log("=== Step 11: waiting for Run 1 to complete ===")
	waitForAllTasksSucceeded(t, ctx, clients, s1, tables)
	verifySchedulerSucceeded(t, ctx, clients, s1)
	t.Log("Run 1 completed successfully")

	// Step 12: Assert task_tracker rows for S1 have non-empty manifest_version.
	t.Log("=== Step 12: verifying task_tracker manifest_version for S1 ===")
	var s1ManifestVersions []string
	err = clients.stateDB.SelectContext(ctx, &s1ManifestVersions,
		`SELECT manifest_version FROM task_tracker WHERE schedule_id = $1`,
		s1,
	)
	require.NoError(t, err, "failed to query manifest_version for Run 1")
	require.NotEmpty(t, s1ManifestVersions, "expected task_tracker rows for Run 1")
	for _, mv := range s1ManifestVersions {
		assert.NotEmpty(t, mv, "task manifest_version must be non-empty for Run 1")
	}
	t.Logf("S1: all %d task_tracker rows have non-empty manifest_version ✓", len(s1ManifestVersions))

	// Step 13: Clean up Run 1 data before triggering Run 2.
	t.Log("=== Step 13: cleaning up Run 1 data ===")
	cleanupTestData(t, ctx, clients, scheduleName)

	// Step 14: Trigger Run 2. Since the graph was reloaded in step 8, Run 2 must
	// pick up G2.
	t.Log("=== Step 14: triggerScheduleHTTP — starting Run 2 (S2) ===")
	scheduleID2Str := triggerScheduleHTTP(t, clients.uiBase, scheduleName)
	t.Logf("Run 2 created: schedule_id=%s", scheduleID2Str)
	s2, err := uuid.Parse(scheduleID2Str)
	require.NoError(t, err, "schedule_id for Run 2 is not a valid UUID")

	// Step 15: Wait for run.entries.dispatched:v1 for S2.
	t.Log("=== Step 15: waiting for run.entries.dispatched:v1 for S2 ===")

	// Record the current last message ID so we only look at new messages.
	dispatchedEntries2, err := clients.redisClient.XRevRangeN(ctx, "run.entries.dispatched:v1", "+", "-", 1).Result()
	lastDispatchedID2 := "0-0"
	if err == nil && len(dispatchedEntries2) > 0 {
		lastDispatchedID2 = dispatchedEntries2[0].ID
	}

	pollUntil(t, ctx, 2*time.Minute, time.Second, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, "run.entries.dispatched:v1", "("+lastDispatchedID2, "+").Result()
		if err != nil {
			return false, nil
		}
		for _, msg := range msgs {
			payloadStr, _ := msg.Values["payload"].(string)
			if payloadStr == "" {
				continue
			}
			var p map[string]interface{}
			if json.Unmarshal([]byte(payloadStr), &p) != nil {
				continue
			}
			if p["schedule_id"] == s2.String() {
				return true, nil
			}
		}
		return false, nil
	}, "Timeout waiting for run.entries.dispatched:v1 for schedule "+s2.String())
	t.Log("run.entries.dispatched:v1 received for S2 — SnapshotGraph has committed")

	// Step 16: Read S2's topology_generation from Neo4j and assert it equals G2.
	t.Log("=== Step 16: verifying S2 topology_generation in Neo4j == G2 ===")
	s2Gen := readRunTopologyGeneration(t, ctx, clients, s2)
	assert.Equal(t, g2, s2Gen,
		"Run 2's topology_generation must equal G2=%d, got %d", g2, s2Gen)
	t.Logf("S2 topology_generation = %d (expected G2=%d) ✓", s2Gen, g2)

	// Step 17: Wait for S2 to complete successfully.
	t.Log("=== Step 17: waiting for Run 2 to complete ===")
	waitForAllTasksSucceeded(t, ctx, clients, s2, tables)
	verifySchedulerSucceeded(t, ctx, clients, s2)
	t.Log("Run 2 completed successfully")

	// Step 18: Assert task_tracker rows for S2 have non-empty manifest_version.
	t.Log("=== Step 18: verifying task_tracker manifest_version for S2 ===")
	var s2ManifestVersions []string
	err = clients.stateDB.SelectContext(ctx, &s2ManifestVersions,
		`SELECT manifest_version FROM task_tracker WHERE schedule_id = $1`,
		s2,
	)
	require.NoError(t, err, "failed to query manifest_version for Run 2")
	require.NotEmpty(t, s2ManifestVersions, "expected task_tracker rows for Run 2")
	for _, mv := range s2ManifestVersions {
		assert.NotEmpty(t, mv, "task manifest_version must be non-empty for Run 2")
	}
	t.Logf("S2: all %d task_tracker rows have non-empty manifest_version ✓", len(s2ManifestVersions))

	t.Log("TestTopologyVersioning_MidRunIsolation PASSED")
}

// readRunTopologyGeneration queries the Neo4j Run node for the given schedule_id
// and returns its topology_generation property.
func readRunTopologyGeneration(
	t *testing.T,
	ctx context.Context,
	clients *testClients,
	scheduleID uuid.UUID,
) int64 {
	t.Helper()

	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode: neo4jdriver.AccessModeRead,
	})
	defer session.Close(ctx)

	result, err := session.Run(ctx,
		`MATCH (r:Run {run_id: $run_id}) RETURN r.topology_generation AS gen`,
		map[string]interface{}{"run_id": scheduleID.String()},
	)
	require.NoError(t, err, "failed to query topology_generation for run_id=%s", scheduleID)
	require.True(t, result.Next(ctx),
		"no Run node found in Neo4j for run_id=%s", scheduleID)

	genVal, ok := result.Record().Get("gen")
	require.True(t, ok, "topology_generation property missing from Run node run_id=%s", scheduleID)
	require.NotNil(t, genVal, "topology_generation is nil on Run node run_id=%s", scheduleID)

	gen, ok := genVal.(int64)
	require.True(t, ok, "topology_generation is not int64 (got %T) for run_id=%s", genVal, scheduleID)

	return gen
}
