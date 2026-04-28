package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testScheduleName        = "e2e-schedule"
	testSchemaName          = "e2e_schema"
	testOwner               = "test"
	failureTestScheduleName = "e2e-schedule-failure"
)

// tableServiceMap maps each happy-path table to its owning service
var tableServiceMap = map[string]string{
	"seed_table_1": "service-1",
	"seed_table_2": "service-1",
	"seed_table_3": "service-1",
	"table_a":      "service-1",
	"table_b":      "service-1",
	"table_c":      "service-1",
	"table_d":      "service-3",
	"table_e":      "service-3",
	"table_f":      "service-3",
	"table_g":      "service-2",
	"table_h":      "service-2",
	"table_i":      "service-3",
	"table_j":      "service-3",
}

// getServiceNameForTable returns the service name for a happy-path table
func getServiceNameForTable(tableName string) string {
	svc, ok := tableServiceMap[tableName]
	if !ok {
		panic(fmt.Sprintf("no service mapping for table %q", tableName))
	}
	return svc
}

// triggerGraphLoad triggers a graph update via the ui-service HTTP endpoint and
// waits until:
//  1. the manifest-controller publishes a new manifest.loaded:v1 message, AND
//  2. the orchestrator processes it (IngestTopology commits) and publishes
//     schedules.loaded:v1 — confirmed by a new message on that stream.
//
// Waiting for schedules.loaded:v1 (not just schedule_catalog row count) is
// critical: schedule_catalog persists across test runs, so a count-based check
// returns immediately with stale data. If we proceed before IngestTopology
// commits, ActivateSchedule can publish scheduler.started:v1 while IngestTopology
// still holds a Postgres transaction, and the two handlers would race.
func triggerGraphLoad(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()

	// Record the last message IDs on both streams before triggering so the
	// polls below prove this specific trigger was processed, not a prior one.
	manifestEntries, err := clients.redisClient.XRevRangeN(ctx, "manifest.loaded:v1", "+", "-", 1).Result()
	lastManifestID := "0-0"
	if err == nil && len(manifestEntries) > 0 {
		lastManifestID = manifestEntries[0].ID
	}

	schedulesEntries, err := clients.redisClient.XRevRangeN(ctx, "schedules.loaded:v1", "+", "-", 1).Result()
	lastSchedulesLoadedID := "0-0"
	if err == nil && len(schedulesEntries) > 0 {
		lastSchedulesLoadedID = schedulesEntries[0].ID
	}

	resp, err := http.Post(
		fmt.Sprintf("%s/api/graph/update", clients.uiBase),
		"application/json",
		strings.NewReader(`{"source":"s3"}`),
	)
	require.NoError(t, err, "POST /api/graph/update: request failed")
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"POST /api/graph/update: expected 200, got %d: %s", resp.StatusCode, string(body))

	t.Log("Published update.graph:v1 via ui-service — waiting for manifest-controller to publish manifest.loaded:v1...")

	// Step 1: wait for the manifest-controller to publish manifest.loaded:v1.
	// XRange with "("+lastManifestID returns only messages strictly after that ID.
	pollUntil(t, ctx, 60*time.Second, 1*time.Second, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, "manifest.loaded:v1", "("+lastManifestID, "+").Result()
		if err != nil {
			return false, nil
		}
		return len(msgs) > 0, nil
	}, "Timeout waiting for manifest-controller to publish manifest.loaded:v1")

	t.Log("manifest-controller published manifest.loaded:v1 — waiting for orchestrator to commit IngestTopology (schedules.loaded:v1)...")

	// Step 2: wait for the orchestrator to commit IngestTopology and publish
	// schedules.loaded:v1 via its outbox processor. This confirms that:
	//   - ApplySnapshot wrote Neo4j changes, AND
	//   - the Postgres transaction committed (inTx = false on the UoW)
	// Only after this point is it safe to call ActivateSchedule: if we activate
	// sooner the scheduler.started:v1 handler could race against IngestTopology.
	pollUntil(t, ctx, 30*time.Second, 500*time.Millisecond, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, "schedules.loaded:v1", "("+lastSchedulesLoadedID, "+").Result()
		if err != nil {
			return false, nil
		}
		return len(msgs) > 0, nil
	}, "Timeout waiting for orchestrator to publish schedules.loaded:v1")

	t.Log("orchestrator published schedules.loaded:v1 — graph and catalog are in sync")
}
