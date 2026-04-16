package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestScheduleCatalog_FullChain tests the complete pipeline:
// manifest load → schedules.loaded:v1 → schedule_catalog → gRPC.
//
// Steps:
//  1. Publish update.graph:v1 to trigger manifest-controller
//  2. Wait for schedules.loaded:v1 to appear in Redis (proves graph load + Redis publish both succeeded)
//  3. Wait for schedule_catalog rows in state DB (proves state consumer processed the event)
//  4. ListAllSchedules returns the expected schedule names
//  5. TriggerSchedule returns a non-empty schedule_id
//  6. Second TriggerSchedule returns FAILED_PRECONDITION
func TestScheduleCatalog_FullChain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)

	// Test fixture: the local manifests define schedule names from tags.
	// Models tagged with "e2e-schedule" and seeds (which default to "seed")
	// are the two schedule names present in the environment manifests.
	expectedSchedules := []string{"e2e-schedule", "seed"}
	testSchedule := "e2e-schedule"

	// Setup: remove any active scheduler_tracker row for testSchedule to avoid
	// cross-run contamination from previous test runs.
	_, err := clients.stateDB.ExecContext(ctx,
		`DELETE FROM scheduler_tracker WHERE schedule_name = $1 AND status IN ('pending', 'running')`,
		testSchedule,
	)
	require.NoError(t, err, "Failed to clean up scheduler_tracker")

	// Step 1: Trigger a manifest load via ui-service HTTP endpoint
	t.Log("Step 1: Triggering graph update via ui-service HTTP")
	resp, err := http.Post(
		fmt.Sprintf("%s/api/graph/update", clients.uiBase),
		"application/json",
		strings.NewReader(`{"source":"local"}`),
	)
	require.NoError(t, err, "POST /api/graph/update: request failed")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"POST /api/graph/update: expected 200, got %d: %s", resp.StatusCode, string(body))
	t.Log("Graph update triggered via ui-service")

	// Step 2: Wait for schedules.loaded:v1 to appear in Redis.
	t.Log("Step 2: Waiting for schedules.loaded:v1 in Redis")
	var schedulesLoadedPayload map[string]interface{}
	pollUntil(t, ctx, 60*time.Second, 1*time.Second, func() (bool, error) {
		messages, err := clients.redisClient.XRange(ctx, "schedules.loaded:v1", "-", "+").Result()
		if err != nil || len(messages) == 0 {
			return false, nil
		}
		for _, msg := range messages {
			payloadStr, _ := msg.Values["payload"].(string)
			if payloadStr == "" {
				continue
			}
			var p map[string]interface{}
			if err := json.Unmarshal([]byte(payloadStr), &p); err != nil {
				continue
			}
			names, _ := p["schedule_names"].([]interface{})
			if len(names) > 0 {
				schedulesLoadedPayload = p
				return true, nil
			}
		}
		return false, nil
	}, "Timeout waiting for schedules.loaded:v1 in Redis")

	t.Logf("schedules.loaded:v1 received: event_id=%v", schedulesLoadedPayload["event_id"])

	// Step 3: Wait for schedule_catalog rows in state DB.
	t.Log("Step 3: Waiting for schedule_catalog rows in state DB")
	pollUntil(t, ctx, 30*time.Second, 500*time.Millisecond, func() (bool, error) {
		var count int
		err := clients.stateDB.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM schedule_catalog WHERE removed_at IS NULL`,
		).Scan(&count)
		if err != nil {
			return false, err
		}
		return count >= len(expectedSchedules), nil
	}, fmt.Sprintf("Timeout waiting for %d active rows in schedule_catalog", len(expectedSchedules)))

	// Verify the specific schedule names are present
	var activeNames []string
	err = clients.stateDB.SelectContext(ctx, &activeNames,
		`SELECT schedule_name FROM schedule_catalog WHERE removed_at IS NULL ORDER BY schedule_name`,
	)
	require.NoError(t, err)
	for _, name := range expectedSchedules {
		assert.Contains(t, activeNames, name, "schedule_catalog missing %q", name)
	}
	t.Logf("schedule_catalog has active rows: %v", activeNames)

	// Step 4: ListAllSchedules returns the expected schedule names
	t.Log("Step 4: Calling ListAllSchedules")
	listResp, err := clients.stateClient.ListAllSchedules(ctx, &statev1.ListAllSchedulesRequest{})
	require.NoError(t, err)
	var listedNames []string
	for _, s := range listResp.Schedules {
		listedNames = append(listedNames, s.ScheduleName)
	}
	for _, name := range expectedSchedules {
		assert.Contains(t, listedNames, name, "ListAllSchedules missing %q", name)
	}
	t.Logf("ListAllSchedules returned: %v", listedNames)

	// Step 5: TriggerSchedule returns a non-empty schedule_id
	t.Log("Step 5: Calling TriggerSchedule")
	triggerResp, err := clients.stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{
		ScheduleName: testSchedule,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, triggerResp.ScheduleId, "TriggerSchedule must return a non-empty schedule_id")

	scheduleID, err := uuid.Parse(triggerResp.ScheduleId)
	require.NoError(t, err, "schedule_id must be a valid UUID")

	// Verify row exists in scheduler_tracker
	var rowCount int
	err = clients.stateDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM scheduler_tracker WHERE schedule_id = $1`,
		scheduleID,
	).Scan(&rowCount)
	require.NoError(t, err)
	assert.Equal(t, 1, rowCount, "Expected one scheduler_tracker row for the triggered run")
	t.Logf("TriggerSchedule created scheduler_tracker row: %s", scheduleID)

	// Step 6: Second TriggerSchedule returns FAILED_PRECONDITION
	t.Log("Step 6: Calling TriggerSchedule a second time (expect FAILED_PRECONDITION)")
	_, err = clients.stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{
		ScheduleName: testSchedule,
	})
	require.Error(t, err, "Second TriggerSchedule must return an error")

	st := status.Convert(err)
	assert.Equal(t, codes.FailedPrecondition, st.Code(),
		"Expected FAILED_PRECONDITION, got %s: %s", st.Code(), st.Message())
	t.Log("Second TriggerSchedule correctly returned FAILED_PRECONDITION")
}
