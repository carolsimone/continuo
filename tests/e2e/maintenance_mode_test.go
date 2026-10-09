package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const maintenanceMessage = "continuo is in maintenance mode: new work is not accepted until it is turned off"

// maintenanceServices are restarted with the flag flipped: state gates runs,
// release-controller gates releases. Each entry is the container, its Go
// module directory under /app and its health URL.
var maintenanceServices = []struct{ container, dir, health string }{
	{"state", "state", "http://state:8082/health"},
	{"release-controller", "release-controller", "http://release-controller:8088/healthz"},
}

// setMaintenance restarts state and release-controller with MAINTENANCE_ENABLED
// set to on, in parallel, and waits until both answer their health checks. The
// restart stops the running `go run` process; the relaunch inherits the
// container's compose environment except for the overridden flag. Each
// service's output goes to /tmp/<container>.log inside its container.
func setMaintenance(t *testing.T, ctx context.Context, on bool) {
	t.Helper()
	start := time.Now()
	// A relaunch that dies on startup leaves the health URL unreachable until
	// the poll times out; the log tail names the cause (a compile error, a bad
	// config). pollUntil ends the test with t.Fatal, which runs this defer.
	defer func() {
		if t.Failed() {
			logServiceTails(t)
		}
	}()
	errs := make(chan error, len(maintenanceServices))
	for _, s := range maintenanceServices {
		go func() {
			if out, err := exec.CommandContext(ctx, "docker", "restart", "-t", "20", s.container).CombinedOutput(); err != nil { //nolint:gosec // fixed container names
				errs <- fmt.Errorf("docker restart %s: %v: %s", s.container, err, out)
				return
			}
			cmd := exec.CommandContext(ctx, "docker", "exec", "-d", "-e", fmt.Sprintf("MAINTENANCE_ENABLED=%t", on), //nolint:gosec // fixed arguments
				s.container, "bash", "-c", "cd /app/"+s.dir+" && go run . > /tmp/"+s.container+".log 2>&1")
			if out, err := cmd.CombinedOutput(); err != nil {
				errs <- fmt.Errorf("relaunch %s: %v: %s", s.container, err, out)
				return
			}
			errs <- nil
		}()
	}
	for range maintenanceServices {
		require.NoError(t, <-errs)
	}
	for _, s := range maintenanceServices {
		pollUntil(t, ctx, 3*time.Minute, time.Second, func() (bool, error) {
			resp, err := http.Get(s.health) //nolint:gosec,noctx // fixed health URL
			if err != nil {
				return false, nil
			}
			_ = resp.Body.Close()
			return resp.StatusCode == http.StatusOK, nil
		}, s.container+" did not become healthy after the maintenance restart")
	}
	t.Logf("maintenance=%t restart took %s", on, time.Since(start).Round(time.Second))
}

// logServiceTails writes the last lines of each restarted service's
// /tmp/<container>.log into the test output.
func logServiceTails(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, s := range maintenanceServices {
		out, err := exec.CommandContext(ctx, "docker", "exec", s.container, "tail", "-n", "50", "/tmp/"+s.container+".log").CombinedOutput() //nolint:gosec // fixed container names
		if err != nil {
			t.Logf("tail of %s log failed: %v: %s", s.container, err, out)
			continue
		}
		t.Logf("last lines of %s:/tmp/%s.log:\n%s", s.container, s.container, out)
	}
}

// reconnecting reports whether a response is the UI's gRPC channel still
// re-establishing to a backend that was just restarted: a 500 whose body names
// an UNAVAILABLE / connection-refused error. `docker restart` drops the UI's
// persistent channel to state (50051) and release-controller, and the client
// reconnects with backoff a moment later, so the first request after a restart
// can land in that window. Such a response is retried, never asserted on.
func reconnecting(status int, body map[string]any) bool {
	if status != http.StatusInternalServerError {
		return false
	}
	msg, _ := body["error"].(string)
	return strings.Contains(msg, "UNAVAILABLE") ||
		strings.Contains(msg, "No connection established") ||
		strings.Contains(msg, "ECONNREFUSED")
}

// afterRestart calls post until the UI has reconnected to the just-restarted
// backend, returning the first settled response. The maintenance assertions
// then test the refusal or acceptance itself, not the reconnect race that a
// `docker restart` opens; a genuine refusal (503) or acceptance (200) is
// returned at once, since only a reconnect-class 500 is retried.
func afterRestart(t *testing.T, ctx context.Context, post func() (int, map[string]any)) (int, map[string]any) {
	t.Helper()
	var status int
	var out map[string]any
	pollUntil(t, ctx, time.Minute, time.Second, func() (bool, error) {
		status, out = post()
		return !reconnecting(status, out), nil
	}, "UI kept returning a gRPC-reconnect error after the maintenance restart")
	return status, out
}

func postJSON(t *testing.T, url string, body any) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := http.Post(url, "application/json", bytes.NewReader(raw)) //nolint:gosec,noctx // test-built URL
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// TestMaintenanceMode proves the upgrade's first step: with maintenance on,
// a new run and a new release are refused with the maintenance message while
// a run already in flight still finishes; with maintenance off, new work is
// accepted again.
func TestMaintenanceMode(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	clients := setupClients(t, ctx)
	defer clients.close(ctx)
	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	cleanupTestData(t, ctx, clients, "maintenance-mode")
	seedTopology(t, ctx, clients)

	const service, schema, table = "service-1", "e2e_schema", "seed_table_1"
	nodeRunURL := fmt.Sprintf("%s/api/nodes/%s/%s/%s/run", clients.uiBase, service, schema, table)

	// Maintenance must be off again whatever happens below, or every later
	// test in the suite would be refused.
	t.Cleanup(func() { setMaintenance(t, context.Background(), false) })

	// 1. A run in flight before maintenance begins.
	resp, err := clients.stateClient.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{
		ServiceName: service, SchemaName: schema, TableName: table, MetadataSource: "latest",
	})
	require.NoError(t, err)
	inFlight := uuid.MustParse(resp.RunId)
	defer cleanupSingleNodeRun(t, ctx, clients, inFlight, resp.ScheduleName)

	// 2. Maintenance on.
	setMaintenance(t, ctx, true)

	// 3. A new run through ui is refused by state, and ui maps the refusal.
	status, body := afterRestart(t, ctx, func() (int, map[string]any) {
		return postJSON(t, nodeRunURL, map[string]string{"operation": "run"})
	})
	require.Equal(t, http.StatusServiceUnavailable, status, "%v", body)
	require.Equal(t, "maintenance", body["code"])
	require.Equal(t, maintenanceMessage, body["error"])

	// 4. A new release through the public API is refused by release-controller.
	token := mintCIToken(t, e2eDbtRepositoryID, "carolsimone/continuo-demo", "maintenance")
	status, out := afterRestart(t, ctx, func() (int, map[string]any) {
		return submitPublicRelease(t, clients, token, map[string]any{
			"service": "service-1", "release_id": "e2e-maintenance-refused", "image_tag": "unused",
		})
	})
	require.Equal(t, http.StatusServiceUnavailable, status, "%v", out)
	require.Equal(t, "maintenance", out["code"])

	// 5. The run that was in flight still finishes.
	verifySchedulerSucceeded(t, ctx, clients, inFlight)

	// 6. Maintenance off: new work is accepted again.
	setMaintenance(t, ctx, false)
	status, body = afterRestart(t, ctx, func() (int, map[string]any) {
		return postJSON(t, nodeRunURL, map[string]string{"operation": "run"})
	})
	require.Equal(t, http.StatusOK, status, "%v", body)
	runIDStr, ok := body["run_id"].(string)
	require.True(t, ok, "run_id missing from node-run response: %v", body)
	scheduleName, ok := body["schedule_name"].(string)
	require.True(t, ok, "schedule_name missing from node-run response: %v", body)
	runID := uuid.MustParse(runIDStr)
	defer cleanupSingleNodeRun(t, ctx, clients, runID, scheduleName)
	verifySchedulerSucceeded(t, ctx, clients, runID)
}
