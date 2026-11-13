package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// seedReloadService is the dbt service that holds seed_table_1 and the view
	// over it.
	seedReloadService = "service-1"
	// seedReloadViewKey is the unique_id of seed_table_1_view, a dbt view over
	// seed_table_1 on the seed-view schedule.
	seedReloadViewKey = seedSchemaName + ".seed_table_1_view"
	// seedReloadOutsideSchema holds the warehouse objects that read
	// seed_table_1 and that no continuo node builds.
	seedReloadOutsideSchema = "e2e_bi"
	// seedReloadOutsideView is a plain view over seed_table_1, and
	// seedReloadOutsideMatView a materialized view over that view: one object
	// between it and the seed's table, which information_schema.view_table_usage
	// does not show and the preflight's recursive pg_depend walk finds.
	seedReloadOutsideView    = seedReloadOutsideSchema + ".seed_reader_base"
	seedReloadOutsideMatView = seedReloadOutsideSchema + ".seed_reader"
)

// seedReloadNotice is the part of one seed reload notice this test reads, in
// the JSON shape both release-controller's GET /releases/{id} and
// `continuo release notices` print.
type seedReloadNotice struct {
	NodeID  string `json:"node_id"`
	Change  string `json:"change"`
	Columns struct {
		Added   []string `json:"added"`
		Removed []string `json:"removed"`
		Retyped []string `json:"retyped"`
	} `json:"columns"`
	Load struct {
		Operation  string `json:"operation"`
		DropsTable bool   `json:"drops_table"`
		Reason     string `json:"reason"`
	} `json:"load"`
	Rebuilds []struct {
		NodeID string `json:"node_id"`
	} `json:"rebuilds"`
	Prediction string `json:"prediction"`
}

// TestSeedReload_RebuildsTheViewAndStopsOnAViewContinuoDoesNotOwn drives a
// seed column change from a release to the warehouse, on Postgres:
//
//  1. production holds seed_table_1 with the columns id and legacy and no
//     seed_table_1_view; run-controller records the seed as loaded at that
//     version. A release of service-1 (whose seed declares only id, and which
//     adds the view) is posted, and release-controller's notice says the load
//     is a full refresh that drops the table, removes legacy and rebuilds the
//     view; `continuo release notices` prints the same notice.
//  2. The release promotes. Its promote-seed run full-refreshes the seed
//     ("columns changed") and runs the view ("rebuild: dropped by the load of
//     e2e_schema.seed_table_1"); both succeed and the view exists.
//  3. A view and a materialized view on it, which no continuo node builds,
//     now read the seed's table. run-controller's recorded version is moved
//     back, so the seed reconcile starts a second promote-seed run, whose
//     check-mode preflight lists both objects (depths 1 and 2, not allowed,
//     completeness exact) and stops the seed before dbt: the seed fails
//     invalid/unowned_dependents, the view's task is skipped and nothing is
//     dropped.
//  4. `continuo node full-refresh --acknowledge` naming both objects reloads
//     the seed and rebuilds the view; the cascade drops the two objects.
//
// The cleanup drops the e2e_bi schema and the view and restores the default
// topology whatever the outcome: on the default DAG the view is no node, so a
// later full refresh of seed_table_1 would find it and stop.
func TestSeedReload_RebuildsTheViewAndStopsOnAViewContinuoDoesNotOwn(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// additive polls: the wait for earlier promote-seed runs (5m) + the
	// default topology (1m) + the notice (10m) + the promotion (10m) + the
	// promote-seed run (2m + 5m) + the reconcile's second run (3m), its seed
	// (5m) and its skipped view (1m) + the acknowledged full refresh (5m) =
	// 47m. The cleanup has a context of its own.
	ctx, cancel := context.WithTimeout(context.Background(), 47*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-seedreload-" + uuid.NewString()[:8]
	t.Logf("release_id=%s service=%s", releaseID, seedReloadService)

	// ── 1. setup and the release's notice ────────────────────────────────────
	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)
	imageTag := allServices[seedReloadService].imageTag
	require.NotEmpty(t, imageTag, "image_tag missing for %s", seedReloadService)

	oldSeedHash := "sha256:e2e-old-seed-" + uuid.NewString()
	var prodNodes []map[string]string
	seedFound, viewFound := false, false
	for svc, si := range allServices {
		for _, n := range si.nodes {
			switch n.uniqueID {
			case seedReloadViewKey:
				viewFound = true
				continue // left out, so the release adds it
			case seedReconcileNodeKey:
				seedFound = true
				prodNodes = append(prodNodes, map[string]string{
					"unique_id":         n.uniqueID,
					"service":           svc,
					"content_hash":      oldSeedHash,
					"seed_columns":      `[{"name":"id","type":""},{"name":"legacy","type":""}]`,
					"seed_columns_hash": "sha256:e2e-old-columns",
				})
				continue
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id": n.uniqueID, "service": svc, "content_hash": n.contentHash,
			})
		}
	}
	require.True(t, seedFound, "%s not found in any baseline manifest", seedReconcileNodeKey)
	require.True(t, viewFound, "%s not found in any baseline manifest — is the model in service-1 and the image rebuilt?", seedReloadViewKey)

	// A promote-seed run an earlier test's release planned would record the
	// seed as loaded after this test sets its version.
	waitForPromoteSeedRunsToEnd(t, ctx, clients, "", 5*time.Minute)
	t.Cleanup(func() { cleanupSeedReload(t, clients, releaseID) })
	// The default DAG's seeds carry no content hash, so no reconcile pass loads
	// seed_table_1 before the release promotes.
	base := seedTopology(t, ctx, clients)
	waitForRunControllerLiveRelease(t, ctx, clients, base)

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProd(t, ctx, clients, allServices)
	setLoadedSeedVersion(t, ctx, clients, oldSeedHash, "sha256:e2e-old-columns")

	postRelease(t, clients, seedReloadService, releaseID, imageTag, false)

	notices := waitForReleaseNotices(t, ctx, clients, releaseID, 10*time.Minute)
	require.Len(t, notices, 1, "the release changes one seed's load: %+v", notices)
	assertSeedColumnNotice(t, "GET /releases/"+releaseID, notices[0])

	bin := buildCLI(t, ctx)
	env := []string{
		"CONTINUO_RUN_CONTROLLER_ADDR=" + getEnv("RUN_CONTROLLER_HOST", "run-controller") + ":50051",
		"CONTINUO_RELEASE_CONTROLLER_ADDR=" + getEnv("RELEASE_CONTROLLER_HOST", "release-controller") + ":50056",
		"CONTINUO_ACTOR=e2e-cli",
	}
	var printed struct {
		ReleaseID string             `json:"release_id"`
		Status    string             `json:"status"`
		Notices   []seedReloadNotice `json:"notices"`
	}
	runCLI(t, ctx, bin, env, &printed, "release", "notices", releaseID)
	assert.Equal(t, releaseID, printed.ReleaseID)
	require.Len(t, printed.Notices, 1, "continuo release notices prints the one notice: %+v", printed.Notices)
	assertSeedColumnNotice(t, "continuo release notices", printed.Notices[0])

	// ── 2. the promotion rebuilds the view ───────────────────────────────────
	waitForReleasePromoted(t, ctx, clients, releaseID, 10*time.Minute)
	var runs []uuid.UUID
	pollUntil(t, ctx, 2*time.Minute, 2*time.Second, func() (bool, error) {
		runs = promoteSeedRuns(t, ctx, clients, releaseID)
		return len(runs) > 0, nil
	}, fmt.Sprintf("timeout waiting for the promote-seed run of release %s", releaseID))
	require.Len(t, runs, 1, "the promotion plans one promote-seed run")
	promoteRun := runs[0]
	waitForTasksToSucceed(t, ctx, clients, promoteRun, []string{"seed_table_1", "seed_table_1_view"}, 5*time.Minute)

	shown := showRun(t, ctx, bin, env, promoteRun)
	seedTask := shown.task(t, seedReloadService+"."+seedReconcileNodeKey)
	assert.Equal(t, "full_refresh", seedTask.Operation, "the column change full-refreshes the seed")
	assert.Equal(t, "columns changed", seedTask.PlanReason)
	viewTask := shown.task(t, seedReloadService+"."+seedReloadViewKey)
	assert.Equal(t, "run", viewTask.Operation, "a view is rebuilt with run")
	assert.Equal(t, "rebuild: dropped by the load of "+seedReconcileNodeKey, viewTask.PlanReason)
	require.True(t, relationExists(t, ctx, clients, seedReloadViewKey), "the promote-seed run built %s", seedReloadViewKey)
	t.Logf("✅ promote-seed run %s full-refreshed the seed and rebuilt %s", promoteRun, seedReloadViewKey)

	// ── 3. a view continuo does not own stops the next load ──────────────────
	_, err := clients.dbtDB.ExecContext(ctx, `
		CREATE SCHEMA IF NOT EXISTS `+seedReloadOutsideSchema+`;
		CREATE OR REPLACE VIEW `+seedReloadOutsideView+` AS SELECT id FROM `+seedReconcileNodeKey+`;
		CREATE MATERIALIZED VIEW IF NOT EXISTS `+seedReloadOutsideMatView+` AS SELECT id FROM `+seedReloadOutsideView)
	require.NoError(t, err, "create the objects outside continuo that read %s", seedReconcileNodeKey)
	setLoadedSeedVersion(t, ctx, clients, "sha256:e2e-old-seed-2-"+uuid.NewString(), "sha256:e2e-old-columns-2")

	// The seed reconcile passes every 60 s.
	pollUntil(t, ctx, 3*time.Minute, 2*time.Second, func() (bool, error) {
		runs = promoteSeedRuns(t, ctx, clients, releaseID)
		return len(runs) >= 2, nil
	}, fmt.Sprintf("timeout waiting for the seed reconcile to start a second promote-seed run of release %s", releaseID))
	stoppedRun := runs[1]

	var seedStatus string
	pollUntil(t, ctx, 5*time.Minute, 2*time.Second, func() (bool, error) {
		seedStatus = runTaskStatus(ctx, clients, stoppedRun, "seed_table_1")
		switch seedStatus {
		case "succeeded", "failed", "cancelled", "skipped":
			return true, nil
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for the seed task of run %s to end", stoppedRun))
	require.Equal(t, "failed", seedStatus, "the preflight stops the seed of run %s", stoppedRun)

	attempt := latestTaskAttempt(t, ctx, clients, stoppedRun, "seed_table_1")
	assert.Equal(t, "invalid", attempt.GetFailureClass())
	assert.Equal(t, "unowned_dependents", attempt.GetFailureReason())
	// The preflight names each object as "<schema>.<name> (<kind>, owner <owner>)",
	// so the " (" keeps seed_reader from matching seed_reader_base.
	assert.Contains(t, attempt.GetErrorMessage(), seedReloadOutsideView+" (")
	assert.Contains(t, attempt.GetErrorMessage(), seedReloadOutsideMatView+" (")
	preflight := attempt.GetSeedPreflight()
	require.NotNil(t, preflight, "the seed's attempt carries its preflight's findings")
	assert.Equal(t, "exact", preflight.GetCompleteness())
	depths := map[string]int32{}
	for _, d := range preflight.GetDependents() {
		name := d.GetSchema() + "." + d.GetName()
		if name != seedReloadOutsideView && name != seedReloadOutsideMatView {
			continue
		}
		assert.False(t, d.GetAllowed(), "%s is neither rebuilt by the run nor named by an operator", name)
		depths[name] = d.GetDepth()
	}
	assert.Equal(t, map[string]int32{seedReloadOutsideView: 1, seedReloadOutsideMatView: 2}, depths,
		"the preflight lists the view at depth 1 and the materialized view on it at depth 2: %+v", preflight.GetDependents())

	pollUntil(t, ctx, time.Minute, time.Second, func() (bool, error) {
		return runTaskStatus(ctx, clients, stoppedRun, "seed_table_1_view") == "skipped", nil
	}, fmt.Sprintf("timeout waiting for the view's task of run %s to be skipped", stoppedRun))
	assert.True(t, relationExists(t, ctx, clients, seedReloadViewKey), "a stopped load drops nothing")
	assert.True(t, relationExists(t, ctx, clients, seedReloadOutsideMatView), "a stopped load drops nothing")
	t.Logf("✅ run %s stopped the seed on %s and %s", stoppedRun, seedReloadOutsideView, seedReloadOutsideMatView)

	// ── 4. the acknowledged full refresh ─────────────────────────────────────
	var triggered struct {
		RunID string `json:"run_id"`
	}
	runCLI(t, ctx, bin, env, &triggered, "node", "full-refresh", seedReloadService, seedSchemaName, "seed_table_1",
		"--acknowledge", seedReloadOutsideView, "--acknowledge", seedReloadOutsideMatView)
	refreshRun, err := uuid.Parse(triggered.RunID)
	require.NoError(t, err, "continuo node full-refresh prints the run id: %q", triggered.RunID)
	waitForTasksToSucceed(t, ctx, clients, refreshRun, []string{"seed_table_1", "seed_table_1_view"}, 5*time.Minute)
	assert.False(t, relationExists(t, ctx, clients, seedReloadOutsideMatView), "the cascade dropped %s, as named", seedReloadOutsideMatView)
	assert.False(t, relationExists(t, ctx, clients, seedReloadOutsideView), "the cascade dropped %s, as named", seedReloadOutsideView)
	assert.True(t, relationExists(t, ctx, clients, seedReloadViewKey), "the run rebuilt %s", seedReloadViewKey)
	t.Logf("✅ acknowledged full refresh %s dropped the named objects and rebuilt %s", refreshRun, seedReloadViewKey)
}

// cleanupSeedReload waits for releaseID to end its pipeline and for its runs
// to end, drops the objects the test made in the warehouse and restores the
// default topology. It runs on a context of its own, because the test's is
// cancelled by the time a cleanup runs.
func cleanupSeedReload(t *testing.T, clients *testClients, releaseID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	// A release that promotes after the restore would be the live release of
	// the tests that follow.
	waitForReleaseToEnd(t, ctx, clients, releaseID, 10*time.Minute)
	// A run still in flight would build the view again after the drop.
	waitForReleaseRunsToEnd(t, ctx, clients, releaseID, 5*time.Minute)
	if _, err := clients.dbtDB.ExecContext(ctx, `
		DROP SCHEMA IF EXISTS `+seedReloadOutsideSchema+` CASCADE;
		DROP VIEW IF EXISTS `+seedReloadViewKey); err != nil {
		t.Errorf("cleanup: drop %s and %s: %v", seedReloadOutsideSchema, seedReloadViewKey, err)
	}
	restoreDefaultTopology(t, ctx, clients)
}

// releaseInFlight holds the statuses of a release whose pipeline has not ended.
var releaseInFlight = map[string]bool{
	"received": true, "compiling": true, "parsing": true, "seed_building": true, "validating": true,
}

// waitForReleaseToEnd waits until releaseID is no longer in its pipeline:
// promoted, rejected, superseded, or never recorded. It returns without
// failing when it still is after timeout.
func waitForReleaseToEnd(t *testing.T, ctx context.Context, clients *testClients, releaseID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		status, found, err := readReleaseStatus(ctx, clients, releaseID)
		if err == nil && (!found || !releaseInFlight[status]) {
			return
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Logf("release %s still %q after %s (last read error: %v)", releaseID, status, timeout, err)
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(3 * time.Second):
		}
	}
}

// readReleaseStatus reads releaseID's status from release-controller's
// GET /releases/{id}; found is false when release-controller has no such release.
func readReleaseStatus(ctx context.Context, clients *testClients, releaseID string) (status string, found bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/releases/%s", clients.releaseBase, releaseID), nil)
	if err != nil {
		return "", false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNotFound:
		return "", false, nil
	case http.StatusOK:
	default:
		return "", false, fmt.Errorf("GET /releases/%s answered %d", releaseID, resp.StatusCode)
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", true, fmt.Errorf("decode GET /releases/%s: %w", releaseID, err)
	}
	return body.Status, true, nil
}

// waitForReleaseRunsToEnd waits until no run of releaseID is active. It
// returns without failing when one still is after timeout.
func waitForReleaseRunsToEnd(t *testing.T, ctx context.Context, clients *testClients, releaseID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var active int
		err := clients.runControllerDB.GetContext(ctx, &active, `
			SELECT count(*) FROM runs
			 WHERE tenant_id = $1 AND release_id = $2 AND status IN ('pending', 'running', 'cancelling')`,
			events.DefaultTenantID, releaseID)
		if err == nil && active == 0 {
			return
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Logf("%d run(s) of release %s still active after %s (last read error: %v)", active, releaseID, timeout, err)
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(3 * time.Second):
		}
	}
}

// setLoadedSeedVersion records in run-controller's node_materializations that
// seed_table_1 was last loaded at contentHash with the column fingerprint
// columnsHash, and that no load of it is requested.
func setLoadedSeedVersion(t *testing.T, ctx context.Context, clients *testClients, contentHash, columnsHash string) {
	t.Helper()
	_, err := clients.runControllerDB.ExecContext(ctx, `
		INSERT INTO node_materializations (tenant_id, node_key, content_hash, seed_columns_hash)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (tenant_id, node_key) DO UPDATE
		SET content_hash = EXCLUDED.content_hash,
		    seed_columns_hash = EXCLUDED.seed_columns_hash,
		    attempt_content_hash = '',
		    attempt_run_id = NULL,
		    attempted_at = NULL`,
		events.DefaultTenantID, seedReconcileNodeKey, contentHash, columnsHash)
	require.NoError(t, err, "record the loaded version of %s", seedReconcileNodeKey)
}

// waitForReleaseNotices polls release-controller's GET /releases/{id} until
// the release has a notice and returns them. A release that is promoted or
// rejected with no notice fails at once.
func waitForReleaseNotices(t *testing.T, ctx context.Context, clients *testClients, releaseID string, timeout time.Duration) []seedReloadNotice {
	t.Helper()
	var body struct {
		Status       string             `json:"status"`
		RejectReason string             `json:"reject_reason"`
		Notices      []seedReloadNotice `json:"notices"`
	}
	pollUntilDescribed(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/releases/%s", clients.releaseBase, releaseID), nil)
		if err != nil {
			return false, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false, nil
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false, nil
		}
		raw, err := io.ReadAll(resp.Body)
		if err != nil || json.Unmarshal(raw, &body) != nil {
			return false, nil
		}
		if len(body.Notices) > 0 {
			return true, nil
		}
		// release-controller writes a release's notices before it promotes or
		// rejects it, so an ended release with none never gets one.
		switch body.Status {
		case "rejected":
			t.Fatalf("release %s rejected with no notice: reason=%q", releaseID, body.RejectReason)
		case "promoted":
			t.Fatalf("release %s promoted with no notice", releaseID)
		}
		return false, nil
	}, func() string {
		return fmt.Sprintf("timeout waiting for release %s to carry a notice (status %q)", releaseID, body.Status)
	})
	return body.Notices
}

// assertSeedColumnNotice asserts n is the notice of seed_table_1 losing its
// legacy column: a full refresh that drops the table, rebuilds the view and is
// predicted to go ahead. source names where n was read.
func assertSeedColumnNotice(t *testing.T, source string, n seedReloadNotice) {
	t.Helper()
	assert.Equal(t, seedReconcileNodeKey, n.NodeID, "%s: node_id", source)
	assert.Equal(t, "columns", n.Change, "%s: change", source)
	assert.Equal(t, []string{"legacy"}, n.Columns.Removed, "%s: columns.removed", source)
	assert.Equal(t, "full_refresh", n.Load.Operation, "%s: load.operation", source)
	assert.True(t, n.Load.DropsTable, "%s: load.drops_table", source)
	if assert.NotEmpty(t, n.Rebuilds, "%s: rebuilds", source) {
		assert.Equal(t, seedReloadViewKey, n.Rebuilds[0].NodeID, "%s: rebuilds[0].node_id", source)
	}
	assert.Equal(t, "reloads", n.Prediction, "%s: prediction", source)
}

// shownRun is the part of `continuo run show` this test reads.
type shownRun struct {
	Tasks []shownTask `json:"tasks"`
}

// shownTask is one task of `continuo run show`.
type shownTask struct {
	Node       string `json:"node"`
	Operation  string `json:"operation"`
	PlanReason string `json:"plan_reason"`
	Status     string `json:"status"`
}

// task returns the task of node, "<service>.<schema>.<table>", and fails the
// test when the run has none.
func (r shownRun) task(t *testing.T, node string) shownTask {
	t.Helper()
	for _, task := range r.Tasks {
		if task.Node == node {
			return task
		}
	}
	require.FailNowf(t, "task not found", "the run has no task of %s: %+v", node, r.Tasks)
	return shownTask{}
}

// showRun runs `continuo run show <runID>`.
func showRun(t *testing.T, ctx context.Context, bin string, env []string, runID uuid.UUID) shownRun {
	t.Helper()
	var shown shownRun
	runCLI(t, ctx, bin, env, &shown, "run", "show", runID.String())
	return shown
}

// waitForTasksToSucceed waits until runID's task of each of tables has
// succeeded. A task that fails, is skipped or is cancelled fails the test at
// once, naming its latest attempt's failure class, reason and message.
func waitForTasksToSucceed(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID, tables []string, timeout time.Duration) {
	t.Helper()
	statuses := map[string]string{}
	pollUntilDescribed(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		done := true
		for _, table := range tables {
			status := runTaskStatus(ctx, clients, runID, table)
			statuses[table] = status
			switch status {
			case "succeeded":
			case "failed", "skipped", "cancelled":
				var class, reason, message string
				if status == "failed" {
					a := latestTaskAttempt(t, ctx, clients, runID, table)
					class, reason, message = a.GetFailureClass(), a.GetFailureReason(), a.GetErrorMessage()
				}
				t.Fatalf("task %s of run %s ended %s (failure_class=%q failure_reason=%q error_message=%q); task statuses: %v",
					table, runID, status, class, reason, message, statuses)
			default:
				done = false
			}
		}
		return done, nil
	}, func() string {
		return fmt.Sprintf("timeout waiting for the tasks %v of run %s to succeed: %v", tables, runID, statuses)
	})
	t.Logf("✅ tasks %v of run %s succeeded", tables, runID)
}

// runTaskStatus reads the status of runID's task of table, "" when it has none.
func runTaskStatus(ctx context.Context, clients *testClients, runID uuid.UUID, table string) string {
	var status string
	if err := clients.runControllerDB.GetContext(ctx, &status,
		`SELECT status FROM tasks WHERE run_id = $1 AND table_name = $2`, runID, table); err != nil {
		return ""
	}
	return status
}

// latestTaskAttempt returns the latest attempt of runID's task of table, as
// ListRunAttempts serves it.
func latestTaskAttempt(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID, table string) *runv1.TaskAttempt {
	t.Helper()
	var taskID uuid.UUID
	require.NoError(t, clients.runControllerDB.GetContext(ctx, &taskID,
		`SELECT task_id FROM tasks WHERE run_id = $1 AND table_name = $2`, runID, table))
	resp, err := clients.runClient.ListRunAttempts(ctx, &runv1.ListRunAttemptsRequest{
		RunId: runID.String(), TaskId: taskID.String(), PageSize: 200,
	})
	require.NoError(t, err, "ListRunAttempts(%s)", runID)
	var latest *runv1.TaskAttempt
	for _, a := range resp.GetAttempts() {
		if latest == nil || a.GetAttempt() > latest.GetAttempt() {
			latest = a
		}
	}
	require.NotNil(t, latest, "the task of %s in run %s has an attempt", table, runID)
	return latest
}

// relationExists reports whether name, "<schema>.<name>", is a relation in
// the warehouse.
func relationExists(t *testing.T, ctx context.Context, clients *testClients, name string) bool {
	t.Helper()
	var exists bool
	require.NoError(t, clients.dbtDB.GetContext(ctx, &exists, `SELECT to_regclass($1) IS NOT NULL`, name))
	return exists
}
