package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// verifyUIHTTPAPI asserts the ui HTTP API returns correct data for the
// run created by the e2e test.  Status fields are polled because the
// run-controller service propagates job-completion events asynchronously after the k8s
// jobs finish (execution-controller → gRPC → run-controller service → DB), so the scheduler
// and task records may still show "pending"/"running" immediately after
// verifyFullDAGExecution returns.
//
// The function is a no-op when UI_HTTP_BASE is unset so developer runs without
// the UI container are not broken.
func verifyUIHTTPAPI(t *testing.T, ctx context.Context, runID string) {
	t.Helper()

	base := os.Getenv("UI_HTTP_BASE")
	if base == "" {
		t.Log("UI_HTTP_BASE not set — skipping ui HTTP assertions")
		return
	}

	t.Logf("Verifying ui at %s (run_id=%s)...", base, runID)

	// ── GET /api/runs/:id — verify specific run is present and succeeded ─
	// verifySchedulerSucceeded already confirmed the run-controller DB shows "succeeded"
	// before this function is called; the UI check confirms the HTTP API also
	// surfaces the correct status.
	runBody := mustGetJSON(t, fmt.Sprintf("%s/api/runs/%s", base, runID))

	completedAt, _ := runBody["completed_at"].(string)
	assert.True(t, isISOTimestamp(completedAt),
		"run completed_at %q should be an ISO-8601 timestamp", completedAt)
	assert.Equal(t, "succeeded", runBody["status"],
		"run status should be 'succeeded'")
	assert.Equal(t, runID, runBody["run_id"], "the run is the one asked for")

	t.Logf("✅ ui /api/runs/%s: present (status=%v)", runID, runBody["status"])

	// ── GET /api/schedules — verify the run's schedule names it as its last run ─
	schedulesBody := mustGetJSON(t, base+"/api/schedules")
	schedulesList, ok := schedulesBody["schedules"].([]interface{})
	require.True(t, ok, "GET /api/schedules: 'schedules' field missing or wrong type")

	var foundSummary map[string]interface{}
	for _, item := range schedulesList {
		s, _ := item.(map[string]interface{})
		if s["last_run_id"] == runID {
			foundSummary = s
			break
		}
	}
	require.NotNil(t, foundSummary, "GET /api/schedules: schedule with last_run_id=%q not found", runID)
	assert.Equal(t, "succeeded", foundSummary["last_run_status"],
		"schedule last_run_status should be 'succeeded'")

	t.Logf("✅ ui /api/schedules: schedule %q present (last_run_status=%v)", foundSummary["schedule_name"], foundSummary["last_run_status"])

	// ── Poll GET /api/runs/:id/tasks until all tasks show "succeeded" ──
	var tasksList []interface{}

	pollUntil(t, ctx, 60*time.Second, 2*time.Second, func() (bool, error) {
		body, err := getJSON(fmt.Sprintf("%s/api/runs/%s/tasks", base, runID))
		if err != nil {
			return false, err
		}
		list, ok := body["tasks"].([]interface{})
		if !ok || len(list) == 0 {
			return false, nil
		}
		for _, item := range list {
			task, _ := item.(map[string]interface{})
			if task["status"] != "succeeded" {
				return false, nil
			}
		}
		tasksList = list
		return true, nil
	}, fmt.Sprintf("Timeout waiting for all tasks of run %s to reach status=succeeded via ui", runID))

	require.NotEmpty(t, tasksList)

	for _, item := range tasksList {
		task, _ := item.(map[string]interface{})
		taskCreatedAt, _ := task["created_at"].(string)
		assert.True(t, isISOTimestamp(taskCreatedAt),
			"task created_at %q should be an ISO-8601 timestamp", taskCreatedAt)
	}

	t.Logf("✅ ui /api/runs/%s/tasks: %d tasks present, all succeeded", runID, len(tasksList))

	// ── GET /api/releases — verify the commit-author enrichment resolves ──
	// ui resolves each release's author from its (repo, commit_sha) via GitHub;
	// in e2e that points at stub-github, which returns a deterministic login.
	// This is the end-to-end check that release-controller's provenance fields,
	// the ui resolver, and the stub's REST commits endpoint all line up — a
	// stub missing that endpoint would leave every author empty. Must match
	// stubAuthorLogin in tests/e2e/stub-github/main.go.
	const stubAuthorLogin = "continuo-e2e"
	releasesBody := mustGetJSON(t, base+"/api/releases")
	releasesList, ok := releasesBody["releases"].([]interface{})
	require.True(t, ok, "GET /api/releases: 'releases' field missing or wrong type")
	require.NotEmpty(t, releasesList, "GET /api/releases: expected at least one release")

	var authoredLogin string
	for _, item := range releasesList {
		rel, _ := item.(map[string]interface{})
		author, _ := rel["author"].(map[string]interface{})
		if login, _ := author["login"].(string); login != "" {
			authoredLogin = login
			break
		}
	}
	assert.Equal(t, stubAuthorLogin, authoredLogin,
		"GET /api/releases: at least one release should carry the enriched commit-author login")

	t.Logf("✅ ui /api/releases: commit-author enrichment resolved login=%q", authoredLogin)
}

// getJSON performs a GET request and returns the parsed JSON body.
// Returns an error on any network, HTTP, or parse failure (safe to use inside pollUntil).
func getJSON(url string) (map[string]interface{}, error) {
	resp, err := http.Get(url) //nolint:noctx,gosec // getJSON is only ever called with internally-built ui base-URL requests from this harness, not external input
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading body from GET %s: %w", url, err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("unmarshalling JSON from GET %s: %w", url, err)
	}
	return result, nil
}

// mustGetJSON wraps getJSON and fails the test immediately on any error.
func mustGetJSON(t *testing.T, url string) map[string]interface{} {
	t.Helper()
	result, err := getJSON(url)
	require.NoError(t, err)
	return result
}

// isISOTimestamp returns true if s looks like an ISO-8601 / RFC-3339 timestamp
// (e.g. "2024-01-02T15:04:05.999Z").
func isISOTimestamp(s string) bool {
	if len(s) < 20 {
		return false
	}
	// Must contain 'T' separator and end with 'Z' or '+HH:MM'
	return strings.Contains(s, "T") && (strings.HasSuffix(s, "Z") || strings.Contains(s[len(s)-6:], ":"))
}
