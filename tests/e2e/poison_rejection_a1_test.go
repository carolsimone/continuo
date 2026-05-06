package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPoisonRejection_A1_NoSidecar verifies the manifest-controller's
// publish-boundary validator (PR #33 A1) refuses to publish manifest.loaded:v1
// when any service's sidecar is missing — and ACKs the triggering
// update.graph:v1 message so the Redis pending list stays clean.
func TestPoisonRejection_A1_NoSidecar(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)

	// Snapshot baselines — manifest.loaded:v1 last ID + mc.log byte size.
	// We assert after the trigger that NEITHER advances by a non-rejection event.
	manifestEntries, err := clients.redisClient.XRevRangeN(ctx, "manifest.loaded:v1", "+", "-", 1).Result()
	require.NoError(t, err, "XRevRangeN manifest.loaded:v1 failed")
	lastManifestID := "0-0"
	if len(manifestEntries) > 0 {
		lastManifestID = manifestEntries[0].ID
	}

	logBaselineBytes := mcLogSize(t)
	t.Logf("baseline: lastManifestID=%s mcLogBytes=%d", lastManifestID, logBaselineBytes)

	// Create the poison condition: delete service-1's sidecar from S3.
	// service-1 was chosen arbitrarily — A1's all-or-nothing semantics mean any
	// single missing sidecar fails the whole batch.
	deleteS3Sidecar(t, "service-1")
	t.Cleanup(func() {
		// Restore the sidecar by re-running dbt_upload load so subsequent tests
		// (and any later interactive use) aren't broken.
		//
		// IMPORTANT: do NOT pass `-e IMAGE_TAG_PER_SERVICE=...=latest`. The
		// dbt-compile-and-load container already has the correct
		// IMAGE_TAG_PER_SERVICE injected by docker-compose (line 481), which
		// in CI / local setup.sh is the SHA-pinned tag of the actually-built
		// images. Overriding it to `latest` here writes
		// `image_tag=latest` into all three sidecars, which then propagates
		// through manifest.loaded:v1 → executor-controller → K8s job spec
		// `service-1:latest`. The kind cluster only has SHA-tagged images
		// loaded, so every subsequent dbt-job pod fails with ErrImagePull,
		// breaking all later tests that wait for tasks to succeed.
		restoreCmd := exec.Command("docker", "exec",
			"dbt-compile-and-load",
			"uv", "run", "python", "-m", "dbt_upload", "load",
			"--target", "localstack",
			"--services-dir", "/app/services")
		out, err := restoreCmd.CombinedOutput()
		if err != nil {
			t.Logf("WARNING: failed to restore sidecars in cleanup: %v\n%s", err, string(out))
		} else {
			t.Logf("restored sidecars after test")
		}
	})

	// Trigger update.graph:v1 via the ui-service HTTP endpoint.
	resp, err := http.Post(
		fmt.Sprintf("%s/api/graph/update", clients.uiBase),
		"application/json",
		strings.NewReader(`{"source":"s3"}`),
	)
	require.NoError(t, err, "POST /api/graph/update failed")
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"POST /api/graph/update: expected 200, got %d: %s", resp.StatusCode, string(body))
	t.Log("Published update.graph:v1 — waiting for manifest-controller to log manifest_publish_rejected...")

	// Poll: manifest_publish_rejected appears in NEW log content (after baseline).
	// 90s timeout (not 30s) tolerates the manifest-controller's known consumer
	// reconnect flake where the Redis consumer group occasionally loses
	// registration and re-creates it (~3-5s blip). With 18 reconnect cycles
	// allowed the test survives all but a permanent stall.
	pollUntil(t, ctx, 90*time.Second, 1*time.Second, func() (bool, error) {
		return mcLogTailContains(t, logBaselineBytes, "manifest_publish_rejected"), nil
	}, "Timeout waiting for manifest-controller to log 'manifest_publish_rejected' (after baseline)")
	t.Log("✅ manifest-controller logged manifest_publish_rejected")

	// Wait an extra 5s for the consumer to ACK round-trip, then assert.
	time.Sleep(5 * time.Second)

	// Assert NO new manifest.loaded:v1 message was published in the window.
	newMsgs, err := clients.redisClient.XRange(ctx, "manifest.loaded:v1", "("+lastManifestID, "+").Result()
	require.NoError(t, err, "XRange manifest.loaded:v1 failed")
	assert.Empty(t, newMsgs,
		"manifest-controller must NOT publish manifest.loaded:v1 when A1 rejects (got %d new messages)", len(newMsgs))
	t.Log("✅ no new manifest.loaded:v1 was published")

	// Assert update.graph:v1 was ACKed (pending count = 0 for the consumer group).
	pending := xpending(t, ctx, clients, "update.graph:v1", "manifest-controller")
	assert.Equal(t, int64(0), pending,
		"update.graph:v1 must be ACKed even when A1 rejects (got pending=%d)", pending)
	t.Log("✅ update.graph:v1 ACKed (pending = 0)")
}

// mcLogSize returns the current byte size of /tmp/mc.log inside the
// manifest-controller container. Used as a baseline so log-substring assertions
// can scope to NEW lines only.
func mcLogSize(t *testing.T) int {
	t.Helper()
	out, err := exec.Command("docker", "exec", "manifest-controller",
		"stat", "-c", "%s", "/tmp/mc.log").Output()
	require.NoError(t, err, "stat /tmp/mc.log failed")
	n, err := strconv.Atoi(strings.TrimSpace(string(out)))
	require.NoError(t, err, "parsing mc.log size failed")
	return n
}

// mcLogTailContains returns true if substr appears in /tmp/mc.log AFTER
// sinceBytes (i.e. only in content written since the baseline snapshot).
// Uses tail -c +N+1 which is the "1-indexed start byte" form supported by
// GNU coreutils inside the manifest-controller container.
func mcLogTailContains(t *testing.T, sinceBytes int, substr string) bool {
	t.Helper()
	out, err := exec.Command("docker", "exec", "manifest-controller",
		"tail", "-c", "+"+strconv.Itoa(sinceBytes+1), "/tmp/mc.log").Output()
	if err != nil {
		t.Logf("docker exec manifest-controller tail failed: %v", err)
		return false
	}
	return strings.Contains(string(out), substr)
}
