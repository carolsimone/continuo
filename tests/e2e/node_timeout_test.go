package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const (
	// timeoutOnlyReleaseService is the service the release is posted for. Its
	// own code does not change.
	timeoutOnlyReleaseService = "service-1"

	// timeoutOnlyDeclaringService owns the node that declares the timeout. Its
	// manifest is staged under its production pointer rather than under the
	// released service's key, because a release's compile Job overwrites
	// <service>/<release_id>/manifest.json for the service it compiles.
	timeoutOnlyDeclaringService = "service-2"

	// timeoutOnlyNodeKey is the declaring node's key in state's topology.
	timeoutOnlyNodeKey = ftableEUniqueID

	// timeoutOnlyMinutes is the declared meta.timeout_minutes, and
	// timeoutOnlySeconds the same value as the topology carries it.
	timeoutOnlyMinutes = 480
	timeoutOnlySeconds = timeoutOnlyMinutes * 60
)

// TestE2E_ReleasePromote_ATimeoutOnlyChangePromotesWithoutValidation proves
// that declaring a node's task timeout is not a code change:
//
//	service-2's production manifest is a copy of its baseline whose ftable_e
//	declares config.meta.timeout_minutes = 480 and nothing else
//	→ POST /releases (service-1, unchanged code) → compile → topology-controller
//	  writes the assembled topology with timeout_seconds = 28800 on ftable_e
//	→ release-controller: every content_hash equals current_prod's, so no node
//	  is changed and the release promotes with no validation.requested:v1 and
//	  no validation Job
//	→ release.promoted:v2 → state stores the release's nodes, ftable_e carrying
//	  28800 and every other node NULL.
//
// meta is outside the content hash, so the seeded current_prod holds the
// baseline's own hashes for every node.
func TestE2E_ReleasePromote_ATimeoutOnlyChangePromotesWithoutValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// compile + parse + promote (10m) + state applying the promotion (2m) +
	// the no-validation grace window, with headroom.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-timeout-" + uuid.NewString()[:8]
	t.Logf("release_id=%s released_service=%s timeout_node=%s (service %s)",
		releaseID, timeoutOnlyReleaseService, timeoutOnlyNodeKey, timeoutOnlyDeclaringService)

	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices, "no baseline manifests in S3 — setup.sh must run before the e2e suite")
	released, ok := allServices[timeoutOnlyReleaseService]
	require.True(t, ok, "%s has no baseline manifest", timeoutOnlyReleaseService)
	require.NotEmpty(t, released.imageTag)
	declaring, ok := allServices[timeoutOnlyDeclaringService]
	require.True(t, ok, "%s has no baseline manifest", timeoutOnlyDeclaringService)

	// Production holds the baseline's own hash for every node, the declaring
	// node included.
	var prodNodes []map[string]string
	for svc, si := range allServices {
		for _, n := range si.nodes {
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"service":      svc,
				"content_hash": n.contentHash,
			})
		}
	}

	// Tap the stream before the release is posted: the trim loop removes
	// consumed entries, so the check below reads what the tap recorded.
	validationTap := startStreamTap(t, ctx, clients.redisClient, streams.ValidationRequestedV1)

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProd(t, ctx, clients, allServices)

	// Copy service-2's baseline manifest, declare the timeout on one model, and
	// pin service-2's production pointer at the copy so the assembled set the
	// release parses carries it.
	stagedReleaseID := releaseID + "-src"
	stagedKey := fmt.Sprintf("%s/%s/manifest.json", timeoutOnlyDeclaringService, stagedReleaseID)
	baselineKey := fmt.Sprintf("%s/%s/manifest.json", timeoutOnlyDeclaringService, e2eBaselineReleaseID)
	putS3Object(t, ctx, clients, stagedKey, manifestDeclaringTimeout(t, getS3Object(t, ctx, clients, baselineKey)))
	// Put the pointer back and drop the staged manifest so no later test reads
	// the declared timeout. Deferred after clients.close(ctx) above, so LIFO
	// runs them while the pools are open.
	defer func() {
		_, err := clients.s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(e2eS3Bucket),
			Key:    aws.String(stagedKey),
		})
		if err != nil {
			t.Logf("delete staged manifest s3://%s/%s: %v", e2eS3Bucket, stagedKey, err)
		}
	}()
	pinServiceProd(t, ctx, clients, timeoutOnlyDeclaringService, stagedReleaseID, declaring.imageTag)
	defer pinServiceProd(t, context.Background(), clients,
		timeoutOnlyDeclaringService, e2eBaselineReleaseID, declaring.imageTag)

	postRelease(t, clients, timeoutOnlyReleaseService, releaseID, released.imageTag, false)

	// No node differs from production, so the release promotes with nothing to
	// validate: no validation.requested:v1 and no validation Job.
	waitForReleasePromoted(t, ctx, clients, releaseID, 10*time.Minute)
	assertNoValidationJobsDispatched(t, ctx, clients, releaseID)
	for _, msg := range validationTap.Entries() {
		payload, _ := msg.Values["payload"].(string)
		var p struct {
			ReleaseID string `json:"release_id"`
		}
		if json.Unmarshal([]byte(payload), &p) == nil {
			require.NotEqual(t, releaseID, p.ReleaseID,
				"a timeout-only release must not request validation")
		}
	}

	// State's live release carries the declared timeout on that node and no
	// other node declares one.
	waitForNodeTimeoutSeconds(t, ctx, clients, releaseID, timeoutOnlyNodeKey, timeoutOnlySeconds)
	requireNoOtherNodeTimeout(t, ctx, clients, releaseID, timeoutOnlyNodeKey)
	t.Logf("✅ timeout-only release %s promoted without validation; %s carries timeout_seconds=%d",
		releaseID, timeoutOnlyNodeKey, timeoutOnlySeconds)
}

// waitForNodeTimeoutSeconds polls state until its live release is releaseID and
// nodeKey carries timeout_seconds = want. The poll retries a missing row (the
// release is not stored yet) silently; any other error and the last observed
// value are kept for the failure message.
func waitForNodeTimeoutSeconds(t *testing.T, ctx context.Context, clients *testClients, releaseID, nodeKey string, want int64) {
	t.Helper()
	var (
		declared sql.NullInt64
		lastErr  error
		observed = "no row yet"
	)
	deadline := time.Now().Add(2 * time.Minute)
	for {
		err := clients.stateDB.GetContext(ctx, &declared, `
			SELECT n.timeout_seconds
			  FROM live_release l
			  JOIN topology_nodes n ON n.tenant_id = l.tenant_id AND n.release_id = l.release_id
			 WHERE l.tenant_id = $1 AND l.release_id = $2 AND n.node_key = $3`,
			events.DefaultTenantID, releaseID, nodeKey)
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			lastErr = err
		default:
			lastErr = nil
			observed = "timeout_seconds=NULL"
			if declared.Valid {
				observed = fmt.Sprintf("timeout_seconds=%d", declared.Int64)
			}
			if declared.Valid && declared.Int64 == want {
				return
			}
		}
		if time.Now().After(deadline) {
			require.FailNow(t, fmt.Sprintf(
				"timeout waiting for state's live release %s to carry timeout_seconds=%d on %s: last observed %s, last query error: %v",
				releaseID, want, nodeKey, observed, lastErr))
		}
		select {
		case <-ctx.Done():
			require.FailNow(t, "context done while waiting for the declared timeout: "+ctx.Err().Error())
		case <-time.After(2 * time.Second):
		}
	}
}

// requireNoOtherNodeTimeout asserts that, in state's copy of releaseID, no node
// other than nodeKey carries a timeout.
func requireNoOtherNodeTimeout(t *testing.T, ctx context.Context, clients *testClients, releaseID, nodeKey string) {
	t.Helper()
	var others int
	require.NoError(t, clients.stateDB.GetContext(ctx, &others, `
		SELECT count(*)
		  FROM live_release l
		  JOIN topology_nodes n ON n.tenant_id = l.tenant_id AND n.release_id = l.release_id
		 WHERE l.tenant_id = $1 AND l.release_id = $2
		   AND n.node_key <> $3 AND n.timeout_seconds IS NOT NULL`,
		events.DefaultTenantID, releaseID, nodeKey))
	require.Zero(t, others, "no node other than %s declares a timeout", nodeKey)
}

// manifestDeclaringTimeout returns body, a dbt manifest.json, re-encoded with
// config.meta.timeout_minutes set on the timeoutOnlyNodeKey model and no other
// change. Numbers round-trip exactly (decodeJSONDoc keeps json.Number), so every
// other byte of every node's content stays what it was.
func manifestDeclaringTimeout(t *testing.T, body []byte) []byte {
	t.Helper()
	doc, err := decodeJSONDoc(body)
	require.NoError(t, err, "parse manifest.json")
	found := false
	for _, nv := range asObjectMap(doc["nodes"]) {
		n, ok := nv.(map[string]interface{})
		if !ok {
			continue
		}
		schema, _ := n["schema"].(string)
		name, _ := n["name"].(string)
		if resourceType, _ := n["resource_type"].(string); resourceType != "model" || schema+"."+name != timeoutOnlyNodeKey {
			continue
		}
		config, ok := n["config"].(map[string]interface{})
		require.True(t, ok, "%s must carry a config object", timeoutOnlyNodeKey)
		meta, ok := config["meta"].(map[string]interface{})
		require.True(t, ok, "%s must carry config.meta (its owner lives there)", timeoutOnlyNodeKey)
		require.NotContains(t, meta, "timeout_minutes", "%s must not already declare a timeout", timeoutOnlyNodeKey)
		meta["timeout_minutes"] = json.Number(fmt.Sprint(timeoutOnlyMinutes))
		found = true
	}
	require.True(t, found, "%s must exist in the %s baseline manifest", timeoutOnlyNodeKey, timeoutOnlyDeclaringService)
	out, err := json.Marshal(doc)
	require.NoError(t, err, "encode manifest.json")
	return out
}
