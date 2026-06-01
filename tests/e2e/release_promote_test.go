package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// e2eS3Bucket is the LocalStack bucket dbt manifests are uploaded to in the e2e
// environment (created by localstack/init/init-s3.sh, written by setup.sh's
// `dbt_upload load --target localstack`).
const e2eS3Bucket = "continuo"

// probeUniqueID is the unique_id manifest-controller derives for the rel_probe
// model (schema_name.table_name). rel_probe is a self-contained leaf
// (`SELECT 1`) on its own "rel-probe" schedule, so it validates cleanly in an
// isolated candidate schema without depending on any production table.
const probeUniqueID = "e2e_schema.rel_probe"

// TestE2E_ReleasePromote_ValidatesAndSwapsTopology drives the blue/green
// release pipeline end-to-end through the production cutover path:
//
//	POST /releases → release.requested:v1 → manifest-controller candidate parse
//	→ manifest.loaded.candidate:v1 → release-controller derives the changed-node
//	set → validation.requested:v1 → executor/k8s run a real dbt --empty job →
//	validation.completed:v1 → release-controller promotes → release.promoted:v1
//	→ orchestrator swaps the Neo4j topology.
//
// To keep validation bounded and deterministic the test pre-seeds current_prod
// with every candidate node EXCEPT rel_probe, so the derived changed set is
// exactly {rel_probe}: one known-good node that validates without touching any
// production table. The release then promotes and the Neo4j swap is asserted.
func TestE2E_ReleasePromote_ValidatesAndSwapsTopology(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-rel-" + uuid.NewString()[:8]
	t.Logf("release_id=%s", releaseID)

	// 1. Read the legacy manifests already in S3 (uploaded by setup.sh), build
	//    the prod snapshot of every candidate node except rel_probe, and copy the
	//    manifests into this release's per-service prefix for the candidate parse.
	manifestKeys := listLatestManifestKeys(t, ctx, clients)
	require.NotEmpty(t, manifestKeys,
		"no manifests under s3://%s/local/manifest/ — setup.sh dbt upload must run first", e2eS3Bucket)

	var prodNodes []map[string]string
	imageTags := map[string]string{}
	probeFound := false
	for service, key := range manifestKeys {
		// The deployed image tag is content-addressed (built and loaded into kind
		// by setup.sh); it lives in the per-service service_metadata.json sidecar.
		// The validation Job pulls service:<tag>, so the POST body must carry the
		// exact tag that kind has — not a hardcoded "latest".
		imageTags[service] = readServiceImageTag(t, ctx, clients, service)
		for _, n := range parseManifestNodes(t, getS3Object(t, ctx, clients, key)) {
			if n.uniqueID == probeUniqueID {
				probeFound = true
				continue // exclude → rel_probe becomes the sole changed node
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"content_hash": n.contentHash,
			})
		}
		dst := fmt.Sprintf("releases/%s/manifests/%s/manifest_v1.json", releaseID, service)
		copyS3Object(t, ctx, clients, key, dst)
	}
	require.True(t, probeFound,
		"rel_probe not found in any manifest — is the model in service-1 and the image rebuilt?")
	t.Logf("seeded prod snapshot with %d nodes (rel_probe excluded)", len(prodNodes))

	// 2. Free the release queue from any prior run and seed current_prod with the
	//    baseline-minus-probe snapshot, so the derived changed set is exactly the
	//    rel_probe node — which builds into the candidate schema in isolation.
	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)

	// 3. POST /releases — the exact request CI's deploy.yml issues.
	postRelease(t, clients, releaseID, imageTags,
		fmt.Sprintf("s3://%s/releases/%s/manifests/", e2eS3Bucket, releaseID))

	// 4. The derived validation set must be exactly the one changed node.
	assertValidationRequestedNodes(t, ctx, clients, releaseID, []string{probeUniqueID})

	// 5. A real dbt validation job runs for rel_probe; on success the release
	//    promotes. A rejection fails the test immediately with the reason.
	waitForReleasePromoted(t, ctx, clients, releaseID, 10*time.Minute)

	// 6. The orchestrator must have swapped the Neo4j topology to this release.
	//    The swap is asynchronous relative to promotion: GET /releases reports
	//    "promoted" the moment release-controller commits, but the topology swap
	//    only lands after the release.promoted:v1 outbox row is published and the
	//    orchestrator consumes it — so poll rather than asserting once.
	waitForTopologySwap(t, ctx, clients, releaseID, probeUniqueID, 2*time.Minute)
	t.Log("✅ release validated, promoted, and Neo4j topology swapped to the new release")
}

// probeUpUniqueID and probeDownUniqueID are the unique_ids manifest-controller
// derives for the rel_probe_up / rel_probe_down models. rel_probe_up is a
// self-contained leaf (`SELECT 1`); rel_probe_down is `SELECT id FROM
// {{ ref('rel_probe_up') }}` — an intra-service ref, so manifest-controller
// derives rel_probe_down's upstream = rel_probe_up (same service-1). The pair
// exercises the gated intra-service path: rel_probe_up builds into the candidate
// schema first, then rel_probe_down validates against it.
const (
	probeUpUniqueID   = "e2e_schema.rel_probe_up"
	probeDownUniqueID = "e2e_schema.rel_probe_down"
)

// ftableD / ftableC are an existing cross-service edge used by the rejection
// test: ftable_d (service-2) reads raw `FROM e2e_schema.ftable_c`, and ftable_c
// lives in service-3 — so manifest-controller resolves ftable_d's upstream to a
// different-service node. Seeding current_prod WITHOUT ftable_c makes ftable_c a
// NEW cross-service upstream of the changed ftable_d, which the release-controller
// must reject early with reason new_cross_service_upstream.
const (
	ftableDUniqueID = "e2e_schema.ftable_d"
	ftableCUniqueID = "e2e_schema.ftable_c"
)

// TestE2E_ReleasePromote_GatedIntraServiceUpstream proves that when a changed
// node depends on a CHANGED intra-service upstream, the upstream builds into the
// candidate schema first (topologically gated) and the dependent validates
// against it; the release then promotes and the candidate schema is torn down.
//
// current_prod is seeded with every candidate node EXCEPT rel_probe_up and
// rel_probe_down. The derived changed set is therefore exactly
// {rel_probe_up, rel_probe_down} with the intra-service gating edge
// rel_probe_down → rel_probe_up. Both validate cleanly with `dbt run --empty`.
func TestE2E_ReleasePromote_GatedIntraServiceUpstream(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-rel-" + uuid.NewString()[:8]
	t.Logf("release_id=%s", releaseID)

	// Build the prod snapshot of every candidate node except the rel_probe* chain
	// so the derived changed set is exactly {rel_probe_up, rel_probe_down}.
	excluded := map[string]bool{
		probeUpUniqueID:   true,
		probeDownUniqueID: true,
	}
	manifestKeys := listLatestManifestKeys(t, ctx, clients)
	require.NotEmpty(t, manifestKeys,
		"no manifests under s3://%s/local/manifest/ — setup.sh dbt upload must run first", e2eS3Bucket)

	var prodNodes []map[string]string
	imageTags := map[string]string{}
	var upFound, downFound bool
	for service, key := range manifestKeys {
		imageTags[service] = readServiceImageTag(t, ctx, clients, service)
		for _, n := range parseManifestNodes(t, getS3Object(t, ctx, clients, key)) {
			switch n.uniqueID {
			case probeUpUniqueID:
				upFound = true
			case probeDownUniqueID:
				downFound = true
			}
			if excluded[n.uniqueID] {
				continue // keep these out of prod → they become the changed set
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"content_hash": n.contentHash,
			})
		}
		dst := fmt.Sprintf("releases/%s/manifests/%s/manifest_v1.json", releaseID, service)
		copyS3Object(t, ctx, clients, key, dst)
	}
	require.True(t, upFound,
		"rel_probe_up not found in any manifest — is the model in service-1 and the image rebuilt + manifests re-uploaded?")
	require.True(t, downFound,
		"rel_probe_down not found in any manifest — is the model in service-1 and the image rebuilt + manifests re-uploaded?")
	t.Logf("seeded prod snapshot with %d nodes (rel_probe* chain excluded)", len(prodNodes))

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)

	postRelease(t, clients, releaseID, imageTags,
		fmt.Sprintf("s3://%s/releases/%s/manifests/", e2eS3Bucket, releaseID))

	// The derived validation set must be exactly the intra-service chain, in
	// topological order (upstream first).
	assertValidationRequestedNodes(t, ctx, clients, releaseID,
		[]string{probeUpUniqueID, probeDownUniqueID})

	// The gated upstream builds into the candidate schema first, the dependent
	// validates against it, and on success the release promotes.
	waitForReleasePromoted(t, ctx, clients, releaseID, 12*time.Minute)

	// The orchestrator must swap the Neo4j topology to this release; assert on
	// the dependent node so both chain members are present.
	waitForTopologySwap(t, ctx, clients, releaseID, probeDownUniqueID, 2*time.Minute)

	// After validation.completed:v1 the executor drops _candidate_<releaseID>
	// from the dbt warehouse. Teardown is asynchronous, so poll.
	assertCandidateSchemaDropped(t, ctx, clients, releaseID, 3*time.Minute)
	t.Log("✅ gated intra-service upstream validated, release promoted, topology swapped, candidate schema dropped")
}

// TestE2E_ReleaseReject_NewCrossServiceUpstream proves that a changed node whose
// cross-service upstream is NOT in prod causes the release to be rejected early
// with reason new_cross_service_upstream and NO validation jobs dispatched.
//
// ftable_d (service-2) reads raw `FROM e2e_schema.ftable_c`, and ftable_c lives
// in service-3. Seeding current_prod with every candidate node EXCEPT ftable_d
// and ftable_c makes ftable_d a changed node with a new cross-service upstream
// ftable_c (absent from prod) — the exact reject condition.
func TestE2E_ReleaseReject_NewCrossServiceUpstream(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-rel-" + uuid.NewString()[:8]
	t.Logf("release_id=%s", releaseID)

	excluded := map[string]bool{
		ftableDUniqueID: true,
		ftableCUniqueID: true,
	}
	manifestKeys := listLatestManifestKeys(t, ctx, clients)
	require.NotEmpty(t, manifestKeys,
		"no manifests under s3://%s/local/manifest/ — setup.sh dbt upload must run first", e2eS3Bucket)

	var prodNodes []map[string]string
	imageTags := map[string]string{}
	var dFound, cFound bool
	for service, key := range manifestKeys {
		imageTags[service] = readServiceImageTag(t, ctx, clients, service)
		for _, n := range parseManifestNodes(t, getS3Object(t, ctx, clients, key)) {
			switch n.uniqueID {
			case ftableDUniqueID:
				dFound = true
			case ftableCUniqueID:
				cFound = true
			}
			if excluded[n.uniqueID] {
				continue // ftable_d changed; ftable_c absent → new cross-service upstream
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"content_hash": n.contentHash,
			})
		}
		dst := fmt.Sprintf("releases/%s/manifests/%s/manifest_v1.json", releaseID, service)
		copyS3Object(t, ctx, clients, key, dst)
	}
	require.True(t, dFound, "ftable_d not found in any manifest")
	require.True(t, cFound, "ftable_c not found in any manifest")
	t.Logf("seeded prod snapshot with %d nodes (ftable_d + ftable_c excluded)", len(prodNodes))

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)

	postRelease(t, clients, releaseID, imageTags,
		fmt.Sprintf("s3://%s/releases/%s/manifests/", e2eS3Bucket, releaseID))

	// The release must end rejected with the cross-service reason.
	waitForReleaseRejected(t, ctx, clients, releaseID, "new_cross_service_upstream", 5*time.Minute)

	// And no validation Job may ever be dispatched for this release. The reject
	// happens before any validation.requested:v1, so no validate-* Job is created.
	assertNoValidationJobsDispatched(t, ctx, clients, releaseID)
	t.Log("✅ new cross-service upstream rejected, no validation jobs dispatched")
}

// requireReleaseControllerHealthy verifies the release-controller HTTP API is
// reachable before the test drives it.
func requireReleaseControllerHealthy(t *testing.T, clients *testClients) {
	t.Helper()
	resp, err := http.Get(clients.releaseBase + "/healthz")
	require.NoError(t, err, "release-controller /healthz unreachable at %s", clients.releaseBase)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "release-controller unhealthy")
}

// manifestNode is the minimal projection of a dbt manifest node needed to seed
// current_prod: the change-detector identity (unique_id) and fingerprint.
type manifestNode struct {
	uniqueID    string
	contentHash string
}

var manifestVersionRe = regexp.MustCompile(`^manifest_v(\d+)\.json$`)

// listLatestManifestKeys lists the legacy per-service manifests under
// local/manifest/<service>/ and returns the highest manifest_v{N}.json key per
// service (mirroring how S3Source resolves the newest version).
func listLatestManifestKeys(t *testing.T, ctx context.Context, clients *testClients) map[string]string {
	t.Helper()
	const prefix = "local/manifest/"
	out, err := clients.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(e2eS3Bucket),
		Prefix: aws.String(prefix),
	})
	require.NoError(t, err, "list manifests in S3")

	best := map[string]int{}
	keys := map[string]string{}
	for _, obj := range out.Contents {
		key := aws.ToString(obj.Key)
		rest := strings.TrimPrefix(key, prefix)
		parts := strings.Split(rest, "/")
		if len(parts) != 2 { // expect <service>/<file>
			continue
		}
		service, file := parts[0], parts[1]
		m := manifestVersionRe.FindStringSubmatch(file)
		if m == nil {
			continue
		}
		var n int
		fmt.Sscanf(m[1], "%d", &n)
		if n > best[service] {
			best[service] = n
			keys[service] = key
		}
	}
	return keys
}

// readServiceImageTag reads the content-addressed image tag from a service's
// service_metadata.json sidecar (written by the legacy dbt_upload path with the
// IMAGE_TAG_PER_SERVICE tags that setup.sh also baked into the kind images).
func readServiceImageTag(t *testing.T, ctx context.Context, clients *testClients, service string) string {
	t.Helper()
	key := fmt.Sprintf("local/manifest/%s/service_metadata.json", service)
	var meta struct {
		ImageTag string `json:"image_tag"`
	}
	require.NoError(t, json.Unmarshal(getS3Object(t, ctx, clients, key), &meta),
		"parse service_metadata.json for %s", service)
	require.NotEmpty(t, meta.ImageTag,
		"service_metadata.json for %s has empty image_tag — validation Job would fail to pull", service)
	return meta.ImageTag
}

// getS3Object downloads an object body from the e2e bucket.
func getS3Object(t *testing.T, ctx context.Context, clients *testClients, key string) []byte {
	t.Helper()
	resp, err := clients.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(e2eS3Bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "get S3 object %s", key)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read S3 object %s", key)
	return body
}

// copyS3Object copies an object to a new key within the e2e bucket.
func copyS3Object(t *testing.T, ctx context.Context, clients *testClients, srcKey, dstKey string) {
	t.Helper()
	_, err := clients.s3Client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(e2eS3Bucket),
		CopySource: aws.String(e2eS3Bucket + "/" + srcKey),
		Key:        aws.String(dstKey),
	})
	require.NoError(t, err, "copy S3 object %s -> %s", srcKey, dstKey)
}

// parseManifestNodes extracts the model/seed/snapshot nodes from a dbt
// manifest.json, mirroring manifest-controller's identity derivation:
// unique_id = "<schema>.<name>" and content_hash = checksum.checksum.
func parseManifestNodes(t *testing.T, body []byte) []manifestNode {
	t.Helper()
	var manifest struct {
		Nodes map[string]struct {
			ResourceType string `json:"resource_type"`
			Schema       string `json:"schema"`
			Name         string `json:"name"`
			Checksum     struct {
				Checksum string `json:"checksum"`
			} `json:"checksum"`
		} `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal(body, &manifest), "parse manifest.json")

	supported := map[string]bool{"model": true, "seed": true, "snapshot": true}
	var nodes []manifestNode
	for _, n := range manifest.Nodes {
		if !supported[n.ResourceType] || n.Checksum.Checksum == "" {
			continue
		}
		nodes = append(nodes, manifestNode{
			uniqueID:    n.Schema + "." + n.Name,
			contentHash: n.Checksum.Checksum,
		})
	}
	return nodes
}

// resetReleaseControllerQueue clears any release left mid-flight by a prior run
// (which would block AdvanceQueue) and any unpublished outbox rows that would
// otherwise re-fire events for now-deleted releases.
func resetReleaseControllerQueue(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()
	_, err := clients.releaseDB.ExecContext(ctx,
		`DELETE FROM releases WHERE status IN ('received','parsing','validating')`)
	require.NoError(t, err, "reset releases queue")
	_, err = clients.releaseDB.ExecContext(ctx,
		`DELETE FROM release_controller_outbox WHERE status = 'pending'`)
	require.NoError(t, err, "reset release outbox")
}

// seedCurrentProd writes the singleton current_prod row. Only unique_id and
// content_hash matter for the change-detector; other Node fields default to
// zero values on the release-controller side. release_id and manifests_uri are
// reset to empty for a clean baseline: manifests_uri is a record-keeping column
// (validation builds the candidate schema directly and never defers to a prior
// manifest), so clearing it just stops a prior promoted release's value from
// lingering across reused-stack runs.
func seedCurrentProd(t *testing.T, ctx context.Context, clients *testClients, nodes []map[string]string) {
	t.Helper()
	topoJSON, err := json.Marshal(nodes)
	require.NoError(t, err, "marshal current_prod snapshot")
	_, err = clients.releaseDB.ExecContext(ctx,
		`INSERT INTO current_prod (id, release_id, manifests_uri, topology_snapshot, updated_at)
		 VALUES (1, '', '', $1, now())
		 ON CONFLICT (id) DO UPDATE SET
		   release_id = EXCLUDED.release_id,
		   manifests_uri = EXCLUDED.manifests_uri,
		   topology_snapshot = EXCLUDED.topology_snapshot,
		   updated_at = EXCLUDED.updated_at`,
		topoJSON)
	require.NoError(t, err, "seed current_prod")
}

// postRelease issues the POST /releases request that CI's deploy.yml fires.
func postRelease(t *testing.T, clients *testClients, releaseID string, imageTags map[string]string, manifestsURI string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"release_id":    releaseID,
		"image_tags":    imageTags,
		"manifests_uri": manifestsURI,
	})
	require.NoError(t, err)

	resp, err := http.Post(clients.releaseBase+"/releases", "application/json", strings.NewReader(string(body)))
	require.NoError(t, err, "POST /releases")
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusAccepted, resp.StatusCode,
		"POST /releases: expected 202, got %d: %s", resp.StatusCode, string(respBody))
	t.Logf("POST /releases accepted: %s", string(respBody))
}

// assertValidationRequestedNodes waits for the validation.requested:v1 message
// for releaseID and asserts its node_ids_in_order matches the expected set.
// This proves the changed-node derivation produced exactly the intended scope.
func assertValidationRequestedNodes(t *testing.T, ctx context.Context, clients *testClients, releaseID string, want []string) {
	t.Helper()
	var got []string
	pollUntil(t, ctx, 3*time.Minute, 1*time.Second, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, streams.ValidationRequestedV1, "-", "+").Result()
		if err != nil {
			return false, nil
		}
		for _, msg := range msgs {
			payload, _ := msg.Values["payload"].(string)
			if payload == "" {
				continue
			}
			var p struct {
				ReleaseID      string   `json:"release_id"`
				NodeIDsInOrder []string `json:"node_ids_in_order"`
			}
			if json.Unmarshal([]byte(payload), &p) != nil || p.ReleaseID != releaseID {
				continue
			}
			got = p.NodeIDsInOrder
			return true, nil
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for validation.requested:v1 for release %s", releaseID))
	require.ElementsMatch(t, want, got,
		"validation set must be exactly the derived changed node(s)")
}

// waitForReleasePromoted polls GET /releases/{id} until the release reaches
// "promoted". A "rejected" status fails immediately with the recorded reason.
func waitForReleasePromoted(t *testing.T, ctx context.Context, clients *testClients, releaseID string, timeout time.Duration) {
	t.Helper()
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		resp, err := http.Get(fmt.Sprintf("%s/releases/%s", clients.releaseBase, releaseID))
		if err != nil {
			return false, nil
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false, nil
		}
		body, _ := io.ReadAll(resp.Body)
		var r struct {
			Status       string   `json:"status"`
			RejectReason string   `json:"reject_reason"`
			FailingNodes []string `json:"failing_nodes"`
		}
		if json.Unmarshal(body, &r) != nil {
			return false, nil
		}
		switch r.Status {
		case "promoted":
			return true, nil
		case "rejected":
			t.Fatalf("release %s rejected: reason=%q failing_nodes=%v", releaseID, r.RejectReason, r.FailingNodes)
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for release %s to reach promoted", releaseID))
}

// waitForTopologySwap polls Neo4j until the orchestrator has applied the
// release.promoted:v1 swap: the :Meta singleton points at releaseID and the
// promoted node is active and stamped with the new release_id. The swap is
// asynchronous relative to the release reaching "promoted", so this must poll.
func waitForTopologySwap(t *testing.T, ctx context.Context, clients *testClients, releaseID, uniqueID string, timeout time.Duration) {
	t.Helper()
	var lastMeta, lastNodeRel string
	var lastActive bool
	pollUntil(t, ctx, timeout, 1*time.Second, func() (bool, error) {
		lastMeta = neo4jScalarString(ctx, clients,
			`MATCH (m:Meta {key: 'current_release'}) RETURN m.release_id AS v`, nil)
		if lastMeta != releaseID {
			return false, nil
		}
		session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
		defer session.Close(ctx)
		res, err := session.Run(ctx,
			`MATCH (t:Table {unique_id: $uid})
			 RETURN COALESCE(t.active, false) AS active, COALESCE(t.release_id, "") AS release_id`,
			map[string]any{"uid": uniqueID})
		if err != nil || !res.Next(ctx) {
			return false, nil
		}
		rec := res.Record()
		a, _ := rec.Get("active")
		r, _ := rec.Get("release_id")
		lastActive, _ = a.(bool)
		lastNodeRel, _ = r.(string)
		return lastActive && lastNodeRel == releaseID, nil
	}, fmt.Sprintf("timeout waiting for Neo4j topology swap to release %s "+
		"(:Meta=%q, node %s active=%v release_id=%q)", releaseID, lastMeta, uniqueID, lastActive, lastNodeRel))
}

// waitForReleaseRejected polls GET /releases/{id} until the release reaches
// "rejected" and asserts the recorded reject_reason matches wantReason. A
// "promoted" status fails immediately (the release should never validate).
func waitForReleaseRejected(t *testing.T, ctx context.Context, clients *testClients, releaseID, wantReason string, timeout time.Duration) {
	t.Helper()
	var gotReason string
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		resp, err := http.Get(fmt.Sprintf("%s/releases/%s", clients.releaseBase, releaseID))
		if err != nil {
			return false, nil
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false, nil
		}
		body, _ := io.ReadAll(resp.Body)
		var r struct {
			Status       string `json:"status"`
			RejectReason string `json:"reject_reason"`
		}
		if json.Unmarshal(body, &r) != nil {
			return false, nil
		}
		switch r.Status {
		case "rejected":
			gotReason = r.RejectReason
			return true, nil
		case "promoted":
			t.Fatalf("release %s promoted but should have been rejected (%s)", releaseID, wantReason)
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for release %s to reach rejected", releaseID))
	require.Equal(t, wantReason, gotReason,
		"release %s rejected for the wrong reason", releaseID)
}

// assertCandidateSchemaDropped polls the dbt warehouse until the release's
// _candidate_<sanitized releaseID> schema is absent from
// information_schema.schemata. The executor drops the schema asynchronously on
// validation.completed:v1, so this must poll. The schema name is computed with
// the same sanitization release-controller applies (non-[A-Za-z0-9_] → _).
func assertCandidateSchemaDropped(t *testing.T, ctx context.Context, clients *testClients, releaseID string, timeout time.Duration) {
	t.Helper()
	schema := "_candidate_" + sanitizeReleaseSchemaSuffix(releaseID)
	pollUntil(t, ctx, timeout, 2*time.Second, func() (bool, error) {
		var n int
		err := clients.dbtDB.GetContext(ctx, &n,
			`SELECT count(*) FROM information_schema.schemata WHERE schema_name = $1`, schema)
		if err != nil {
			return false, nil
		}
		return n == 0, nil
	}, fmt.Sprintf("timeout waiting for candidate schema %q to be dropped from the dbt warehouse", schema))
	t.Logf("✅ candidate schema %q dropped", schema)
}

// assertNoValidationJobsDispatched confirms no mode=validation K8s Job was ever
// created for releaseID. A rejected release never emits validation.requested:v1,
// so the executor never dispatches a job. A short grace window absorbs any
// in-flight dispatch race before the final assertion.
func assertNoValidationJobsDispatched(t *testing.T, ctx context.Context, clients *testClients, releaseID string) {
	t.Helper()
	selector := fmt.Sprintf("mode=validation,release-id=%s", releaseID)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		jobs, err := getK8sJobs(ctx, selector)
		require.NoError(t, err, "list validation jobs for release %s", releaseID)
		require.Empty(t, jobs.Items,
			"expected no validation jobs for rejected release %s, found %d", releaseID, len(jobs.Items))
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
	t.Logf("✅ no validation jobs dispatched for rejected release %s", releaseID)
}

// sanitizeReleaseSchemaSuffix mirrors release-controller's sanitizeSchemaSuffix:
// every character that is not [A-Za-z0-9_] becomes _. Kept local to the e2e
// package so the test computes the candidate schema name independently of the
// service-internal helper.
func sanitizeReleaseSchemaSuffix(s string) string {
	return releaseSchemaSuffixRe.ReplaceAllString(s, "_")
}

var releaseSchemaSuffixRe = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// neo4jScalarString runs a query expected to return a single string column `v`
// and returns it (empty string if no row).
func neo4jScalarString(ctx context.Context, clients *testClients, cypher string, params map[string]any) string {
	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer session.Close(ctx)
	res, err := session.Run(ctx, cypher, params)
	if err != nil || !res.Next(ctx) {
		return ""
	}
	v, _ := res.Record().Get("v")
	s, _ := v.(string)
	return s
}
