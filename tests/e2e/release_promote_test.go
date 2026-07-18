package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
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
// `dbt_upload load --target localstack --release-id e2e-baseline`).
const e2eS3Bucket = "continuo"

// e2eBaselineReleaseID is the fixed release-id under which setup.sh uploads
// all services' manifests. The canonical S3 key for each service is:
//
//	<service>/e2e-baseline/manifest.json
//
// Tests seed service_prod rows pointing at these keys for the unchanged services
// so that release-controller assembly can reconstruct the full topology.
const e2eBaselineReleaseID = "e2e-baseline"

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
//	validation.result:v1 terminal (kind=complete) → release-controller promotes
//	→ release.promoted:v1 → orchestrator swaps the Neo4j topology.
//
// The changed service is service-1 (which contains rel_probe). current_prod is
// seeded with every node EXCEPT rel_probe, so the derived changed set is exactly
// {rel_probe}: one known-good node that validates without touching any
// production table. service_prod is seeded for the other services so that
// assembly can reconstruct the full manifest list at advance-time.
func TestE2E_ReleasePromote_ValidatesAndSwapsTopology(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// additive polls: assertValidationRequestedNodes(8m) + waitForReleasePromoted(10m)
	// + waitForTopologySwap(2m) = 20m; bumped from 15m to match Gated tests.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-rel-" + uuid.NewString()[:8]
	changedService := "service-1"
	t.Logf("release_id=%s changed_service=%s", releaseID, changedService)

	// 1. Read the baseline manifests from S3 (uploaded by setup.sh with
	//    --release-id e2e-baseline). Build the prod snapshot of every node except
	//    rel_probe so the derived changed set is exactly {rel_probe}.
	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag,
		"image_tag missing for %s — setup.sh must seed service_prod", changedService)

	var prodNodes []map[string]string
	probeFound := false
	for _, si := range allServices {
		for _, n := range si.nodes {
			if n.uniqueID == probeUniqueID {
				probeFound = true
				continue // exclude → rel_probe becomes the sole changed node
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"content_hash": n.contentHash,
			})
		}
	}
	require.True(t, probeFound,
		"rel_probe not found in any manifest — is the model in service-1 and the image rebuilt?")
	t.Logf("seeded prod snapshot with %d nodes (rel_probe excluded)", len(prodNodes))

	// 2. Reset the queue, seed current_prod, and seed service_prod for all
	//    other services (so assembly reconstructs the full topology).
	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)

	// 4. POST /releases with the single-service per-service body.
	postRelease(t, clients, changedService, releaseID, changedImageTag, false)

	// 5. The derived validation set must be exactly the one changed node.
	assertValidationRequestedNodes(t, ctx, clients, releaseID, []string{probeUniqueID})

	// 6. A real dbt validation job runs for rel_probe; on success the release
	//    promotes. A rejection fails the test immediately with the reason.
	waitForReleasePromoted(t, ctx, clients, releaseID, 10*time.Minute)

	// 7. The orchestrator must have swapped the Neo4j topology to this release.
	//    The swap is asynchronous relative to promotion, so poll.
	waitForTopologySwap(t, ctx, clients, releaseID, probeUniqueID, 2*time.Minute)

	// 8. The promoted release must surface in the history list and carry per-node
	//    validation results (with a dbt log URI) retrievable through the UI BFF.
	assertReleaseHistoryAndPerNodeLog(t, ctx, clients, releaseID, probeUniqueID)
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

// xprobeUp / xprobeDown are a CLEAN cross-service chain used by the self-contained
// validation test: xprobe_down (service-2) reads `FROM {{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }}.xprobe_up`,
// and xprobe_up lives in service-3 — so manifest-controller resolves xprobe_down's
// upstream to a different-service node. Neither has any broken or downstream model,
// so the full closure is exactly the pair.
const (
	xprobeUpUniqueID   = "e2e_schema.xprobe_up"
	xprobeDownUniqueID = "e2e_schema.xprobe_down"
)

// TestE2E_ReleasePromote_GatedIntraServiceUpstream proves that when a changed
// node depends on a CHANGED intra-service upstream, the upstream builds into the
// candidate schema first (topologically gated) and the dependent validates
// against it; the release then promotes and the candidate schema is torn down.
//
// Both rel_probe_up and rel_probe_down live in service-1. The test posts a
// per-service release for service-1. current_prod is seeded with every candidate
// node EXCEPT rel_probe_up and rel_probe_down. The derived changed set is
// therefore exactly {rel_probe_up, rel_probe_down} with the intra-service
// gating edge rel_probe_down → rel_probe_up.
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
	changedService := "service-1"
	t.Logf("release_id=%s changed_service=%s", releaseID, changedService)

	excluded := map[string]bool{
		probeUpUniqueID:   true,
		probeDownUniqueID: true,
	}
	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag,
		"image_tag missing for %s — setup.sh must seed service_prod", changedService)

	var prodNodes []map[string]string
	var upFound, downFound bool
	for _, si := range allServices {
		for _, n := range si.nodes {
			switch n.uniqueID {
			case probeUpUniqueID:
				upFound = true
			case probeDownUniqueID:
				downFound = true
			}
			if excluded[n.uniqueID] {
				continue
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"content_hash": n.contentHash,
			})
		}
	}
	require.True(t, upFound,
		"rel_probe_up not found in any manifest — is the model in service-1 and the image rebuilt + manifests re-uploaded?")
	require.True(t, downFound,
		"rel_probe_down not found in any manifest — is the model in service-1 and the image rebuilt + manifests re-uploaded?")
	t.Logf("seeded prod snapshot with %d nodes (rel_probe* chain excluded)", len(prodNodes))

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)

	postRelease(t, clients, changedService, releaseID, changedImageTag, false)

	// The derived validation set must be exactly the intra-service chain, in
	// topological order (upstream first).
	assertValidationRequestedNodes(t, ctx, clients, releaseID,
		[]string{probeUpUniqueID, probeDownUniqueID})

	waitForReleasePromoted(t, ctx, clients, releaseID, 12*time.Minute)
	waitForTopologySwap(t, ctx, clients, releaseID, probeDownUniqueID, 2*time.Minute)
	assertCandidateSchemaDropped(t, ctx, clients, releaseID, 3*time.Minute)
	t.Log("✅ gated intra-service upstream validated, release promoted, topology swapped, candidate schema dropped")
}

// TestE2E_ReleasePromote_GatedCrossServiceUpstream proves self-contained
// validation: a changed node whose upstream lives in ANOTHER service and is NOT
// materialized in prod still validates, because the upstream is built into the
// candidate schema first (cross-service topological gating) and the dependent's
// {{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }} ref is redirected there
// via DBT_UPSTREAM_SCHEMA. The release then promotes and the candidate schema is
// torn down.
//
// xprobe_down lives in service-2, xprobe_up lives in service-3. The test posts
// a per-service release for service-2. Assembly attaches service-3's current
// baseline manifest (via service_prod), so manifest-controller sees xprobe_up
// as part of the candidate topology. current_prod is seeded with every node
// EXCEPT the xprobe pair, so the derived changed set is exactly
// {xprobe_up, xprobe_down} with the cross-service gating edge
// xprobe_down -> xprobe_up.
func TestE2E_ReleasePromote_GatedCrossServiceUpstream(t *testing.T) {
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
	// xprobe_down is in service-2; posting a release for service-2 causes
	// assembly to attach service-3's manifest (which contains xprobe_up) from
	// service_prod, exposing the cross-service gating edge.
	changedService := "service-2"
	t.Logf("release_id=%s changed_service=%s", releaseID, changedService)

	excluded := map[string]bool{
		xprobeUpUniqueID:   true,
		xprobeDownUniqueID: true,
	}
	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag,
		"image_tag missing for %s — setup.sh must seed service_prod", changedService)

	var prodNodes []map[string]string
	var upFound, downFound bool
	for _, si := range allServices {
		for _, n := range si.nodes {
			switch n.uniqueID {
			case xprobeUpUniqueID:
				upFound = true
			case xprobeDownUniqueID:
				downFound = true
			}
			if excluded[n.uniqueID] {
				continue
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"content_hash": n.contentHash,
			})
		}
	}
	require.True(t, upFound,
		"xprobe_up not found in any manifest — is the model in service-3 and the image rebuilt + manifests re-uploaded?")
	require.True(t, downFound,
		"xprobe_down not found in any manifest — is the model in service-2 and the image rebuilt + manifests re-uploaded?")
	t.Logf("seeded prod snapshot with %d nodes (xprobe pair excluded)", len(prodNodes))

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)

	postRelease(t, clients, changedService, releaseID, changedImageTag, false)

	assertValidationRequestedNodes(t, ctx, clients, releaseID,
		[]string{xprobeUpUniqueID, xprobeDownUniqueID})

	waitForReleasePromoted(t, ctx, clients, releaseID, 12*time.Minute)
	waitForTopologySwap(t, ctx, clients, releaseID, xprobeDownUniqueID, 2*time.Minute)
	assertCandidateSchemaDropped(t, ctx, clients, releaseID, 3*time.Minute)
	t.Log("✅ gated CROSS-service upstream validated self-contained, promoted, candidate schema dropped")
}

// TestE2E_ReleasePromote_BootstrapSkipsValidation proves that a release posted
// with bootstrap=true promotes directly against an empty current_prod without
// dispatching any validation jobs. This covers the first-ever deploy scenario
// where there is no production baseline to diff against.
//
// Both current_prod and service_prod are cleared to simulate a fresh environment.
// A per-service bootstrap release is posted for service-1; it promotes immediately
// and swaps the Neo4j topology.
func TestE2E_ReleasePromote_BootstrapSkipsValidation(t *testing.T) {
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

	releaseID := "e2e-boot-" + uuid.NewString()[:8]
	changedService := "service-1"
	t.Logf("release_id=%s changed_service=%s", releaseID, changedService)

	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag, "image_tag missing for %s", changedService)

	// Empty current_prod and service_prod: a non-bootstrap release against an
	// empty baseline would treat every node as changed and could not promote.
	// A bootstrap release skips validation entirely.
	resetReleaseControllerQueue(t, ctx, clients)
	_, err := clients.releaseDB.ExecContext(ctx, "DELETE FROM current_prod")
	require.NoError(t, err, "clear current_prod")
	_, err = clients.releaseDB.ExecContext(ctx, "DELETE FROM service_prod")
	require.NoError(t, err, "clear service_prod")

	postRelease(t, clients, changedService, releaseID, changedImageTag, true)

	// The bootstrap release must promote (no validation) and swap Neo4j topology.
	pollUntil(t, ctx, 5*time.Minute, 2*time.Second, func() (bool, error) {
		return neo4jScalarString(ctx, clients,
			`MATCH (m:Meta {key: 'current_release'}) RETURN m.release_id AS v`, nil) == releaseID, nil
	}, "timeout waiting for bootstrap release "+releaseID+" to promote + swap topology")

	// No validation Job is ever dispatched (bootstrap skips validation).
	assertNoValidationJobsDispatched(t, ctx, clients, releaseID)
	t.Log("✅ bootstrap release promoted without validation; topology swapped")
}

// TestE2E_ReleasePromote_PerServiceLeavesOthersIntact verifies the anti-amputation
// guarantee: promoting a release for ONE service must not remove the other services'
// nodes from the Neo4j topology. After promotion the topology must contain nodes
// from both the changed service and the unchanged services.
//
// service_prod is seeded for all three services at the baseline; then a release is
// posted for service-1 only (using rel_probe as the changed node). After promotion
// the topology must still contain nodes that belong to service-2 and service-3.
func TestE2E_ReleasePromote_PerServiceLeavesOthersIntact(t *testing.T) {
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
	changedService := "service-1"
	t.Logf("release_id=%s changed_service=%s", releaseID, changedService)

	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag, "image_tag missing for %s", changedService)

	// Collect the service-2 and service-3 node IDs we expect to survive.
	survivingServices := []string{"service-2", "service-3"}
	var expectedSurvivingNodes []string
	for _, svc := range survivingServices {
		for _, n := range allServices[svc].nodes {
			expectedSurvivingNodes = append(expectedSurvivingNodes, n.uniqueID)
		}
	}
	require.NotEmpty(t, expectedSurvivingNodes,
		"expected at least one service-2/3 node in baseline — setup.sh must have uploaded their manifests")

	// Seed current_prod with ALL nodes except rel_probe (changed node for service-1).
	var prodNodes []map[string]string
	probeFound := false
	for _, si := range allServices {
		for _, n := range si.nodes {
			if n.uniqueID == probeUniqueID {
				probeFound = true
				continue
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"content_hash": n.contentHash,
			})
		}
	}
	require.True(t, probeFound,
		"rel_probe not found in baseline — service-1 manifests must include it")

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)

	postRelease(t, clients, changedService, releaseID, changedImageTag, false)

	// rel_probe is the only changed node — validate + promote.
	assertValidationRequestedNodes(t, ctx, clients, releaseID, []string{probeUniqueID})
	waitForReleasePromoted(t, ctx, clients, releaseID, 10*time.Minute)
	waitForTopologySwap(t, ctx, clients, releaseID, probeUniqueID, 2*time.Minute)

	// Anti-amputation assertion: service-2 and service-3 nodes must still be
	// present in the Neo4j topology with the new release stamped on them.
	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeRead})
	defer session.Close(ctx)
	for _, uid := range expectedSurvivingNodes {
		res, err := session.Run(ctx,
			`MATCH (t:Table {unique_id: $uid}) RETURN t.unique_id AS uid`,
			map[string]any{"uid": uid})
		require.NoError(t, err, "Neo4j query for node %s", uid)
		require.True(t, res.Next(ctx),
			"node %s from non-changed service must survive a service-1-only release (anti-amputation)", uid)
	}
	t.Logf("✅ per-service release for %s left %d service-2/3 nodes intact in Neo4j",
		changedService, len(expectedSurvivingNodes))
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

// serviceInfo bundles the parsed nodes and image tag for a single dbt service's
// baseline manifest, as uploaded by setup.sh.
type serviceInfo struct {
	imageTag string
	nodes    []manifestNode
}

// canonicalManifestS3URI returns the full s3:// URI for a per-release manifest,
// mirroring the CanonicalManifestKey convention in release-controller.
func canonicalManifestS3URI(service, releaseID string) string {
	return fmt.Sprintf("s3://%s/%s/%s/manifest.json", e2eS3Bucket, service, releaseID)
}

// baselineServices loads every service's baseline manifest from S3 (written by
// setup.sh with --release-id e2e-baseline) and its current image tag from the
// release-controller service_prod table, returning a map keyed by service name.
// The map is used to seed current_prod topology snapshots and service_prod rows.
func baselineServices(t *testing.T, ctx context.Context, clients *testClients) map[string]serviceInfo {
	t.Helper()
	// Re-establish the per-service image pointers a prior blue/green test may
	// have mutated, so readServiceImageTag below sees the full baseline.
	seedBaselineServiceProd(t, ctx, clients)
	// Discover all services by listing objects under their baseline prefix. The
	// listing MUST paginate: ListObjectsV2 returns at most 1000 keys per page, and
	// a long-lived stack accumulates far more than that in per-run dbt logs under
	// dbt-runs/, so a single un-paginated page can miss the e2e-baseline manifests
	// entirely and silently report "no baseline manifests".
	services := map[string]serviceInfo{}
	suffix := "/" + e2eBaselineReleaseID + "/manifest.json"
	var token *string
	for {
		out, err := clients.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(e2eS3Bucket),
			ContinuationToken: token,
		})
		require.NoError(t, err, "list S3 objects for baseline discovery")
		for _, obj := range out.Contents {
			key := aws.ToString(obj.Key)
			// Match <service>/e2e-baseline/manifest.json
			if !strings.HasSuffix(key, suffix) {
				continue
			}
			service := strings.TrimSuffix(key, suffix)
			if strings.Contains(service, "/") {
				continue // skip keys with extra path segments
			}
			nodes := parseManifestNodes(t, getS3Object(t, ctx, clients, key))
			imageTag := readServiceImageTag(t, ctx, clients, service)
			services[service] = serviceInfo{imageTag: imageTag, nodes: nodes}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		token = out.NextContinuationToken
	}
	return services
}

// readServiceImageTag resolves a dbt service's image tag from the
// release-controller's service_prod pointer table (DB continuo_release) — the
// production source of truth for each service's current image. setup.sh /
// provision-k8s-test-env.sh seed one service_prod row per service with the real
// content-addressed tag the kind dbt images are loaded under. The executor
// composes the dbt job image as service-<n>:<image_tag>, so this tag must match
// the kind image exactly.
func readServiceImageTag(t *testing.T, ctx context.Context, clients *testClients, service string) string {
	t.Helper()
	var tag string
	err := clients.releaseDB.QueryRowContext(ctx,
		`SELECT image_tag FROM service_prod WHERE service_name = $1`, service).Scan(&tag)
	require.NoError(t, err,
		"read service_prod.image_tag for %s — setup.sh must seed service_prod before e2e", service)
	require.NotEmpty(t, tag,
		"service_prod.image_tag is empty for %s — setup.sh seeded an empty tag", service)
	return tag
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

// parseManifestNodes extracts the model/seed/snapshot nodes from a dbt
// manifest.json and derives each node's identity exactly as manifest-controller
// does: unique_id = "<schema>.<name>" and content_hash = the macro-aware
// fingerprint computed by macroAwareContentHash. Seeding current_prod with this
// same fingerprint is what lets a re-released, unmodified node match production.
// A node whose hash folds in macro sources — an incremental model reaches
// is_incremental — would otherwise never equal a raw dbt checksum and would be
// derived as perpetually changed, dragging its downstream into validation. The
// baseline manifest keeps the full `macros` map (filter_manifest prunes only
// nodes), so the macro bodies needed for the fold are present here.
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
			DependsOn struct {
				Macros []string `json:"macros"`
			} `json:"depends_on"`
		} `json:"nodes"`
		Macros map[string]struct {
			MacroSQL  string `json:"macro_sql"`
			DependsOn struct {
				Macros []string `json:"macros"`
			} `json:"depends_on"`
		} `json:"macros"`
	}
	require.NoError(t, json.Unmarshal(body, &manifest), "parse manifest.json")

	// macroDeps maps every macro id to its own direct macro dependencies (the
	// edge set the transitive walk follows); macroSQL holds each macro's source.
	macroDeps := make(map[string][]string, len(manifest.Macros))
	macroSQL := make(map[string]string, len(manifest.Macros))
	for id, m := range manifest.Macros {
		macroDeps[id] = m.DependsOn.Macros
		macroSQL[id] = m.MacroSQL
	}

	supported := map[string]bool{"model": true, "seed": true, "snapshot": true}
	var nodes []manifestNode
	for _, n := range manifest.Nodes {
		if !supported[n.ResourceType] || n.Checksum.Checksum == "" {
			continue
		}
		nodes = append(nodes, manifestNode{
			uniqueID:    n.Schema + "." + n.Name,
			contentHash: macroAwareContentHash(n.Checksum.Checksum, n.DependsOn.Macros, macroDeps, macroSQL),
		})
	}
	return nodes
}

// transitiveMacroIDs resolves the transitive closure of a node's macro
// dependencies, mirroring manifest-controller's _transitive_macro_ids: it walks
// macro->macro edges so a change to a macro reached only through another macro
// still re-fingerprints the node. Ids absent from the macro map stay in the
// closure but expand no further (and contribute no source hash later).
func transitiveMacroIDs(direct []string, macroDeps map[string][]string) map[string]bool {
	seen := map[string]bool{}
	stack := append([]string(nil), direct...)
	for len(stack) > 0 {
		mid := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[mid] {
			continue
		}
		seen[mid] = true
		if deps, ok := macroDeps[mid]; ok {
			stack = append(stack, deps...)
		}
	}
	return seen
}

// macroAwareContentHash reproduces manifest-controller's _content_hash
// byte-for-byte: the node's base source hash (its dbt checksum here), then —
// only when the node has transitive macro dependencies present in the manifest —
// the base concatenated with the sorted sha256 hex of each such macro's source,
// re-hashed under a "sha256:" prefix. A node with no macro dependencies keeps
// its base unchanged. The concatenation order (base first, then the ascending
// macro hashes joined with no separator) and the empty-macro-source-as-"" rule
// must match the Python exactly, or a macro node stays perpetually changed.
func macroAwareContentHash(base string, directMacros []string, macroDeps map[string][]string, macroSQL map[string]string) string {
	ids := transitiveMacroIDs(directMacros, macroDeps)
	var macroHashes []string
	for id := range ids {
		sql, ok := macroSQL[id]
		if !ok {
			continue
		}
		sum := sha256.Sum256([]byte(sql))
		macroHashes = append(macroHashes, hex.EncodeToString(sum[:]))
	}
	if len(macroHashes) == 0 {
		return base
	}
	sort.Strings(macroHashes)
	folded := sha256.Sum256([]byte(base + strings.Join(macroHashes, "")))
	return "sha256:" + hex.EncodeToString(folded[:])
}

// resetReleaseControllerQueue clears any release left mid-flight by a prior run
// (which would block AdvanceQueue) and any unpublished outbox rows that would
// otherwise re-fire events for now-deleted releases.
func resetReleaseControllerQueue(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()
	_, err := clients.releaseDB.ExecContext(ctx,
		`DELETE FROM releases WHERE status IN ('received','compiling','parsing','seed_building','validating')`)
	require.NoError(t, err, "reset releases queue")
	_, err = clients.releaseDB.ExecContext(ctx,
		`DELETE FROM release_controller_outbox WHERE status = 'pending'`)
	require.NoError(t, err, "reset release outbox")
}

// seedCurrentProd writes the singleton current_prod row. Only unique_id and
// content_hash matter for the change-detector; other Node fields default to
// zero values on the release-controller side. release_id is reset to empty for
// a clean baseline so a prior promoted release's value does not linger across
// reused-stack runs.
func seedCurrentProd(t *testing.T, ctx context.Context, clients *testClients, nodes []map[string]string) {
	t.Helper()
	topoJSON, err := json.Marshal(nodes)
	require.NoError(t, err, "marshal current_prod snapshot")
	_, err = clients.releaseDB.ExecContext(ctx,
		`INSERT INTO current_prod (id, release_id, topology_snapshot, updated_at)
		 VALUES (1, '', $1, now())
		 ON CONFLICT (id) DO UPDATE SET
		   release_id = EXCLUDED.release_id,
		   topology_snapshot = EXCLUDED.topology_snapshot,
		   updated_at = EXCLUDED.updated_at`,
		topoJSON)
	require.NoError(t, err, "seed current_prod")
}

// seedServiceProdExcept seeds the service_prod table with the baseline manifest
// key and image_tag for every service in allServices EXCEPT changedService.
// This gives release-controller assembly the other services' manifest pointers
// at advance-time without re-uploading their manifests (they already live at
// their e2e-baseline canonical keys).
func seedServiceProdExcept(t *testing.T, ctx context.Context, clients *testClients, allServices map[string]serviceInfo, changedService string) {
	t.Helper()
	for svc, si := range allServices {
		if svc == changedService {
			continue
		}
		s3URI := canonicalManifestS3URI(svc, e2eBaselineReleaseID)
		_, err := clients.releaseDB.ExecContext(ctx,
			`INSERT INTO service_prod (service_name, release_id, manifest_s3_key, image_tag, updated_at)
			 VALUES ($1, $2, $3, $4, now())
			 ON CONFLICT (service_name) DO UPDATE SET
			   release_id = EXCLUDED.release_id,
			   manifest_s3_key = EXCLUDED.manifest_s3_key,
			   image_tag = EXCLUDED.image_tag,
			   updated_at = EXCLUDED.updated_at`,
			svc, e2eBaselineReleaseID, s3URI, si.imageTag)
		require.NoError(t, err, "seed service_prod for %s", svc)
	}
	// Ensure the changed service has NO row in service_prod so assembly
	// derives a fresh canonical key from the new release_id.
	_, err := clients.releaseDB.ExecContext(ctx,
		`DELETE FROM service_prod WHERE service_name = $1`, changedService)
	require.NoError(t, err, "clear service_prod row for changed service %s", changedService)
}

// postRelease issues the per-service POST /releases request. When bootstrap is
// true the release promotes without validation, covering the first-ever deploy
// scenario.
func postRelease(t *testing.T, clients *testClients, service, releaseID, imageTag string, bootstrap bool) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"service":    service,
		"release_id": releaseID,
		"image_tag":  imageTag,
		"bootstrap":  bootstrap,
		"repo":       "carolsimone/continuo-dbt-demo",
		"commit_sha": imageTag,
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
	pollUntil(t, ctx, 8*time.Minute, 1*time.Second, func() (bool, error) {
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

// assertCandidateSchemaDropped polls the dbt warehouse until the release's
// _candidate_<sanitized releaseID> schema is absent from
// information_schema.schemata. The executor drops the schema asynchronously on
// the validation.result:v1 terminal (kind=complete), so this must poll. The
// schema name is computed with the same sanitization release-controller
// applies (non-[A-Za-z0-9_] → _).
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
// created for releaseID. A bootstrap release never emits validation.requested:v1,
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
			"expected no validation jobs for bootstrap release %s, found %d", releaseID, len(jobs.Items))
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
	t.Logf("✅ no validation jobs dispatched for bootstrap release %s", releaseID)
}

// sanitizeReleaseSchemaSuffix mirrors release-controller's sanitizeSchemaSuffix:
// every character that is not [A-Za-z0-9_] becomes _. Kept local to the e2e
// package so the test computes the candidate schema name independently of the
// service-internal helper.
func sanitizeReleaseSchemaSuffix(s string) string {
	return releaseSchemaSuffixRe.ReplaceAllString(s, "_")
}

var releaseSchemaSuffixRe = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// assertReleaseHistoryAndPerNodeLog verifies the read surfaces the Releases tab
// depends on: the promoted release appears in the paginated history list, its
// detail carries per-node validation results with a dbt log URI, and that log is
// retrievable through the UI BFF's log proxy. The UI-BFF check is skipped when
// UI_HTTP_BASE is unset (developer runs without the UI container).
func assertReleaseHistoryAndPerNodeLog(t *testing.T, ctx context.Context, clients *testClients, releaseID, nodeID string) {
	t.Helper()

	// 1. History list includes this release with status=promoted.
	listResp, err := http.Get(clients.releaseBase + "/releases?status=promoted&limit=50")
	require.NoError(t, err, "GET /releases history list")
	defer listResp.Body.Close()
	require.Equal(t, http.StatusOK, listResp.StatusCode)
	var list struct {
		Releases []struct {
			ReleaseID string `json:"release_id"`
			Status    string `json:"status"`
			NodeCount int    `json:"node_count"`
		} `json:"releases"`
		NextCursor string `json:"next_cursor"`
	}
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&list))
	var listed bool
	for _, r := range list.Releases {
		if r.ReleaseID == releaseID {
			listed = true
			require.Equal(t, "promoted", r.Status, "history row status for %s", releaseID)
		}
	}
	require.True(t, listed, "release %s must appear in GET /releases history", releaseID)

	// 2. Detail carries per-node results with a dbt log URI for the validated node.
	detailResp, err := http.Get(fmt.Sprintf("%s/releases/%s", clients.releaseBase, releaseID))
	require.NoError(t, err, "GET /releases/{id}")
	defer detailResp.Body.Close()
	require.Equal(t, http.StatusOK, detailResp.StatusCode)
	var detail struct {
		PerNodeResults []struct {
			NodeID    string `json:"node_id"`
			Status    string `json:"status"`
			DBTLogURI string `json:"dbt_log_uri"`
		} `json:"per_node_results"`
	}
	require.NoError(t, json.NewDecoder(detailResp.Body).Decode(&detail))
	require.NotEmpty(t, detail.PerNodeResults, "release %s detail must carry per-node results", releaseID)

	var logURI string
	for _, n := range detail.PerNodeResults {
		if n.NodeID == nodeID {
			require.Equal(t, "ok", n.Status, "validated node %s status", nodeID)
			logURI = n.DBTLogURI
		}
	}
	require.NotEmpty(t, logURI, "per-node result for %s must carry a dbt_log_uri", nodeID)

	// 3. The dbt log is retrievable through the UI BFF log proxy (skipped without UI).
	uiBase := os.Getenv("UI_HTTP_BASE")
	if uiBase == "" {
		t.Log("UI_HTTP_BASE not set — skipping UI BFF per-node log assertion")
		return
	}
	logResp, err := http.Get(fmt.Sprintf("%s/api/releases/log?key=%s", uiBase, url.QueryEscape(logURI))) //nolint:gosec // uiBase is the trusted UI_HTTP_BASE harness env var and logURI is query-escaped, not raw external input
	require.NoError(t, err, "GET ui /api/releases/log")
	defer logResp.Body.Close()
	require.Equal(t, http.StatusOK, logResp.StatusCode, "UI BFF should stream the per-node dbt log")
	body, _ := io.ReadAll(logResp.Body)
	require.NotEmpty(t, body, "per-node dbt log should not be empty")
	t.Logf("✅ release history + per-node log surfaced (node %s log %d bytes)", nodeID, len(body))
}

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
