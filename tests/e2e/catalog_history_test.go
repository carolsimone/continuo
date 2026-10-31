package e2e

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	catalogv1 "github.com/carolsimone/continuo/catalog/api/catalog/v1"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestDroppedNodeKeepsItsHistory proves that a release which drops a node
// retires it and keeps what is known about it:
//
//	a release of service-1 promotes with no changed node (production holds the
//	baseline's own hashes), so it needs no validation, and its promotion
//	carries the release's code bundle
//	→ catalog's topology group makes rel_probe active and its versions group
//	  records the node's code version and points :CURRENT at it
//	→ a second release, the e2e DAG without rel_probe, is promoted
//	→ catalog retires rel_probe instead of deleting it
//
// Through the gRPC CatalogQuery, rel_probe is then found by GetNode only when
// the request sets include_inactive (and reported inactive), NOT_FOUND
// otherwise, and GetNodeVersions lists the versions it had before the drop,
// none of them current.
//
// The test changes the live topology of catalog and run-controller twice and
// puts current_prod on its own release. A deferred restore announces the
// default e2e DAG again, which every test that needs the DAG does at its start
// as well, so the next test finds the topology it expects: the e2e DAG live in
// catalog and run-controller, with rel_probe active again and the versions it
// recorded. The restore also waits for the promote-seed runs the first release
// planned, so no seed load of this test is in flight when the next test
// starts. current_prod stays on the first release, which holds the baseline's
// nodes with their own hashes; every test that reads it seeds it first, as
// after any other release test. The file sorts after cancel_test.go and before
// chat_test.go: the restore is what makes that position safe for the tests
// after it, and the test itself needs only what setup.sh leaves (the baseline
// manifests and service_prod), not a test before it.
func TestDroppedNodeKeepsItsHistory(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// compile + parse + promote (10m) + the swap (2m) + the versions (2m) + the
	// drop (2m), with headroom. The restore has a context of its own.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-history-" + uuid.NewString()[:8]
	const changedService = "service-1"
	t.Logf("release_id=%s changed_service=%s node=%s", releaseID, changedService, probeUniqueID)

	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)
	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag, "image_tag missing for %s — setup.sh must seed service_prod", changedService)

	// Production holds the baseline's own hash for every node, rel_probe
	// included, so no node is changed and the release promotes with nothing to
	// validate.
	var prodNodes []map[string]string
	probeFound := false
	for svc, si := range allServices {
		for _, n := range si.nodes {
			probeFound = probeFound || n.uniqueID == probeUniqueID
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"service":      svc,
				"content_hash": n.contentHash,
			})
		}
	}
	require.True(t, probeFound,
		"rel_probe not found in any manifest — is the model in service-1 and the image rebuilt?")

	// From here the test moves the live topology and current_prod.
	defer restoreDefaultTopologyAfter(t, clients, releaseID)

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProd(t, ctx, clients, allServices)
	postRelease(t, clients, changedService, releaseID, changedImageTag, false)
	waitForReleasePromoted(t, ctx, clients, releaseID, 10*time.Minute)

	// 1. rel_probe is live and has a current version. The topology group's swap
	//    and the versions group are separate consumers, so wait for both: the
	//    node's version watermark reaches the promotion's seq once the versions
	//    group has applied this release.
	waitForTopologySwap(t, ctx, clients, releaseID, probeUniqueID, 2*time.Minute)
	promoted := readLivePointer(ctx, clients)
	require.Equal(t, releaseID, promoted.releaseID, "catalog's live release must be the promoted one")
	waitForVersionsOfPromotion(t, ctx, clients, probeUniqueID, promoted.promotionSeq, 2*time.Minute)

	live, err := clients.catalogClient.GetNode(ctx, probeNodeRequest(false))
	require.NoError(t, err, "GetNode(%s) of the live node", probeUniqueID)
	require.False(t, live.GetInactive(), "a live node is not inactive")
	before, err := clients.catalogClient.GetNodeVersions(ctx, probeVersionsRequest(false))
	require.NoError(t, err, "GetNodeVersions(%s) of the live node", probeUniqueID)
	require.NotEmpty(t, before.GetVersions(), "the promotion's code bundle must have recorded a version of %s", probeUniqueID)
	require.Equal(t, 1, currentVersions(before.GetVersions()), "exactly one version of a live node is current")
	t.Logf("%s is live with %d recorded version(s), one current", probeUniqueID, len(before.GetVersions()))

	// 2. A second release drops rel_probe: the e2e DAG without it.
	nodes, _ := e2eTopology(t, ctx, clients)
	kept, keptSchedules := withoutNode(nodes, probeUniqueID)
	require.Len(t, kept, len(nodes)-1, "the e2e DAG holds %s exactly once", probeUniqueID)
	dropped := announceTopology(t, ctx, "e2e-drop-"+uuid.NewString()[:8], kept)
	waitForAnnouncedTopology(t, ctx, clients, dropped, keptSchedules)

	// 3. The node is retired, not deleted. The swap that moved the live pointer
	//    retired it in the same transaction; the poll only absorbs the read.
	var retired *catalogv1.GetNodeResponse
	var lastErr error
	pollUntilDescribed(t, ctx, time.Minute, time.Second, func() (bool, error) {
		retired, lastErr = clients.catalogClient.GetNode(ctx, probeNodeRequest(true))
		return lastErr == nil && retired.GetInactive(), nil
	}, func() string {
		return fmt.Sprintf("timeout waiting for GetNode(include_inactive) of the dropped node %s to report it inactive: last response %v, last error %v",
			probeUniqueID, retired, lastErr)
	})

	_, err = clients.catalogClient.GetNode(ctx, probeNodeRequest(false))
	require.Equal(t, codes.NotFound, status.Code(err),
		"GetNode of the dropped node without include_inactive must be NOT_FOUND, got: %v", err)

	// 4. Its history is intact, and none of it is current any more.
	after, err := clients.catalogClient.GetNodeVersions(ctx, probeVersionsRequest(false))
	require.NoError(t, err, "GetNodeVersions of the dropped node must still answer")
	require.ElementsMatch(t, versionHashes(before.GetVersions()), versionHashes(after.GetVersions()),
		"the dropped node keeps every version it had")
	require.Zero(t, currentVersions(after.GetVersions()), "no version of a retired node is current")
	currentOnly, err := clients.catalogClient.GetNodeVersions(ctx, probeVersionsRequest(true))
	require.NoError(t, err, "GetNodeVersions(current_only) of the dropped node")
	require.Empty(t, currentOnly.GetVersions(), "current_only of a retired node returns an empty list")
	t.Logf("✅ %s was retired by %s: found only with include_inactive (inactive), %d version(s) kept, none current",
		probeUniqueID, dropped.ReleaseID, len(after.GetVersions()))
}

// restoreDefaultTopologyAfter puts the stack back the way the next test finds
// it: the default e2e DAG is announced again, and the promote-seed runs the
// promotion of releaseID planned are waited for. It runs on a context of its
// own, because the test's is cancelled by the time a cleanup runs.
func restoreDefaultTopologyAfter(t *testing.T, clients *testClients, releaseID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	seedTopology(t, ctx, clients)
	waitForPromoteSeedRunsToEnd(t, ctx, clients, releaseID, 5*time.Minute)
}

// waitForPromoteSeedRunsToEnd waits until no promote-seed run of releaseID is
// active. It returns without failing when one still is after timeout: a seed
// load that runs long is not what the calling test asserts.
func waitForPromoteSeedRunsToEnd(t *testing.T, ctx context.Context, clients *testClients, releaseID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var active int
		err := clients.runControllerDB.GetContext(ctx, &active, `
			SELECT count(*) FROM runs
			 WHERE tenant_id = $1 AND kind = 'promote_seed' AND release_id = $2
			   AND status IN ('pending', 'running', 'cancelling')`,
			events.DefaultTenantID, releaseID)
		if err == nil && active == 0 {
			return
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Logf("%d promote-seed run(s) of release %s still active after %s (last read error: %v)", active, releaseID, timeout, err)
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(3 * time.Second):
		}
	}
}

// waitForVersionsOfPromotion polls Neo4j until the versions group has applied
// the promotion with seq promotionSeq to uniqueID: the group advances the
// version watermark of every node its bundle carries to the promotion's seq in
// the transaction that records the node's version and points :CURRENT at it.
func waitForVersionsOfPromotion(t *testing.T, ctx context.Context, clients *testClients, uniqueID string, promotionSeq int64, timeout time.Duration) {
	t.Helper()
	var watermark int64
	pollUntilDescribed(t, ctx, timeout, time.Second, func() (bool, error) {
		watermark = neo4jScalarInt(ctx, clients,
			`MATCH (n:Node {tenant_id: 'default', unique_id: $uid}) RETURN n.code_version_promotion_seq AS v`,
			map[string]any{"uid": uniqueID})
		return watermark >= promotionSeq, nil
	}, func() string {
		return fmt.Sprintf("timeout waiting for the versions group to apply promotion seq %d to %s (version watermark %d)",
			promotionSeq, uniqueID, watermark)
	})
}

// probeNodeRequest addresses rel_probe by its (service, schema, table)
// identity, as GetNode reads it.
func probeNodeRequest(includeInactive bool) *catalogv1.GetNodeRequest {
	return &catalogv1.GetNodeRequest{
		ServiceName:     "service-1",
		SchemaName:      seedSchemaName,
		TableName:       "rel_probe",
		IncludeInactive: includeInactive,
	}
}

// probeVersionsRequest asks for rel_probe's recorded versions, at most the
// limit the proto allows.
func probeVersionsRequest(currentOnly bool) *catalogv1.GetNodeVersionsRequest {
	return &catalogv1.GetNodeVersionsRequest{UniqueId: probeUniqueID, Limit: 200, CurrentOnly: currentOnly}
}

// currentVersions counts the versions marked is_current.
func currentVersions(versions []*catalogv1.VersionView) int {
	n := 0
	for _, v := range versions {
		if v.GetIsCurrent() {
			n++
		}
	}
	return n
}

// versionHashes returns the content hashes of versions, sorted.
func versionHashes(versions []*catalogv1.VersionView) []string {
	hashes := make([]string, 0, len(versions))
	for _, v := range versions {
		hashes = append(hashes, v.GetContentHash())
	}
	sort.Strings(hashes)
	return hashes
}

// withoutNode returns nodes without uniqueID, and the sorted schedules the
// remaining nodes declare, which is what waitForAnnouncedTopology needs to find
// a model of in run-controller's live release.
func withoutNode(nodes []topologyartifact.Node, uniqueID string) ([]topologyartifact.Node, []string) {
	kept := make([]topologyartifact.Node, 0, len(nodes))
	scheduleSet := map[string]bool{}
	for _, n := range nodes {
		if n.UniqueID == uniqueID {
			continue
		}
		kept = append(kept, n)
		scheduleSet[n.Schedule] = true
	}
	schedules := make([]string, 0, len(scheduleSet))
	for s := range scheduleSet {
		schedules = append(schedules, s)
	}
	sort.Strings(schedules)
	return kept, schedules
}
