package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// The service-2 fixture pair for the verification seed-set leg: ybreak_up no
// longer emits amount_eur, and ybreak_down still reads it.
const (
	ybreakUpUniqueID   = "e2e_schema.ybreak_up"
	ybreakDownUniqueID = "e2e_schema.ybreak_down"
)

// TestE2E_Verification_FailsConsumerFixThatIgnoresChangedUpstream proves that a
// fix verification measures a consumer against the shape the rejected release
// produced, not against production. ybreak_up is seeded in production at a
// stale hash, so a service-2 release changes it (validated ok) and breaks
// ybreak_down. The stub proposes a consumer edit that still reads the dropped
// column. The verification must rebuild ybreak_up from the candidate and fail
// ybreak_down; a run that cloned ybreak_up from production would pass the
// broken fix.
//
// Both fixtures live in service-2 on purpose: a cross-service failure is fixed
// at the producer, so only a same-service pair exercises a consumer-side fix.
//
// The attempt cap lets the agent retry the same stub answer after this test
// returns; those later verification runs fail the same way and drain through
// the release FIFO without touching any other test's state.
func TestE2E_Verification_FailsConsumerFixThatIgnoresChangedUpstream(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), batchCtxBudget)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-ver-seed-" + uuid.NewString()[:8]
	changedService := "service-2"
	t.Logf("release_id=%s changed_service=%s changed_node=%s expected_failure=%s",
		releaseID, changedService, ybreakUpUniqueID, ybreakDownUniqueID)

	// 1. Production holds every baseline node; ybreak_up at a stale hash so it
	//    is the release's only changed node, ybreak_down at its real hash so it
	//    is pulled in only as a descendant.
	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)
	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag, "image_tag missing for %s — setup.sh must seed service_prod", changedService)

	seen := map[string]bool{}
	var prodNodes []map[string]string
	for _, si := range allServices {
		for _, n := range si.nodes {
			seen[n.uniqueID] = true
			hash := n.contentHash
			if n.uniqueID == ybreakUpUniqueID {
				hash = "stale-" + hash
			}
			prodNodes = append(prodNodes, map[string]string{"unique_id": n.uniqueID, "content_hash": hash})
		}
	}
	for _, id := range []string{ybreakUpUniqueID, ybreakDownUniqueID} {
		require.True(t, seen[id], "%s not found in any baseline manifest — is the model in service-2 and the image rebuilt?", id)
	}

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)
	seedModelTopologyNodes(t, ctx, clients,
		topologyModel{uniqueID: ybreakUpUniqueID, schema: "e2e_schema", table: "ybreak_up", service: changedService, filePath: "models/ybreak_up.sql"},
		topologyModel{uniqueID: ybreakDownUniqueID, schema: "e2e_schema", table: "ybreak_down", service: changedService, filePath: "models/ybreak_down.sql"},
	)

	// 2. The release is rejected on the consumer alone.
	postRelease(t, clients, changedService, releaseID, changedImageTag, false)
	waitForReleaseRejected(t, ctx, clients, releaseID, batchRejectBudget)
	failing := releaseFailingNodes(t, ctx, clients, releaseID)
	require.Equal(t, []string{ybreakDownUniqueID}, failing, "only the consumer of the dropped column may fail; got %v", failing)

	// 3. One failing node, whose changed ancestor is in its own service, so the
	//    agent fixes the consumer itself.
	trigger := waitForBatchedTrigger(t, ctx, clients, releaseID, []string{ybreakDownUniqueID}, batchTriggerBudget)
	require.Len(t, trigger.Nodes, 1, "the trigger must carry exactly the failing consumer; got %v", triggerNodeIDs(trigger))
	down, ok := trigger.findNode(ybreakDownUniqueID)
	require.True(t, ok)
	require.Equal(t, []string{ybreakUpUniqueID}, down.ancestorIDs(), "the consumer's only changed ancestor")

	verifying := waitForBatchProposal(t, ctx, clients, releaseID, 1, "verifying", batchProposalBudget)
	edits := decodeFileEdits(t, verifying.FileEdits)
	require.Len(t, edits, 1)
	require.Equal(t, "services/service-2/models/ybreak_down.sql", edits[0].Path, "the fix edits the consumer")
	require.Equal(t, ybreakDownUniqueID, edits[0].TargetNodeID)

	verifications := decodeVerifications(t, verifying.Verifications)
	require.Len(t, verifications, 1)
	require.Equal(t, changedService, verifications[0].Service)
	runID := verifications[0].RunID
	require.NotEmpty(t, runID)
	assertPipelineNamedVerification(t, ctx, clients, runID, batchVerifyBudget)

	// 4. The verification rebuilds ybreak_up from the candidate (ok) and fails
	//    the consumer that still reads the dropped column. Under a
	//    clone-from-production baseline the consumer would pass instead.
	waitForVerificationStatus(t, ctx, clients, runID, "failed", batchVerifyBudget)
	perNode := verificationPerNodeStatus(t, ctx, clients, runID)
	require.Equal(t, "ok", perNode[ybreakUpUniqueID],
		"the changed upstream is rebuilt from the candidate, not cloned from production; per-node=%v", perNode)
	require.Equal(t, "failed", perNode[ybreakDownUniqueID],
		"the consumer that still reads the dropped column must fail; per-node=%v", perNode)

	// 5. The attempt records the verdict.
	failed := waitForBatchProposal(t, ctx, clients, releaseID, 1, "failed", batchProposalBudget)
	require.Contains(t, failed.VerifyError, "ybreak_down",
		"the recorded reason must name the node the verification failed; got %q", failed.VerifyError)
	t.Logf("✅ verification %s failed the consumer-side fix: %s", runID, failed.VerifyError)
}

// verificationPerNodeStatus reads a verification run's per-node validation
// results from release-controller, keyed by node id.
func verificationPerNodeStatus(t *testing.T, ctx context.Context, clients *testClients, runID string) map[string]string {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/verification-runs/%s", clients.releaseBase, runID), nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var body struct {
		PerNodeResults []struct {
			Stage  string `json:"stage"`
			NodeID string `json:"node_id"`
			Status string `json:"status"`
		} `json:"per_node_results"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	out := map[string]string{}
	for _, n := range body.PerNodeResults {
		if n.Stage == "validation" || n.Stage == "" {
			out[n.NodeID] = n.Status
		}
	}
	return out
}
