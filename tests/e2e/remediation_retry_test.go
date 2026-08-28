package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestE2E_RemediationRetry_RoundTwo: a rejected release whose remediation
// stopped can be retried from the API. The dead end is produced by marking the
// first round's proposal failed directly in the agent's table — the outcome the
// model reaches when it cannot fix the failure — so the test controls the state
// without a second LLM fixture. Round 2 then reaches the model again: a new
// trigger carries remediation_round=2 and the proposal continues the attempt
// numbering. A retry while that proposal is open is refused with the link.
func TestE2E_RemediationRetry_RoundTwo(t *testing.T) {
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

	// 1. Same healable rejection the agent test drives (validation failure on
	//    ftable_e, service-2).
	releaseID := "e2e-rem-retry-" + uuid.NewString()[:8]
	changedService := "service-2"
	t.Logf("release_id=%s changed_service=%s changed_node=%s", releaseID, changedService, ftableEUniqueID)

	// Read baseline manifests from S3 (uploaded by setup.sh). Build the prod
	// snapshot of every node except ftable_e so the derived changed set is
	// exactly {ftable_e} — the deliberately-broken node.
	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag,
		"image_tag missing for %s — setup.sh must seed service_prod", changedService)

	var prodNodes []map[string]string
	ftableFound := false
	for _, si := range allServices {
		for _, n := range si.nodes {
			if n.uniqueID == ftableEUniqueID {
				ftableFound = true
				continue // exclude → ftable_e becomes the sole changed node
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"content_hash": n.contentHash,
			})
		}
	}
	require.True(t, ftableFound,
		"ftable_e not found in any manifest — is the model in service-2 and the image rebuilt?")
	t.Logf("seeded prod snapshot with %d nodes (ftable_e excluded)", len(prodNodes))

	// Reset the queue, seed current_prod (all nodes except ftable_e), and seed
	// service_prod for all services except service-2.
	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)

	// Seed the ftable_e :Table topology node in Neo4j so the agent-remediation's
	// Locator can resolve the node's file path and service name for both rounds.
	seedFTableETopologyNode(t, ctx, clients)

	// POST /releases for service-2. ftable_e's SQL references public.wrong_name
	// (a relation that does not exist), so validation fails.
	postRelease(t, clients, changedService, releaseID, changedImageTag, false)
	waitForReleaseRejected(t, ctx, clients, releaseID, 10*time.Minute)

	// 2. Round 1 proposes a fix (stub LLM answers high confidence).
	var round1 retryProposalRow
	pollUntil(t, ctx, 3*time.Minute, 2*time.Second, func() (bool, error) {
		err := clients.agentRemediationDB.GetContext(ctx, &round1,
			`SELECT source, release_id, node_id, status, attempt, remediation_round FROM proposal
			  WHERE release_id=$1 AND node_id=$2 AND status='proposed'`, releaseID, ftableEUniqueID)
		return err == nil, nil
	}, "round-1 proposal")
	require.Equal(t, 1, round1.RemediationRound)

	// 3. While a proposal is open, a retry is refused and names it.
	st, body := postRetry(t, clients, releaseID)
	require.Equal(t, http.StatusConflict, st, "body=%v", body)
	require.Equal(t, "proposal_open", body["error"])

	// 4. Make the release a dead end: the round-1 attempt ends failed.
	_, err := clients.agentRemediationDB.ExecContext(ctx,
		`UPDATE proposal SET status='failed', rationale='e2e: forced dead end' WHERE release_id=$1`, releaseID)
	require.NoError(t, err)

	// 5. Retry → round 2.
	st, body = postRetry(t, clients, releaseID)
	require.Equal(t, http.StatusAccepted, st, "body=%v", body)
	require.EqualValues(t, 2, body["remediation_round"])

	// 6. The round has been started but its first proposal row has not landed
	//    yet, so a second click cannot spend another round.
	st, body = postRetry(t, clients, releaseID)
	require.Equal(t, http.StatusConflict, st, "body=%v", body)
	require.Equal(t, "retry_in_progress", body["error"])

	// 7. A round-2 trigger reaches the agent and a new proposal continues the
	//    attempt numbering.
	var round2 retryProposalRow
	pollUntil(t, ctx, 3*time.Minute, 2*time.Second, func() (bool, error) {
		err := clients.agentRemediationDB.GetContext(ctx, &round2,
			`SELECT source, release_id, node_id, status, attempt, remediation_round FROM proposal
			  WHERE release_id=$1 AND node_id=$2 AND remediation_round=2 AND status='proposed'`, releaseID, ftableEUniqueID)
		return err == nil, nil
	}, "round-2 proposal")
	require.Equal(t, round1.Attempt+1, round2.Attempt, "attempt numbering continues across rounds")

	var decisions int
	require.NoError(t, clients.remediationDB.GetContext(ctx, &decisions,
		`SELECT count(*) FROM classification_decision WHERE release_id=$1 AND node_id=$2`, releaseID, ftableEUniqueID))
	require.Equal(t, 2, decisions, "one classification per round")

	// 8. Release record shows the round.
	detail := getReleaseDetail(t, clients, releaseID)
	require.EqualValues(t, 2, detail["remediation_round"])

	// 9. Fourth retry while round 2's proposal is open → refused again.
	st, body = postRetry(t, clients, releaseID)
	require.Equal(t, http.StatusConflict, st, "body=%v", body)
	require.Equal(t, "proposal_open", body["error"])
}

// retryProposalRow captures the round-scoped proposal fields asserted across
// remediation retry rounds in continuo_agent_remediation.proposal.
type retryProposalRow struct {
	Source           string `db:"source"`
	ReleaseID        string `db:"release_id"`
	NodeID           string `db:"node_id"`
	Status           string `db:"status"`
	Attempt          int    `db:"attempt"`
	RemediationRound int    `db:"remediation_round"`
}

// postRetry issues POST /releases/{id}/retry-remediation against the
// release-controller and returns the status code and decoded JSON body — a
// 202 body carries release_id/remediation_round, a 409 body carries an error
// reason and, for proposal_open, the open proposal's id and PR URL.
func postRetry(t *testing.T, clients *testClients, releaseID string) (int, map[string]any) {
	t.Helper()
	url := fmt.Sprintf("%s/releases/%s/retry-remediation", clients.releaseBase, releaseID)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte{}))
	require.NoError(t, err, "build POST retry-remediation request")
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	require.NoError(t, err, "POST %s", url)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read body from POST %s", url)

	var body map[string]any
	if len(raw) > 0 {
		require.NoError(t, json.Unmarshal(raw, &body), "unmarshal body from POST %s: %s", url, raw)
	}
	return resp.StatusCode, body
}

// getReleaseDetail fetches GET /releases/{id} from the release-controller and
// returns the decoded JSON body.
func getReleaseDetail(t *testing.T, clients *testClients, releaseID string) map[string]any {
	t.Helper()
	url := fmt.Sprintf("%s/releases/%s", clients.releaseBase, releaseID)
	resp, err := http.Get(url) //nolint:gosec // url is built from clients.releaseBase and a caller-supplied releaseID within this e2e test, not external input
	require.NoError(t, err, "GET %s", url)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read body from GET %s", url)
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s: %s", url, raw)

	var body map[string]any
	require.NoError(t, json.Unmarshal(raw, &body), "unmarshal body from GET %s: %s", url, raw)
	return body
}
