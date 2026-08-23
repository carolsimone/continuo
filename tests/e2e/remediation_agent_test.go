package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/carolsimone/continuo/pkg/streams"
	remediationv1 "github.com/carolsimone/continuo/agent-remediation/api/remediation/v1"
	"github.com/google/uuid"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// TestE2E_AgentRemediation_ProposesFixForRejection drives a full remediation
// chain from a validation rejection through to a persisted fix proposal:
//
//	POST /releases (service-2, ftable_e as changed node)
//	→ validation fails (ftable_e references public.wrong_name)
//	→ release reaches "rejected" status
//	→ remediation classifier emits remediation.requested:v1
//	→ agent-remediation consumes remediation.requested:v1
//	→ calls stub-llm (propose_fix tool call → deterministic SQL fix)
//	→ writes proposed SQL and diff to S3
//	→ inserts proposal row (status=proposed, attempt=1)
//	→ emits remediation.proposed:v1 via outbox publisher
//
// The stub-llm (tests/e2e/stub-llm/main.go) detects the propose_fix tool in
// the request and returns a deterministic non-streaming response, so the
// agent-remediation behaves exactly as it would with a real LLM endpoint.
func TestE2E_AgentRemediation_ProposesFixForRejection(t *testing.T) {
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

	releaseID := "e2e-rem-agent-" + uuid.NewString()[:8]
	changedService := "service-2"
	t.Logf("release_id=%s changed_service=%s changed_node=%s", releaseID, changedService, ftableEUniqueID)

	// 1. Read baseline manifests from S3 (uploaded by setup.sh). Build the prod
	//    snapshot of every node except ftable_e so the derived changed set is
	//    exactly {ftable_e} — the deliberately-broken node.
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

	// 2. Reset the queue, seed current_prod (all nodes except ftable_e), and seed
	//    service_prod for all services except service-2.
	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)

	// 3b. Seed the ftable_e :Table topology node in Neo4j so that the
	//     agent-remediation's Locator (GetNodeLocation gRPC) can resolve the
	//     node's file path and service name. In a cold e2e no release has been
	//     promoted yet, so Neo4j is empty; without this seed, GetNodeLocation
	//     returns NOT_FOUND and Step-2 source resolution degrades silently to
	//     source_resolved=false.
	seedFTableETopologyNode(t, ctx, clients)

	// 4. POST /releases for service-2. ftable_e's SQL references
	//    public.wrong_name (a relation that does not exist), so validation fails.
	postRelease(t, clients, changedService, releaseID, changedImageTag, false)

	// 5. Wait for the release to reach "rejected" status.
	waitForReleaseRejected(t, ctx, clients, releaseID, 10*time.Minute)

	// 6. Poll remediation.requested:v1 to confirm the classifier consumed the
	//    rejection and emitted a trigger for ftable_e. This is a precondition for
	//    the agent-remediation to pick it up.
	pollUntil(t, ctx, 3*time.Minute, 2*time.Second, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, streams.RemediationRequestedV1, "-", "+").Result()
		if err != nil {
			return false, nil
		}
		for _, msg := range msgs {
			raw, ok := msg.Values["payload"].(string)
			if !ok || raw == "" {
				continue
			}
			var p remediationRequestedPayload
			if json.Unmarshal([]byte(raw), &p) != nil {
				continue
			}
			if p.ReleaseID == releaseID && p.NodeID == ftableEUniqueID {
				return true, nil
			}
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for remediation.requested:v1 for release %s node %s", releaseID, ftableEUniqueID))

	// 7. Poll remediation.proposed:v1. The agent-remediation consumes the trigger,
	//    calls the stub-llm (which returns a deterministic propose_fix tool call),
	//    writes artifacts to S3, persists the proposal row, and publishes this event.
	var proposed remediationProposedPayload
	pollUntil(t, ctx, 5*time.Minute, 3*time.Second, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, streams.RemediationProposedV1, "-", "+").Result()
		if err != nil {
			return false, nil
		}
		for _, msg := range msgs {
			raw, ok := msg.Values["payload"].(string)
			if !ok || raw == "" {
				continue
			}
			var p remediationProposedPayload
			if json.Unmarshal([]byte(raw), &p) != nil {
				continue
			}
			if p.ReleaseID == releaseID && p.NodeID == ftableEUniqueID {
				proposed = p
				return true, nil
			}
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for remediation.proposed:v1 for release %s node %s", releaseID, ftableEUniqueID))

	// (a) Assert remediation.proposed:v1 payload fields.
	require.Equal(t, ftableEUniqueID, proposed.NodeID, "proposed node_id")
	require.Equal(t, releaseID, proposed.ReleaseID, "proposed release_id")
	require.NotEmpty(t, proposed.ProposedSQLURI, "proposed_sql_uri must be non-empty")
	require.NotEmpty(t, proposed.DiffURI, "diff_uri must be non-empty")
	require.NotEmpty(t, proposed.Confidence, "confidence must be set")
	require.Equal(t, "validation", proposed.Source, "source must be 'validation'")
	t.Logf("remediation.proposed:v1 received: confidence=%s sql_uri=%s", proposed.Confidence, proposed.ProposedSQLURI)

	// (b) Assert a proposal row in continuo_agent_remediation.
	var row proposalRow
	pollUntil(t, ctx, 30*time.Second, 1*time.Second, func() (bool, error) {
		err := clients.agentRemediationDB.GetContext(ctx, &row,
			`SELECT source, release_id, node_id, status, attempt,
			        source_resolved, proposed_sql_uri
			   FROM proposal
			  WHERE release_id = $1 AND node_id = $2
			  LIMIT 1`,
			releaseID, ftableEUniqueID)
		return err == nil, nil
	}, fmt.Sprintf("timeout waiting for proposal row for release %s node %s", releaseID, ftableEUniqueID))

	require.Equal(t, "validation", row.Source, "proposal source")
	require.Equal(t, releaseID, row.ReleaseID, "proposal release_id")
	require.Equal(t, ftableEUniqueID, row.NodeID, "proposal node_id")
	require.Equal(t, "proposed", row.Status, "proposal status must be 'proposed'")
	require.Equal(t, 1, row.Attempt, "first proposal attempt must be 1")
	t.Logf("proposal row confirmed: status=%s attempt=%d", row.Status, row.Attempt)

	// (d) Assert the source-resolved path succeeded: stub-github served the real
	//     model source and stub-llm (Step-2 branch) returned the corrected source.
	//     The proposal row must reflect source_resolved=true and the URI must end
	//     with ".source.sql" (the key suffix written by the handler for Step-2).
	require.True(t, row.SourceResolved, "proposal row must have source_resolved=true (stub-github served real source)")
	require.True(t,
		strings.HasSuffix(row.ProposedSQLURI, ".source.sql"),
		"proposed_sql_uri must end with .source.sql when source_resolved=true; got %s", row.ProposedSQLURI)
	t.Logf("source-resolved confirmed: proposed_sql_uri=%s", row.ProposedSQLURI)

	// (e) Assert the remediation.proposed:v1 event carries source_resolved=true.
	//     This field is set by the handler's enqueue call and is forwarded on
	//     the wire so downstream consumers can distinguish source-level proposals
	//     from candidate-SQL proposals.
	require.True(t, proposed.SourceResolved,
		"remediation.proposed:v1 must carry source_resolved=true; stream=%s", streams.RemediationProposedV1)

	// (c) Assert the proposed-SQL S3 object exists at proposed_sql_uri and
	//     contains real dbt {{ ref(...) }} macros — confirming the Step-2 source
	//     fix (not the Step-1 candidate SQL) was stored as the final proposal.
	sqlKey := stripS3Prefix(proposed.ProposedSQLURI)
	require.NotEmpty(t, sqlKey, "could not parse key from proposed_sql_uri=%s", proposed.ProposedSQLURI)
	sqlBody := getS3ObjectByKey(t, ctx, clients, sqlKey)
	require.NotEmpty(t, sqlBody, "proposed SQL object at %s must not be empty", proposed.ProposedSQLURI)
	require.True(t,
		strings.Contains(string(sqlBody), "{{ ref('table_b') }}"),
		"proposed SQL at %s must contain {{ ref('table_b') }} (real source, not candidate schema); got %q",
		proposed.ProposedSQLURI, strings.TrimSpace(string(sqlBody)))
	t.Logf("proposed SQL object fetched from %s: %q", proposed.ProposedSQLURI, strings.TrimSpace(string(sqlBody)))

	// (f) SELECT the proposal's id from the DB so we can call the PR-creation endpoint.
	var proposalID string
	pollUntil(t, ctx, 10*time.Second, 1*time.Second, func() (bool, error) {
		err := clients.agentRemediationDB.GetContext(ctx, &proposalID,
			`SELECT id FROM proposal WHERE release_id = $1 AND node_id = $2 AND attempt = 1 LIMIT 1`,
			releaseID, ftableEUniqueID)
		return err == nil && proposalID != "", nil
	}, fmt.Sprintf("timeout fetching proposal id for release %s node %s", releaseID, ftableEUniqueID))
	t.Logf("proposal id: %s", proposalID)

	// (g) POST /api/remediation/proposals/{id}/pull-request — expect 200 with
	//     pr_url and pr_number from stub-github.
	createPRResp := callCreatePREndpoint(t, ctx, clients.uiBase, proposalID, http.StatusOK)
	require.NotEmpty(t, createPRResp.PRUrl, "pr_url must be non-empty on first create")
	// stub-github assigns PR numbers on an auto-incrementing, shared-process
	// counter (see tests/e2e/stub-github/main.go), so only positivity is
	// guaranteed here, not a specific value.
	require.Greater(t, createPRResp.PRNumber, 0, "pr_number must be a positive stub-github PR number")
	t.Logf("PR created: pr_url=%s pr_number=%d", createPRResp.PRUrl, createPRResp.PRNumber)

	// (h) Idempotency: a second POST to the same endpoint must not create a second
	//     PR. The proposal's pr_state is now 'open', so beginPullRequest CAS
	//     conflicts and the service returns FAILED_PRECONDITION. The route maps
	//     this to 409 and echoes the existing pr_url in the body.
	dupResp := callCreatePREndpoint409(t, ctx, clients.uiBase, proposalID)
	require.NotEmpty(t, dupResp.PRUrl, "409 body must carry the existing pr_url")
	require.Equal(t, createPRResp.PRUrl, dupResp.PRUrl,
		"idempotent 409 must return the SAME pr_url as the first call")
	t.Logf("idempotency confirmed: second POST returned 409 with pr_url=%s", dupResp.PRUrl)

	// (i) Assert the DB proposal row now reflects pr_state='open', a non-empty
	//     pr_url, and the same pr_number the create call returned.
	var finalRow prRow
	pollUntil(t, ctx, 30*time.Second, 1*time.Second, func() (bool, error) {
		err := clients.agentRemediationDB.GetContext(ctx, &finalRow,
			`SELECT pr_state, pr_url, pr_number FROM proposal WHERE id = $1`,
			proposalID)
		if err != nil {
			return false, nil
		}
		return finalRow.PRState == "open", nil
	}, fmt.Sprintf("timeout waiting for proposal %s to reach pr_state='open'", proposalID))

	require.Equal(t, "open", finalRow.PRState, "proposal pr_state must be 'open'")
	require.NotEmpty(t, finalRow.PRUrl, "proposal pr_url must be non-empty")
	require.Equal(t, createPRResp.PRNumber, finalRow.PRNumber, "proposal pr_number must match the number the create call returned")
	t.Logf("proposal PR state confirmed: pr_state=%s pr_url=%s pr_number=%d",
		finalRow.PRState, finalRow.PRUrl, finalRow.PRNumber)

	// (j) Merge the PR in stub-github; the reconciler must observe the merge
	//     and flip the proposal to the terminal pr_state='merged' with
	//     pr_closed_at set.
	var repoName string
	require.NoError(t, clients.agentRemediationDB.GetContext(ctx, &repoName,
		`SELECT repo FROM proposal WHERE id = $1`, proposalID))
	// The e2e suite runs inside the orchestrator container, so stub-github is
	// reached by its compose service name (the same base URL the
	// agent-remediation uses via GITHUB_BASE_URL).
	mergeURL := fmt.Sprintf("http://stub-github:9200/repos/%s/pulls/%d/merge", repoName, finalRow.PRNumber)
	mergeReq, err := http.NewRequestWithContext(ctx, http.MethodPut, mergeURL, nil)
	require.NoError(t, err, "build stub merge request")
	mergeResp, err := (&http.Client{Timeout: 10 * time.Second}).Do(mergeReq)
	require.NoError(t, err, "PUT %s", mergeURL)
	defer mergeResp.Body.Close()
	require.Equal(t, http.StatusOK, mergeResp.StatusCode, "stub merge must succeed")

	var closedRow struct {
		PRState    string     `db:"pr_state"`
		PRClosedAt *time.Time `db:"pr_closed_at"`
	}
	pollUntil(t, ctx, 60*time.Second, 2*time.Second, func() (bool, error) {
		err := clients.agentRemediationDB.GetContext(ctx, &closedRow,
			`SELECT pr_state, pr_closed_at FROM proposal WHERE id = $1`, proposalID)
		if err != nil {
			return false, nil
		}
		return closedRow.PRState == "merged", nil
	}, fmt.Sprintf("timeout waiting for proposal %s to reach pr_state='merged'", proposalID))
	require.NotNil(t, closedRow.PRClosedAt, "pr_closed_at must be set on merge")

	// (k) The terminal outcome was emitted atomically: a remediation_pr_closed
	//     outbox row exists for this proposal (pending or already published).
	var outboxCount int
	require.NoError(t, clients.agentRemediationDB.GetContext(ctx, &outboxCount,
		`SELECT count(*) FROM remediation_agent_outbox
		  WHERE event_type = 'remediation_pr_closed'
		    AND payload->>'proposal_id' = $1`, proposalID))
	require.GreaterOrEqual(t, outboxCount, 1, "expected a remediation.pr_closed:v1 outbox row for this proposal")
	t.Logf("close-loop confirmed: pr_state=merged pr_closed_at=%s", closedRow.PRClosedAt)
}

// TestE2E_AgentRemediation_OpeningSweepRecoversStrandedPR drives the
// "create-succeeded/record-failed" scenario the opening sweep exists to
// recover: a proposal claimed for PR creation (pr_state='opening') whose PR
// already exists on GitHub for its deterministic branch, but was never
// recorded onto the row. It seeds a proposal row directly (bypassing the full
// validation-rejection-to-remediation pipeline covered by
// TestE2E_AgentRemediation_ProposesFixForRejection above, since this test
// isolates the reconciler's opening sweep rather than that pipeline), claims
// it via the real BeginPullRequest RPC, then creates the PR directly on
// stub-github for the branch BeginPullRequest's response reports — reading
// the persisted branch name from the system rather than re-deriving the
// naming rule, so a change to that rule can't silently keep this test
// passing. It then verifies the sweep finds the PR via stub-github's
// GET /repos/{repo}/pulls?head=... (tests/e2e/stub-github/main.go's
// listPulls) and records it, well inside the 15s
// REMEDIATION_PR_OPENING_GRACE_PERIOD this compose stack sets, proving
// recovery rather than the fail path.
//
// There is no fault-injection hook in ui to force its own
// RecordPullRequest call to fail after a successful PR creation, so this test
// does not drive that failure directly; it reproduces the row state that
// failure leaves behind, which is exactly what the opening sweep resolves
// regardless of how the row got stuck there.
func TestE2E_AgentRemediation_OpeningSweepRecoversStrandedPR(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	releaseID := "e2e-opening-sweep-" + uuid.NewString()[:8]
	const nodeID = "model.p.opening_sweep"
	const repo = "e2e-owner/e2e-opening-sweep-repo"
	const attempt = 1

	// 1. Seed a proposal row directly in 'proposed'/source_resolved=true — the
	//    only precondition the PR-creation path checks — sidestepping the full
	//    validation-rejection pipeline, which this test does not exercise.
	var proposalID string
	require.NoError(t, clients.agentRemediationDB.GetContext(ctx, &proposalID, `
		INSERT INTO proposal
			(source, release_id, node_id, error_signature, attempt, status,
			 source_resolved, repo, commit_sha, file_path, model, created_at)
		VALUES ('validation', $1, $2, 'e2e-opening-sweep', $3, 'proposed',
			true, $4, 'deadbeef', 'models/opening_sweep.sql', 'claude-3-5-sonnet', NOW())
		RETURNING id`,
		releaseID, nodeID, attempt, repo))
	t.Logf("seeded proposal id=%s release_id=%s node_id=%s", proposalID, releaseID, nodeID)

	// 2. Claim the row for PR creation via the real BeginPullRequest RPC — the
	//    same public gRPC call ui's BeginPullRequest step makes. This
	//    performs the actual pr_state ''->'opening' transition and
	//    pr_claimed_at stamp, and returns the branch name the system actually
	//    computed and is about to look for, so the test reads it from the
	//    system rather than re-deriving the naming rule itself.
	claim, err := clients.agentRemediationClient.BeginPullRequest(ctx,
		&remediationv1.BeginPullRequestRequest{Id: proposalID})
	require.NoError(t, err, "BeginPullRequest")
	branch := claim.GetBranch()
	require.NotEmpty(t, branch, "BeginPullRequest response must carry the claimed branch name")
	_, err = time.Parse(time.RFC3339, claim.GetClaimedAt())
	require.NoError(t, err, "BeginPullRequest response must carry a parseable claimed_at — the token a failure callback must CAS against")
	t.Logf("claimed proposal via BeginPullRequest: branch=%s claimed_at=%s", branch, claim.GetClaimedAt())

	// 3. Create the PR directly on stub-github for that SAME branch — the
	//    "create succeeded" half of the scenario. The e2e suite runs inside
	//    the orchestrator container, so stub-github is reached by its compose
	//    service name, same as the agent-remediation's own GITHUB_BASE_URL.
	prBody, err := json.Marshal(map[string]string{
		"title": "e2e opening sweep recovery", "head": branch, "base": "main",
	})
	require.NoError(t, err)
	postURL := fmt.Sprintf("http://stub-github:9200/repos/%s/pulls", repo)
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, postURL, bytes.NewReader(prBody))
	require.NoError(t, err, "build stub-github create-PR request")
	postReq.Header.Set("Content-Type", "application/json")
	postResp, err := (&http.Client{Timeout: 10 * time.Second}).Do(postReq)
	require.NoError(t, err, "POST %s", postURL)
	defer postResp.Body.Close()
	require.Equal(t, http.StatusCreated, postResp.StatusCode, "stub-github PR creation must succeed")

	var created struct {
		Number    int    `json:"number"`
		HTMLURL   string `json:"html_url"`
		CreatedAt string `json:"created_at"`
	}
	require.NoError(t, json.NewDecoder(postResp.Body).Decode(&created))
	require.NotZero(t, created.Number)
	t.Logf("stub-github PR created: number=%d html_url=%s created_at=%s branch=%s",
		created.Number, created.HTMLURL, created.CreatedAt, branch)

	githubCreatedAt, err := time.Parse(time.RFC3339, created.CreatedAt)
	require.NoError(t, err, "parse stub-github created_at")

	// 4. The row is now in exactly the "create-succeeded/record-failed" end
	//    state: pr_state='opening' with a claim time, and a PR that exists on
	//    GitHub for its branch but was never recorded. Wait for the
	//    reconciler's opening sweep (polling every REMEDIATION_PR_POLL_INTERVAL,
	//    5s in this compose stack) to find it via GET pulls?head=... and
	//    record it — well inside REMEDIATION_PR_OPENING_GRACE_PERIOD (15s),
	//    proving recovery, not the fail path.
	var row struct {
		PRState     string     `db:"pr_state"`
		PRUrl       string     `db:"pr_url"`
		PRNumber    int        `db:"pr_number"`
		PRClaimedAt *time.Time `db:"pr_claimed_at"`
		PROpenedAt  *time.Time `db:"pr_opened_at"`
	}
	pollUntil(t, ctx, 30*time.Second, 1*time.Second, func() (bool, error) {
		err := clients.agentRemediationDB.GetContext(ctx, &row,
			`SELECT pr_state, pr_url, pr_number, pr_claimed_at, pr_opened_at FROM proposal WHERE id = $1`,
			proposalID)
		if err != nil {
			return false, nil
		}
		return row.PRState == "open", nil
	}, fmt.Sprintf("timeout waiting for the opening sweep to recover proposal %s", proposalID))

	require.Equal(t, "open", row.PRState, "the opening sweep must have recorded the found PR")
	require.Equal(t, created.HTMLURL, row.PRUrl)
	require.Equal(t, created.Number, row.PRNumber)
	require.Nil(t, row.PRClaimedAt, "recording must clear pr_claimed_at back to NULL")
	require.NotNil(t, row.PROpenedAt)
	require.WithinDuration(t, githubCreatedAt, *row.PROpenedAt, time.Second,
		"a recovered PR must use GitHub's own created_at, not the recovery moment")
	t.Logf("opening sweep recovered the stranded PR: pr_state=%s pr_url=%s pr_number=%d pr_opened_at=%s",
		row.PRState, row.PRUrl, row.PRNumber, row.PROpenedAt)
}

// remediationProposedPayload mirrors the remediation.proposed:v1 wire shape
// produced by the agent-remediation's outbox publisher.
type remediationProposedPayload struct {
	EventID        string `json:"event_id"`
	Source         string `json:"source"`
	ReleaseID      string `json:"release_id"`
	NodeID         string `json:"node_id"`
	ErrorSignature string `json:"error_signature"`
	ProposedSQLURI string `json:"proposed_sql_uri"`
	DiffURI        string `json:"diff_uri"`
	Rationale      string `json:"rationale"`
	Confidence     string `json:"confidence"`
	Model          string `json:"model"`
	Attempt        int    `json:"attempt"`
	SourceResolved bool   `json:"source_resolved"`
	ProposedAt     string `json:"proposed_at"`
}

// proposalRow captures the fields asserted from continuo_agent_remediation.proposal.
type proposalRow struct {
	Source         string `db:"source"`
	ReleaseID      string `db:"release_id"`
	NodeID         string `db:"node_id"`
	Status         string `db:"status"`
	Attempt        int    `db:"attempt"`
	SourceResolved bool   `db:"source_resolved"`
	ProposedSQLURI string `db:"proposed_sql_uri"`
}

// prRow captures the PR-state fields asserted after calling the create-PR endpoint.
type prRow struct {
	PRState  string `db:"pr_state"`
	PRUrl    string `db:"pr_url"`
	PRNumber int    `db:"pr_number"`
}

// createPRResponse captures the JSON body returned by a successful POST
// /api/remediation/proposals/:id/pull-request (HTTP 200).
type createPRResponse struct {
	PRUrl    string `json:"pr_url"`
	PRNumber int    `json:"pr_number"`
}

// createPR409Response captures the JSON body returned by a conflicting POST
// /api/remediation/proposals/:id/pull-request (HTTP 409).
type createPR409Response struct {
	Error string `json:"error"`
	PRUrl string `json:"pr_url"`
}

// callCreatePREndpoint POSTs to POST /api/remediation/proposals/{id}/pull-request,
// asserts the given expected status code, and returns the parsed 200-response body.
// It is called with wantStatus=200 for the first (successful) invocation.
func callCreatePREndpoint(t *testing.T, ctx context.Context, uiBase, proposalID string, wantStatus int) createPRResponse {
	t.Helper()
	url := fmt.Sprintf("%s/api/remediation/proposals/%s/pull-request", uiBase, proposalID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte{}))
	require.NoError(t, err, "build POST pull-request request")
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	require.NoError(t, err, "POST %s", url)
	defer resp.Body.Close()

	require.Equal(t, wantStatus, resp.StatusCode,
		"unexpected status from POST %s (want %d)", url, wantStatus)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read body from POST %s", url)

	var result createPRResponse
	require.NoError(t, json.Unmarshal(raw, &result), "unmarshal 200 body from POST %s: %s", url, raw)
	return result
}

// callCreatePREndpoint409 POSTs to the create-PR endpoint a second time,
// asserts 409, and returns the conflict-response body (carries the existing pr_url).
func callCreatePREndpoint409(t *testing.T, ctx context.Context, uiBase, proposalID string) createPR409Response {
	t.Helper()
	url := fmt.Sprintf("%s/api/remediation/proposals/%s/pull-request", uiBase, proposalID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte{}))
	require.NoError(t, err, "build duplicate POST pull-request request")
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	require.NoError(t, err, "duplicate POST %s", url)
	defer resp.Body.Close()

	require.Equal(t, http.StatusConflict, resp.StatusCode,
		"second POST to %s must return 409 (pr_state already 'open')", url)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read body from duplicate POST %s", url)

	var result createPR409Response
	require.NoError(t, json.Unmarshal(raw, &result), "unmarshal 409 body from POST %s: %s", url, raw)
	return result
}

// getS3ObjectByKey downloads an S3 object from the e2e bucket by key (no s3:// prefix).
func getS3ObjectByKey(t *testing.T, ctx context.Context, clients *testClients, key string) []byte {
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

// stripS3Prefix strips the "s3://<bucket>/" prefix from a URI and returns the
// remaining key. Returns an empty string if the URI does not start with "s3://".
func stripS3Prefix(uri string) string {
	rest, ok := strings.CutPrefix(uri, "s3://")
	if !ok {
		return ""
	}
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return rest[i+1:]
	}
	return ""
}

// seedFTableETopologyNode inserts a minimal :Table node for e2e_schema.ftable_e
// into Neo4j. In a cold e2e no release has been promoted, so Neo4j is empty.
// The agent-remediation's Step-2 source resolution calls GetNodeLocation to
// obtain the node's original_file_path and service_name (the candidate source
// itself is read from the release's code bundle first, falling back to a
// GitHub repo read only when the bundle misses); without this seed,
// GetNodeLocation returns NOT_FOUND and source_resolved stays false.
//
// The node uses the same unique_id key format that the release-promotion handler
// writes: "{schema_name}.{table_name}" (e.g. "e2e_schema.ftable_e"). The
// original_file_path matches the dbt manifest value ("models/ftable_e.sql"),
// which the agent-remediation joins with the service_repos.yaml prefix
// ("services/service-2") to form the full path passed to stub-github.
//
// The node is cleaned up via t.Cleanup so it does not leak into other tests.
func seedFTableETopologyNode(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()
	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode: neo4jdriver.AccessModeWrite,
	})
	defer session.Close(ctx)

	_, err := session.Run(ctx, `
		MERGE (t:Table {unique_id: $uid})
		SET t.schema_name        = $schema_name,
		    t.table_name         = $table_name,
		    t.service_name       = $service_name,
		    t.original_file_path = $file_path,
		    t.node_type          = 'model',
		    t.active             = true
	`, map[string]interface{}{
		"uid":          "e2e_schema.ftable_e",
		"schema_name":  "e2e_schema",
		"table_name":   "ftable_e",
		"service_name": "service-2",
		"file_path":    "models/ftable_e.sql",
	})
	require.NoError(t, err, "seed ftable_e topology node in Neo4j")

	// Seed one upstream :Table with a DEPENDS_ON edge from ftable_e. The
	// agent-remediation's upstream evidence now comes from GetUpstreamChanges,
	// which ranks ancestors by their :NodeVersion history rather than by :Table
	// properties; this cold e2e seeds no :NodeVersion for the ancestor, so it
	// carries no change evidence and GetUpstreamChanges skips it. Kept as a
	// topology fixture only — nothing in this test asserts on it.
	// This ancestor node is private to this test (not the shared ftable_c fixture owned
	// by seed_topology_test.go) so cleanup cannot corrupt or delete a node other e2e
	// tests depend on.
	_, err = session.Run(ctx, `
		MERGE (up:Table {unique_id: $up_uid})
		SET up.schema_name        = $up_schema,
		    up.table_name         = $up_table,
		    up.service_name       = $up_service,
		    up.original_file_path = $up_file,
		    up.node_type          = 'model',
		    up.active             = true
		WITH up
		MATCH (t:Table {unique_id: $uid})
		MERGE (t)-[:DEPENDS_ON]->(up)
	`, map[string]interface{}{
		"up_uid":     "e2e_schema.ftable_upstream_diff",
		"up_schema":  "e2e_schema",
		"up_table":   "ftable_upstream_diff",
		"up_service": "service-2",
		"up_file":    "models/ftable_upstream_diff.sql",
		"uid":        "e2e_schema.ftable_e",
	})
	require.NoError(t, err, "seed ftable_upstream_diff upstream ancestor in Neo4j")

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupSession := clients.neo4jDriver.NewSession(cleanupCtx, neo4jdriver.SessionConfig{
			AccessMode: neo4jdriver.AccessModeWrite,
		})
		defer cleanupSession.Close(cleanupCtx)
		_, _ = cleanupSession.Run(cleanupCtx,
			`MATCH (t:Table) WHERE t.unique_id IN ['e2e_schema.ftable_e', 'e2e_schema.ftable_upstream_diff'] DETACH DELETE t`,
			nil,
		)
	})
	t.Log("seeded ftable_e topology node + ftable_upstream_diff upstream ancestor in Neo4j")
}
