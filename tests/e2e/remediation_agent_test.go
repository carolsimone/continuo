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
	remediationv1 "github.com/carolsimone/continuo/agent-remediation/api/remediation/v1"
	"github.com/carolsimone/continuo/pkg/streams"
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
//	→ remediation classifier emits one remediation.requested:v2 for the release
//	→ agent-remediation consumes it and groups its failing set
//	→ calls stub-llm (propose_fix tool call → deterministic SQL fix)
//	→ writes proposed SQL and diff to S3
//	→ inserts proposal row (status=verifying, attempt=1) and posts one
//	  verification run per edited service, which lays the proposed source over
//	  the service's dbt project and runs the real compile + validation pipeline
//	→ the verification run stops at "passed"
//	→ the reconciler flips the proposal to status=proposed
//	→ emits remediation.proposed:v1 via outbox publisher
//
// The stub-llm (tests/e2e/stub-llm/main.go) detects the propose_fix tool in
// the request and returns a deterministic non-streaming response, so the
// agent-remediation behaves exactly as it would with a real LLM endpoint.
func TestE2E_AgentRemediation_ProposesFixForRejection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// 35 minutes: the rejected release and the verification run that verifies
	// the fix each run a full compile + validation pipeline in kind, and the
	// verification run only runs after the model has answered.
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
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

	// 6. Poll remediation.requested:v2 to confirm the classifier consumed the
	//    rejection and emitted the release's batched trigger with an entry for
	//    ftable_e. This is a precondition for the agent-remediation to pick it up.
	pollUntil(t, ctx, 3*time.Minute, 2*time.Second, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, streams.RemediationRequestedV2, "-", "+").Result()
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
			if p.ReleaseID != releaseID {
				continue
			}
			if _, ok := p.findNode(ftableEUniqueID); ok {
				return true, nil
			}
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for %s for release %s node %s", streams.RemediationRequestedV2, releaseID, ftableEUniqueID))

	// 7. Poll remediation.proposed:v1. The agent-remediation consumes the trigger,
	//    calls the stub-llm (which returns a deterministic propose_fix tool call),
	//    writes artifacts to S3, persists the proposal row, and posts a
	//    verification run that compiles and validates the proposed source. This
	//    event is published only once that run reports back passed, so the
	//    budget covers a whole second pipeline run, not just the model call.
	var proposed remediationProposedPayload
	pollUntil(t, ctx, 20*time.Minute, 3*time.Second, func() (bool, error) {
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
	}, fmt.Sprintf("timeout waiting for %s for release %s node %s", streams.RemediationProposedV1, releaseID, ftableEUniqueID))

	// (a) Assert remediation.proposed:v1 payload fields.
	require.Equal(t, ftableEUniqueID, proposed.NodeID, "proposed node_id")
	require.Equal(t, releaseID, proposed.ReleaseID, "proposed release_id")
	require.Equal(t, []string{ftableEUniqueID}, proposed.ResolvedNodeIDs,
		"ftable_e is the release's only failing node, so the attempt resolves exactly it")
	require.NotEmpty(t, proposed.ProposedSQLURI, "proposed_sql_uri must be non-empty")
	require.NotEmpty(t, proposed.DiffURI, "diff_uri must be non-empty")
	require.NotEmpty(t, proposed.Confidence, "confidence must be set")
	require.Equal(t, "validation", proposed.Source, "source must be 'validation'")
	t.Logf("%s received: confidence=%s sql_uri=%s", streams.RemediationProposedV1, proposed.Confidence, proposed.ProposedSQLURI)

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
		"%s must carry source_resolved=true", streams.RemediationProposedV1)

	// (c) Assert the proposed-SQL S3 object exists at proposed_sql_uri and holds
	//     the repaired model source: the surviving read of ftable_c is kept and
	//     the join to the relation that does not exist is gone.
	sqlKey := stripS3Prefix(proposed.ProposedSQLURI)
	require.NotEmpty(t, sqlKey, "could not parse key from proposed_sql_uri=%s", proposed.ProposedSQLURI)
	sqlBody := getS3ObjectByKey(t, ctx, clients, sqlKey)
	require.NotEmpty(t, sqlBody, "proposed SQL object at %s must not be empty", proposed.ProposedSQLURI)
	require.Contains(t, string(sqlBody), "e2e_schema.ftable_c",
		"proposed SQL at %s must keep reading the upstream the model depends on; got %q",
		proposed.ProposedSQLURI, strings.TrimSpace(string(sqlBody)))
	require.NotContains(t, string(sqlBody), "wrong_name",
		"proposed SQL at %s must no longer join the relation that does not exist; got %q",
		proposed.ProposedSQLURI, strings.TrimSpace(string(sqlBody)))
	t.Logf("proposed SQL object fetched from %s: %q", proposed.ProposedSQLURI, strings.TrimSpace(string(sqlBody)))

	// (c2) The proposal was verified by a real dbt verification run, not
	//      accepted on the model's word: the row names one verification, for a
	//      dbt-kind manifest, and that run reached "passed". It is also never
	//      readable as a release. Both waits below return immediately here —
	//      the event in (7) is emitted only after the verdict, so the run is
	//      already terminal — reading it rather than waiting for it.
	var verificationsJSON []byte
	require.NoError(t, clients.agentRemediationDB.GetContext(ctx, &verificationsJSON,
		`SELECT verifications FROM proposal WHERE release_id = $1 AND node_id = $2 AND attempt = 1`,
		releaseID, ftableEUniqueID))
	verifications := decodeVerifications(t, verificationsJSON)
	require.Len(t, verifications, 1,
		"the fix edits one service, so exactly one verification run judged it")
	require.Equal(t, "dbt", verifications[0].Kind,
		"a dbt model's fix must be verified under a dbt manifest, not a python contract")
	require.NotEmpty(t, verifications[0].RunID,
		"a verification must name the run that produced its verdict")
	assertPipelineNamedVerification(t, ctx, clients, verifications[0].RunID, 2*time.Minute)
	waitForVerificationStatus(t, ctx, clients, verifications[0].RunID, "passed", 2*time.Minute)
	assertNotListedAsRelease(t, ctx, clients, verifications[0].RunID)
	t.Logf("verification confirmed: service=%s kind=%s run=%s",
		verifications[0].Service, verifications[0].Kind, verifications[0].RunID)

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
	//     one pull_requests entry from stub-github. ftable_e's fix touches one
	//     service (service-2), and every edit now carries its cluster's member
	//     node ids, so PRServices never falls back to the legacy [""] group —
	//     the route opens exactly one pull request, attributed to service-2.
	createPRResp := callCreatePREndpoint(t, ctx, clients.uiBase, proposalID, http.StatusOK)
	require.Empty(t, createPRResp.Errors, "no per-service group should fail to open a pull request; got %+v", createPRResp.Errors)
	require.Len(t, createPRResp.PullRequests, 1,
		"a single-service proposal must open exactly one pull request; got %+v", createPRResp.PullRequests)
	pr := createPRResp.PullRequests[0]
	require.Equal(t, changedService, pr.Service, "the pull request must be attributed to ftable_e's owning service")
	require.NotEmpty(t, pr.PRUrl, "pr_url must be non-empty on first create")
	// stub-github assigns PR numbers on an auto-incrementing, shared-process
	// counter (see tests/e2e/stub-github/main.go), so only positivity is
	// guaranteed here, not a specific value.
	require.Greater(t, pr.PRNumber, 0, "pr_number must be a positive stub-github PR number")
	t.Logf("PR created: service=%s pr_url=%s pr_number=%d", pr.Service, pr.PRUrl, pr.PRNumber)

	// (h) Idempotency: a second POST to the same endpoint must not create a
	//     second PR. The service's claim is now 'open', so beginPullRequest CAS
	//     conflicts with FAILED_PRECONDITION; the route recognizes the embedded
	//     pr_url in that error and reports the existing pull request as a
	//     success rather than an error — so the whole route still answers 200
	//     with the SAME pull_requests set, not a 409.
	dupResp := callCreatePREndpoint(t, ctx, clients.uiBase, proposalID, http.StatusOK)
	require.Empty(t, dupResp.Errors, "a duplicate POST must not report any per-service failure; got %+v", dupResp.Errors)
	require.Equal(t, createPRResp.PullRequests, dupResp.PullRequests,
		"a second POST must return the SAME pull request set, opening nothing new")
	t.Logf("idempotency confirmed: second POST returned the same set: %+v", dupResp.PullRequests)

	// (i) Assert the (proposal, service) child row now reflects pr_state='open',
	//     a non-empty pr_url, the same pr_number the create call returned, and a
	//     branch carrying the /<service> suffix BuildBranch appends for a real
	//     (non-legacy) owning-service group. The parent proposal's singular
	//     pr_state/pr_url/pr_number columns are not written once a proposal
	//     enters the per-service split (db/migration/agent_remediation/V17__
	//     proposal_pull_requests.sql) — every PR-lifecycle read now goes
	//     through proposal_pull_request, keyed by (proposal_id, service).
	finalRow := pollChildPRState(t, ctx, clients, proposalID, changedService, "open", 30*time.Second)
	require.NotEmpty(t, finalRow.PRUrl, "proposal_pull_request.pr_url must be non-empty")
	require.Equal(t, pr.PRNumber, finalRow.PRNumber, "the row must record the number the create call returned")
	require.True(t, strings.HasSuffix(finalRow.Branch, "/"+changedService),
		"a per-service pull request's branch must carry the /<service> suffix; got %q", finalRow.Branch)
	t.Logf("proposal_pull_request state confirmed: service=%s pr_state=%s pr_url=%s pr_number=%d branch=%s",
		changedService, finalRow.PRState, finalRow.PRUrl, finalRow.PRNumber, finalRow.Branch)

	// (j) Merge the PR in stub-github; the reconciler must observe the merge
	//     and flip the (proposal, service) child row to the terminal
	//     pr_state='merged' with pr_closed_at set. repo is still a column on
	//     the parent proposal row — it names the single monorepo every service
	//     fixture shares, and is written once when the attempt is created,
	//     independent of the per-service PR split.
	var repoName string
	require.NoError(t, clients.agentRemediationDB.GetContext(ctx, &repoName,
		`SELECT repo FROM proposal WHERE id = $1`, proposalID))
	mergeCommitSHA := mergePullRequestViaStub(t, ctx, repoName, finalRow.PRNumber)
	t.Logf("merged PR #%d via stub-github: merge_commit_sha=%s", finalRow.PRNumber, mergeCommitSHA)

	closedRow := pollChildPRState(t, ctx, clients, proposalID, changedService, "merged", 60*time.Second)
	require.NotNil(t, closedRow.PRClosedAt, "pr_closed_at must be set on merge")

	// (k) The terminal outcome was emitted atomically: a remediation_pr_closed
	//     outbox row exists for this proposal and service, and its payload
	//     carries the service the PR belongs to and the non-empty edits it
	//     closed with.
	closedPayload := latestPRClosedPayload(t, ctx, clients, proposalID, changedService)
	require.Equal(t, "merged", closedPayload.Outcome, "pr_closed payload outcome")
	require.Equal(t, changedService, closedPayload.Service, "pr_closed payload must carry the service this PR belongs to")
	require.NotEmpty(t, closedPayload.Edits, "pr_closed payload must carry the merged PR's edits")
	t.Logf("close-loop confirmed: pr_state=merged pr_closed_at=%s service=%s edits=%d",
		closedRow.PRClosedAt, closedPayload.Service, len(closedPayload.Edits))
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
	//
	//    This proposal was seeded directly (no edits/member node ids), so
	//    PRServices reads it as the legacy, unsplit group — service "" — and
	//    every write BeginPullRequest/the opening sweep make lands on the
	//    (proposal, "") child row in proposal_pull_request, not on the parent
	//    proposal's own (unwritten, post-split) pr_* columns.
	row := pollChildPRState(t, ctx, clients, proposalID, "", "open", 30*time.Second)

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
	EventID   string `json:"event_id"`
	Source    string `json:"source"`
	ReleaseID string `json:"release_id"`
	NodeID    string `json:"node_id"`
	// ResolvedNodeIDs is every failing node this attempt addressed, sorted.
	// One attempt now repairs a release's whole failing set, so NodeID is only
	// the representative of this list.
	ResolvedNodeIDs []string `json:"resolved_node_ids"`
	ErrorSignature  string   `json:"error_signature"`
	ProposedSQLURI  string   `json:"proposed_sql_uri"`
	DiffURI         string   `json:"diff_uri"`
	Rationale       string   `json:"rationale"`
	Confidence      string   `json:"confidence"`
	Model           string   `json:"model"`
	Attempt         int      `json:"attempt"`
	SourceResolved  bool     `json:"source_resolved"`
	ProposedAt      string   `json:"proposed_at"`
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

// prRow captures the PR-lifecycle fields asserted after calling the
// create-PR endpoint, read from the (proposal, service) child row —
// proposal_pull_request — the table every pull request's lifecycle now lives
// on (db/migration/agent_remediation/V17__proposal_pull_requests.sql). The
// parent proposal's singular pr_* columns stop being written once a proposal
// enters the per-service split; they remain only for a legacy (pre-split) row.
type prRow struct {
	PRState     string     `db:"pr_state"`
	PRUrl       string     `db:"pr_url"`
	PRNumber    int        `db:"pr_number"`
	Branch      string     `db:"branch"`
	PRClosedAt  *time.Time `db:"pr_closed_at"`
	PRClaimedAt *time.Time `db:"pr_claimed_at"`
	PROpenedAt  *time.Time `db:"pr_opened_at"`
}

// pollChildPRState polls the (proposal, service) pull-request child row until
// pr_state reaches want, and returns it. Every status change is logged, so a
// row stuck in 'opening' or 'open' is distinguishable from one that was never
// claimed at all.
func pollChildPRState(
	t *testing.T, ctx context.Context, clients *testClients,
	proposalID, service, want string, timeout time.Duration,
) prRow {
	t.Helper()
	var row prRow
	var last string
	pollUntil(t, ctx, timeout, 1*time.Second, func() (bool, error) {
		err := clients.agentRemediationDB.GetContext(ctx, &row, `
			SELECT pr_state, pr_url, pr_number, branch, pr_closed_at, pr_claimed_at, pr_opened_at
			  FROM proposal_pull_request
			 WHERE proposal_id = $1 AND service = $2`,
			proposalID, service)
		if err != nil {
			return false, nil
		}
		if row.PRState != last {
			t.Logf("proposal_pull_request %s/%s: pr_state=%s", proposalID, service, row.PRState)
			last = row.PRState
		}
		return row.PRState == want, nil
	}, fmt.Sprintf("timeout waiting for proposal %s service %q to reach pr_state=%q (see the logged status changes above)",
		proposalID, service, want))
	return row
}

// pullRequestResult mirrors one entry of the create-PR route's pull_requests
// array: a pull request that exists after the request completes, either just
// opened or already open from an earlier attempt.
type pullRequestResult struct {
	Service  string `json:"service"`
	PRUrl    string `json:"pr_url"`
	PRNumber int    `json:"pr_number"`
}

// pullRequestRouteError mirrors one entry of the create-PR route's errors
// array: an owning-service group that failed to produce a pull request.
type pullRequestRouteError struct {
	Service string `json:"service"`
	Error   string `json:"error"`
}

// createPRRouteResponse captures the JSON body returned by POST
// /api/remediation/proposals/:id/pull-request: 200 when every owning-service
// group succeeded, 207 when some but not all did, 502 when none did. There is
// no longer a singular pr_url/pr_number at the top level — a proposal can
// open more than one pull request, one per owning service.
type createPRRouteResponse struct {
	PullRequests []pullRequestResult     `json:"pull_requests"`
	Errors       []pullRequestRouteError `json:"errors"`
}

// callCreatePREndpoint POSTs to POST /api/remediation/proposals/{id}/pull-request,
// asserts the given expected status code, and returns the parsed response
// body. A proposal whose groups all already have a pull request (a duplicate
// call) still answers 200 with the same pull_requests set — beginPullRequest's
// FAILED_PRECONDITION is recognized by the route as "already open" and
// reported as a success, not surfaced as a per-group error — so this same
// helper covers both the first call and the idempotency check.
func callCreatePREndpoint(t *testing.T, ctx context.Context, uiBase, proposalID string, wantStatus int) createPRRouteResponse {
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

	var result createPRRouteResponse
	require.NoError(t, json.Unmarshal(raw, &result), "unmarshal body from POST %s: %s", url, raw)
	return result
}

// mergePullRequestViaStub PUTs the stub-github merge endpoint for pull request
// number n on repo, asserts it succeeds, and returns the merge commit sha the
// stub reports. The e2e suite runs inside the orchestrator container, so
// stub-github is reached by its compose service name — the same base URL
// agent-remediation and ui use.
func mergePullRequestViaStub(t *testing.T, ctx context.Context, repo string, n int) string {
	t.Helper()
	url := fmt.Sprintf("http://stub-github:9200/repos/%s/pulls/%d/merge", repo, n)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, nil)
	require.NoError(t, err, "build stub merge request")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	require.NoError(t, err, "PUT %s", url)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "stub merge must succeed")

	var body struct {
		SHA    string `json:"sha"`
		Merged bool   `json:"merged"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body), "decode merge response from %s", url)
	require.True(t, body.Merged, "stub merge response must report merged=true")
	return body.SHA
}

// closePullRequestViaStub PATCHes the stub-github pull request n on repo to
// state "closed" without merging — the "closed" half of the merge/close-loop
// contract stub-github's PATCH endpoint implements
// (tests/e2e/stub-github/main.go). A PR closed this way is observed by the
// reconciler as PROutcomeRejected, drawing no case-base provenance edges.
func closePullRequestViaStub(t *testing.T, ctx context.Context, repo string, n int) {
	t.Helper()
	url := fmt.Sprintf("http://stub-github:9200/repos/%s/pulls/%d", repo, n)
	body, err := json.Marshal(map[string]string{"state": "closed"})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewReader(body))
	require.NoError(t, err, "build stub close request")
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	require.NoError(t, err, "PATCH %s", url)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "stub close must succeed")
}

// closedEdit mirrors one element of the remediation.pr_closed:v1 payload's
// edits array: this PR's per-file close detail, including whether a human
// amended it before merge.
type closedEdit struct {
	Path         string `json:"path"`
	TargetNodeID string `json:"target_node_id"`
	Amended      bool   `json:"amended"`
	Diff         string `json:"diff"`
}

// prClosedPayload mirrors the remediation.pr_closed:v1 wire shape emitted
// when one (proposal, service) pull request reaches a terminal outcome.
type prClosedPayload struct {
	ProposalID      string       `json:"proposal_id"`
	ReleaseID       string       `json:"release_id"`
	ResolvedNodeIDs []string     `json:"resolved_node_ids"`
	Service         string       `json:"service"`
	Outcome         string       `json:"outcome"`
	Edits           []closedEdit `json:"edits"`
}

// latestPRClosedPayload reads the most recent remediation_pr_closed outbox row
// for (proposalID, service) and decodes its payload. It is keyed on both
// columns because a multi-service proposal's PRs close independently, each
// with its own outbox row naming only its own service.
func latestPRClosedPayload(t *testing.T, ctx context.Context, clients *testClients, proposalID, service string) prClosedPayload {
	t.Helper()
	var raw []byte
	require.NoError(t, clients.agentRemediationDB.GetContext(ctx, &raw,
		`SELECT payload FROM remediation_agent_outbox
		  WHERE event_type = 'remediation_pr_closed'
		    AND payload->>'proposal_id' = $1
		    AND payload->>'service' = $2
		  ORDER BY created_at DESC LIMIT 1`,
		proposalID, service),
		"expected a remediation.pr_closed:v1 outbox row for proposal %s service %s", proposalID, service)
	var payload prClosedPayload
	require.NoError(t, json.Unmarshal(raw, &payload), "decode pr_closed payload: %s", raw)
	return payload
}

// amendedFile is one file's content to write into a new commit pushed
// directly onto a PR branch through stub-github's git-write endpoints,
// simulating a human amending the PR before merge.
type amendedFile struct {
	Path    string
	Content string
}

// stubGitTreeEntry mirrors one entry of a POST git/trees request body — the
// shape octokit's createTree call sends and tests/e2e/stub-github/main.go's
// handleGitTrees records.
type stubGitTreeEntry struct {
	Path string `json:"path"`
	Mode string `json:"mode"`
	Type string `json:"type"`
	SHA  string `json:"sha"`
}

// pushAmendedCommit pushes a new commit directly onto branch through
// stub-github's git-write endpoints (POST git/blobs, POST git/trees, POST
// git/commits, PATCH git/refs/heads/{branch}) — the same primitives the ui
// pull-request creator uses (ui/src/server/github/pull-request-creator.ts),
// called directly here to simulate a human editing the PR before merge.
//
// The stub does NOT inherit a tree's base_tree entries: POST git/trees
// records exactly the entries it is given, and a later read resolves a path
// only against that SAME tree's own entries (tests/e2e/stub-github/main.go's
// commitTreeBlob), never by walking base_tree to an earlier commit's files.
// So files must list every path this commit should resolve to — not just the
// one(s) actually changing — or an untouched path in the same PR would read
// back 404 (ErrSourceNotFound) at merge time and be misread as amended too.
func pushAmendedCommit(t *testing.T, ctx context.Context, repo, branch string, files []amendedFile) {
	t.Helper()
	require.NotEmpty(t, files, "pushAmendedCommit needs at least one file")
	httpClient := &http.Client{Timeout: 10 * time.Second}

	entries := make([]stubGitTreeEntry, 0, len(files))
	for _, f := range files {
		blobBody, err := json.Marshal(map[string]string{"content": f.Content, "encoding": "utf-8"})
		require.NoError(t, err, "marshal blob body for %s", f.Path)
		blobURL := fmt.Sprintf("http://stub-github:9200/repos/%s/git/blobs", repo)
		blobReq, err := http.NewRequestWithContext(ctx, http.MethodPost, blobURL, bytes.NewReader(blobBody))
		require.NoError(t, err, "build blob request for %s", f.Path)
		blobReq.Header.Set("Content-Type", "application/json")
		blobResp, err := httpClient.Do(blobReq)
		require.NoError(t, err, "POST %s", blobURL)
		require.Equal(t, http.StatusCreated, blobResp.StatusCode, "create blob for %s", f.Path)
		var blob struct {
			SHA string `json:"sha"`
		}
		require.NoError(t, json.NewDecoder(blobResp.Body).Decode(&blob), "decode blob response for %s", f.Path)
		blobResp.Body.Close()
		entries = append(entries, stubGitTreeEntry{Path: f.Path, Mode: "100644", Type: "blob", SHA: blob.SHA})
	}

	treeBody, err := json.Marshal(map[string]any{"tree": entries})
	require.NoError(t, err, "marshal tree body")
	treeURL := fmt.Sprintf("http://stub-github:9200/repos/%s/git/trees", repo)
	treeReq, err := http.NewRequestWithContext(ctx, http.MethodPost, treeURL, bytes.NewReader(treeBody))
	require.NoError(t, err, "build tree request")
	treeReq.Header.Set("Content-Type", "application/json")
	treeResp, err := httpClient.Do(treeReq)
	require.NoError(t, err, "POST %s", treeURL)
	require.Equal(t, http.StatusCreated, treeResp.StatusCode, "create tree")
	var tree struct {
		SHA string `json:"sha"`
	}
	require.NoError(t, json.NewDecoder(treeResp.Body).Decode(&tree), "decode tree response")
	treeResp.Body.Close()

	commitBody, err := json.Marshal(map[string]any{
		"message": "amend: human edit pushed before merge",
		"tree":    tree.SHA,
	})
	require.NoError(t, err, "marshal commit body")
	commitURL := fmt.Sprintf("http://stub-github:9200/repos/%s/git/commits", repo)
	commitReq, err := http.NewRequestWithContext(ctx, http.MethodPost, commitURL, bytes.NewReader(commitBody))
	require.NoError(t, err, "build commit request")
	commitReq.Header.Set("Content-Type", "application/json")
	commitResp, err := httpClient.Do(commitReq)
	require.NoError(t, err, "POST %s", commitURL)
	require.Equal(t, http.StatusCreated, commitResp.StatusCode, "create commit")
	var commit struct {
		SHA string `json:"sha"`
	}
	require.NoError(t, json.NewDecoder(commitResp.Body).Decode(&commit), "decode commit response")
	commitResp.Body.Close()

	refBody, err := json.Marshal(map[string]any{"sha": commit.SHA, "force": true})
	require.NoError(t, err, "marshal ref-update body")
	refURL := fmt.Sprintf("http://stub-github:9200/repos/%s/git/refs/heads/%s", repo, branch)
	refReq, err := http.NewRequestWithContext(ctx, http.MethodPatch, refURL, bytes.NewReader(refBody))
	require.NoError(t, err, "build ref-update request")
	refReq.Header.Set("Content-Type", "application/json")
	refResp, err := httpClient.Do(refReq)
	require.NoError(t, err, "PATCH %s", refURL)
	require.Equal(t, http.StatusOK, refResp.StatusCode, "move branch to amended commit")
	refResp.Body.Close()

	t.Logf("pushed amended commit %s to %s@%s (%d file(s))", commit.SHA, repo, branch, len(files))
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
