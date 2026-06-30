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
	"github.com/google/uuid"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// TestE2E_RemediationAgent_ProposesFixForRejection drives a full remediation
// chain from a validation rejection through to a persisted fix proposal:
//
//	POST /releases (service-2, ftable_e as changed node)
//	→ validation fails (ftable_e references public.wrong_name)
//	→ release reaches "rejected" status
//	→ remediation classifier emits remediation.requested:v1
//	→ remediation-agent consumes remediation.requested:v1
//	→ calls stub-llm (propose_fix tool call → deterministic SQL fix)
//	→ writes proposed SQL and diff to S3
//	→ inserts proposal row (status=proposed, attempt=1)
//	→ emits remediation.proposed:v1 via outbox publisher
//
// The stub-llm (tests/e2e/stub-llm/main.go) detects the propose_fix tool in
// the request and returns a deterministic non-streaming response, so the
// remediation-agent behaves exactly as it would with a real LLM endpoint.
func TestE2E_RemediationAgent_ProposesFixForRejection(t *testing.T) {
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
	//     remediation-agent's NodeContext (GetNodeAncestry gRPC) can resolve the
	//     node's file path and service name. In a cold e2e no release has been
	//     promoted yet, so Neo4j is empty; without this seed, NodeContext returns
	//     NOT_FOUND and Step-2 source resolution degrades silently to
	//     source_resolved=false.
	seedFTableETopologyNode(t, ctx, clients)

	// 4. POST /releases for service-2. ftable_e's SQL references
	//    public.wrong_name (a relation that does not exist), so validation fails.
	postRelease(t, clients, changedService, releaseID, changedImageTag, false)

	// 5. Wait for the release to reach "rejected" status.
	waitForReleaseRejected(t, ctx, clients, releaseID, 10*time.Minute)

	// 6. Poll remediation.requested:v1 to confirm the classifier consumed the
	//    rejection and emitted a trigger for ftable_e. This is a precondition for
	//    the remediation-agent to pick it up.
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

	// 7. Poll remediation.proposed:v1. The remediation-agent consumes the trigger,
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

	// (b) Assert a proposal row in continuo_remediation_agent.
	var row proposalRow
	pollUntil(t, ctx, 30*time.Second, 1*time.Second, func() (bool, error) {
		err := clients.remediationAgentDB.GetContext(ctx, &row,
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
		err := clients.remediationAgentDB.GetContext(ctx, &proposalID,
			`SELECT id FROM proposal WHERE release_id = $1 AND node_id = $2 AND attempt = 1 LIMIT 1`,
			releaseID, ftableEUniqueID)
		return err == nil && proposalID != "", nil
	}, fmt.Sprintf("timeout fetching proposal id for release %s node %s", releaseID, ftableEUniqueID))
	t.Logf("proposal id: %s", proposalID)

	// (g) POST /api/remediation/proposals/{id}/pull-request — expect 200 with
	//     pr_url and pr_number from stub-github.
	createPRResp := callCreatePREndpoint(t, ctx, clients.uiBase, proposalID, http.StatusOK)
	require.NotEmpty(t, createPRResp.PRUrl, "pr_url must be non-empty on first create")
	require.Equal(t, 1, createPRResp.PRNumber, "pr_number must be 1 (stub-github)")
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
	//     pr_url, and pr_number=1.
	var finalRow prRow
	pollUntil(t, ctx, 30*time.Second, 1*time.Second, func() (bool, error) {
		err := clients.remediationAgentDB.GetContext(ctx, &finalRow,
			`SELECT pr_state, pr_url, pr_number FROM proposal WHERE id = $1`,
			proposalID)
		if err != nil {
			return false, nil
		}
		return finalRow.PRState == "open", nil
	}, fmt.Sprintf("timeout waiting for proposal %s to reach pr_state='open'", proposalID))

	require.Equal(t, "open", finalRow.PRState, "proposal pr_state must be 'open'")
	require.NotEmpty(t, finalRow.PRUrl, "proposal pr_url must be non-empty")
	require.Equal(t, 1, finalRow.PRNumber, "proposal pr_number must be 1")
	t.Logf("proposal PR state confirmed: pr_state=%s pr_url=%s pr_number=%d",
		finalRow.PRState, finalRow.PRUrl, finalRow.PRNumber)
}

// remediationProposedPayload mirrors the remediation.proposed:v1 wire shape
// produced by the remediation-agent's outbox publisher.
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

// proposalRow captures the fields asserted from continuo_remediation_agent.proposal.
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
// The remediation-agent's Step-2 source resolution calls GetNodeAncestry to
// obtain the node's original_file_path and service_name; without this seed,
// the gRPC call returns NOT_FOUND and source_resolved stays false.
//
// The node uses the same unique_id key format that the release-promotion handler
// writes: "{schema_name}.{table_name}" (e.g. "e2e_schema.ftable_e"). The
// original_file_path matches the dbt manifest value ("models/ftable_e.sql"),
// which the remediation-agent joins with the service_repos.yaml prefix
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

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupSession := clients.neo4jDriver.NewSession(cleanupCtx, neo4jdriver.SessionConfig{
			AccessMode: neo4jdriver.AccessModeWrite,
		})
		defer cleanupSession.Close(cleanupCtx)
		_, _ = cleanupSession.Run(cleanupCtx,
			`MATCH (t:Table {unique_id: 'e2e_schema.ftable_e'}) DETACH DELETE t`,
			nil,
		)
	})
	t.Log("seeded ftable_e topology node in Neo4j (unique_id=e2e_schema.ftable_e, service_name=service-2)")
}
