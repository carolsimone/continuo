package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
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

	// 2. Copy the changed service's baseline manifest to the new release key.
	copyS3Object(t, ctx, clients,
		baselineManifestKey(changedService),
		canonicalManifestObjectKey(changedService, releaseID),
	)

	// 3. Reset the queue, seed current_prod (all nodes except ftable_e), and seed
	//    service_prod for all services except service-2.
	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)

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
