package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// ftableEUniqueID is the unique_id for the deliberately-broken ftable_e model
// in service-2. Its SQL references public.wrong_name — a relation that does not
// exist — so dbt emits a "does not exist" error during validation, which the
// classifier categorises as "logic" and routes to emit.
const ftableEUniqueID = "e2e_schema.ftable_e"

// TestE2E_Remediation_ValidationRejectionEmitsTrigger drives the remediation
// classifier end-to-end through the realistic emit path:
//
//	POST /releases (service-2, ftable_e as changed node)
//	→ validation.requested:v1 → executor/k8s run a real dbt --empty job
//	→ ftable_e fails (references public.wrong_name which does not exist)
//	→ validation.completed:v1 (release rejected)
//	→ release.rejected:v1 → remediation classifier classifies the node
//	→ remediation.requested:v1 emitted with category=logic
//	→ classification_decision row: source=validation, decision=emit, category=logic
//
// The changed node is ftable_e (service-2), whose SQL body is:
//
//	SELECT c.id FROM e2e_schema.ftable_c c LEFT JOIN public.wrong_name w ON c.id = w.id
//
// The "does not exist" error in the dbt log classifies as category=logic
// (rule: "does not exist" → "logic:missing_object") and decision=emit.
//
// Scoping note: the DROP path (infra_transient) is NOT driven in e2e. Forcing a
// real infra failure (warehouse connection refused, OOMKilled) mid-validation is
// impractical to stage deterministically in a shared cluster. That path is
// covered by the Task 4 unit tests in remediation/domain/failure/classify_test.go.
// This test covers the realistic, drivable EMIT path only.
func TestE2E_Remediation_ValidationRejectionEmitsTrigger(t *testing.T) {
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

	releaseID := "e2e-rem-" + uuid.NewString()[:8]
	changedService := "service-2"
	t.Logf("release_id=%s changed_service=%s changed_node=%s", releaseID, changedService, ftableEUniqueID)

	// 1. Read baseline manifests from S3 (uploaded by setup.sh). Build the prod
	//    snapshot of every node EXCEPT ftable_e, so the derived changed set is
	//    exactly {ftable_e} — the one deliberately-broken node.
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

	// 2. Copy the changed service's baseline manifest to the new release key so
	//    manifest-controller can fetch it for this release.
	copyS3Object(t, ctx, clients,
		baselineManifestKey(changedService),
		canonicalManifestObjectKey(changedService, releaseID),
	)

	// 3. Reset the queue, seed current_prod (all nodes except ftable_e), and seed
	//    service_prod for all services except service-2 so assembly can reconstruct
	//    the full topology.
	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)

	// 4. POST /releases for service-2 with the new release ID.
	postRelease(t, clients, changedService, releaseID, changedImageTag, false)

	// 5. Confirm the derived validation set is exactly {ftable_e}.
	assertValidationRequestedNodes(t, ctx, clients, releaseID, []string{ftableEUniqueID})

	// 6. Wait for the release to reach "rejected" status. ftable_e's SQL
	//    references public.wrong_name (does not exist), so validation fails.
	//    Do NOT use waitForReleasePromoted — it fatals on rejected status.
	waitForReleaseRejected(t, ctx, clients, releaseID, 10*time.Minute)

	// 7. Poll remediation.requested:v1 for a trigger whose payload matches this
	//    release + node. The classifier receives release.rejected:v1, fetches the
	//    dbt log from S3, classifies the "does not exist" error as logic, and
	//    publishes the trigger via the outbox publisher.
	var trigger remediationRequestedPayload
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
				trigger = p
				return true, nil
			}
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for remediation.requested:v1 for release %s node %s", releaseID, ftableEUniqueID))

	// 8. Assert trigger payload fields.
	require.Equal(t, releaseID, trigger.ReleaseID, "trigger release_id")
	require.Equal(t, ftableEUniqueID, trigger.NodeID, "trigger node_id")
	require.Equal(t, "logic", trigger.Category,
		"ftable_e references public.wrong_name → 'does not exist' → category=logic")
	require.Equal(t, "validation", trigger.Source,
		"trigger source must be the literal 'validation', not the stream name")
	require.NotEmpty(t, trigger.ErrorSignature, "trigger must carry a non-empty error_signature")
	require.NotEmpty(t, trigger.DBTLogURI, "trigger must carry a non-empty dbt_log_uri")
	t.Logf("✅ remediation.requested:v1 received: category=%s signature=%s", trigger.Category, trigger.ErrorSignature)

	// 9. Query classification_decision in continuo_remediation to confirm the
	//    decision was persisted.
	var decision classificationDecisionRow
	pollUntil(t, ctx, 30*time.Second, 1*time.Second, func() (bool, error) {
		err := clients.remediationDB.GetContext(ctx, &decision,
			`SELECT source, release_id, node_id, category, decision
			   FROM classification_decision
			  WHERE source = 'validation' AND release_id = $1 AND node_id = $2
			  LIMIT 1`,
			releaseID, ftableEUniqueID)
		return err == nil, nil
	}, fmt.Sprintf("timeout waiting for classification_decision row for release %s node %s", releaseID, ftableEUniqueID))

	require.Equal(t, "validation", decision.Source, "decision source")
	require.Equal(t, releaseID, decision.ReleaseID, "decision release_id")
	require.Equal(t, ftableEUniqueID, decision.NodeID, "decision node_id")
	require.Equal(t, "logic", decision.Category, "decision category")
	require.Equal(t, "emit", decision.Decision, "logic failures must route to emit, not drop")
	t.Log("✅ classification_decision row persisted: source=validation decision=emit category=logic")
}

// waitForReleaseRejected polls GET /releases/{id} until the release reaches
// "rejected" status with at least one failing node. This is used instead of
// waitForReleasePromoted so a rejected release is treated as success — it is
// the expected outcome of a deliberately-broken node under validation.
func waitForReleaseRejected(t *testing.T, ctx context.Context, clients *testClients, releaseID string, timeout time.Duration) {
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
			FailingNodes []string `json:"failing_nodes"`
		}
		if json.Unmarshal(body, &r) != nil {
			return false, nil
		}
		return r.Status == "rejected" && len(r.FailingNodes) > 0, nil
	}, fmt.Sprintf("timeout waiting for release %s to reach rejected status", releaseID))
	t.Logf("✅ release %s reached rejected status (ftable_e failed validation as expected)", releaseID)
}

// remediationRequestedPayload mirrors the remediation.requested:v1 wire shape
// produced by the remediation classifier's outbox publisher.
type remediationRequestedPayload struct {
	EventID        string `json:"event_id"`
	Source         string `json:"source"`
	ReleaseID      string `json:"release_id"`
	NodeID         string `json:"node_id"`
	Category       string `json:"category"`
	ErrorSignature string `json:"error_signature"`
	DBTLogURI      string `json:"dbt_log_uri"`
}

// classificationDecisionRow captures the fields asserted from
// continuo_remediation.classification_decision.
type classificationDecisionRow struct {
	Source    string `db:"source"`
	ReleaseID string `db:"release_id"`
	NodeID    string `db:"node_id"`
	Category  string `db:"category"`
	Decision  string `db:"decision"`
}
