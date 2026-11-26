package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
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
//	→ validation.requested:v1 → execution-controller runs a real dbt --empty job
//	→ ftable_e fails (references public.wrong_name which does not exist)
//	→ validation.result:v1 terminal (kind=complete, release rejected)
//	→ release.rejected:v1 → remediation classifier classifies the node
//	→ remediation.requested:v3 emitted for the release, its ftable_e entry
//	  carrying category=logic and no unfixable mark
//	→ classification_decision row: source=validation, decision=emit, category=logic
//
// The changed node is ftable_e (service-2), whose SQL body is:
//
//	SELECT c.id FROM e2e_schema.ftable_c c LEFT JOIN public.wrong_name w ON c.id = w.id
//
// The "does not exist" error in the dbt log classifies as category=logic
// (rule: "does not exist" → "logic:missing_object") and decision=emit.
//
// This test covers the EMIT path. The DROP path (infra_transient) is driven by
// TestE2E_Remediation_EveryNodeUnfixableIsHandedOver, whose model fails with
// an error that names the classifier's "connection refused" phrase.
func TestE2E_Remediation_ValidationRejectionEmitsTrigger(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyComponentsHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-rem-" + uuid.NewString()[:8]
	changedProject := "service-2"
	t.Logf("release_id=%s changed_project=%s changed_node=%s", releaseID, changedProject, ftableEUniqueID)

	// 1. Read baseline manifests from S3 (uploaded by setup.sh). Build the prod
	//    snapshot of every node EXCEPT ftable_e, so the derived changed set is
	//    exactly {ftable_e} — the one deliberately-broken node.
	allProjects := baselineProjects(t, ctx, clients)
	require.NotEmpty(t, allProjects,
		"no baseline manifests under s3://%s/<project>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allProjects[changedProject].imageTag
	require.NotEmpty(t, changedImageTag,
		"image_tag missing for %s — setup.sh must seed service_prod", changedProject)

	var prodNodes []map[string]string
	ftableFound := false
	for svc, si := range allProjects {
		for _, n := range si.nodes {
			if n.uniqueID == ftableEUniqueID {
				ftableFound = true
				continue // exclude → ftable_e becomes the sole changed node
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"project":      svc,
				"content_hash": n.contentHash,
			})
		}
	}
	require.True(t, ftableFound,
		"ftable_e not found in any manifest — is the model in service-2 and the image rebuilt?")
	t.Logf("seeded prod snapshot with %d nodes (ftable_e excluded)", len(prodNodes))

	// 2. Reset the queue, seed current_prod (all nodes except ftable_e), and seed
	//    service_prod for every baseline project so assembly can reconstruct
	//    the full topology.
	// Tap the remediation streams before the release is posted: the trim loop
	// removes consumed entries, so the polls below read what the taps recorded.
	rejectedTap := startStreamTap(t, ctx, clients.redisClient, streams.ReleaseRejectedV1)
	requestedTap := startStreamTap(t, ctx, clients.redisClient, streams.RemediationRequestedV3)

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProd(t, ctx, clients, allProjects)

	// 4. POST /releases for service-2 with the new release ID.
	postRelease(t, clients, changedProject, releaseID, changedImageTag, false)

	// 5. Wait for the release to reach "rejected" status. ftable_e's SQL
	//    references public.wrong_name (does not exist), so validation fails.
	//    Do NOT use waitForReleasePromoted — it fatals on rejected status.
	waitForReleaseRejected(t, ctx, clients, releaseID, 10*time.Minute)

	// 6. Assert release.rejected:v1 carries run_results_uri for the failing node.
	//    the validation runner emits a structured result block (status=error,
	//    message "...does not exist") which execution-controller uploads to S3; the URI
	//    threads through the validation.result:v1 terminal (kind=complete) →
	//    release.rejected:v1. Its presence
	//    is what makes the classifier take the structured branch below.
	var rejectedRunResultsURI string
	pollUntil(t, ctx, 2*time.Minute, 2*time.Second, func() (bool, error) {
		msgs := rejectedTap.Entries()
		for _, msg := range msgs {
			raw, ok := msg.Values["payload"].(string)
			if !ok || raw == "" {
				continue
			}
			var p struct {
				ReleaseID string `json:"release_id"`
				PerNode   []struct {
					NodeID        string `json:"node_id"`
					RunResultsURI string `json:"run_results_uri"`
				} `json:"per_node"`
			}
			if json.Unmarshal([]byte(raw), &p) != nil || p.ReleaseID != releaseID {
				continue
			}
			for _, n := range p.PerNode {
				if n.NodeID == ftableEUniqueID && n.RunResultsURI != "" {
					rejectedRunResultsURI = n.RunResultsURI
					return true, nil
				}
			}
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for release.rejected:v1 with run_results_uri for node %s", ftableEUniqueID))
	require.NotEmpty(t, rejectedRunResultsURI,
		"ftable_e must carry run_results_uri on release.rejected:v1 (structured validation result)")
	t.Logf("✅ release.rejected:v1 carries run_results_uri=%s for ftable_e", rejectedRunResultsURI)

	// 7. Poll remediation.requested:v3 for the release's batched trigger and
	//    the entry it carries for ftable_e. The classifier receives
	//    release.rejected:v1, fetches the dbt log from S3, classifies the
	//    "does not exist" error as logic, and publishes one trigger for the
	//    whole rejected release via the outbox publisher.
	trigger := waitForBatchedTrigger(t, ctx, requestedTap, releaseID, []string{ftableEUniqueID}, 3*time.Minute)
	node, _ := trigger.findNode(ftableEUniqueID)

	// 8. Assert trigger payload fields.
	require.Equal(t, releaseID, trigger.ReleaseID, "trigger release_id")
	require.Equal(t, string(model.RemediationSourceValidation), trigger.Source,
		"trigger source must be the remediation_source value, not the stream name")
	require.Equal(t, ftableEUniqueID, node.NodeID, "trigger node_id")
	require.Equal(t, "logic", node.Category,
		"ftable_e references public.wrong_name → structured status=error, message 'does not exist' → category=logic")
	require.Nil(t, node.Unfixable, "a logic failure is one a fix can reach, so the trigger must not mark it unfixable")
	require.NotEmpty(t, node.ErrorSignature, "trigger must carry a non-empty error_signature")
	require.NotEmpty(t, node.DBTLogURI, "trigger must carry a non-empty dbt_log_uri")
	t.Logf("✅ %s received: category=%s signature=%s", streams.RemediationRequestedV3, node.Category, node.ErrorSignature)

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

// ancestorIDs is the changed-ancestor ids of one trigger entry, for assertions
// that only care which ancestors were named. The changed ancestors are what
// lets the agent group several failures that share one changed ancestor into a
// single upstream fix, and — because each carries the location the rejected
// release's candidate declares — edit that ancestor where THIS release holds
// it.
func ancestorIDs(n pkgevents.RemediationRequestedNode) []string {
	ids := make([]string, 0, len(n.ChangedAncestors))
	for _, a := range n.ChangedAncestors {
		ids = append(ids, a.NodeID)
	}
	return ids
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
