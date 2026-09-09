package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// tbind / tbind_ok are service-2's dbt-test bind-check fixtures (see
// dbt/services/service-2/models/tbind.yml,
// dbt/services/service-2/tests/assert_tbind_amount_positive.sql, and the
// dbt_project.yml tags that keep them off every shared schedule). tbind's
// tests name amount_eur, a column tbind does not emit, so both its generic
// not_null test and its singular assert test fail their bind check while the
// model itself validates. tbind_ok's own not_null test binds. The three test
// manifest ids are dbt-generated (a hash suffix for the generic test, none
// for the singular one) and were captured against the real baseline manifest
// by the Task 0 spike.
const (
	tbindUniqueID        = "e2e_schema.tbind"
	tbindOkUniqueID      = "e2e_schema.tbind_ok"
	tbindNotNullTestID   = "test.service_2.not_null_tbind_amount_eur.0b6064e439"
	tbindSingularTestID  = "test.service_2.assert_tbind_amount_positive"
	tbindOkNotNullTestID = "test.service_2.not_null_tbind_ok_id.8f19f4e52d"
)

// TestE2E_ReleaseValidation_TestThatNoLongerBindsRejects proves that a dbt
// test naming a column its model no longer emits rejects the release on the
// tests alone: the model itself builds fine, both of its tests fail their
// bind check, the classifier carries both tests in one trigger, and the
// remediation attempt skips them as non-targets rather than trying to "fix" a
// test.
func TestE2E_ReleaseValidation_TestThatNoLongerBindsRejects(t *testing.T) {
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

	releaseID := "e2e-tbind-" + uuid.NewString()[:8]
	changedService := "service-2"
	allServices := baselineServices(t, ctx, clients)
	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag)

	// Production holds everything, tbind at a stale hash so it is the only
	// changed node; its two tests are pulled in as its descendants.
	seen := map[string]bool{}
	var prodNodes []map[string]string
	for _, si := range allServices {
		for _, n := range si.nodes {
			seen[n.uniqueID] = true
			hash := n.contentHash
			if n.uniqueID == tbindUniqueID {
				hash = "stale-" + hash
			}
			prodNodes = append(prodNodes, map[string]string{"unique_id": n.uniqueID, "content_hash": hash})
		}
	}
	for _, id := range []string{tbindUniqueID, tbindNotNullTestID, tbindSingularTestID} {
		require.True(t, seen[id], "%s missing from the baseline manifests — rebuild service-2 and re-upload", id)
	}
	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)

	postRelease(t, clients, changedService, releaseID, changedImageTag, false)
	assertValidationRequestedNodes(t, ctx, clients, releaseID, []string{tbindUniqueID, tbindNotNullTestID, tbindSingularTestID})
	waitForReleaseRejected(t, ctx, clients, releaseID, batchRejectBudget)

	detail := getReleaseJSON(t, clients, releaseID)
	require.Equal(t, "validation_failed", detail["reject_reason"])
	failing := releaseFailingNodes(t, ctx, clients, releaseID)
	require.ElementsMatch(t, []string{tbindNotNullTestID, tbindSingularTestID}, failing, "only the tests fail; the model binds")

	perNode := releasePerNodeResults(t, ctx, clients, releaseID)
	require.Equal(t, "ok", perNode[tbindUniqueID].Status)
	require.Equal(t, "dbt-test", perNode[tbindNotNullTestID].NodeType)
	require.Equal(t, "failed", perNode[tbindNotNullTestID].Status)

	// The classifier carries both tests; the agent skips them as non-targets.
	trigger := waitForBatchedTrigger(t, ctx, clients, releaseID, []string{tbindNotNullTestID, tbindSingularTestID}, batchTriggerBudget)
	for _, n := range trigger.Nodes {
		require.Equal(t, "dbt-test", n.NodeType)
	}
	row := waitForBatchProposal(t, ctx, clients, releaseID, 1, "skipped", batchProposalBudget)
	outcomes := decodeNodeOutcomes(t, row.NodeOutcomes)
	for _, id := range []string{tbindNotNullTestID, tbindSingularTestID} {
		require.Equal(t, "skipped", outcomes[id].Status)
		require.Equal(t, "dbt tests are not fix targets; fix the model or edit the test by hand", outcomes[id].Reason)
	}
	t.Logf("✅ release %s rejected on tbind's two tests alone; remediation skipped both as non-targets", releaseID)
}

// TestE2E_ReleaseValidation_TestsBindAndArePromotedInvisibly proves that a dbt
// test whose bind check passes is fully invisible downstream of validation:
// tbind_ok and its test are new to production, both bind-check ok, the
// release promotes, and the orchestrator's Neo4j graph gains the model but no
// node for the test — while current_prod still records the test so it is not
// re-checked as "changed" on the next release.
func TestE2E_ReleaseValidation_TestsBindAndArePromotedInvisibly(t *testing.T) {
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

	releaseID := "e2e-tbind-ok-" + uuid.NewString()[:8]
	changedService := "service-2"
	allServices := baselineServices(t, ctx, clients)
	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag)

	// tbind_ok and its test are held back entirely from production, so both
	// arrive as new nodes in the assembled candidate topology.
	held := map[string]bool{tbindOkUniqueID: false, tbindOkNotNullTestID: false}
	var prodNodes []map[string]string
	for _, si := range allServices {
		for _, n := range si.nodes {
			if _, excluded := held[n.uniqueID]; excluded {
				held[n.uniqueID] = true
				continue
			}
			prodNodes = append(prodNodes, map[string]string{"unique_id": n.uniqueID, "content_hash": n.contentHash})
		}
	}
	for id, found := range held {
		require.True(t, found, "%s missing from the baseline manifests — rebuild service-2 and re-upload", id)
	}
	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)

	postRelease(t, clients, changedService, releaseID, changedImageTag, false)
	assertValidationRequestedNodes(t, ctx, clients, releaseID, []string{tbindOkUniqueID, tbindOkNotNullTestID})
	waitForReleasePromoted(t, ctx, clients, releaseID, batchRejectBudget)
	// The orchestrator swaps the Neo4j topology asynchronously after the release
	// reports promoted; wait for tbind_ok to be current for this release before
	// reading its :Table properties (test_count), or the read races the swap.
	waitForTopologySwap(t, ctx, clients, releaseID, tbindOkUniqueID, 2*time.Minute)

	perNode := releasePerNodeResults(t, ctx, clients, releaseID)
	require.Equal(t, "ok", perNode[tbindOkNotNullTestID].Status, "the test binds")

	rows := queryNeo4jRows(t, ctx, clients, `MATCH (t:Table {unique_id: $uid}) RETURN t.unique_id AS uid`, map[string]any{"uid": tbindOkNotNullTestID})
	require.Empty(t, rows, "a test is never promoted to the graph")
	require.EqualValues(t, 1, neo4jScalarInt(ctx, clients, `MATCH (t:Table {unique_id: $uid}) RETURN t.test_count AS v`, map[string]any{"uid": tbindOkUniqueID}),
		"test_count on the tested model is unchanged by tests being nodes")

	var prodHasTest int
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT count(*) FROM current_prod, jsonb_array_elements(topology_snapshot) n WHERE n->>'unique_id' = $1`, tbindOkNotNullTestID).Scan(&prodHasTest))
	require.Equal(t, 1, prodHasTest, "current_prod keeps the test so it is not re-checked next release")
	t.Logf("✅ release %s promoted invisibly: tbind_ok's test binds but never reaches Neo4j", releaseID)
}
