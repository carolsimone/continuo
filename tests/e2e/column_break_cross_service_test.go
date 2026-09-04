package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// xbreakUp / xbreakDown are a DELIBERATELY-BROKEN cross-service chain that pins
// the migration scenario docs/try-it-locally.md depends on: an upstream release
// drops a column a DIFFERENT service still reads.
//
// xbreak_down (service-2) reads the amount_eur column of xbreak_up (service-3)
// by raw schema-qualified name:
//
//	SELECT amount_eur FROM e2e_schema.xbreak_up
//
// The current xbreak_up model, however, produces only `id` (SELECT 1 AS id) —
// amount_eur has been dropped upstream, mirroring a real
// fx_transactions_eur.amount_eur removal. BOTH models are valid SQL that COMPILES;
// the break is not a missing relation. It surfaces only when the downstream is
// validated against the candidate-built upstream, where the executor's
// execution-time SQL rewrite (approach B — see the revert in
// dbt/services/service-2/models/xprobe_down.sql's history) redirects
// e2e_schema.xbreak_up to the candidate schema and the warehouse raises
// `column "amount_eur" does not exist`.
//
// This is the CLEAN counterpart to xprobe_up/xprobe_down (the happy-path
// cross-service pair validated by TestE2E_ReleasePromote_GatedCrossServiceUpstream):
// same cross-service wiring and seeding, but the downstream reads a column the
// upstream no longer emits.
const (
	xbreakUpUniqueID   = "e2e_schema.xbreak_up"
	xbreakDownUniqueID = "e2e_schema.xbreak_down"
)

// TestE2E_ReleasePromote_RejectsColumnBreakAcrossServices proves the cross-service
// column-break guard end to end, the one scenario no existing single test covers:
//
//	POST /releases (service-2) → cross-service closure schedules
//	{xbreak_up, xbreak_down} with the gating edge xbreak_down -> xbreak_up →
//	executor/k8s build xbreak_up (service-3) into the candidate schema first →
//	xbreak_down validates against it and fails on the DROPPED amount_eur column
//	(valid SQL, column simply gone — not a missing relation) →
//	validation.result:v1 terminal rejects the release with reject_reason
//	validation_failed → current_prod does NOT move.
//
// xbreak_down lives in service-2, xbreak_up lives in service-3. The test posts a
// per-service release for service-2. Assembly attaches service-3's baseline
// manifest (via service_prod), so topology-controller sees xbreak_up as part of
// the candidate topology. current_prod is seeded with every node EXCEPT the
// xbreak pair, so the derived changed set is exactly {xbreak_up, xbreak_down}
// with the cross-service gating edge xbreak_down -> xbreak_up — identical seeding
// to the happy-path cross-service test, so only the column mismatch differs.
func TestE2E_ReleasePromote_RejectsColumnBreakAcrossServices(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// additive polls: assertValidationRequestedNodes(8m) + waitForReleaseRejected(12m),
	// the latter covering the upstream candidate build plus the downstream's failing
	// validation job — two sequential dbt jobs, as in the cross-service promote test.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-colbreak-" + uuid.NewString()[:8]
	// xbreak_down is in service-2; posting a release for service-2 causes assembly
	// to attach service-3's manifest (which contains xbreak_up) from service_prod,
	// exposing the cross-service gating edge.
	changedService := "service-2"
	t.Logf("release_id=%s changed_service=%s", releaseID, changedService)

	excluded := map[string]bool{
		xbreakUpUniqueID:   true,
		xbreakDownUniqueID: true,
	}
	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices,
		"no baseline manifests under s3://%s/<service>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allServices[changedService].imageTag
	require.NotEmpty(t, changedImageTag,
		"image_tag missing for %s — setup.sh must seed service_prod", changedService)

	var prodNodes []map[string]string
	var upFound, downFound bool
	for _, si := range allServices {
		for _, n := range si.nodes {
			switch n.uniqueID {
			case xbreakUpUniqueID:
				upFound = true
			case xbreakDownUniqueID:
				downFound = true
			}
			if excluded[n.uniqueID] {
				continue
			}
			prodNodes = append(prodNodes, map[string]string{
				"unique_id":    n.uniqueID,
				"content_hash": n.contentHash,
			})
		}
	}
	require.True(t, upFound,
		"xbreak_up not found in any manifest — is the model in service-3 and the image rebuilt + manifests re-uploaded?")
	require.True(t, downFound,
		"xbreak_down not found in any manifest — is the model in service-2 and the image rebuilt + manifests re-uploaded?")
	t.Logf("seeded prod snapshot with %d nodes (xbreak pair excluded)", len(prodNodes))

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)

	// Capture current_prod's release pointer AFTER seeding (seedCurrentProd resets
	// it to ""). A rejected release must leave this exact value in place.
	var currentProdBefore string
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT release_id FROM current_prod`).Scan(&currentProdBefore))

	postRelease(t, clients, changedService, releaseID, changedImageTag, false)

	// The cross-service closure must schedule BOTH nodes (upstream first), proving
	// the downstream was pulled in as a consumer of the changed upstream and that
	// the break is exercised across a service boundary — not within one service.
	assertValidationRequestedNodes(t, ctx, clients, releaseID,
		[]string{xbreakUpUniqueID, xbreakDownUniqueID})

	// xbreak_up builds cleanly into the candidate schema; xbreak_down's
	// SELECT amount_eur then fails against it (the column was dropped upstream),
	// so the release is rejected. Do NOT use waitForReleasePromoted — it fatals on
	// rejected status, which is the expected outcome here.
	waitForReleaseRejected(t, ctx, clients, releaseID, 12*time.Minute)

	// The rejection must be a VALIDATION failure specifically — not a parse-time
	// duplicate_table, a compile_failed, or a missing-relation error. reject_reason
	// is the enum release-controller records in Run.Fail("validation_failed", ...)
	// (release-controller/service/handlers/handle_validation_result.go) and surfaces
	// verbatim through GET /releases/{id}.reject_reason.
	detail := getReleaseJSON(t, clients, releaseID)
	assert.Equal(t, "rejected", detail["status"], "release must be rejected")
	assert.Equal(t, "validation_failed", detail["reject_reason"],
		"a downstream column break (valid SQL, column gone) must reject as validation_failed")

	// The failing node must be the downstream that read the dropped column — the
	// upstream itself validates fine (it just no longer emits amount_eur).
	failingRaw, _ := detail["failing_nodes"].([]any)
	failing := make([]string, 0, len(failingRaw))
	for _, f := range failingRaw {
		if s, ok := f.(string); ok {
			failing = append(failing, s)
		}
	}
	assert.Contains(t, failing, xbreakDownUniqueID,
		"the downstream node that read the dropped amount_eur column must be the failing node")

	// current_prod must NOT have moved to the rejected release: a rejected release
	// is never promoted, so the production pointer stays exactly where it was.
	var currentProdAfter string
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT release_id FROM current_prod`).Scan(&currentProdAfter))
	assert.NotEqual(t, releaseID, currentProdAfter,
		"a rejected release must never become current_prod")
	assert.Equal(t, currentProdBefore, currentProdAfter,
		"current_prod must be unchanged by a rejected release")
	t.Logf("✅ cross-service column break rejected as validation_failed; current_prod unchanged (release_id=%q)", currentProdAfter)
}
