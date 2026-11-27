package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// xbreakUp / xbreakDown are a DELIBERATELY-BROKEN cross-project chain that pins
// the migration scenario docs/try-it-locally.md depends on: an upstream release
// drops a column a DIFFERENT project still reads.
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
// validated against the candidate-built upstream, where execution-controller's
// execution-time SQL rewrite (approach B — see the revert in
// dbt/projects/service-2/models/xprobe_down.sql's history) redirects
// e2e_schema.xbreak_up to the candidate schema and the warehouse raises
// `column "amount_eur" does not exist`.
//
// This is the CLEAN counterpart to xprobe_up/xprobe_down (the happy-path
// cross-project pair validated by TestE2E_ReleasePromote_GatedCrossProjectUpstream):
// same cross-project wiring and seeding, but the downstream reads a column the
// upstream no longer emits.
const (
	xbreakUpUniqueID   = "e2e_schema.xbreak_up"
	xbreakDownUniqueID = "e2e_schema.xbreak_down"
)

// TestE2E_ReleasePromote_RejectsColumnBreakAcrossProjects proves the cross-project
// column-break guard end to end, the one scenario no existing single test covers:
//
//	POST /releases (service-2) → cross-project closure schedules
//	{xbreak_up, xbreak_down} with the gating edge xbreak_down -> xbreak_up →
//	execution-controller builds xbreak_up (service-3) into the candidate schema first →
//	xbreak_down validates against it and fails on the DROPPED amount_eur column
//	(valid SQL, column simply gone — not a missing relation) →
//	validation.result:v1 terminal rejects the release with reject_reason
//	validation_failed → the live release does NOT move.
//
// xbreak_down lives in service-2, xbreak_up lives in service-3. The test posts a
// per-project release for service-2. Assembly attaches service-3's baseline
// manifest (via the production pointers), so topology-controller sees xbreak_up as part of
// the candidate topology. The live release is seeded with every node EXCEPT the
// xbreak pair, so the derived changed set is exactly {xbreak_up, xbreak_down}
// with the cross-project gating edge xbreak_down -> xbreak_up — identical seeding
// to the happy-path cross-project test, so only the column mismatch differs.
func TestE2E_ReleasePromote_RejectsColumnBreakAcrossProjects(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// additive polls: assertValidationRequestedNodes(8m) + waitForReleaseRejected(12m),
	// the latter covering the upstream candidate build plus the downstream's failing
	// validation job — two sequential dbt jobs, as in the cross-project promote test.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyComponentsHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-colbreak-" + uuid.NewString()[:8]
	// xbreak_down is in service-2; posting a release for service-2 causes assembly
	// to attach service-3's manifest (which contains xbreak_up) from the production pointers,
	// exposing the cross-project gating edge.
	changedProject := "service-2"
	t.Logf("release_id=%s changed_project=%s", releaseID, changedProject)

	excluded := map[string]bool{
		xbreakUpUniqueID:   true,
		xbreakDownUniqueID: true,
	}
	allProjects := baselineProjects(t, ctx, clients)
	require.NotEmpty(t, allProjects,
		"no baseline manifests under s3://%s/<project>/e2e-baseline/ — setup.sh must run first", e2eS3Bucket)

	changedImageTag := allProjects[changedProject].imageTag
	require.NotEmpty(t, changedImageTag,
		"image_tag missing for %s — setup.sh must seed the production pointers", changedProject)

	var prodNodes []map[string]string
	var upFound, downFound bool
	for svc, si := range allProjects {
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
				"project":      svc,
				"content_hash": n.contentHash,
			})
		}
	}
	require.True(t, upFound,
		"xbreak_up not found in any manifest — is the model in service-3 and the image rebuilt + manifests re-uploaded?")
	require.True(t, downFound,
		"xbreak_down not found in any manifest — is the model in service-2 and the image rebuilt + manifests re-uploaded?")
	t.Logf("seeded the live release with %d nodes (xbreak pair excluded)", len(prodNodes))

	// Tap the streams before the release is posted: the trim loop removes
	// consumed entries, so the checks below read what the taps recorded.
	validationTap := startStreamTap(t, ctx, clients.redisClient, streams.ValidationRequestedV1)

	resetReleaseControllerQueue(t, ctx, clients)
	seedLiveRelease(t, ctx, clients, prodNodes)
	seedProductionPointers(t, ctx, clients, allProjects)

	// Capture the live release after seeding its fresh artifact.
	// A rejected release must leave this exact value in place.
	var liveReleaseBefore string
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT release_id FROM live_release`).Scan(&liveReleaseBefore))

	postRelease(t, clients, changedProject, releaseID, changedImageTag, false)

	// The cross-project closure must schedule BOTH nodes (upstream first), proving
	// the downstream was pulled in as a consumer of the changed upstream and that
	// the break is exercised across a project boundary — not within one project.
	assertValidationRequestedNodes(t, ctx, validationTap, releaseID,
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

	// The live release must NOT have moved to the rejected release: a rejected release
	// is never promoted, so the live release stays exactly where it was.
	var liveReleaseAfter string
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT release_id FROM live_release`).Scan(&liveReleaseAfter))
	assert.NotEqual(t, releaseID, liveReleaseAfter,
		"a rejected release must never become the live release")
	assert.Equal(t, liveReleaseBefore, liveReleaseAfter,
		"the live release must be unchanged by a rejected release")
	t.Logf("✅ cross-project column break rejected as validation_failed; the live release is unchanged (release_id=%q)", liveReleaseAfter)
}
