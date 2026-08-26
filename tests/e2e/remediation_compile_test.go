package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// The malformed-Jinja defect this scenario stages is the real
// daily_transactions.sql failure: the config() call is closed too early and
// tags is hoisted outside it, so dbt cannot parse the model and `dbt compile`
// aborts for the whole service project:
//
//	{{ config(materialized='table'), tags=[...])}}
//	select 1 as id
//
// This is documentation of the fixture a cold-stack harness must bake into
// the COMPILE_FIXTURE_SERVICE image. It is NOT compiled by anything in this
// package (tests/e2e is a Go module; *.sql files here are inert), and it is
// deliberately NOT placed under dbt/services/* — setup.sh compiles every
// manifest under dbt/services via `dbt_upload load --services-dir /app/services`,
// so a malformed model there would break the baseline manifest build and every
// other e2e test. See task-7.1-report.md for the full enablement recipe.

// TestE2E_Remediation_CompileFailureProposesFix drives a dbt COMPILE failure end
// to end and proves it (a) rejects the release with per-node compile detail and
// (b) produces a remediation proposal:
//
//	POST /releases (COMPILE_FIXTURE_SERVICE, malformed-Jinja model baked in image)
//	→ release.requested → AdvanceQueue: Received → Compiling → compile.requested:v1
//	→ executor/k8s run `dbt compile` in the fixture image → dbt parse aborts
//	→ compile.node.completed:v1 (outcome=failed, dbt_log_uri) → compile.completed:v1 (status=failed)
//	→ release-controller TransitionToRejected("compile_failed"), RecordStageResults("compile")
//	→ release.rejected:v1 {stage:"compile", per_node:[{node_id:<service>, status:"failed", dbt_log_uri}]}
//	→ remediation classifier maps stage→SourceCompile, ExtractDbtFilePath(log)→file_path,
//	  classifies the parse/compilation error as healable → remediation.requested:v1 (source="compile")
//	→ agent-remediation proposeFromSource: reads model source from GitHub (stub-github),
//	  asks stub-llm to fix it → proposal row (source="compile", file_path) → remediation.proposed:v1.
//
// Unlike the validation path, the compile leg is the FIRST leg and rejects
// before any change-derivation, so no current_prod/service_prod baseline seeding
// is required — the compile job runs `dbt compile` against the fixture image's
// baked source and fails on the malformed model alone.
//
// Harness prerequisite (skip-gated). A clean `dbt compile` is required for the
// three real e2e services (setup.sh compiles their manifests), so the failing
// model cannot live in service-1/2/3. The scenario needs a DEDICATED fixture
// service whose image is built with the malformed-Jinja model source documented
// above and loaded into kind,
// whose image tag is reachable, and whose name is mapped in
// agent-remediation/config/service_repos.yaml (so the agent resolves the repo
// prefix). Until a cold-stack harness provisions that fixture and exports
// COMPILE_FIXTURE_SERVICE / COMPILE_FIXTURE_IMAGE_TAG, this test skips so it
// never destabilises the green suite. See task-7.1-report.md.
func TestE2E_Remediation_CompileFailureProposesFix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	fixtureService := os.Getenv("COMPILE_FIXTURE_SERVICE")
	if fixtureService == "" {
		t.Skip("COMPILE_FIXTURE_SERVICE not set — compile-failure fixture not provisioned " +
			"in this harness; see .superpowers/sdd/task-7.1-report.md for the cold-stack " +
			"enablement recipe (fixture dbt image + kind load + service_repos.yaml mapping)")
	}
	fixtureImageTag := os.Getenv("COMPILE_FIXTURE_IMAGE_TAG")
	require.NotEmpty(t, fixtureImageTag,
		"COMPILE_FIXTURE_IMAGE_TAG must be set alongside COMPILE_FIXTURE_SERVICE")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-cmp-" + uuid.NewString()[:8]
	t.Logf("release_id=%s fixture_service=%s image_tag=%s", releaseID, fixtureService, fixtureImageTag)

	// Clear any mid-flight release so AdvanceQueue can activate this one.
	resetReleaseControllerQueue(t, ctx, clients)

	// POST /releases for the fixture service. AdvanceQueue moves it
	// Received → Compiling and emits compile.requested:v1; the compile job runs
	// `dbt compile` in the fixture image and aborts on the malformed model.
	postRelease(t, clients, fixtureService, releaseID, fixtureImageTag, false)

	// 1. The release must reach "rejected" with at least one failing node (the
	//    compile node, whose node_id is the service name).
	waitForReleaseRejected(t, ctx, clients, releaseID, 12*time.Minute)

	// 2. GET /releases/{id}: reject_reason == "compile_failed", and per_node_results
	//    carries a stage=="compile" entry for the service with a non-empty
	//    dbt_log_uri (the per-node compile detail the UI surfaces).
	assertCompileRejection(t, ctx, clients, releaseID, fixtureService)

	// 3. release.rejected:v1 must carry stage=="compile" for this release — the
	//    discriminator the remediation classifier maps to SourceCompile.
	assertRejectedStageCompile(t, ctx, clients, releaseID)

	// 4. remediation.requested:v1 must be emitted with source=="compile" and a
	//    non-empty file_path (derived by the classifier via ExtractDbtFilePath on
	//    the dbt compile log). The compile node_id is the service name.
	var trigger compileTriggerPayload
	pollUntil(t, ctx, 4*time.Minute, 2*time.Second, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, streams.RemediationRequestedV1, "-", "+").Result()
		if err != nil {
			return false, nil
		}
		for _, msg := range msgs {
			raw, ok := msg.Values["payload"].(string)
			if !ok || raw == "" {
				continue
			}
			var p compileTriggerPayload
			if json.Unmarshal([]byte(raw), &p) != nil {
				continue
			}
			if p.ReleaseID == releaseID && p.NodeID == fixtureService && p.Source == "compile" {
				trigger = p
				return true, nil
			}
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for remediation.requested:v1 (source=compile) for release %s", releaseID))

	require.Equal(t, "compile", trigger.Source, "compile-stage trigger source must be 'compile'")
	require.Equal(t, fixtureService, trigger.NodeID, "compile trigger node_id is the service name")
	require.NotEmpty(t, trigger.FilePath,
		"compile trigger must carry a file_path extracted from the dbt compile log")
	require.NotEmpty(t, trigger.DBTLogURI, "compile trigger must carry a dbt_log_uri")
	// The excerpt is the text the signature was derived from and what the case
	// base records as precedent. On a real dbt log it must be the compilation
	// message naming the broken model — not dbt's `Encountered an error:`
	// lead-in, which precedes every error and would give every compile failure
	// of a service the same signature.
	require.Contains(t, trigger.ErrorExcerpt, "Compilation Error in model daily_transactions",
		"compile trigger error_excerpt must carry the compilation message, got %q", trigger.ErrorExcerpt)
	t.Logf("✅ remediation.requested:v1 (compile): file_path=%s signature=%s excerpt=%q", trigger.FilePath, trigger.ErrorSignature, trigger.ErrorExcerpt)

	// 5. classification_decision row must record source=='compile'.
	var decision classificationDecisionRow
	pollUntil(t, ctx, 30*time.Second, 1*time.Second, func() (bool, error) {
		err := clients.remediationDB.GetContext(ctx, &decision,
			`SELECT source, release_id, node_id, category, decision
			   FROM classification_decision
			  WHERE source = 'compile' AND release_id = $1 AND node_id = $2
			  LIMIT 1`,
			releaseID, fixtureService)
		return err == nil, nil
	}, fmt.Sprintf("timeout waiting for compile classification_decision for release %s", releaseID))
	require.Equal(t, "compile", decision.Source, "decision source must be 'compile'")
	require.Equal(t, "emit", decision.Decision, "a compile parse/syntax error must route to emit, not drop")

	// 6. remediation.proposed:v1 must be emitted for the compile failure with
	//    source=="compile".
	var proposed remediationProposedPayload
	pollUntil(t, ctx, 6*time.Minute, 3*time.Second, func() (bool, error) {
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
			if p.ReleaseID == releaseID && p.NodeID == fixtureService {
				proposed = p
				return true, nil
			}
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for remediation.proposed:v1 (compile) for release %s", releaseID))

	require.Equal(t, "compile", proposed.Source, "proposed event source must be 'compile'")
	require.Equal(t, fixtureService, proposed.NodeID, "proposed node_id is the service name")
	require.NotEmpty(t, proposed.ProposedSQLURI, "compile proposal must carry a proposed_sql_uri")
	require.NotEmpty(t, proposed.DiffURI, "compile proposal must carry a diff_uri")
	require.True(t, proposed.SourceResolved,
		"compile proposals are source-resolved (read from GitHub, not candidate SQL)")

	// 7. The proposal row must persist source=='compile', a non-empty file_path,
	//    and status=='proposed'.
	var row compileProposalRow
	pollUntil(t, ctx, 30*time.Second, 1*time.Second, func() (bool, error) {
		err := clients.agentRemediationDB.GetContext(ctx, &row,
			`SELECT source, release_id, node_id, status, file_path, source_resolved
			   FROM proposal
			  WHERE release_id = $1 AND node_id = $2
			  LIMIT 1`,
			releaseID, fixtureService)
		return err == nil, nil
	}, fmt.Sprintf("timeout waiting for compile proposal row for release %s", releaseID))

	require.Equal(t, "compile", row.Source, "proposal source must be 'compile'")
	require.Equal(t, "proposed", row.Status, "proposal status must be 'proposed'")
	require.NotEmpty(t, row.FilePath, "compile proposal must persist the offending file_path")
	require.True(t, row.SourceResolved, "compile proposal row must have source_resolved=true")
	t.Logf("✅ compile failure rejected with per-node detail and a remediation proposal: file_path=%s sql_uri=%s",
		row.FilePath, proposed.ProposedSQLURI)
}

// assertCompileRejection asserts GET /releases/{id} reports reject_reason
// "compile_failed" and carries a stage=="compile" per-node result for the
// service with a non-empty dbt_log_uri.
func assertCompileRejection(t *testing.T, ctx context.Context, clients *testClients, releaseID, service string) {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("%s/releases/%s", clients.releaseBase, releaseID))
	require.NoError(t, err, "GET /releases/{id}")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var detail struct {
		RejectReason   string `json:"reject_reason"`
		PerNodeResults []struct {
			NodeID    string `json:"node_id"`
			Stage     string `json:"stage"`
			Status    string `json:"status"`
			DBTLogURI string `json:"dbt_log_uri"`
			FilePath  string `json:"file_path"`
		} `json:"per_node_results"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&detail))
	require.Equal(t, "compile_failed", detail.RejectReason,
		"reject_reason must be compile_failed for a dbt compile abort")
	require.NotEmpty(t, detail.PerNodeResults, "release detail must carry per-node compile results")

	var found bool
	for _, n := range detail.PerNodeResults {
		if n.Stage == "compile" && n.NodeID == service {
			found = true
			require.Equal(t, "failed", n.Status, "compile per-node status must be 'failed'")
			require.NotEmpty(t, n.DBTLogURI,
				"compile per-node result must carry a non-empty dbt_log_uri")
		}
	}
	require.True(t, found,
		"per_node_results must contain a stage=='compile' entry for service %s", service)
	t.Logf("✅ release %s rejected: reject_reason=compile_failed with a stage=compile per-node result", releaseID)
}

// assertRejectedStageCompile asserts the release.rejected:v1 event for releaseID
// carries stage=="compile" — the discriminator the remediation classifier maps
// to SourceCompile.
func assertRejectedStageCompile(t *testing.T, ctx context.Context, clients *testClients, releaseID string) {
	t.Helper()
	pollUntil(t, ctx, 2*time.Minute, 2*time.Second, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, streams.ReleaseRejectedV1, "-", "+").Result()
		if err != nil {
			return false, nil
		}
		for _, msg := range msgs {
			raw, ok := msg.Values["payload"].(string)
			if !ok || raw == "" {
				continue
			}
			var p struct {
				ReleaseID string `json:"release_id"`
				Stage     string `json:"stage"`
			}
			if json.Unmarshal([]byte(raw), &p) != nil || p.ReleaseID != releaseID {
				continue
			}
			return p.Stage == "compile", nil
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for release.rejected:v1 with stage=compile for release %s", releaseID))
}

// compileTriggerPayload mirrors remediation.requested:v1 including the file_path
// field the compile path populates (the shared remediationRequestedPayload in
// remediation_classifier_test.go omits file_path, which is empty for validation).
type compileTriggerPayload struct {
	Source         string `json:"source"`
	ReleaseID      string `json:"release_id"`
	NodeID         string `json:"node_id"`
	Category       string `json:"category"`
	ErrorSignature string `json:"error_signature"`
	ErrorExcerpt   string `json:"error_excerpt"`
	DBTLogURI      string `json:"dbt_log_uri"`
	FilePath       string `json:"file_path"`
}

// compileProposalRow captures the proposal fields asserted for a compile-stage
// fix: source, status, the persisted offending file_path, and source_resolved.
type compileProposalRow struct {
	Source         string `db:"source"`
	ReleaseID      string `db:"release_id"`
	NodeID         string `db:"node_id"`
	Status         string `db:"status"`
	FilePath       string `db:"file_path"`
	SourceResolved bool   `db:"source_resolved"`
}
