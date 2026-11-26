package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// The malformed-Jinja defect this scenario stages is the real
// daily_transactions.sql failure: the config() call is closed too early and
// tags is hoisted outside it, so dbt cannot parse the model and `dbt compile`
// aborts for the whole project:
//
//	{{ config(materialized='table'), tags=[...])}}
//	select 1 as id
//
// This is documentation of the fixture a cold-stack harness must bake into
// the COMPILE_FIXTURE_PROJECT image. It is NOT compiled by anything in this
// package (tests/e2e is a Go module; *.sql files here are inert), and it is
// deliberately NOT placed under dbt/projects/* — setup.sh compiles every
// manifest under dbt/projects via `dbt_upload load --projects-dir /app/projects`,
// so a malformed model there would break the baseline manifest build and every
// other e2e test. See task-7.1-report.md for the full enablement recipe.

// TestE2E_Remediation_CompileFailureProposesFix drives a dbt COMPILE failure end
// to end and proves it (a) rejects the release with per-node compile detail and
// (b) produces a remediation proposal:
//
//	POST /releases (COMPILE_FIXTURE_PROJECT, malformed-Jinja model baked in image)
//	→ release.requested → AdvanceQueue: Received → Compiling → compile.requested:v1
//	→ execution-controller runs `dbt compile` in the fixture image → dbt parse aborts
//	→ execution-controller records the outcome in-process (outcome=failed, dbt_log_uri) → compile.completed:v1 (status=failed)
//	→ release-controller TransitionToRejected("compile_failed"), RecordStageResults("compile")
//	→ release.rejected:v1 {stage:"compile", per_node:[{node_id:<project>, status:"failed", dbt_log_uri}]}
//	→ remediation classifier maps stage→SourceCompile, ExtractDbtFilePath(log)→file_path,
//	  classifies the parse/compilation error as healable → remediation.requested:v3 (source="compile")
//	→ agent-remediation proposeFromSource: reads model source from GitHub (stub-github),
//	  asks stub-llm to fix it → proposal row (source="compile", file_path)
//	→ a verification run lays the corrected file over the fixture project's dbt
//	  project and compiles and validates it; when it reports passed the
//	  proposal is finalized → remediation.proposed:v1.
//
// Unlike the validation path, the compile leg is the FIRST leg and rejects
// before any change-derivation, so no current_prod/service_prod baseline seeding
// is required — the compile job runs `dbt compile` against the fixture image's
// baked source and fails on the malformed model alone.
//
// Harness prerequisite (skip-gated). A clean `dbt compile` is required for the
// three real e2e projects (setup.sh compiles their manifests), so the failing
// model cannot live in service-1/2/3. The scenario needs a DEDICATED fixture
// project whose image is built with the malformed-Jinja model source documented
// above and loaded into kind,
// whose image tag is reachable, and whose name is mapped in
// tests/e2e/config/project_repos.yaml (so the agent resolves the repo
// prefix). Until a cold-stack harness provisions that fixture and exports
// COMPILE_FIXTURE_PROJECT / COMPILE_FIXTURE_IMAGE_TAG, this test skips so it
// never destabilises the green suite. See task-7.1-report.md.
func TestE2E_Remediation_CompileFailureProposesFix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	fixtureProject := os.Getenv("COMPILE_FIXTURE_PROJECT")
	if fixtureProject == "" {
		t.Skip("COMPILE_FIXTURE_PROJECT not set — compile-failure fixture not provisioned " +
			"in this harness; see .superpowers/sdd/task-7.1-report.md for the cold-stack " +
			"enablement recipe (fixture dbt image + kind load + project_repos.yaml mapping); " +
			"the fixture project must also be bound in tests/e2e/ci-auth.json under repository id " +
			e2eDbtRepositoryID + " so postRelease can submit it through the public API")
	}
	fixtureImageTag := os.Getenv("COMPILE_FIXTURE_IMAGE_TAG")
	require.NotEmpty(t, fixtureImageTag,
		"COMPILE_FIXTURE_IMAGE_TAG must be set alongside COMPILE_FIXTURE_PROJECT")

	// 35 minutes: strictly greater than the 34 this test's stage budgets sum to
	// (rejection 12 + rejected event 2 + trigger 4 + decision 0.5 + proposal 15
	// + proposal row 0.5). The proposal budget covers the verification run that
	// lays the corrected file over the fixture project and compiles it.
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyComponentsHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-cmp-" + uuid.NewString()[:8]
	t.Logf("release_id=%s fixture_project=%s image_tag=%s", releaseID, fixtureProject, fixtureImageTag)

	// Clear any mid-flight release so AdvanceQueue can activate this one.
	// Tap the remediation streams before the release is posted: the trim loop
	// removes consumed entries, so the polls below read what the taps recorded.
	requestedTap := startStreamTap(t, ctx, clients.redisClient, streams.RemediationRequestedV3)
	proposedTap := startStreamTap(t, ctx, clients.redisClient, streams.RemediationProposedV1)
	rejectedTap := startStreamTap(t, ctx, clients.redisClient, streams.ReleaseRejectedV1)

	resetReleaseControllerQueue(t, ctx, clients)

	// POST /releases for the fixture project. AdvanceQueue moves it
	// Received → Compiling and emits compile.requested:v1; the compile job runs
	// `dbt compile` in the fixture image and aborts on the malformed model.
	postRelease(t, clients, fixtureProject, releaseID, fixtureImageTag, false)

	// 1. The release must reach "rejected" with at least one failing node (the
	//    compile node, whose node_id is the project name).
	waitForReleaseRejected(t, ctx, clients, releaseID, 12*time.Minute)

	// 2. GET /releases/{id}: reject_reason == "compile_failed", and per_node_results
	//    carries a stage=="compile" entry for the project with a non-empty
	//    dbt_log_uri (the per-node compile detail the UI surfaces).
	assertCompileRejection(t, ctx, clients, releaseID, fixtureProject)

	// 3. release.rejected:v1 must carry stage=="compile" for this release — the
	//    discriminator the remediation classifier maps to SourceCompile.
	assertRejectedStageCompile(t, ctx, rejectedTap, releaseID)

	// 4. remediation.requested:v3 must be emitted with source=="compile" and an
	//    entry carrying a non-empty file_path (derived by the classifier via
	//    ExtractDbtFilePath on the dbt compile log). The compile node_id is the
	//    project name.
	trigger := waitForBatchedTrigger(t, ctx, requestedTap, releaseID, []string{fixtureProject}, 4*time.Minute)
	require.Equal(t, string(model.RemediationSourceCompile), trigger.Source, "compile trigger source")
	node, _ := trigger.findNode(fixtureProject)

	require.Equal(t, fixtureProject, node.NodeID, "compile trigger node_id is the project name")
	require.Nil(t, node.Unfixable, "a compilation error is one a fix can reach, so the trigger must not mark it unfixable")
	require.NotEmpty(t, node.FilePath,
		"compile trigger must carry a file_path extracted from the dbt compile log")
	require.NotEmpty(t, node.DBTLogURI, "compile trigger must carry a dbt_log_uri")
	// The excerpt is the text the signature was derived from and what the case
	// base records as precedent. On a real dbt log it must be the compilation
	// message naming the broken model — not dbt's `Encountered an error:`
	// lead-in, which precedes every error and would give every compile failure
	// of a project the same signature.
	require.Contains(t, node.ErrorExcerpt, "Compilation Error in model daily_transactions",
		"compile trigger error_excerpt must carry the compilation message, got %q", node.ErrorExcerpt)
	t.Logf("✅ %s (compile): file_path=%s signature=%s excerpt=%q", streams.RemediationRequestedV3, node.FilePath, node.ErrorSignature, node.ErrorExcerpt)

	// 5. classification_decision row must record source=='compile'.
	var decision classificationDecisionRow
	pollUntil(t, ctx, 30*time.Second, 1*time.Second, func() (bool, error) {
		err := clients.remediationDB.GetContext(ctx, &decision,
			`SELECT source, release_id, node_id, category, decision
			   FROM classification_decision
			  WHERE source = 'compile' AND release_id = $1 AND node_id = $2
			  LIMIT 1`,
			releaseID, fixtureProject)
		return err == nil, nil
	}, fmt.Sprintf("timeout waiting for compile classification_decision for release %s", releaseID))
	require.Equal(t, "compile", decision.Source, "decision source must be 'compile'")
	require.Equal(t, "emit", decision.Decision, "a compile parse/syntax error must route to emit, not drop")

	// 6. remediation.proposed:v1 must be emitted for the compile failure with
	//    source=="compile". The agent posts a verification run that lays the
	//    corrected file over the fixture project's dbt project and compiles it;
	//    the event follows only once that run reports passed, so this budget
	//    covers a whole second pipeline run.
	var proposed remediationProposedPayload
	pollUntil(t, ctx, 15*time.Minute, 3*time.Second, func() (bool, error) {
		msgs := proposedTap.Entries()
		for _, msg := range msgs {
			raw, ok := msg.Values["payload"].(string)
			if !ok || raw == "" {
				continue
			}
			var p remediationProposedPayload
			if json.Unmarshal([]byte(raw), &p) != nil {
				continue
			}
			if p.ReleaseID == releaseID && p.NodeID == fixtureProject {
				proposed = p
				return true, nil
			}
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for %s (compile) for release %s", streams.RemediationProposedV1, releaseID))

	require.Equal(t, "compile", proposed.Source, "proposed event source must be 'compile'")
	require.Equal(t, fixtureProject, proposed.NodeID, "proposed node_id is the project name")
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
			releaseID, fixtureProject)
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
// project with a non-empty dbt_log_uri.
func assertCompileRejection(t *testing.T, ctx context.Context, clients *testClients, releaseID, project string) {
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
		if n.Stage == "compile" && n.NodeID == project {
			found = true
			require.Equal(t, "failed", n.Status, "compile per-node status must be 'failed'")
			require.NotEmpty(t, n.DBTLogURI,
				"compile per-node result must carry a non-empty dbt_log_uri")
		}
	}
	require.True(t, found,
		"per_node_results must contain a stage=='compile' entry for project %s", project)
	t.Logf("✅ release %s rejected: reject_reason=compile_failed with a stage=compile per-node result", releaseID)
}

// assertRejectedStageCompile asserts the release.rejected:v1 event for releaseID
// carries stage=="compile" — the discriminator the remediation classifier maps
// to SourceCompile.
func assertRejectedStageCompile(t *testing.T, ctx context.Context, rejectedTap *streamTap, releaseID string) {
	t.Helper()
	pollUntil(t, ctx, 2*time.Minute, 2*time.Second, func() (bool, error) {
		msgs := rejectedTap.Entries()
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
