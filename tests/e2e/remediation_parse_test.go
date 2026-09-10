package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// brokenCompiledSQL is compiled SQL sqlglot cannot parse under any dialect: the
// WHERE has no predicate. topology-controller raises InvalidCompiledSqlError
// for the node before any validation Job runs.
const brokenCompiledSQL = "select c.id from e2e_schema.ftable_c c where"

// parseFixReleaseService is the service whose release drives this scenario. It
// is deliberately NOT the service holding the unparseable node: a release's
// compile Job uploads its freshly compiled manifest to
// <service>/<release_id>/manifest.json — the very key topology-controller then
// reads (executor-controller/service/artifacts/parse_cache.go ManifestURI and
// s3-sidecar/compile_uploader.py) — so a manifest doctored under the changed
// service's own key is overwritten before the parse leg ever sees it. Only the
// changed service is compiled, so the broken node is staged in another
// service's production-pinned manifest, which nothing in this release rewrites.
const parseFixReleaseService = "service-1"

// parseFixBrokenService is the service whose production-pinned manifest carries
// the unparseable node, and the service whose source the fix edits. Both halves
// live in one repository (services/<name> per tests/e2e/config/service_repos.yaml),
// so the repo and commit the release carries are the ones the fixer reads the
// offending model from.
const parseFixBrokenService = "service-2"

// TestE2E_Remediation_ParseFailureProposesFix drives a SQL syntax error caught
// by topology-controller's parse leg end to end:
//
//	pin service-2's production manifest to a copy of its baseline with
//	ftable_e's compiled_code replaced by SQL that does not parse
//	→ POST /releases (service-1) → compile service-1 → release.requested:v1
//	→ topology-controller resolves the whole assembled set and sqlglot rejects
//	  ftable_e → manifest.loaded.candidate:v1
//	  {status: failed, failure_kind: invalid_sql, failed_nodes: [ftable_e]}
//	→ release-controller: Fail(invalid_sql), RecordStageResults("parse")
//	→ release.rejected:v1 {stage: parse, per_node: [{kind, detail, file_path, service}]}
//	→ remediation: ClassifyParse → remediation.requested:v2 (source=parse)
//	→ agent-remediation parseFixer: reads the model from stub-github under
//	  service-2's repo prefix, asks stub-llm → proposal row (source=parse)
//	→ one fix-verification run lays the corrected model over service-2's dbt
//	  project, compiles and validates it, and passes → proposal proposed
//	→ remediation.proposed:v1, and the retry endpoint accepts the reason as
//	  healable.
//
// ftable_e is the node under repair because the whole rest of the suite already
// proves this exact corrected source compiles and validates in service-2's
// project: it is what the validation lane proposes for the same model.
func TestE2E_Remediation_ParseFailureProposesFix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// 32 minutes: strictly greater than the 28 this test's stage budgets sum to
	// (rejection 10 + rejected event 2 + trigger 4 + proposal 10 + proposed
	// event 2). The rejection budget covers service-1's compile Job and the
	// parse leg; the proposal budget covers the fix-verification run, which is
	// a whole second pipeline: compile service-2 with the overlay, build the
	// candidate schema, validate ftable_e.
	ctx, cancel := context.WithTimeout(context.Background(), 32*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-parse-" + uuid.NewString()[:8]
	t.Logf("release_id=%s released_service=%s broken_node=%s (service %s)",
		releaseID, parseFixReleaseService, ftableEUniqueID, parseFixBrokenService)

	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices, "no baseline manifests in S3 — setup.sh must run before the e2e suite")
	released, ok := allServices[parseFixReleaseService]
	require.True(t, ok, "%s has no baseline manifest", parseFixReleaseService)
	require.NotEmpty(t, released.imageTag)
	broken, ok := allServices[parseFixBrokenService]
	require.True(t, ok, "%s has no baseline manifest", parseFixBrokenService)

	// Seed production as every node except ftable_e, exactly as the validation
	// scenario does, so ftable_e is the one changed node the fix-verification
	// run has to build — its verdict is about this fix and nothing else.
	var prodNodes []map[string]string
	for _, si := range allServices {
		for _, n := range si.nodes {
			if n.uniqueID == ftableEUniqueID {
				continue
			}
			prodNodes = append(prodNodes, map[string]string{"unique_id": n.uniqueID, "content_hash": n.contentHash})
		}
	}
	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, parseFixReleaseService)

	// Break ftable_e's compiled SQL in a copy of service-2's baseline manifest
	// and pin service-2's production pointer at it, so the assembled set the
	// parse leg reads carries the broken node.
	brokenReleaseID := releaseID + "-src"
	baselineKey := fmt.Sprintf("%s/%s/manifest.json", parseFixBrokenService, e2eBaselineReleaseID)
	doc, err := decodeJSONDoc(getS3Object(t, ctx, clients, baselineKey))
	require.NoError(t, err)
	var filePath string
	for _, nv := range asObjectMap(doc["nodes"]) {
		n, ok := nv.(map[string]interface{})
		if !ok {
			continue
		}
		schema, _ := n["schema"].(string)
		name, _ := n["name"].(string)
		if schema+"."+name != ftableEUniqueID {
			continue
		}
		n["compiled_code"] = brokenCompiledSQL
		filePath, _ = n["original_file_path"].(string)
	}
	require.NotEmpty(t, filePath, "ftable_e must exist in the %s baseline manifest", parseFixBrokenService)
	body, err := json.Marshal(doc)
	require.NoError(t, err)
	putS3Object(t, ctx, clients,
		fmt.Sprintf("%s/%s/manifest.json", parseFixBrokenService, brokenReleaseID), body)
	pinServiceProd(t, ctx, clients, parseFixBrokenService, brokenReleaseID, broken.imageTag)
	// Put the pointer back so no later test parses the broken manifest. Deferred
	// after clients.close(ctx) above, so LIFO runs it while the pool is open.
	defer pinServiceProd(t, context.Background(), clients,
		parseFixBrokenService, e2eBaselineReleaseID, broken.imageTag)

	postRelease(t, clients, parseFixReleaseService, releaseID, released.imageTag, false)

	// 1. Rejected at parse time with the node named.
	waitForReleaseRejected(t, ctx, clients, releaseID, 10*time.Minute)
	detail := getReleaseJSON(t, clients, releaseID)
	assert.Equal(t, "invalid_sql", detail["reject_reason"])
	rejectDetail, _ := detail["reject_detail"].(string)
	assert.Contains(t, rejectDetail, ftableEUniqueID)
	assert.NotContains(t, rejectDetail, "\x1b", "terminal escapes must not reach the release row")

	var parseRow map[string]any
	for _, raw := range detail["per_node_results"].([]any) {
		row := raw.(map[string]any)
		if row["stage"] == "parse" && row["node_id"] == ftableEUniqueID {
			parseRow = row
		}
	}
	require.NotNil(t, parseRow, "per_node_results must carry a parse row for ftable_e: %v", detail["per_node_results"])
	assert.Equal(t, "failed", parseRow["status"])
	assert.Equal(t, filePath, parseRow["file_path"])

	// 2. release.rejected:v1 carries stage=parse and the per-node kind/detail.
	assertRejectedParseStage(t, ctx, clients, releaseID, filePath)

	// 3. remediation.requested:v2 carries source=parse with the parser detail.
	var trigger parseTriggerPayload
	pollUntil(t, ctx, 4*time.Minute, 2*time.Second, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, streams.RemediationRequestedV2, "-", "+").Result()
		if err != nil {
			return false, nil
		}
		for _, msg := range msgs {
			raw, _ := msg.Values["payload"].(string)
			var p parseTriggerPayload
			if raw == "" || json.Unmarshal([]byte(raw), &p) != nil || p.ReleaseID != releaseID {
				continue
			}
			trigger = p
			return true, nil
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for %s for release %s", streams.RemediationRequestedV2, releaseID))
	require.Equal(t, "parse", trigger.Source)
	require.Len(t, trigger.Nodes, 1)
	assert.Equal(t, ftableEUniqueID, trigger.Nodes[0].NodeID)
	assert.Equal(t, filePath, trigger.Nodes[0].FilePath)
	// The service is what resolves the repository prefix for a parse fix: a
	// parse failure's node id is a real node id, not compile's synthetic
	// service id, so the fixer cannot derive the prefix from it.
	assert.Equal(t, parseFixBrokenService, trigger.Nodes[0].Service)
	assert.NotEmpty(t, trigger.Nodes[0].ErrorExcerpt)
	assert.Empty(t, trigger.Nodes[0].DBTLogURI, "a parse failure has no log")

	// 4. A proposal is produced, verified by a real fix-verification run, and
	//    announced for human review.
	var row compileProposalRow
	var last string
	pollUntil(t, ctx, 10*time.Minute, 3*time.Second, func() (bool, error) {
		err := clients.agentRemediationDB.GetContext(ctx, &row,
			`SELECT source, release_id, node_id, status, file_path, source_resolved
			   FROM proposal WHERE release_id = $1 AND node_id = $2 LIMIT 1`, releaseID, ftableEUniqueID)
		if err != nil {
			return false, nil
		}
		if row.Status != last {
			t.Logf("proposal %s/%s: status=%s", releaseID, ftableEUniqueID, row.Status)
			last = row.Status
		}
		return row.Status == "proposed" || row.Status == "failed" || row.Status == "skipped" || row.Status == "escalated", nil
	}, fmt.Sprintf("timeout waiting for a parse proposal for release %s", releaseID))
	require.Equal(t, "parse", row.Source)
	require.Equal(t, "proposed", row.Status, "the verified fix must be offered")
	require.True(t, strings.HasSuffix(row.FilePath, filePath), "proposal file_path %q must end with %q", row.FilePath, filePath)
	require.True(t, row.SourceResolved)
	waitForRemediationProposed(t, ctx, clients, releaseID, ftableEUniqueID, 2*time.Minute)

	// 5. The retry endpoint no longer refuses the reason as unhealable. With a
	//    proposal already open it answers 409 for that reason instead.
	resp, err := http.Post(fmt.Sprintf("%s/releases/%s/retry-remediation", clients.releaseBase, releaseID), "application/json", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	var refusal struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&refusal)
	assert.NotEqual(t, "not_healable", refusal.Error, "invalid_sql must be a healable reason (status %d)", resp.StatusCode)
	assert.Contains(t, []int{http.StatusAccepted, http.StatusConflict}, resp.StatusCode)
}

// pinServiceProd points a service's production manifest pointer at one release
// id's canonical dbt manifest key, keeping its image tag.
func pinServiceProd(t *testing.T, ctx context.Context, clients *testClients, service, releaseID, imageTag string) {
	t.Helper()
	_, err := clients.releaseDB.ExecContext(ctx,
		`INSERT INTO service_prod (service_name, release_id, manifest_s3_key, image_tag, manifest_kind, updated_at)
		 VALUES ($1, $2, $3, $4, 'dbt', now())
		 ON CONFLICT (service_name) DO UPDATE SET
		   release_id = EXCLUDED.release_id,
		   manifest_s3_key = EXCLUDED.manifest_s3_key,
		   image_tag = EXCLUDED.image_tag,
		   manifest_kind = EXCLUDED.manifest_kind,
		   updated_at = EXCLUDED.updated_at`,
		service, releaseID, canonicalManifestS3URI(service, releaseID), imageTag)
	require.NoError(t, err, "pin service_prod for %s at %s", service, releaseID)
}

// parseTriggerPayload mirrors remediation.requested:v2 for a parse-stage
// rejection. It carries `service` alongside the compile-stage fields because
// that is what resolves the repository prefix on this lane.
type parseTriggerPayload struct {
	Source    string           `json:"source"`
	ReleaseID string           `json:"release_id"`
	Nodes     []parseNodeEntry `json:"nodes"`
}

// parseNodeEntry is one failing node inside a parse-stage batched trigger.
type parseNodeEntry struct {
	NodeID         string `json:"node_id"`
	Category       string `json:"category"`
	ErrorSignature string `json:"error_signature"`
	ErrorExcerpt   string `json:"error_excerpt"`
	DBTLogURI      string `json:"dbt_log_uri"`
	FilePath       string `json:"file_path"`
	Service        string `json:"service"`
	NodeType       string `json:"node_type"`
}

// assertRejectedParseStage waits for this release's release.rejected:v1 and
// checks the parse-leg shape: stage=parse, a per_node entry for ftable_e with
// its kind, a detail free of terminal escapes, and the candidate's file path.
func assertRejectedParseStage(t *testing.T, ctx context.Context, clients *testClients, releaseID, filePath string) {
	t.Helper()
	pollUntil(t, ctx, 2*time.Minute, 2*time.Second, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, streams.ReleaseRejectedV1, "-", "+").Result()
		if err != nil {
			return false, nil
		}
		for _, msg := range msgs {
			raw, _ := msg.Values["payload"].(string)
			var p struct {
				ReleaseID string `json:"release_id"`
				Stage     string `json:"stage"`
				Reason    string `json:"reason"`
				Repo      string `json:"repo"`
				PerNode   []struct {
					NodeID   string `json:"node_id"`
					Kind     string `json:"kind"`
					Detail   string `json:"detail"`
					FilePath string `json:"file_path"`
					Service  string `json:"service"`
				} `json:"per_node"`
			}
			if raw == "" || json.Unmarshal([]byte(raw), &p) != nil || p.ReleaseID != releaseID {
				continue
			}
			require.Equal(t, "parse", p.Stage)
			require.Equal(t, "invalid_sql", p.Reason)
			require.NotEmpty(t, p.Repo)
			require.NotContains(t, raw, `"error_class"`)
			require.Len(t, p.PerNode, 1)
			require.Equal(t, ftableEUniqueID, p.PerNode[0].NodeID)
			require.Equal(t, "invalid_sql", p.PerNode[0].Kind)
			require.NotEmpty(t, p.PerNode[0].Detail)
			require.NotContains(t, p.PerNode[0].Detail, "\x1b")
			require.Equal(t, filePath, p.PerNode[0].FilePath)
			require.Equal(t, parseFixBrokenService, p.PerNode[0].Service)
			return true, nil
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for release.rejected:v1 for release %s", releaseID))
}
