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
// for the node before any Job runs.
const brokenCompiledSQL = "select c.id from e2e_schema.ftable_c c where"

// TestE2E_Remediation_ParseFailureProposesFix drives a SQL syntax error caught
// by topology-controller's parse leg end to end:
//
//	upload service-2's baseline manifest under a new release id with ftable_e's
//	compiled_code replaced by SQL that does not parse
//	→ POST /releases (service-2) → release.requested:v1
//	→ topology-controller: sqlglot rejects ftable_e → manifest.loaded.candidate:v1
//	  {status: failed, failure_kind: invalid_sql, failed_nodes: [ftable_e]}
//	→ release-controller: Fail(invalid_sql), RecordStageResults("parse")
//	→ release.rejected:v1 {stage: parse, per_node: [{kind, detail, file_path, service}]}
//	→ remediation: ClassifyParse → remediation.requested:v2 (source=parse)
//	→ agent-remediation parseFixer: reads the model from stub-github, asks
//	  stub-llm → proposal row (source=parse) → verification run → proposed.
//
// No fixture image is needed: the parse leg reads the manifest, never the
// service image.
func TestE2E_Remediation_ParseFailureProposesFix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	requireReleaseControllerHealthy(t, clients)

	releaseID := "e2e-parse-" + uuid.NewString()[:8]
	const changedService = "service-2"

	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices, "no baseline manifests in S3 — setup.sh must run before the e2e suite")
	imageTag := allServices[changedService].imageTag
	require.NotEmpty(t, imageTag)

	// Seed production as every node except ftable_e, exactly as the validation
	// scenario does, so the only difference between prod and this candidate is
	// the node whose SQL is broken.
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
	seedServiceProdExcept(t, ctx, clients, allServices, changedService)

	// Break ftable_e's compiled SQL in a copy of the baseline manifest and
	// upload it where release-controller expects this release's manifest.
	baselineKey := fmt.Sprintf("%s/%s/manifest.json", changedService, e2eBaselineReleaseID)
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
	require.NotEmpty(t, filePath, "ftable_e must exist in the %s baseline manifest", changedService)
	body, err := json.Marshal(doc)
	require.NoError(t, err)
	putS3Object(t, ctx, clients, fmt.Sprintf("%s/%s/manifest.json", changedService, releaseID), body)

	postRelease(t, clients, changedService, releaseID, imageTag, false)

	// 1. Rejected at parse time with the node named.
	waitForReleaseRejected(t, ctx, clients, releaseID, 5*time.Minute)
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
	var trigger compileTriggerPayload
	pollUntil(t, ctx, 4*time.Minute, 2*time.Second, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, streams.RemediationRequestedV2, "-", "+").Result()
		if err != nil {
			return false, nil
		}
		for _, msg := range msgs {
			raw, _ := msg.Values["payload"].(string)
			var p compileTriggerPayload
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
	assert.NotEmpty(t, trigger.Nodes[0].ErrorExcerpt)
	assert.Empty(t, trigger.Nodes[0].DBTLogURI, "a parse failure has no log")

	// 4. A proposal is produced, verified by a real run, and announced.
	var row compileProposalRow
	pollUntil(t, ctx, 12*time.Minute, 3*time.Second, func() (bool, error) {
		err := clients.agentRemediationDB.GetContext(ctx, &row,
			`SELECT source, release_id, node_id, status, file_path, source_resolved
			   FROM proposal WHERE release_id = $1 AND node_id = $2 LIMIT 1`, releaseID, ftableEUniqueID)
		if err != nil {
			return false, nil
		}
		t.Logf("proposal status=%s", row.Status)
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
			require.Equal(t, "service-2", p.PerNode[0].Service)
			return true, nil
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for release.rejected:v1 for release %s", releaseID))
}
