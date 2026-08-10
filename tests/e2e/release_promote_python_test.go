package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	pyE2EService    = "svc-py-e2e"
	pyProbeUniqueID = "e2e_schema.py_probe"
)

// pythonContractYAML renders a minimal, valid contract_version-1 artifact for
// the python e2e service. The single node reads from pg_catalog — always
// present in the warehouse and outside Continuo's registry, so it produces no
// upstream edge and no rewrite. This test deliberately avoids cross-kind
// upstream edges so it stays independent of warehouse table state. The hash
// parts fold exactly as manifest-controller recomputes them, so the artifact
// passes the parser's content_hash check.
func pythonContractYAML() string {
	sourceHash := sha256Hex("def run(ctx):\n    return ctx.read(\"tables\")\n")
	configHash := sha256Hex("{}")
	contentHash := "sha256:" + sha256Hex(sourceHash+"|"+""+"|"+configHash)
	return fmt.Sprintf(`contract_version: 1
service: %s
nodes:
  - schema: e2e_schema
    table: py_probe
    owner: e2e
    schedule: daily
    criticality: SECONDARY
    script: scripts/py_probe.py
    reads:
      tables: select schemaname from pg_catalog.pg_tables
    output_columns:
      - {name: id, type: INTEGER, nullable: false}
      - {name: created_on, type: DATE}
    source_hash: "%s"
    shared_code_hash: ""
    config_hash: "%s"
    content_hash: "%s"
`, pyE2EService, sourceHash, configHash, contentHash)
}

// TestE2E_ReleasePromote_PythonContractSkipsCompileAndPromotes proves the
// python release path end to end: a kind=python release skips the compile
// leg, parses the contract, validates its node via a real build_from_columns
// Job against the published runner image, promotes, swaps the Neo4j
// topology, and records the python kind + contract pointer on service_prod.
func TestE2E_ReleasePromote_PythonContractSkipsCompileAndPromotes(t *testing.T) {
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

	releaseID := "e2e-py-" + uuid.NewString()[:8]

	// Baseline: every dbt service keeps its live pointer; the python service
	// is brand new, so py_probe is the sole changed node.
	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices)
	var prodNodes []map[string]string
	for _, si := range allServices {
		for _, n := range si.nodes {
			prodNodes = append(prodNodes, map[string]string{
				"unique_id": n.uniqueID, "content_hash": n.contentHash,
			})
		}
	}

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProdExcept(t, ctx, clients, allServices, pyE2EService)
	// A leftover pointer would drag svc-py-e2e's contract into every later
	// test's assembled manifest set, so this MUST run before the DB pool
	// closes. Registered as a plain defer (not t.Cleanup) and placed after
	// `defer clients.close(ctx)` above: defers run LIFO, so this one — the
	// later-registered defer — fires first, while releaseDB is still open.
	// Moving this back into t.Cleanup would run it after all deferred calls,
	// including clients.close(ctx), against an already-closed pool.
	defer func() {
		if _, err := clients.releaseDB.ExecContext(context.Background(),
			`DELETE FROM service_prod WHERE service_name = $1`, pyE2EService); err != nil {
			t.Errorf("cleanup: delete %s service_prod row: %v", pyE2EService, err)
		}
	}()

	// CI-analogue: the contract artifact is uploaded BEFORE the POST, mirroring
	// the ordering contract for domain-repo CI (upload completes before
	// /releases is called).
	contractKey := fmt.Sprintf("%s/%s/contract.yaml", pyE2EService, releaseID)
	putS3Object(t, ctx, clients, contractKey, []byte(pythonContractYAML()))

	postPythonRelease(t, clients, pyE2EService, releaseID, "ghcr.io/e2e/py-probe:v1")

	// Skip-compile threading: the validation request names exactly the python
	// node, routed to build_from_columns with a .json spec URI, and no
	// compile.requested was ever emitted for this release.
	assertValidationRequestedNodes(t, ctx, clients, releaseID, []string{pyProbeUniqueID})
	assertValidationNodeOp(t, ctx, clients, releaseID, pyProbeUniqueID, "build_from_columns", ".json")
	assertNoCompileRequested(t, ctx, clients, releaseID)

	// A real build_from_columns Job runs in kind against the published runner
	// image; on success the release promotes and the topology swaps.
	waitForReleasePromoted(t, ctx, clients, releaseID, 10*time.Minute)
	waitForTopologySwap(t, ctx, clients, releaseID, pyProbeUniqueID, 2*time.Minute)

	// The promoted pointer records the python kind and the contract artifact.
	var kind, s3Key string
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT manifest_kind, manifest_s3_key FROM service_prod WHERE service_name = $1`,
		pyE2EService).Scan(&kind, &s3Key))
	assert.Equal(t, "python", kind)
	assert.True(t, strings.HasSuffix(s3Key, "/contract.yaml"),
		"pointer must be the contract artifact, got %s", s3Key)
	t.Log("✅ python release skipped compile, validated via build_from_columns, promoted, and swapped topology")
}

// postPythonRelease issues POST /releases with kind=python. Kept separate from
// postRelease so the many existing dbt callers stay untouched.
func postPythonRelease(t *testing.T, clients *testClients, service, releaseID, imageTag string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"service":    service,
		"release_id": releaseID,
		"image_tag":  imageTag,
		"repo":       "carolsimone/continuo-py-demo",
		"commit_sha": releaseID,
		"kind":       "python",
	})
	require.NoError(t, err)
	resp, err := http.Post(clients.releaseBase+"/releases", "application/json", strings.NewReader(string(body)))
	require.NoError(t, err, "POST /releases (python)")
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusAccepted, resp.StatusCode,
		"POST /releases: expected 202, got %d: %s", resp.StatusCode, string(respBody))
}

// putS3Object uploads an object into the e2e bucket.
func putS3Object(t *testing.T, ctx context.Context, clients *testClients, key string, body []byte) {
	t.Helper()
	_, err := clients.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(e2eS3Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	require.NoError(t, err, "put S3 object %s", key)
}

// assertValidationNodeOp finds the release's validation.requested:v1 message
// and asserts the named node's validation_op and candidate_artifact_uri suffix.
func assertValidationNodeOp(t *testing.T, ctx context.Context, clients *testClients, releaseID, nodeID, wantOp, wantURISuffix string) {
	t.Helper()
	pollUntil(t, ctx, 2*time.Minute, 1*time.Second, func() (bool, error) {
		msgs, err := clients.redisClient.XRange(ctx, streams.ValidationRequestedV1, "-", "+").Result()
		if err != nil {
			return false, nil
		}
		for _, msg := range msgs {
			payload, _ := msg.Values["payload"].(string)
			if payload == "" {
				continue
			}
			var p struct {
				ReleaseID string `json:"release_id"`
				Nodes     []struct {
					UniqueID             string `json:"unique_id"`
					ValidationOp         string `json:"validation_op"`
					CandidateArtifactURI string `json:"candidate_artifact_uri"`
				} `json:"nodes"`
			}
			if json.Unmarshal([]byte(payload), &p) != nil || p.ReleaseID != releaseID {
				continue
			}
			for _, n := range p.Nodes {
				if n.UniqueID != nodeID {
					continue
				}
				require.Equal(t, wantOp, n.ValidationOp, "validation_op for %s", nodeID)
				require.True(t, strings.HasSuffix(n.CandidateArtifactURI, wantURISuffix),
					"candidate_artifact_uri %q must end with %q", n.CandidateArtifactURI, wantURISuffix)
				return true, nil
			}
		}
		return false, nil
	}, fmt.Sprintf("timeout waiting for validation.requested node %s", nodeID))
}

// assertNoCompileRequested asserts no compile.requested:v1 message exists for
// the release — the skip-compile branch's negative proof. Called after the
// validation request has been observed, so ordering is settled.
func assertNoCompileRequested(t *testing.T, ctx context.Context, clients *testClients, releaseID string) {
	t.Helper()
	msgs, err := clients.redisClient.XRange(ctx, streams.CompileRequestedV1, "-", "+").Result()
	require.NoError(t, err)
	for _, msg := range msgs {
		payload, _ := msg.Values["payload"].(string)
		var p struct {
			ReleaseID string `json:"release_id"`
		}
		if json.Unmarshal([]byte(payload), &p) == nil && p.ReleaseID == releaseID {
			t.Fatalf("compile.requested emitted for python release %s — skip-compile branch did not take effect", releaseID)
		}
	}
}
