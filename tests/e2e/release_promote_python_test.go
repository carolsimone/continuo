package e2e

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/carolsimone/continuo/pkg/streams"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const (
	pyE2EService       = "svc-py-e2e"
	pyProbeUniqueID    = "e2e_schema.py_probe"
	pyBadShapeUniqueID = "e2e_schema.py_bad_shape"
	pyCsvUniqueID      = "e2e_schema.py_csv"

	// pyFixtureImage is the domain image scripts/setup.sh builds and side-loads
	// into kind. The release posts it as image_tag and the executor runs it
	// verbatim, so this string must match setup.sh's PY_FIXTURE_IMAGE.
	pyFixtureImage = "continuo-e2e-py-probe:latest"

	// pyCsvSourceKey is the S3 key the py_csv node's contract points at
	// (s3://<e2eS3Bucket>/pyCsvSourceKey). Seeded from pyCsvFixtureData before
	// the release posts, so both the validation runner's header-only fetch and
	// the run harness's full fetch resolve against real content.
	pyCsvSourceKey = "fixtures/orders.csv"
)

// The fixture's authored contracts and scripts are embedded from the same
// files the fixture image's Dockerfile COPYs, so the artifact this test
// uploads and the artifact the container runs cannot drift. Embedding rather
// than reading from disk also frees the test from assuming where the
// repository is mounted.
//
//go:embed fixtures/py-probe/contracts/py_probe.yml
var pyFixtureContract []byte

//go:embed fixtures/py-probe/contracts/py_csv.yml
var pyCsvFixtureContract []byte

//go:embed fixtures/py-probe/scripts
var pyFixtureScripts embed.FS

// pyCsvFixtureData is the csv object the py_csv node's contract names — it is
// never baked into the fixture image (only contracts/ and scripts/ are), so
// the test itself seeds it into S3 before posting the release.
//
//go:embed fixtures/py-probe/data/orders.csv
var pyCsvFixtureData []byte

// pythonContractYAML renders the merged contract_version-1 wire artifact for the
// python e2e service from the fixture image's own authored contract files,
// adding the per-node hash fields a domain repository's CI would compute.
//
// Deriving it from the same files the image bakes is what keeps the shape
// validation checks and the shape the container actually runs identical;
// duplicating the node definitions here would let the two drift, so a release
// could validate one contract while the pod executed another.
//
// A scripted node's source_hash is the real sha256 of its script file, so
// editing a script genuinely re-fingerprints its node. A python-csv node runs
// no script, but the shipped merge tool's node_entry() still emits
// "script": node.script unconditionally, so its wire entry carries an empty
// string rather than an absent key — mirroring what a domain repository's CI
// computes for one (see topology-controller/tests/test_python_contract_parser.py's
// make_csv_entry), its source_hash is the sha256 of its declared csv uri
// instead, so editing the uri re-fingerprints the node. shared_code_hash is
// empty for every node (nothing here imports in-repo code). topology-controller
// recomputes only the fold, not config_hash, so any deterministic config_hash
// value is accepted as long as the fold matches.
func pythonContractYAML(t *testing.T) string {
	t.Helper()

	var nodes []map[string]any
	for _, raw := range [][]byte{pyFixtureContract, pyCsvFixtureContract} {
		var doc struct {
			Nodes []map[string]any `yaml:"nodes"`
		}
		require.NoError(t, yaml.Unmarshal(raw, &doc), "parse fixture contract")
		require.NotEmpty(t, doc.Nodes, "fixture contract declares no nodes")
		nodes = append(nodes, doc.Nodes...)
	}

	for _, node := range nodes {
		var sourceHash string
		if scriptPath, _ := node["script"].(string); scriptPath != "" {
			script, err := fs.ReadFile(pyFixtureScripts, path.Join("fixtures/py-probe", scriptPath))
			require.NoError(t, err, "read fixture script %s", scriptPath)
			sourceHash = sha256Hex(string(script))
		} else {
			reads, _ := node["reads"].(map[string]any)
			csvURI, _ := reads["csv"].(string)
			require.NotEmpty(t, csvURI, "python-csv fixture node is missing reads.csv")
			sourceHash = sha256Hex(csvURI)
			// The shipped merge tool's node_entry() emits "script": node.script
			// unconditionally, so every real csv wire entry carries an empty
			// string, never an absent key. Match that shape here so this test
			// exercises the document topology-controller actually receives.
			node["script"] = ""
		}

		entryJSON, err := json.Marshal(node)
		require.NoError(t, err)

		configHash := sha256Hex(string(entryJSON))
		node["source_hash"] = sourceHash
		node["shared_code_hash"] = ""
		node["config_hash"] = configHash
		node["content_hash"] = "sha256:" + sha256Hex(sourceHash+"|"+""+"|"+configHash)
	}

	merged, err := yaml.Marshal(map[string]any{
		"contract_version": 1,
		"service":          pyE2EService,
		"nodes":            nodes,
	})
	require.NoError(t, err, "marshal merged contract")
	return string(merged)
}

// TestE2E_ReleasePromote_PythonContractSkipsCompileAndPromotes proves the
// python release path end to end: a kind=python release skips the compile
// leg, parses the contract, validates every node — including a python-csv
// node in the same mixed DAG — via a real build_from_columns Job against the
// published runner image, promotes, swaps the Neo4j topology, and records
// the python kind + contract pointer on service_prod.
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

	releaseID, cleanup := promotePythonFixtureRelease(t, ctx, clients)
	defer cleanup()

	// Skip-compile threading: the validation request names exactly the python
	// nodes, routed to build_from_columns with a .json spec URI, and no
	// compile.requested was ever emitted for this release. The csv node takes
	// the identical validation_op — its spec carries csv_source instead of a
	// script, and the runner's header check against the real minio
	// object (seeded above) must have passed for the release to promote at
	// all, which waitForReleasePromoted inside promotePythonFixtureRelease
	// already proved.
	assertValidationNodeOp(t, ctx, clients, releaseID, pyProbeUniqueID, "build_from_columns", ".json")
	assertValidationNodeOp(t, ctx, clients, releaseID, pyCsvUniqueID, "build_from_columns", ".json")
	assertNoCompileRequested(t, ctx, clients, releaseID)

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

// promotePythonFixtureRelease drives the python fixture service through a full
// release: it seeds the baseline pointers, uploads the merged contract to S3
// before the POST (the ordering a domain repository's CI must honour), posts
// kind=python naming the side-loaded fixture image, waits for promotion, and
// waits for the topology swap. It returns the release id and a cleanup the
// caller MUST defer.
//
// The cleanup is returned rather than deferred here because it must run while
// releaseDB is still open: the caller registers it after `defer
// clients.close(ctx)`, and defers run LIFO, so it fires first. A t.Cleanup
// would instead run after every deferred call, against a closed pool. A
// leftover service_prod pointer would drag this service's contract into every
// later test's assembled manifest set.
func promotePythonFixtureRelease(t *testing.T, ctx context.Context, clients *testClients) (string, func()) {
	t.Helper()

	releaseID := "e2e-py-" + uuid.NewString()[:8]

	// Baseline: every dbt service keeps its live pointer; the python service is
	// brand new, so both of its nodes are changed nodes.
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
	cleanup := func() {
		if _, err := clients.releaseDB.ExecContext(context.Background(),
			`DELETE FROM service_prod WHERE service_name = $1`, pyE2EService); err != nil {
			t.Errorf("cleanup: delete %s service_prod row: %v", pyE2EService, err)
		}
	}

	contractKey := fmt.Sprintf("%s/%s/contract.yaml", pyE2EService, releaseID)
	putS3Object(t, ctx, clients, contractKey, []byte(pythonContractYAML(t)))

	// The py_csv node's contract names this exact key; seeded before the
	// release posts so both the validation runner's header-only fetch and (on
	// a later run) the harness's full fetch resolve against real content.
	putS3Object(t, ctx, clients, pyCsvSourceKey, pyCsvFixtureData)

	postPythonRelease(t, clients, pyE2EService, releaseID, pyFixtureImage)

	assertValidationRequestedNodes(t, ctx, clients, releaseID,
		[]string{pyProbeUniqueID, pyBadShapeUniqueID, pyCsvUniqueID})

	// Real build_from_columns Jobs run in kind against the published runner
	// image; on success the release promotes and the topology swaps.
	waitForReleasePromoted(t, ctx, clients, releaseID, 10*time.Minute)
	waitForTopologySwap(t, ctx, clients, releaseID, pyProbeUniqueID, 2*time.Minute)

	return releaseID, cleanup
}

// TestE2E_PythonNodeRun_MaterializesAndReportsFailures proves the executor's
// python runtime dispatch end to end. It promotes the python fixture service,
// then runs each of its nodes: the conforming script node materializes real
// rows in the warehouse through the harness, the non-conforming one fails
// with the harness's deterministic error class reaching the control plane as
// structured JSON rather than scraped text, and the python-csv node — a
// scriptless node in the same mixed DAG — fetches its declared S3 source
// itself and materializes the csv's rows with the declared column types.
func TestE2E_PythonNodeRun_MaterializesAndReportsFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	_, cleanup := promotePythonFixtureRelease(t, ctx, clients)
	defer cleanup()

	t.Run("conforming node materializes its declared rows", func(t *testing.T) {
		runID, scheduleName := triggerPythonNodeRun(t, ctx, clients, "py_probe")
		defer cleanupSingleNodeRun(t, ctx, clients, runID, scheduleName)

		verifySchedulerSucceeded(t, ctx, clients, runID)

		var count int
		require.NoError(t, clients.dbtDB.QueryRowContext(ctx,
			`SELECT count(*) FROM e2e_schema.py_probe`).Scan(&count))
		assert.Equal(t, 3, count, "the harness must have written the script's rows")

		var labelType string
		require.NoError(t, clients.dbtDB.QueryRowContext(ctx, `
			SELECT data_type FROM information_schema.columns
			WHERE table_schema = 'e2e_schema' AND table_name = 'py_probe' AND column_name = 'label'
		`).Scan(&labelType))
		assert.Equal(t, "character varying", labelType,
			"the declared VARCHAR column type must be what the table carries")

		t.Log("✅ python node ran its own image and materialized rows in the warehouse")
	})

	t.Run("non-conforming node fails with its error class on the wire", func(t *testing.T) {
		runID, scheduleName := triggerPythonNodeRun(t, ctx, clients, "py_bad_shape")
		defer cleanupSingleNodeRun(t, ctx, clients, runID, scheduleName)

		verifySchedulerFailed(t, ctx, clients, runID)

		var runResultsURI string
		pollUntil(t, ctx, 3*time.Minute, 2*time.Second, func() (bool, error) {
			err := clients.stateDB.QueryRowContext(ctx, `
				SELECT COALESCE(te.run_results_uri, '')
				FROM task_execution te
				JOIN task_tracker t ON t.task_id = te.task_id
				WHERE t.schedule_id = $1
				ORDER BY te.created_at DESC
				LIMIT 1
			`, runID).Scan(&runResultsURI)
			if err != nil {
				return false, nil
			}
			return runResultsURI != "", nil
		}, "task_execution never recorded a run_results_uri for the failed python node")

		body := getS3Object(t, ctx, clients, runResultsURI)
		assert.Contains(t, string(body), `"status":"error"`)
		assert.Contains(t, string(body), "ConformError:",
			"the structured result must carry the harness's deterministic error class")

		t.Log("✅ a failing python node surfaced its A6 error class as structured JSON")
	})

	t.Run("csv node materializes the seeded csv's rows", func(t *testing.T) {
		runID, scheduleName := triggerPythonNodeRun(t, ctx, clients, "py_csv")
		defer cleanupSingleNodeRun(t, ctx, clients, runID, scheduleName)

		verifySchedulerSucceeded(t, ctx, clients, runID)

		type orderRow struct {
			OrderID int     `db:"order_id"`
			Amount  float64 `db:"amount"`
		}
		var rows []orderRow
		require.NoError(t, clients.dbtDB.SelectContext(ctx, &rows,
			`SELECT order_id, amount FROM e2e_schema.py_csv ORDER BY order_id`))
		require.Len(t, rows, 3, "the harness must have loaded every data row from the seeded csv")
		assert.Equal(t, []orderRow{
			{OrderID: 1, Amount: 19.99},
			{OrderID: 2, Amount: 5.5},
			{OrderID: 3, Amount: 100},
		}, rows, "row values must match the seeded csv exactly")

		columnTypes := map[string]string{}
		typeRows, err := clients.dbtDB.QueryContext(ctx, `
			SELECT column_name, data_type FROM information_schema.columns
			WHERE table_schema = 'e2e_schema' AND table_name = 'py_csv'
		`)
		require.NoError(t, err)
		defer typeRows.Close()
		for typeRows.Next() {
			var name, dataType string
			require.NoError(t, typeRows.Scan(&name, &dataType))
			columnTypes[name] = dataType
		}
		require.NoError(t, typeRows.Err())
		assert.Equal(t, "integer", columnTypes["order_id"],
			"the declared INTEGER column type must be what the table carries")
		assert.Equal(t, "double precision", columnTypes["amount"],
			"the declared DOUBLE PRECISION column type must be what the table carries")

		t.Log("✅ python-csv node fetched its S3 source and materialized the csv's rows in the warehouse")
	})
}

// triggerPythonNodeRun starts a single-node run of the named python fixture
// node and returns its run id and schedule name.
func triggerPythonNodeRun(t *testing.T, ctx context.Context, clients *testClients, table string) (uuid.UUID, string) {
	t.Helper()
	resp, err := clients.stateClient.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{
		ServiceName:    pyE2EService,
		SchemaName:     "e2e_schema",
		TableName:      table,
		MetadataSource: "latest",
	})
	require.NoError(t, err, "TriggerSingleNodeRun(%s)", table)
	require.NotEmpty(t, resp.RunId, "TriggerSingleNodeRun must return a run_id")
	runID, err := uuid.Parse(resp.RunId)
	require.NoError(t, err, "run_id must be a valid UUID")
	t.Logf("python single-node run: node=%s run_id=%s", table, runID)
	return runID, resp.ScheduleName
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
