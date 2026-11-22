package e2e

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os/exec"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/streams"
	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
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
	pyAPIUniqueID      = "e2e_schema.py_api"
	pyUpsertUniqueID   = "e2e_schema.py_upsert"

	// pyAPISecret is the continuo-api-* Secret the py_api node's contract
	// names in secretRef. The run subtests create and delete it themselves in
	// the namespace the e2e Jobs run in, so its presence is under test control.
	pyAPISecret = "continuo-api-probe"

	// pyUpsertSecret is the continuo-api-* Secret the py_upsert node's contract
	// names in secretRef. It holds UPSERT_PHASE, the key that picks the rows the
	// node's script returns, and setUpsertPhase rewrites it before every run.
	pyUpsertSecret = "continuo-api-upsert"

	// pyFixtureImage is the domain image scripts/setup.sh builds and side-loads
	// into kind. The release posts it as image_tag and execution-controller runs it
	// verbatim, so this string must match setup.sh's PY_FIXTURE_IMAGE.
	pyFixtureImage = "continuo-e2e-py-probe:latest"

	// pyCsvSourceKey is the S3 key the py_csv node's contract points at
	// (s3://<e2eS3Bucket>/pyCsvSourceKey). Seeded from pyCsvFixtureData before
	// the release posts, so both the validation runner's header-only fetch and
	// the run harness's full fetch resolve against real content.
	pyCsvSourceKey = "fixtures/orders.csv"
)

// pyFixtureRoot is the py-probe fixture's directory inside pyFixtureRepo: the
// repository root its contracts name their scripts from.
const pyFixtureRoot = "fixtures/py-probe"

// pyFixtureRepo holds the fixture's ODCS contracts and scripts, embedded from
// the same files the fixture image's Dockerfile COPYs, so the bundle this test
// uploads and the contracts the container runs cannot drift. Embedding rather
// than reading from disk also frees the test from assuming where the
// repository is mounted.
//
//go:embed fixtures/py-probe/contracts fixtures/py-probe/scripts
var pyFixtureRepo embed.FS

// pyCsvFixtureData is the csv object the py_csv node's contract names — it is
// never baked into the fixture image (only contracts/ and scripts/ are), so
// the test itself seeds it into S3 before posting the release.
//
//go:embed fixtures/py-probe/data/orders.csv
var pyCsvFixtureData []byte

const (
	// odcsBundleName is the object a python-odcs service's CI uploads for a
	// release, at <service>/<release_id>/odcs-bundle.json.
	odcsBundleName = "odcs-bundle.json"
	// pyBundleRuntime and pyBundleContracts are the producer versions a
	// hand-built bundle names: continuo-python-runtime 0.13.0, the runtime the
	// fixture image is built on, and the continuo-contracts 0.2.0 it carries.
	pyBundleRuntime   = "0.13.0"
	pyBundleContracts = "0.2.0"
)

// odcsBundle is the document a python-odcs service's CI uploads, as
// `continuo-runtime pack` writes it.
type odcsBundle struct {
	BundleVersion int               `json:"bundle_version"`
	Service       string            `json:"service"`
	Producer      map[string]string `json:"producer"`
	ImagePackages map[string]string `json:"image_packages"`
	Nodes         []odcsBundleNode  `json:"nodes"`
}

// odcsBundleNode is one contract of a bundle: its path in the repository, its
// text as written, and the two hash parts the producer computes from the
// repository's files.
type odcsBundleNode struct {
	Path           string `json:"path"`
	Text           string `json:"text"`
	SourceHash     string `json:"source_hash"`
	SharedCodeHash string `json:"shared_code_hash"`
}

// odcsContracts returns the text of every contract file in the contracts/
// folder under root in repo, keyed by its path relative to root, the path a
// bundle names it by.
func odcsContracts(t *testing.T, repo fs.FS, root string) map[string]string {
	t.Helper()
	paths, err := fs.Glob(repo, path.Join(root, "contracts", "*.odcs.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "%s/contracts holds no .odcs.yaml file", root)
	contracts := make(map[string]string, len(paths))
	for _, p := range paths {
		text, err := fs.ReadFile(repo, p)
		require.NoError(t, err, "read %s", p)
		contracts[strings.TrimPrefix(p, root+"/")] = string(text)
	}
	return contracts
}

// buildODCSBundle builds the odcs-bundle.json of service from contracts (path
// relative to root → text as written), the bundle `continuo-runtime pack`
// writes for the same files, without running the runtime's checks: a test
// that releases a contract the runtime refuses builds its bundle here.
//
// A node's source_hash is the sha256 of the script its contract names, read
// from root in repo, or of its csvSource uri for a python-csv node, as the
// runtime computes it, so the hash a release stores is the one the fixture
// image's harness recomputes at run time. shared_code_hash is empty: no
// fixture script imports code of its repository or declares an asset.
// topology-controller computes the third part, the config hash, from the
// contract itself.
func buildODCSBundle(t *testing.T, repo fs.FS, root, service string, imagePackages map[string]string, contracts map[string]string) []byte {
	t.Helper()
	nodes := make([]odcsBundleNode, 0, len(contracts))
	for p, text := range contracts {
		props := continuoProperties(t, p, text)
		script, _ := props["script"].(string)
		csvSource, _ := props["csvSource"].(string)
		var sourceHash string
		switch {
		case script != "":
			body, err := fs.ReadFile(repo, path.Join(root, script))
			require.NoError(t, err, "read the script %s that %s names", script, p)
			sourceHash = sha256Hex(string(body))
		case csvSource != "":
			sourceHash = sha256Hex(csvSource)
		default:
			require.Failf(t, "contract names no source", "%s names neither a script nor a csvSource", p)
		}
		nodes = append(nodes, odcsBundleNode{Path: p, Text: text, SourceHash: sourceHash})
	}
	return encodeODCSBundle(t, service, imagePackages, nodes)
}

// continuoProperties returns the continuo custom properties the ODCS contract
// text at contractPath declares, by property name.
func continuoProperties(t *testing.T, contractPath, text string) map[string]any {
	t.Helper()
	var doc struct {
		CustomProperties []struct {
			Vendor   string `yaml:"vendor"`
			Property string `yaml:"property"`
			Value    any    `yaml:"value"`
		} `yaml:"customProperties"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(text), &doc), "parse the contract %s", contractPath)
	props := map[string]any{}
	for _, prop := range doc.CustomProperties {
		if prop.Vendor == "continuo" {
			props[prop.Property] = prop.Value
		}
	}
	return props
}

// encodeODCSBundle writes the bundle of service holding nodes, in path order,
// with the producer versions of a bundle runtime 0.13.0 packs.
func encodeODCSBundle(t *testing.T, service string, imagePackages map[string]string, nodes []odcsBundleNode) []byte {
	t.Helper()
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Path < nodes[j].Path })
	raw, err := json.Marshal(odcsBundle{
		BundleVersion: 1,
		Service:       service,
		Producer:      map[string]string{"continuo_python_runtime": pyBundleRuntime, "continuo_contracts": pyBundleContracts},
		ImagePackages: imagePackages,
		Nodes:         nodes,
	})
	require.NoError(t, err, "encode the bundle of %s", service)
	return raw
}

// imagePackages returns the package list image carries, as the image's own
// `continuo-runtime packages` prints it: the list `continuo-runtime pack
// --image <image>` records in a bundle.
func imagePackages(t *testing.T, ctx context.Context, image string) map[string]string {
	t.Helper()
	cmd := exec.CommandContext(ctx, "docker", "run", "--rm", "--entrypoint", "continuo-runtime", image, "packages") //nolint:gosec // fixed arguments; the image is a test constant
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	require.NoError(t, cmd.Run(), "continuo-runtime packages in %s (rebuild it with scripts/setup.sh): %s", image, stderr.String())
	var packages map[string]string
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &packages), "continuo-runtime packages in %s printed %s", image, stdout.String())
	require.NotEmpty(t, packages, "continuo-runtime packages in %s printed no package", image)
	return packages
}

// pyFixtureBundle is the odcs-bundle.json of the python e2e service, built
// from the fixture image's own contract files with contracts' text replaced
// where the caller names a path, and the package list of the fixture image.
func pyFixtureBundle(t *testing.T, ctx context.Context, replace map[string]string) []byte {
	t.Helper()
	contracts := odcsContracts(t, pyFixtureRepo, pyFixtureRoot)
	for p, text := range replace {
		require.Contains(t, contracts, p, "the fixture has no contract %s to replace", p)
		contracts[p] = text
	}
	return buildODCSBundle(t, pyFixtureRepo, pyFixtureRoot, pyE2EService, imagePackages(t, ctx, pyFixtureImage), contracts)
}

// TestE2E_ReleasePromote_PythonContractSkipsCompileAndPromotes proves the
// python release path end to end: a python-odcs release skips the compile
// leg, reads every ODCS contract of its bundle, validates every node —
// including a python-csv node in the same mixed DAG — via a real
// build_from_columns Job against the published runner image, promotes, swaps
// the Neo4j topology, and records the python-odcs kind and the bundle pointer
// on service_prod.
func TestE2E_ReleasePromote_PythonContractSkipsCompileAndPromotes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	// Tap the streams before the release is posted: the trim loop removes
	// consumed entries, so the checks below read what the taps recorded.
	validationTap := startStreamTap(t, ctx, clients.redisClient, streams.ValidationRequestedV1)
	compileTap := startStreamTap(t, ctx, clients.redisClient, streams.CompileRequestedV1)

	releaseID, cleanup := promotePythonFixtureRelease(t, ctx, clients, validationTap)
	defer cleanup()

	// Skip-compile threading: the validation request names exactly the python
	// nodes, routed to build_from_columns with a .json spec URI, and no
	// compile.requested was ever emitted for this release. The csv node takes
	// the identical validation_op — its spec carries csv_source instead of a
	// script, and the runner's header check against the real minio
	// object (seeded above) must have passed for the release to promote at
	// all, which waitForReleasePromoted inside promotePythonFixtureRelease
	// already proved.
	assertValidationNodeOp(t, ctx, validationTap, releaseID, pyProbeUniqueID, model.ValidationOpBuildFromColumns, ".json")
	assertValidationNodeOp(t, ctx, validationTap, releaseID, pyCsvUniqueID, model.ValidationOpBuildFromColumns, ".json")
	assertValidationNodeOp(t, ctx, validationTap, releaseID, pyAPIUniqueID, model.ValidationOpBuildFromColumns, ".json")
	assertValidationNodeOp(t, ctx, validationTap, releaseID, pyUpsertUniqueID, model.ValidationOpBuildFromColumns, ".json")
	assertNoCompileRequested(t, compileTap, releaseID)

	// The promoted pointer records the python-odcs kind and the bundle.
	var kind, s3Key string
	require.NoError(t, clients.releaseDB.QueryRowContext(ctx,
		`SELECT manifest_kind, manifest_s3_key FROM service_prod WHERE service_name = $1`,
		pyE2EService).Scan(&kind, &s3Key))
	assert.Equal(t, "python-odcs", kind)
	assert.True(t, strings.HasSuffix(s3Key, "/"+odcsBundleName),
		"pointer must be the service's bundle, got %s", s3Key)
	t.Log("✅ python release skipped compile, validated via build_from_columns, promoted, and swapped topology")
}

// promotePythonFixtureRelease promotes the python fixture service's bundle
// (see pyFixtureBundle): it seeds the csv object the py_csv node reads, then
// drives the release through promotePythonContractRelease. The caller MUST
// defer the returned cleanup.
func promotePythonFixtureRelease(t *testing.T, ctx context.Context, clients *testClients, validationTap *streamTap) (string, func()) {
	t.Helper()

	// The py_csv node's contract names this exact key; seeded before the
	// release posts so both the validation runner's header-only fetch and (on
	// a later run) the harness's full fetch resolve against real content.
	putS3Object(t, ctx, clients, pyCsvSourceKey, pyCsvFixtureData)

	return promotePythonContractRelease(t, ctx, clients, validationTap, pyFixtureBundle(t, ctx, nil),
		[]string{pyProbeUniqueID, pyBadShapeUniqueID, pyCsvUniqueID, pyAPIUniqueID, pyUpsertUniqueID}, pyProbeUniqueID)
}

// promotePythonContractRelease drives pyE2EService through a full release of
// the given odcs-bundle.json: it seeds the baseline pointers, uploads the
// bundle to S3 before the POST (the ordering a domain repository's CI must
// honour), posts a python-odcs release naming the side-loaded fixture image, checks the
// validation request names exactly validationNodes, waits for promotion, and
// waits for swapNode to appear in the swapped topology. It returns the release
// id and a cleanup the caller MUST defer.
//
// The cleanup is returned for the caller to defer, so it runs when the test
// function returns, while releaseDB is still open: the cleanup setupClients
// registered closes the pools only after every deferred call. A leftover
// service_prod pointer would drag this service's contract into every later
// test's assembled manifest set.
func promotePythonContractRelease(t *testing.T, ctx context.Context, clients *testClients, validationTap *streamTap,
	bundle []byte, validationNodes []string, swapNode string) (string, func()) {
	t.Helper()

	releaseID := "e2e-py-" + uuid.NewString()[:8]

	// Baseline: every dbt service keeps its live pointer; the python service is
	// brand new, so every one of its nodes is a changed node.
	allServices := baselineServices(t, ctx, clients)
	require.NotEmpty(t, allServices)
	var prodNodes []map[string]string
	for svc, si := range allServices {
		for _, n := range si.nodes {
			prodNodes = append(prodNodes, map[string]string{
				"unique_id": n.uniqueID, "service": svc, "content_hash": n.contentHash,
			})
		}
	}

	resetReleaseControllerQueue(t, ctx, clients)
	seedCurrentProd(t, ctx, clients, prodNodes)
	seedServiceProd(t, ctx, clients, allServices)
	cleanup := func() {
		if _, err := clients.releaseDB.ExecContext(context.Background(),
			`DELETE FROM service_prod WHERE service_name = $1`, pyE2EService); err != nil {
			t.Errorf("cleanup: delete %s service_prod row: %v", pyE2EService, err)
		}
	}

	putODCSBundle(t, ctx, clients, pyE2EService, releaseID, bundle)
	postPythonRelease(t, clients, pyE2EService, releaseID, pyFixtureImage, false)

	assertValidationRequestedNodes(t, ctx, validationTap, releaseID, validationNodes)

	// Real build_from_columns Jobs run in kind against the published runner
	// image; on success the release promotes and the topology swaps.
	waitForReleasePromoted(t, ctx, clients, releaseID, 10*time.Minute)
	waitForTopologySwap(t, ctx, clients, releaseID, swapNode, 2*time.Minute)

	return releaseID, cleanup
}

// TestE2E_PythonNodeRun_MaterializesAndReportsFailures proves execution-controller's
// python runtime dispatch end to end. It promotes the python fixture service,
// then runs each of its nodes: the conforming script node materializes real
// rows in the warehouse through the harness, the non-conforming one fails
// with the harness's deterministic error class reaching the control plane as
// structured JSON rather than scraped text, and the python-csv node — a
// scriptless node in the same mixed DAG — fetches its declared S3 source
// itself and materializes the csv's rows with the declared column types.
// Finally the python-api node runs twice: with its continuo-api-* Secret
// present the script sees the Secret's key as an env var, and with the Secret
// deleted the pod cannot start and the run fails rather than hanging.
func TestE2E_PythonNodeRun_MaterializesAndReportsFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	// Tap the streams before the release is posted: the trim loop removes
	// consumed entries, so the checks below read what the taps recorded.
	validationTap := startStreamTap(t, ctx, clients.redisClient, streams.ValidationRequestedV1)

	_, cleanup := promotePythonFixtureRelease(t, ctx, clients, validationTap)
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

		body := failedRunResults(t, ctx, clients, runID)
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

	t.Run("python-api node receives its Secret as env vars", func(t *testing.T) {
		createAPISecret(t, ctx)
		defer deleteAPISecret(t, ctx)
		runID, scheduleName := triggerPythonNodeRun(t, ctx, clients, "py_api")
		defer cleanupSingleNodeRun(t, ctx, clients, runID, scheduleName)

		verifySchedulerSucceeded(t, ctx, clients, runID)

		var present int
		require.NoError(t, clients.dbtDB.QueryRowContext(ctx,
			`SELECT key_present FROM e2e_schema.py_api WHERE id = 1`).Scan(&present))
		assert.Equal(t, 1, present, "PROBE_API_KEY from continuo-api-probe must reach the script")
		t.Log("✅ python-api node ran with its continuo-api-* Secret attached")
	})

	t.Run("python-api node fails when its Secret is missing", func(t *testing.T) {
		deleteAPISecret(t, ctx)
		runID, scheduleName := triggerPythonNodeRun(t, ctx, clients, "py_api")
		defer cleanupSingleNodeRun(t, ctx, clients, runID, scheduleName)

		verifySchedulerFailed(t, ctx, clients, runID)
		t.Log("✅ a missing continuo-api-* Secret fails the run instead of hanging it")
	})
}

// TestE2E_PythonUpsertNode_UpdatesByKeyKeepsOldRowsAndRefusesDrift proves the
// write path of a python-api node with modeType upsert end to end. It promotes
// the python fixture service, then runs py_upsert in phases, each phase being
// the UPSERT_PHASE its Secret names: the first run creates the table with the
// key as its primary key and inserts the rows, the second updates the row whose
// id it returns again, adds the new id and keeps the row it no longer returns,
// a run whose script returns no row leaves the table as it is, and a run
// against a table that gained a column the contract does not declare fails
// before it writes. The sub-tests run in order: each starts from the table the
// one before left.
func TestE2E_PythonUpsertNode_UpdatesByKeyKeepsOldRowsAndRefusesDrift(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	// Tap the streams before the release is posted: the trim loop removes
	// consumed entries, so the checks below read what the taps recorded.
	validationTap := startStreamTap(t, ctx, clients.redisClient, streams.ValidationRequestedV1)

	_, cleanup := promotePythonFixtureRelease(t, ctx, clients, validationTap)
	defer cleanup()

	// The runtime under test creates the table in the warehouse, so a table an
	// earlier run left, with its rows and columns, is dropped before the first
	// run and again when the test ends, together with the Secret that holds the
	// phase. The deferred calls run while the pools are still open.
	dropPyUpsertTable(t, ctx, clients)
	defer dropPyUpsertTable(t, context.Background(), clients)
	defer deleteUpsertSecret(t, context.Background())

	firstRows := []pyUpsertRow{{1, "a"}, {2, "b"}, {3, "c"}}
	updatedRows := []pyUpsertRow{{1, "a"}, {2, "B"}, {3, "c"}, {4, "d"}}

	ok := t.Run("phase 1 creates the table and inserts the rows", func(t *testing.T) {
		setUpsertPhase(t, ctx, 1)
		runID, scheduleName := triggerPythonNodeRun(t, ctx, clients, "py_upsert")
		defer cleanupSingleNodeRun(t, ctx, clients, runID, scheduleName)

		verifySchedulerSucceeded(t, ctx, clients, runID)

		assert.Equal(t, firstRows, readPyUpsertRows(t, ctx, clients),
			"the first run must insert exactly the rows of phase 1")
		t.Log("✅ upsert node created its table and inserted the first rows")
	})
	if !ok {
		return
	}

	ok = t.Run("phase 2 updates by key and keeps the rows the script no longer returns", func(t *testing.T) {
		setUpsertPhase(t, ctx, 2)
		runID, scheduleName := triggerPythonNodeRun(t, ctx, clients, "py_upsert")
		defer cleanupSingleNodeRun(t, ctx, clients, runID, scheduleName)

		verifySchedulerSucceeded(t, ctx, clients, runID)

		assert.Equal(t, updatedRows, readPyUpsertRows(t, ctx, clients),
			"id 2 must carry its new label, id 4 must be added and id 1, which phase 2 does not return, must stay")

		var keyColumns []string
		require.NoError(t, clients.dbtDB.SelectContext(ctx, &keyColumns, `
			SELECT kcu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
			  ON kcu.constraint_schema = tc.constraint_schema
			 AND kcu.constraint_name = tc.constraint_name
			 AND kcu.table_name = tc.table_name
			WHERE tc.table_schema = 'e2e_schema' AND tc.table_name = 'py_upsert'
			  AND tc.constraint_type = 'PRIMARY KEY'
			ORDER BY kcu.ordinal_position
		`))
		assert.Equal(t, []string{"id"}, keyColumns,
			"the contract's key must be the table's primary key")
		t.Log("✅ upsert node updated by key, added the new row and kept the old one")
	})
	if !ok {
		return
	}

	ok = t.Run("phase 3 with no rows leaves the table unchanged", func(t *testing.T) {
		setUpsertPhase(t, ctx, 3)
		runID, scheduleName := triggerPythonNodeRun(t, ctx, clients, "py_upsert")
		defer cleanupSingleNodeRun(t, ctx, clients, runID, scheduleName)

		verifySchedulerSucceeded(t, ctx, clients, runID)

		assert.Equal(t, updatedRows, readPyUpsertRows(t, ctx, clients),
			"an upsert of no row must not delete or change any row")
		t.Log("✅ upsert node with an empty result kept every row")
	})
	if !ok {
		return
	}

	t.Run("a table that gained a column fails the run and keeps its rows", func(t *testing.T) {
		_, err := clients.dbtDB.ExecContext(ctx, `ALTER TABLE e2e_schema.py_upsert ADD COLUMN extra INTEGER`)
		require.NoError(t, err, "add a column the contract does not declare")
		defer func() {
			dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer dropCancel()
			if _, err := clients.dbtDB.ExecContext(dropCtx,
				`ALTER TABLE e2e_schema.py_upsert DROP COLUMN IF EXISTS extra`); err != nil {
				t.Errorf("cleanup: drop column extra: %v", err)
			}
		}()

		setUpsertPhase(t, ctx, 2)
		runID, scheduleName := triggerPythonNodeRun(t, ctx, clients, "py_upsert")
		defer cleanupSingleNodeRun(t, ctx, clients, runID, scheduleName)

		verifySchedulerFailed(t, ctx, clients, runID)

		body := failedRunResults(t, ctx, clients, runID)
		assert.Contains(t, string(body), `"status":"error"`)
		assert.Contains(t, string(body), "LoadError:",
			"the structured result must carry the harness's load error class")
		assert.Contains(t, string(body), "unexpected column(s)",
			"the run must fail on the column the contract does not declare, not for another reason")

		assert.Equal(t, updatedRows, readPyUpsertRows(t, ctx, clients),
			"a refused run must leave the table's rows as they were")
		t.Log("✅ upsert node refused a table whose columns drifted from its contract")
	})
}

// pyUpsertRow is one row of the py_upsert node's table.
type pyUpsertRow struct {
	ID    int    `db:"id"`
	Label string `db:"label"`
}

// readPyUpsertRows returns every row of the py_upsert table, ordered by id.
func readPyUpsertRows(t *testing.T, ctx context.Context, clients *testClients) []pyUpsertRow {
	t.Helper()
	var rows []pyUpsertRow
	require.NoError(t, clients.dbtDB.SelectContext(ctx, &rows,
		`SELECT id, label FROM e2e_schema.py_upsert ORDER BY id`))
	return rows
}

// dropPyUpsertTable drops the py_upsert node's table, whose rows and columns a
// run builds on, so a test starts from the table its first run creates and
// leaves none behind.
func dropPyUpsertTable(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()
	dropCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := clients.dbtDB.ExecContext(dropCtx, `DROP TABLE IF EXISTS e2e_schema.py_upsert`)
	require.NoError(t, err, "drop e2e_schema.py_upsert")
}

// failedRunResults waits until the latest attempt of the run's task has
// recorded its run results, and returns the structured result object the
// attempt uploaded.
func failedRunResults(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID) []byte {
	t.Helper()
	var runResultsURI string
	pollUntil(t, ctx, 3*time.Minute, 2*time.Second, func() (bool, error) {
		err := clients.runControllerDB.QueryRowContext(ctx, `
			SELECT COALESCE(a.run_results_uri, '')
			FROM task_attempts a
			JOIN tasks t ON t.task_id = a.task_id
			WHERE t.run_id = $1
			ORDER BY a.last_report_at DESC NULLS LAST
			LIMIT 1
		`, runID).Scan(&runResultsURI)
		if err != nil {
			return false, nil
		}
		return runResultsURI != "", nil
	}, "task_attempts never recorded a run_results_uri for the failed python node")
	return getS3Object(t, ctx, clients, runResultsURI)
}

// createAPISecret (re)creates the py_api node's continuo-api-* Secret in the
// namespace the e2e Jobs run in, holding a dummy PROBE_API_KEY.
func createAPISecret(t *testing.T, ctx context.Context) {
	t.Helper()
	_ = exec.CommandContext(ctx, "kubectl", "delete", "secret", pyAPISecret, "-n", "default", "--ignore-not-found").Run()
	out, err := exec.CommandContext(ctx, "kubectl", "create", "secret", "generic", pyAPISecret,
		"-n", "default", "--from-literal=PROBE_API_KEY=e2e-dummy").CombinedOutput()
	require.NoError(t, err, "create %s: %s", pyAPISecret, out)
}

// deleteAPISecret removes the py_api node's continuo-api-* Secret; deleting an
// absent Secret is not an error.
func deleteAPISecret(t *testing.T, ctx context.Context) {
	t.Helper()
	out, err := exec.CommandContext(ctx, "kubectl", "delete", "secret", pyAPISecret,
		"-n", "default", "--ignore-not-found").CombinedOutput()
	require.NoError(t, err, "delete %s: %s", pyAPISecret, out)
}

// setUpsertPhase (re)creates the py_upsert node's continuo-api-* Secret in the
// namespace the e2e Jobs run in, holding UPSERT_PHASE=<phase>: the run that
// starts next reads the phase from it.
func setUpsertPhase(t *testing.T, ctx context.Context, phase int) {
	t.Helper()
	_ = exec.CommandContext(ctx, "kubectl", "delete", "secret", pyUpsertSecret, "-n", "default", "--ignore-not-found").Run()
	out, err := exec.CommandContext(ctx, "kubectl", "create", "secret", "generic", pyUpsertSecret, //nolint:gosec // fixed arguments; phase is an int
		"-n", "default", fmt.Sprintf("--from-literal=UPSERT_PHASE=%d", phase)).CombinedOutput()
	require.NoError(t, err, "create %s: %s", pyUpsertSecret, out)
}

// deleteUpsertSecret removes the py_upsert node's continuo-api-* Secret;
// deleting an absent Secret is not an error.
func deleteUpsertSecret(t *testing.T, ctx context.Context) {
	t.Helper()
	out, err := exec.CommandContext(ctx, "kubectl", "delete", "secret", pyUpsertSecret,
		"-n", "default", "--ignore-not-found").CombinedOutput()
	require.NoError(t, err, "delete %s: %s", pyUpsertSecret, out)
}

// triggerPythonNodeRun starts a single-node run of the named python fixture
// node and returns its run id and schedule name.
func triggerPythonNodeRun(t *testing.T, ctx context.Context, clients *testClients, table string) (uuid.UUID, string) {
	t.Helper()
	resp, err := clients.runClient.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{RequestId: uuid.NewString(),
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

// postPythonRelease submits a python-odcs release through the public API as
// the CI pipeline of the python fixture repository. When bootstrap is true the
// release promotes without validation.
func postPythonRelease(t *testing.T, clients *testClients, service, releaseID, imageTag string, bootstrap bool) {
	t.Helper()
	token := mintCIToken(t, e2ePyRepositoryID, "carolsimone/continuo-py-demo", releaseID)
	status, body := submitPublicRelease(t, clients, token, map[string]any{
		"service":    service,
		"release_id": releaseID,
		"image_tag":  imageTag,
		"kind":       "python-odcs",
		"bootstrap":  bootstrap,
	})
	require.Equal(t, http.StatusAccepted, status, "POST /api/v1/releases: expected 202, got %d: %v", status, body)
}

// putODCSBundle uploads bundle as service's odcs-bundle.json for releaseID,
// the object a python-odcs release reads, and returns its key.
func putODCSBundle(t *testing.T, ctx context.Context, clients *testClients, service, releaseID string, bundle []byte) string {
	t.Helper()
	key := fmt.Sprintf("%s/%s/%s", service, releaseID, odcsBundleName)
	putS3Object(t, ctx, clients, key, bundle)
	return key
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
func assertValidationNodeOp(t *testing.T, ctx context.Context, validationTap *streamTap, releaseID, nodeID string, wantOp model.ValidationOp, wantURISuffix string) {
	t.Helper()
	pollUntil(t, ctx, 2*time.Minute, 1*time.Second, func() (bool, error) {
		msgs := validationTap.Entries()
		for _, msg := range msgs {
			payload, _ := msg.Values["payload"].(string)
			if payload == "" {
				continue
			}
			var p struct {
				ReleaseID string `json:"release_id"`
				Nodes     []struct {
					UniqueID             string             `json:"unique_id"`
					ValidationOp         model.ValidationOp `json:"validation_op"`
					CandidateArtifactURI string             `json:"candidate_artifact_uri"`
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
func assertNoCompileRequested(t *testing.T, compileTap *streamTap, releaseID string) {
	t.Helper()
	msgs := compileTap.Entries()
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
