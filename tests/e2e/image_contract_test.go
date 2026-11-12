package e2e

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"io/fs"
	"os/exec"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const (
	// pyMismatchService is the python service whose release names the mismatch
	// image. tests/e2e/ci-auth.json binds it to the python fixture repository.
	pyMismatchService = "svc-py-mismatch-e2e"
	// pyMismatchImage is the py-probe fixture's mismatch target, which
	// scripts/setup.sh builds and side-loads into kind under this exact tag.
	pyMismatchImage = "continuo-e2e-py-probe:mismatch"
	// pyMismatchUniqueID is the node both mismatch contracts declare, in
	// schedule pyMismatchSchedule, and pyMismatchTable its table.
	pyMismatchUniqueID = "e2e_schema.py_mismatch"
	pyMismatchTable    = "py_mismatch"
	pyMismatchSchedule = "e2e-py-mismatch"
	// pyMismatchScript is the script both contracts name, relative to the
	// fixture repository's root.
	pyMismatchScript = "scripts/py_probe.py"
)

// pyMismatchReleasedContract is the authored contract the release carries. The
// mismatch image bakes fixtures/py-probe/mismatch/py_mismatch.baked.yml
// instead, which differs from it in one read.
//
//go:embed fixtures/py-probe/mismatch/py_mismatch.released.yml
var pyMismatchReleasedContract []byte

// TestE2E_PythonImageContractMismatch_FailsInvalid proves that a python node
// whose image holds another contract than the one its release hashed never
// runs:
//
//  1. the released contract is built from py_mismatch.released.yml by the
//     runtime's own `continuo-runtime merge`, run in the fixture image, so its
//     entry carries the keys and defaults the harness hashes;
//  2. a bootstrap python release of svc-py-mismatch-e2e names the mismatch
//     image, whose baked contract reads another column, and promotes;
//  3. StartRun(e2e-py-mismatch) runs the node: the harness recomputes the
//     node's content_hash from the image, finds it differs from the CONTENT_HASH
//     execution-controller passes, and refuses to start, so the task fails with
//     failure class invalid and reason image_contract_mismatch after one
//     attempt, since run-controller retries no invalid failure.
func TestE2E_PythonImageContractMismatch_FailsInvalid(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	// additive polls: the promotion (10m) + catalog's swap (2m) + run-controller
	// applying it (2m) + the release's seed loads (5m) + the task (5m) = 24m.
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	// ── 1. the released contract, built by the runtime ───────────────────────
	script, err := fs.ReadFile(pyFixtureScripts, "fixtures/py-probe/"+pyMismatchScript)
	require.NoError(t, err, "read the fixture script %s", pyMismatchScript)
	repo := fstest.MapFS{
		"contracts/py_mismatch.yml": {Data: pyMismatchReleasedContract},
		pyMismatchScript:            {Data: script},
	}
	contract := mergeContractWithRuntime(t, ctx, pyFixtureImage, pyMismatchService, repo, ".")
	requireImageReproducesAnotherHash(t, ctx, contract)

	releaseID := "e2e-pymm-" + uuid.NewString()[:8]
	t.Logf("release_id=%s service=%s image=%s", releaseID, pyMismatchService, pyMismatchImage)

	// ── 2. a bootstrap release names the mismatch image and promotes ─────────
	// Production keeps every dbt service live, so the release's topology holds
	// them beside the python node.
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
	// A service_prod pointer left behind would put this service's contract into
	// every later release's topology. Deferred, so it runs while the pools are
	// open.
	defer func() {
		if _, err := clients.releaseDB.ExecContext(context.Background(),
			`DELETE FROM service_prod WHERE service_name = $1`, pyMismatchService); err != nil {
			t.Errorf("cleanup: delete %s service_prod row: %v", pyMismatchService, err)
		}
	}()

	contractKey := fmt.Sprintf("%s/%s/contract.yaml", pyMismatchService, releaseID)
	putS3Object(t, ctx, clients, contractKey, contract)
	defer func() {
		if _, err := clients.s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(e2eS3Bucket), Key: aws.String(contractKey),
		}); err != nil {
			t.Logf("delete s3://%s/%s: %v", e2eS3Bucket, contractKey, err)
		}
	}()
	postPythonRelease(t, clients, pyMismatchService, releaseID, pyMismatchImage, true)

	waitForReleasePromoted(t, ctx, clients, releaseID, 10*time.Minute)
	waitForTopologySwap(t, ctx, clients, releaseID, pyMismatchUniqueID, 2*time.Minute)
	waitForRunControllerToApply(t, ctx, clients, releaseID, pyMismatchSchedule, 2*time.Minute)
	// This promotion is neither the install's first nor a rebaseline, so it
	// adopts no seed, and its seed pass may plan promote-seed runs. Wait for
	// them, so the run cleanup below deletes none of their Jobs.
	waitForPromoteSeedRunsToEnd(t, ctx, clients, releaseID, 5*time.Minute)

	// ── 3. the node's run fails invalid image_contract_mismatch, once ────────
	started, err := clients.runClient.StartRun(ctx, &runv1.StartRunRequest{
		RequestId: uuid.NewString(), ScheduleName: pyMismatchSchedule,
	})
	require.NoError(t, err, "StartRun(%s)", pyMismatchSchedule)
	runID, err := uuid.Parse(started.GetRunId())
	require.NoError(t, err, "StartRun must return the run's id")
	defer cleanupSingleNodeRun(t, ctx, clients, runID, pyMismatchSchedule)
	t.Logf("StartRun(%s) created run %s", pyMismatchSchedule, runID)

	ended := waitForTaskEnded(t, ctx, clients, taskOfRun(t, ctx, clients, runID, pyMismatchTable), pyMismatchTable, 5*time.Minute)
	assert.Equal(t, "failed", ended.status, "the node whose image does not hold its release's contract fails")
	want := fmt.Sprintf("0:failed:%s:%s", pkgmodel.FailureReasonImageContractMismatch.Class(), pkgmodel.FailureReasonImageContractMismatch)
	assert.Equal(t, []string{want}, ended.attempts,
		"one attempt, failed %s %s: run-controller retries no invalid failure",
		pkgmodel.FailureClassInvalid, pkgmodel.FailureReasonImageContractMismatch)
	verifySchedulerFailed(t, ctx, clients, runID)
	t.Logf("✅ %s failed %s after one attempt: its image holds another contract than release %s",
		pyMismatchUniqueID, want, releaseID)
}

// requireImageReproducesAnotherHash asserts the precondition the test rests
// on: the content_hash the merged contract carries for the mismatch node is not
// the one the mismatch image computes from its baked contract with
// `continuo-runtime hash`. A failure means the two fixture contracts no longer
// differ in meaning.
func requireImageReproducesAnotherHash(t *testing.T, ctx context.Context, contract []byte) {
	t.Helper()
	var doc struct {
		Nodes []struct {
			Schema      string `yaml:"schema"`
			Table       string `yaml:"table"`
			ContentHash string `yaml:"content_hash"`
		} `yaml:"nodes"`
	}
	require.NoError(t, yaml.Unmarshal(contract, &doc), "parse the merged contract")
	released := ""
	for _, n := range doc.Nodes {
		if n.Schema+"."+n.Table == pyMismatchUniqueID {
			released = n.ContentHash
		}
	}
	require.NotEmpty(t, released, "the merged contract carries a content_hash for %s", pyMismatchUniqueID)

	cmd := exec.CommandContext(ctx, "docker", "run", "--rm", "--entrypoint", "continuo-runtime", //nolint:gosec // fixed arguments; the image is a test constant
		pyMismatchImage, "hash", "/app/contracts", "--repo-root", "/app", "--dialect", "postgres")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	require.NoError(t, cmd.Run(), "continuo-runtime hash in %s (rebuild it with scripts/setup.sh): %s", pyMismatchImage, stderr.String())
	baked := ""
	for _, line := range strings.Split(stdout.String(), "\n") {
		if fields := strings.Fields(line); len(fields) == 2 && fields[0] == pyMismatchUniqueID {
			baked = fields[1]
		}
	}
	require.NotEmpty(t, baked, "%s's baked contracts declare %s: %s", pyMismatchImage, pyMismatchUniqueID, stdout.String())
	require.NotEqual(t, released, baked,
		"the mismatch image must compute another content_hash for %s than its release carries", pyMismatchUniqueID)
	t.Logf("released content_hash %s, the mismatch image's %s", released, baked)
}
