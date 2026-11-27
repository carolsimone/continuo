package e2e

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"io/fs"
	"os/exec"
	"path"
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
)

const (
	// pyMismatchProject is the python project whose release names the mismatch
	// image. tests/e2e/ci-auth.json binds it to the python fixture repository.
	pyMismatchProject = "svc-py-mismatch-e2e"
	// pyMismatchImage is the py-probe fixture's mismatch target, which
	// scripts/setup.sh builds and side-loads into kind under this exact tag.
	pyMismatchImage = "continuo-e2e-py-probe:mismatch"
	// pyMismatchUniqueID is the node both mismatch contracts declare, in
	// schedule pyMismatchSchedule, and pyMismatchTable its table.
	pyMismatchUniqueID = "e2e_schema.py_mismatch"
	pyMismatchTable    = "py_mismatch"
	pyMismatchSchedule = "e2e-py-mismatch"
	// pyMismatchScript is the script both contracts name, relative to the
	// fixture repository's root, and pyMismatchContract the path the released
	// contract is packed from, the name the runtime requires of its id.
	pyMismatchScript   = "scripts/py_probe.py"
	pyMismatchContract = "contracts/py_mismatch.odcs.yaml"
)

// pyMismatchReleasedContract is the ODCS contract the release carries. The
// mismatch image bakes fixtures/py-probe/mismatch/py_mismatch.baked.odcs.yaml
// instead, which differs from it in one read.
//
//go:embed fixtures/py-probe/mismatch/py_mismatch.released.odcs.yaml
var pyMismatchReleasedContract []byte

// TestE2E_PythonImageContractMismatch_FailsInvalid proves that a python node
// whose image holds another contract than the one its release hashed never
// runs:
//
//  1. the release's bundle is packed from py_mismatch.released.odcs.yaml by
//     the runtime's own `continuo-runtime pack`, run in the mismatch image, so
//     it carries the hash parts the harness computes and the image's package
//     list;
//  2. a bootstrap Python release of svc-py-mismatch-e2e names the mismatch
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

	verifyComponentsHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	// ── 1. the release's bundle, packed by the runtime ───────────────────────
	script, err := fs.ReadFile(pyFixtureRepo, path.Join(pyFixtureRoot, pyMismatchScript))
	require.NoError(t, err, "read the fixture script %s", pyMismatchScript)
	repo := fstest.MapFS{
		pyMismatchContract: {Data: pyMismatchReleasedContract},
		pyMismatchScript:   {Data: script},
	}
	bundle := packBundleWithRuntime(t, ctx, pyMismatchImage, pyMismatchProject, repo, ".")
	requireImageReproducesAnotherHash(t, ctx, repo)

	releaseID := "e2e-pymm-" + uuid.NewString()[:8]
	t.Logf("release_id=%s project=%s image=%s", releaseID, pyMismatchProject, pyMismatchImage)

	// ── 2. a bootstrap release names the mismatch image and promotes ─────────
	// Production keeps every dbt project live, so the release's topology holds
	// them beside the python node.
	allProjects := baselineProjects(t, ctx, clients)
	require.NotEmpty(t, allProjects)
	var prodNodes []map[string]string
	for svc, si := range allProjects {
		for _, n := range si.nodes {
			prodNodes = append(prodNodes, map[string]string{
				"unique_id": n.uniqueID, "project": svc, "content_hash": n.contentHash,
			})
		}
	}
	resetReleaseControllerQueue(t, ctx, clients)
	seedLiveRelease(t, ctx, clients, prodNodes)
	seedProductionPointers(t, ctx, clients, allProjects)
	// A production pointer left behind would put this project's contracts
	// into every later release's topology. Deferred, so it runs while the
	// pools are open.
	defer func() {
		if _, err := clients.releaseDB.ExecContext(context.Background(),
			`DELETE FROM production_pointers WHERE project_name = $1`, pyMismatchProject); err != nil {
			t.Errorf("cleanup: delete %s production pointer: %v", pyMismatchProject, err)
		}
	}()

	bundleKey := putODCSBundle(t, ctx, clients, pyMismatchProject, releaseID, bundle)
	defer func() {
		if _, err := clients.s3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(e2eS3Bucket), Key: aws.String(bundleKey),
		}); err != nil {
			t.Logf("delete s3://%s/%s: %v", e2eS3Bucket, bundleKey, err)
		}
	}()
	postPythonRelease(t, clients, pyMismatchProject, releaseID, pyMismatchImage, true)

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
// on: the content_hash `continuo-runtime hash` computes for the mismatch node
// from the released contract in repo is not the one the mismatch image
// computes from its baked contract. A failure means the two fixture contracts
// no longer differ in meaning.
func requireImageReproducesAnotherHash(t *testing.T, ctx context.Context, repo fstest.MapFS) {
	t.Helper()
	released := hashContractsWithRuntime(t, ctx, pyMismatchImage, repo, ".")[pyMismatchUniqueID]
	require.NotEmpty(t, released, "the released contract declares %s", pyMismatchUniqueID)

	cmd := exec.CommandContext(ctx, "docker", "run", "--rm", "--entrypoint", "continuo-runtime", //nolint:gosec // fixed arguments; the image is a test constant
		pyMismatchImage, "hash", "/app/contracts", "--repo-root", "/app", "--dialect", "postgres")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	require.NoError(t, cmd.Run(), "continuo-runtime hash in %s (rebuild it with scripts/setup.sh): %s", pyMismatchImage, stderr.String())
	baked := parseRuntimeHashes(stdout.String())[pyMismatchUniqueID]
	require.NotEmpty(t, baked, "%s's baked contracts declare %s: %s", pyMismatchImage, pyMismatchUniqueID, stdout.String())
	require.NotEqual(t, released, baked,
		"the mismatch image must compute another content_hash for %s than its release carries", pyMismatchUniqueID)
	t.Logf("released content_hash %s, the mismatch image's %s", released, baked)
}
