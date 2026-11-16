package e2e

import (
	"archive/tar"
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"io/fs"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/stretchr/testify/require"
)

const (
	// pyTimeoutUniqueID declares timeoutMinutes in its contract;
	// pyNoTimeoutUniqueID, in the contract beside it, declares none.
	pyTimeoutUniqueID   = "e2e_schema.py_timeout"
	pyNoTimeoutUniqueID = "e2e_schema.py_no_timeout"

	// pyTimeoutContract and pyNoTimeoutContract are the two contract files,
	// by the path the bundle names them.
	pyTimeoutContract   = "contracts/py_timeout.odcs.yaml"
	pyNoTimeoutContract = "contracts/py_no_timeout.odcs.yaml"

	// pyTimeoutMinutes is the authored timeoutMinutes, and pyTimeoutSeconds
	// the same value as the topology carries it.
	pyTimeoutMinutes = 90
	pyTimeoutSeconds = pyTimeoutMinutes * 60

	// pyTimeoutFixtureRoot is the fixture's directory inside pyTimeoutFixture.
	pyTimeoutFixtureRoot = "fixtures/py-timeout"

	// packBundleScript builds a service's odcs-bundle.json from a tar archive
	// of a domain repository (contracts/ and scripts/) read on stdin, as a
	// domain repository's CI runs `continuo-runtime pack`, and writes the
	// bundle to stdout. The package list it records is the one the image's
	// own `continuo-runtime packages` prints, given to pack with
	// --image-packages: the image the command runs in is the image the
	// release names. The CLI logs to stderr. $1 is the service name.
	// --dialect postgres matches the e2e stack's warehouse engine.
	packBundleScript = `set -eu
work="$(mktemp -d)"
tar -x -C "$work"
continuo-runtime packages > "$work/image-packages.json"
continuo-runtime pack "$work/contracts" --service "$1" --repo-root "$work" --image-packages "$work/image-packages.json" --dialect postgres --out "$work/odcs-bundle.json" >&2
cat "$work/odcs-bundle.json"`

	// hashContractsScript prints the relation and content_hash of every node
	// of a domain repository read on stdin as a tar archive, one node per
	// line, as `continuo-runtime hash` computes them.
	hashContractsScript = `set -eu
work="$(mktemp -d)"
tar -x -C "$work"
continuo-runtime hash "$work/contracts" --repo-root "$work" --dialect postgres`
)

// pyTimeoutFixture is the ODCS contracts and scripts of the timeout fixture.
// It is not baked into the fixture image: the test streams it into the
// image's runtime CLI.
//
//go:embed fixtures/py-timeout/contracts fixtures/py-timeout/scripts
var pyTimeoutFixture embed.FS

// TestE2E_PythonNodeTimeout_ReachesRunControllerThroughTheRuntimeCLI proves that a
// Python node's timeoutMinutes travels from its ODCS contract to run-controller's
// topology through the real runtime, not through a bundle the test writes:
//
//	py_timeout.odcs.yaml declares timeoutMinutes: 90, py_no_timeout.odcs.yaml
//	  none
//	→ `continuo-runtime pack`, run inside the fixture image (built on the
//	  runtime base image the Helm chart's validation.imageTag defaults to),
//	  checks both contracts and writes the bundle
//	→ the bundle is uploaded and a python-odcs release posted, as a domain
//	  repository's CI does
//	→ topology-controller publishes timeout_seconds = 5400 on that node
//	→ the release validates, promotes and run-controller stores it: that node carries
//	  5400 and every other node NULL but service-1's ftable_deadline, which
//	  declares its own 60 seconds.
func TestE2E_PythonNodeTimeout_ReachesRunControllerThroughTheRuntimeCLI(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	requireReleaseControllerHealthy(t, clients)

	bundle := packBundleWithRuntime(t, ctx, pyFixtureImage, pyE2EService, pyTimeoutFixture, pyTimeoutFixtureRoot)
	requireBundledTimeout(t, bundle, pyTimeoutContract, pyTimeoutMinutes)
	requireBundledTimeout(t, bundle, pyNoTimeoutContract, 0)

	// Tap the stream before the release is posted: the trim loop removes
	// consumed entries, so the validation check reads what the tap recorded.
	validationTap := startStreamTap(t, ctx, clients.redisClient, streams.ValidationRequestedV1)

	releaseID, cleanup := promotePythonContractRelease(t, ctx, clients, validationTap, bundle,
		[]string{pyTimeoutUniqueID, pyNoTimeoutUniqueID}, pyTimeoutUniqueID)
	defer cleanup()

	waitForNodeTimeoutSeconds(t, ctx, clients, releaseID, pyTimeoutUniqueID, pyTimeoutSeconds)
	requireNoOtherNodeTimeout(t, ctx, clients, releaseID, pyTimeoutUniqueID)
	requireNodeWithoutTimeout(t, ctx, clients, releaseID, pyNoTimeoutUniqueID)
	t.Logf("✅ release %s promoted; %s carries timeout_seconds=%d, every other node NULL but the deadline fixture",
		releaseID, pyTimeoutUniqueID, pyTimeoutSeconds)
}

// packBundleWithRuntime runs the runtime CLI's `pack` inside image over a
// domain repository fixture, the tree under root in fixture (its contracts/
// and scripts/), and returns the odcs-bundle.json it writes, recording the
// package list of image.
func packBundleWithRuntime(t *testing.T, ctx context.Context, image, service string, fixture fs.FS, root string) []byte {
	t.Helper()
	bundle := runRuntimeOverFixture(t, ctx, image, fixture, root, packBundleScript, service)
	require.NotEmpty(t, bundle, "continuo-runtime pack in %s wrote no bundle", image)
	return bundle
}

// hashContractsWithRuntime runs the runtime CLI's `hash` inside image over the
// tree under root in fixture and returns each node's content_hash by relation.
func hashContractsWithRuntime(t *testing.T, ctx context.Context, image string, fixture fs.FS, root string) map[string]string {
	t.Helper()
	return parseRuntimeHashes(string(runRuntimeOverFixture(t, ctx, image, fixture, root, hashContractsScript)))
}

// parseRuntimeHashes reads the output of `continuo-runtime hash`, one
// "<relation> <content_hash>" line per node.
func parseRuntimeHashes(out string) map[string]string {
	hashes := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		if fields := strings.Fields(line); len(fields) == 2 {
			hashes[fields[0]] = fields[1]
		}
	}
	return hashes
}

// runRuntimeOverFixture runs script with sh inside image, the tree under root
// in fixture streamed to it on stdin as a tar archive, and returns what it
// prints to stdout. args are the script's positional parameters. The fixture
// travels on stdin, so the run needs no bind mount: the docker daemon may live
// on another filesystem than this process.
func runRuntimeOverFixture(t *testing.T, ctx context.Context, image string, fixture fs.FS, root, script string, args ...string) []byte {
	t.Helper()

	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	tree, err := fs.Sub(fixture, root)
	require.NoError(t, err)
	require.NoError(t, fs.WalkDir(tree, ".", func(name string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || name == "." {
			return walkErr
		}
		if d.IsDir() {
			return tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: name + "/", Mode: 0o755})
		}
		body, err := fs.ReadFile(tree, name)
		if err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: name, Mode: 0o644, Size: int64(len(body))}); err != nil {
			return err
		}
		_, err = tw.Write(body)
		return err
	}), "archive the fixture under %s", root)
	require.NoError(t, tw.Close())

	argv := append([]string{"run", "--rm", "-i", "--entrypoint", "sh", image, "-c", script, "sh"}, args...)
	cmd := exec.CommandContext(ctx, "docker", argv...) //nolint:gosec // fixed arguments; image, script and args are test constants
	cmd.Stdin = &archive
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	require.NoError(t, cmd.Run(),
		"continuo-runtime in %s failed (an image built on a runtime older than 0.13.0 reads no .odcs.yaml file; rebuild it with scripts/setup.sh): %s",
		image, stderr.String())
	return stdout.Bytes()
}

// requireBundledTimeout asserts the bundle's contract at contractPath carries
// the continuo property timeoutMinutes = want, or none at all when want is 0.
func requireBundledTimeout(t *testing.T, bundle []byte, contractPath string, want int) {
	t.Helper()
	var doc odcsBundle
	require.NoError(t, json.Unmarshal(bundle, &doc), "parse the bundle")
	for _, node := range doc.Nodes {
		if node.Path != contractPath {
			continue
		}
		got, declared := continuoProperties(t, contractPath, node.Text)["timeoutMinutes"]
		if want == 0 {
			require.False(t, declared, "%s must declare no timeoutMinutes", contractPath)
			return
		}
		require.True(t, declared, "%s must declare timeoutMinutes in the bundle", contractPath)
		require.EqualValues(t, want, got, "timeoutMinutes of %s", contractPath)
		return
	}
	require.Failf(t, "contract missing", "%s is not in the bundle", contractPath)
}

// requireNodeWithoutTimeout asserts that run-controller's copy of releaseID holds nodeKey
// with no timeout, so the absence of a timeout elsewhere is not an absent node.
func requireNodeWithoutTimeout(t *testing.T, ctx context.Context, clients *testClients, releaseID, nodeKey string) {
	t.Helper()
	var timeout sql.NullInt64
	require.NoError(t, clients.runControllerDB.GetContext(ctx, &timeout, `
		SELECT n.timeout_seconds
		  FROM live_release l
		  JOIN topology_nodes n ON n.tenant_id = l.tenant_id AND n.release_id = l.release_id
		 WHERE l.tenant_id = $1 AND l.release_id = $2 AND n.node_key = $3`,
		events.DefaultTenantID, releaseID, nodeKey), "%s must be stored in release %s", nodeKey, releaseID)
	require.False(t, timeout.Valid, "%s declares no timeout", nodeKey)
}
