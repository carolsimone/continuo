package e2e

import (
	"archive/tar"
	"bytes"
	"context"
	"database/sql"
	"embed"
	"io/fs"
	"os/exec"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const (
	// pyTimeoutUniqueID declares timeout_minutes in its authored contract;
	// pyNoTimeoutUniqueID, in the same contract, declares none.
	pyTimeoutUniqueID   = "e2e_schema.py_timeout"
	pyNoTimeoutUniqueID = "e2e_schema.py_no_timeout"

	// pyTimeoutMinutes is the authored timeout_minutes, and pyTimeoutSeconds
	// the same value as the topology carries it.
	pyTimeoutMinutes = 90
	pyTimeoutSeconds = pyTimeoutMinutes * 60

	// pyTimeoutFixtureRoot is the fixture's directory inside pyTimeoutFixture.
	pyTimeoutFixtureRoot = "fixtures/py-timeout"

	// mergeContractScript builds the wire contract from a tar archive of a
	// domain repository (contracts/ and scripts/) read on stdin, exactly as a
	// domain repository's CI runs `continuo-runtime merge`, and writes the
	// merged contract to stdout. The CLI logs to stderr. $1 is the service name.
	// --dialect postgres matches the e2e stack's warehouse engine.
	mergeContractScript = `set -eu
work="$(mktemp -d)"
tar -x -C "$work"
continuo-runtime merge "$work/contracts" --service "$1" --repo-root "$work" --dialect postgres --out "$work/contract.yaml" >&2
cat "$work/contract.yaml"`
)

// pyTimeoutFixture is the authored contracts and scripts of the timeout
// fixture. It is not baked into the fixture image: the test streams it into the
// image's runtime CLI.
//
//go:embed fixtures/py-timeout/contracts fixtures/py-timeout/scripts
var pyTimeoutFixture embed.FS

// TestE2E_PythonNodeTimeout_ReachesStateThroughTheRuntimeCLI proves that a
// Python node's timeout_minutes travels from its authored contract to state's
// topology through the real runtime, not through a contract the test writes:
//
//	py_timeout.yml declares timeout_minutes: 90 on one of its two nodes
//	→ `continuo-runtime merge`, run inside the fixture image (built on the
//	  runtime base image the Helm chart's validation.imageTag defaults to),
//	  writes the wire contract, with timeout_minutes outside config_hash
//	→ the contract is uploaded and a kind=python release posted, as a domain
//	  repository's CI does
//	→ topology-controller publishes timeout_seconds = 5400 on that node
//	→ the release validates, promotes and state stores it: that node carries
//	  5400 and every other node NULL.
func TestE2E_PythonNodeTimeout_ReachesStateThroughTheRuntimeCLI(t *testing.T) {
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

	contract := mergeContractWithRuntime(t, ctx, pyFixtureImage, pyE2EService)
	requireWireEntryTimeout(t, contract, pyTimeoutUniqueID, pyTimeoutMinutes)
	requireWireEntryTimeout(t, contract, pyNoTimeoutUniqueID, 0)

	// Tap the stream before the release is posted: the trim loop removes
	// consumed entries, so the validation check reads what the tap recorded.
	validationTap := startStreamTap(t, ctx, clients.redisClient, streams.ValidationRequestedV1)

	releaseID, cleanup := promotePythonContractRelease(t, ctx, clients, validationTap, string(contract),
		[]string{pyTimeoutUniqueID, pyNoTimeoutUniqueID}, pyTimeoutUniqueID)
	defer cleanup()

	waitForNodeTimeoutSeconds(t, ctx, clients, releaseID, pyTimeoutUniqueID, pyTimeoutSeconds)
	requireNoOtherNodeTimeout(t, ctx, clients, releaseID, pyTimeoutUniqueID)
	requireNodeWithoutTimeout(t, ctx, clients, releaseID, pyNoTimeoutUniqueID)
	t.Logf("✅ release %s promoted; %s carries timeout_seconds=%d, every other node NULL",
		releaseID, pyTimeoutUniqueID, pyTimeoutSeconds)
}

// mergeContractWithRuntime runs the runtime CLI's `merge` inside image over the
// timeout fixture and returns the wire contract it writes. The fixture travels
// on stdin as a tar archive, so the run needs no bind mount: the docker daemon
// may live on another filesystem than this process.
func mergeContractWithRuntime(t *testing.T, ctx context.Context, image, service string) []byte {
	t.Helper()

	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	root, err := fs.Sub(pyTimeoutFixture, pyTimeoutFixtureRoot)
	require.NoError(t, err)
	require.NoError(t, fs.WalkDir(root, ".", func(name string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || name == "." {
			return walkErr
		}
		if d.IsDir() {
			return tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: name + "/", Mode: 0o755})
		}
		body, err := fs.ReadFile(root, name)
		if err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: name, Mode: 0o644, Size: int64(len(body))}); err != nil {
			return err
		}
		_, err = tw.Write(body)
		return err
	}), "archive the timeout fixture")
	require.NoError(t, tw.Close())

	cmd := exec.CommandContext(ctx, "docker", "run", "--rm", "-i", "--entrypoint", "sh", //nolint:gosec // fixed arguments; image and service are test constants
		image, "-c", mergeContractScript, "sh", service)
	cmd.Stdin = &archive
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	require.NoError(t, cmd.Run(),
		"continuo-runtime merge in %s failed (a stale local image built on an older runtime rejects timeout_minutes; rebuild it with scripts/setup.sh): %s",
		image, stderr.String())
	require.NotEmpty(t, stdout.Bytes(), "continuo-runtime merge wrote no contract: %s", stderr.String())
	return stdout.Bytes()
}

// requireWireEntryTimeout asserts the merged contract carries timeout_minutes =
// want on the node, or no timeout_minutes at all when want is 0. The key is a
// sibling of config, never inside it.
func requireWireEntryTimeout(t *testing.T, contract []byte, uniqueID string, want int) {
	t.Helper()
	var doc struct {
		Nodes []map[string]any `yaml:"nodes"`
	}
	require.NoError(t, yaml.Unmarshal(contract, &doc), "parse the merged contract")
	for _, node := range doc.Nodes {
		schema, _ := node["schema"].(string)
		table, _ := node["table"].(string)
		if schema+"."+table != uniqueID {
			continue
		}
		got, declared := node["timeout_minutes"]
		if want == 0 {
			require.False(t, declared, "%s must carry no timeout_minutes", uniqueID)
			return
		}
		require.True(t, declared, "%s must carry timeout_minutes in the merged contract", uniqueID)
		require.EqualValues(t, want, got, "timeout_minutes of %s", uniqueID)
		require.NotContains(t, node["config"], "timeout_minutes", "timeout_minutes belongs outside config")
		return
	}
	require.Failf(t, "node missing", "%s is not in the merged contract", uniqueID)
}

// requireNodeWithoutTimeout asserts that state's copy of releaseID holds nodeKey
// with no timeout, so the absence of a timeout elsewhere is not an absent node.
func requireNodeWithoutTimeout(t *testing.T, ctx context.Context, clients *testClients, releaseID, nodeKey string) {
	t.Helper()
	var timeout sql.NullInt64
	require.NoError(t, clients.stateDB.GetContext(ctx, &timeout, `
		SELECT n.timeout_seconds
		  FROM live_release l
		  JOIN topology_nodes n ON n.tenant_id = l.tenant_id AND n.release_id = l.release_id
		 WHERE l.tenant_id = $1 AND l.release_id = $2 AND n.node_key = $3`,
		events.DefaultTenantID, releaseID, nodeKey), "%s must be stored in release %s", nodeKey, releaseID)
	require.False(t, timeout.Valid, "%s declares no timeout", nodeKey)
}
