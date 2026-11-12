package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// decodeJSONDoc decodes a JSON document (e.g. a full dbt manifest.json) into
// generic Go values, using json.Number for numbers so integer/float literals
// round-trip exactly instead of collapsing through float64: a test that edits
// one field of a manifest and re-encodes it leaves every other node's bytes,
// and so its content_hash, as they were.
func decodeJSONDoc(body []byte) (map[string]interface{}, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var doc map[string]interface{}
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	return doc, nil
}

// asObjectMap returns v as map[string]interface{}, or an empty map if v is
// not an object (nil, wrong type, or absent key) — matching how a missing
// dbt manifest section (e.g. no "macros" key) behaves like {}.
func asObjectMap(v interface{}) map[string]interface{} {
	if m, ok := v.(map[string]interface{}); ok {
		return m
	}
	return map[string]interface{}{}
}

// sha256Hex returns the lowercase hex sha256 digest of s (no prefix),
// matching hashlib.sha256(...).hexdigest().
func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// topologyControllerContainer is the docker-compose container whose parser
// computes the baseline hashes.
const topologyControllerContainer = "topology-controller"

// contentHashScript parses the manifest.json in the directory named by its one
// argument, with the compile-extras.json beside it when there is one, through
// topology-controller's own parser, and prints {unique_id: content_hash} of
// every node the parser yields as JSON, its only stdout line. Diagnostics go
// to stderr.
const contentHashScript = `
import json, logging, os, sys
logging.basicConfig(stream=sys.stderr, level=logging.WARNING)
from service.compile_extras import load_compile_extras
from service.parser import parse_manifest
d = sys.argv[1]
extras_path = os.path.join(d, "compile-extras.json")
extras = load_compile_extras(extras_path, "") if os.path.exists(extras_path) else None
parsed = parse_manifest(os.path.join(d, "manifest.json"), "", extras)
print(json.dumps({n.unique_id: n.content_hash for n in parsed.nodes}, sort_keys=True))
`

var (
	contentHashCacheMu sync.Mutex
	contentHashCache   = map[string]map[string]string{}
)

// contentHashesOf returns the content_hash topology-controller computes for
// every node it yields from a dbt manifest and that compile's
// compile-extras.json (nil when the compile wrote none), keyed by unique_id.
// topology-controller's parser is the one implementation of the formula, so
// the harness asks it rather than keeping a copy: both files are copied into
// the topology-controller container and a short script there runs
// parse_manifest over them. Results are kept for the life of the process,
// keyed by the SHA-256 of both inputs, since every test seeds production from
// the same baseline manifests.
func contentHashesOf(t *testing.T, ctx context.Context, manifest, extras []byte) map[string]string {
	t.Helper()
	cacheKey := sha256Hex(string(manifest)) + ":"
	if extras != nil {
		cacheKey += sha256Hex(string(extras))
	}
	contentHashCacheMu.Lock()
	defer contentHashCacheMu.Unlock()
	if hashes, ok := contentHashCache[cacheKey]; ok {
		return copyHashes(hashes)
	}

	local := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(local, "manifest.json"), manifest, 0o644)) //nolint:gosec // test fixture read by the container
	if extras != nil {
		require.NoError(t, os.WriteFile(filepath.Join(local, "compile-extras.json"), extras, 0o644)) //nolint:gosec // test fixture read by the container
	}

	out, err := exec.CommandContext(ctx, "docker", "exec", topologyControllerContainer, "mktemp", "-d").Output()
	require.NoError(t, err, "create a work directory in %s: %s", topologyControllerContainer, exitStderr(err))
	remote := strings.TrimSpace(string(out))
	defer func() {
		if out, err := exec.CommandContext(context.Background(), "docker", "exec", topologyControllerContainer, "rm", "-rf", remote).CombinedOutput(); err != nil { //nolint:gosec // remote is the directory mktemp named
			t.Logf("remove %s:%s: %v: %s", topologyControllerContainer, remote, err, out)
		}
	}()

	names := []string{"manifest.json"}
	if extras != nil {
		names = append(names, "compile-extras.json")
	}
	for _, name := range names {
		dst := topologyControllerContainer + ":" + remote + "/" + name
		out, err := exec.CommandContext(ctx, "docker", "cp", filepath.Join(local, name), dst).CombinedOutput() //nolint:gosec // fixed container, paths the test created
		require.NoError(t, err, "docker cp %s: %s", dst, out)
	}

	out, err = exec.CommandContext(ctx, "docker", "exec", "-w", "/app", "-e", "PYTHONPATH=/app/proto", //nolint:gosec // fixed script, directory mktemp named
		topologyControllerContainer, "uv", "run", "--frozen", "python", "-c", contentHashScript, remote).Output()
	require.NoError(t, err, "hash the manifest with topology-controller's parser: %s", exitStderr(err))

	var hashes map[string]string
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(out), &hashes),
		"topology-controller's hash script must print one JSON object, got %q", out)
	contentHashCache[cacheKey] = hashes
	return copyHashes(hashes)
}

// copyHashes returns a copy of hashes, so no caller can change the cached map.
func copyHashes(hashes map[string]string) map[string]string {
	out := make(map[string]string, len(hashes))
	for k, v := range hashes {
		out[k] = v
	}
	return out
}

// exitStderr returns the stderr a failed command captured, or "" when err
// carries none.
func exitStderr(err error) string {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return string(exitErr.Stderr)
	}
	return ""
}
