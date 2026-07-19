package parsecache

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
)

// TestContractMatchesPythonFetcher guards the cross-language parse-cache
// hydration contract. The hydrate-parse-cache initContainer (Python,
// dbt/s3-sidecar/parse_cache_fetcher.py) writes its outcome to the
// container's termination message; k8s-controller parses that message using
// the constants in this package, and executor-controller names the
// initContainer it builds with ContainerName. Nothing generates one side
// from the other, so a drift would silently turn every execution's
// parse_cache into unknown/absent with no error surfaced. This test reads
// the Python source of truth and asserts the Go constants still match it,
// failing CI the moment they diverge.
//
// It lives in pkg (not k8s-controller or executor-controller) because the
// per-service test containers hold only their own module — the dbt/ tree is
// present only in the full-repo checkout, where the pkg static-guard suite
// runs.
func TestContractMatchesPythonFetcher(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// <root>/pkg/parsecache/<this> → up two to the repo root.
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	pyPath := filepath.Join(repoRoot, "dbt", "s3-sidecar", "parse_cache_fetcher.py")
	src, err := os.ReadFile(pyPath) //nolint:gosec // G304: pyPath is built from runtime.Caller(0) plus fixed literal segments, not external input
	if err != nil {
		t.Fatalf("read python contract %s: %v", pyPath, err)
	}

	containerRe := regexp.MustCompile("Runs as the `([^`]+)` initContainer")
	if m := containerRe.FindSubmatch(src); m == nil {
		t.Errorf("container name docstring not found in %s", pyPath)
	} else if py := string(m[1]); py != ContainerName {
		t.Errorf("container name drift: python docstring %q != go %q", py, ContainerName)
	}

	hydratedRe := regexp.MustCompile(`_terminate\("([^"]+)"\)`)
	if m := hydratedRe.FindSubmatch(src); m == nil {
		t.Errorf("success termination literal not found in %s", pyPath)
	} else if py := string(m[1]); py != Hydrated {
		t.Errorf("hydrated marker drift: python %q != go %q", py, Hydrated)
	}

	degradedRe := regexp.MustCompile(`_terminate\(f"(degraded:)\{reason\}"\)`)
	if m := degradedRe.FindSubmatch(src); m == nil {
		t.Errorf("degraded prefix literal not found in %s", pyPath)
	} else if py := string(m[1]); py != DegradedPrefix {
		t.Errorf("degraded prefix drift: python %q != go %q", py, DegradedPrefix)
	}
}
