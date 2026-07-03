package validationresult

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
)

// TestSentinelMarkersMatchPythonContract guards the cross-language structured-
// result contract. The validation pod (Python, validation-runner/validation_result.py)
// prints the result block framed by sentinel markers, and the Go side splits the
// pod log on the constants in this package. Nothing generates one side from the
// other, so a drift would silently empty run_results_uri and degrade remediation
// to the text-log path with no error surfaced. This test reads the Python source
// of truth and asserts the Go constants still match it, failing CI the moment
// they diverge.
//
// It lives in pkg (not k8s-controller) because the per-service test containers
// hold only their own module — the validation-runner tree is present only in the
// full-repo checkout, where the pkg static-guard suite runs.
func TestSentinelMarkersMatchPythonContract(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// <root>/pkg/validationresult/<this> → up two to the repo root.
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	pyPath := filepath.Join(repoRoot, "validation-runner", "validation_result.py")
	src, err := os.ReadFile(pyPath) //nolint:gosec // G304: pyPath is built from runtime.Caller(0) plus fixed literal segments, not external input
	if err != nil {
		t.Fatalf("read python contract %s: %v", pyPath, err)
	}

	pyConst := func(name string) string {
		re := regexp.MustCompile(`(?m)^` + name + `\s*=\s*"([^"]*)"`)
		m := re.FindSubmatch(src)
		if m == nil {
			t.Fatalf("%s assignment not found in %s", name, pyPath)
		}
		return string(m[1])
	}

	if py := pyConst("SENTINEL_BEGIN"); py != SentinelBegin {
		t.Errorf("sentinel BEGIN drift: python %q != go %q", py, SentinelBegin)
	}
	if py := pyConst("SENTINEL_END"); py != SentinelEnd {
		t.Errorf("sentinel END drift: python %q != go %q", py, SentinelEnd)
	}
}
