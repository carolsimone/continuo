package handlers

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
)

// TestSentinelMarkersMatchPythonContract guards the cross-language structured-
// result contract. The validation pod (Python, dbt/base/validation_result.py)
// prints the result block framed by sentinel markers, and this package splits
// the pod log on byte-identical Go literals (validationResultBegin/End). Nothing
// generates one side from the other, so a drift on either side would silently
// empty run_results_uri and degrade remediation to the text-log path with no
// error surfaced. This test reads the Python source of truth and asserts the Go
// constants still match it, failing CI the moment they diverge.
func TestSentinelMarkersMatchPythonContract(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// <root>/k8s-controller/service/handlers/<this> → up three to the repo root.
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..", "..")
	pyPath := filepath.Join(repoRoot, "dbt", "base", "validation_result.py")
	src, err := os.ReadFile(pyPath)
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

	if py := pyConst("SENTINEL_BEGIN"); py != validationResultBegin {
		t.Errorf("sentinel BEGIN drift: python %q != go %q", py, validationResultBegin)
	}
	if py := pyConst("SENTINEL_END"); py != validationResultEnd {
		t.Errorf("sentinel END drift: python %q != go %q", py, validationResultEnd)
	}
}
