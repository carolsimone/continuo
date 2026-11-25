//go:build integration

package packaging

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// minRuntime is the lowest continuo-runtime that has the pack command.
var minRuntime = [3]int{0, 13, 0}

// requirePackCLI skips the test unless the continuo-runtime on PATH is
// minRuntime or later: the binary ships inside the agent-remediation image,
// and a bare host may hold none or an older one without pack.
func requirePackCLI(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("continuo-runtime"); err != nil {
		t.Skipf("continuo-runtime not on PATH (%v): this binary ships only inside the agent-remediation image, not on a bare host or CI runner. It is exercised in CI by the dedicated in-container step that runs this package's integration test inside the running agent-remediation container.", err)
	}
	out, err := exec.Command("continuo-runtime", "--version").Output()
	if err != nil || !atLeast(strings.TrimSpace(string(out)), minRuntime) {
		t.Skipf("continuo-runtime on PATH is not 0.13.0 or later (%q, %v): this binary ships only inside the agent-remediation image, not on a bare host or CI runner. It is exercised in CI by the dedicated in-container step that runs this package's integration test inside the running agent-remediation container.", out, err)
	}
}

// atLeast reports whether "continuo-runtime X.Y.Z" names min or a later version.
func atLeast(version string, min [3]int) bool {
	fields := strings.Fields(version)
	if len(fields) != 2 || fields[0] != "continuo-runtime" {
		return false
	}
	parts := strings.Split(fields[1], ".")
	if len(parts) != 3 {
		return false
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return false
		}
		if n != min[i] {
			return n > min[i]
		}
	}
	return true
}

// writeTree writes files under root, creating their directories.
func writeTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for path, body := range files {
		full := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
}

const probeContract = `apiVersion: v3.2.0
kind: DataContract
id: e2e_schema.py_probe
name: py_probe
version: "0"
status: active
description:
  purpose: "probe node"
team:
  name: e2e
customProperties:
  - {vendor: continuo, property: schedule, value: daily}
  - {vendor: continuo, property: criticality, value: SECONDARY}
  - {vendor: continuo, property: script, value: scripts/probe.py}
  - vendor: continuo
    property: readQueries
    value:
      tables: select schemaname from pg_catalog.pg_tables
schema:
  - name: py_probe
    physicalType: table
    properties:
      - {name: id, physicalType: INTEGER, required: true}
      - {name: label, physicalType: VARCHAR(16)}
`

const probeScript = `import pyarrow as pa


def run(ctx):
    ctx.read("tables")
    return pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "label": pa.array(["a", "b", "c"], type=pa.string()),
        }
    )
`

// TestCLIPackager_RunsPackWithTheImagePackages runs the image's real
// continuo-runtime pack over one ODCS contract and its script, and checks the
// odcs-bundle.json it returns: the project, the package list it was given, the
// producer versions and the contract with its source hash. A wrong argument
// order, a wrong --out path or a CLI that never ran fails here as a missing
// field or a parse failure, which is how topology-controller would refuse it.
func TestCLIPackager_RunsPackWithTheImagePackages(t *testing.T) {
	requirePackCLI(t)
	repoRoot := t.TempDir()
	writeTree(t, repoRoot, map[string]string{
		"contracts/py_probe.odcs.yaml": probeContract,
		"scripts/probe.py":             probeScript,
	})
	p, err := NewCLIPackager()
	if err != nil {
		t.Fatalf("NewCLIPackager: %v", err)
	}
	packages := map[string]string{"pyarrow": "26.0.0", "duckdb": "1.4.0"}

	out, err := p.Pack(context.Background(), filepath.Join(repoRoot, "contracts"), repoRoot, "py-probe-test", "postgres", packages)
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}

	var bundle struct {
		BundleVersion int               `json:"bundle_version"`
		Project       string            `json:"project"`
		ImagePackages map[string]string `json:"image_packages"`
		Producer      struct {
			ContinuoPythonRuntime string `json:"continuo_python_runtime"`
			ContinuoContracts     string `json:"continuo_contracts"`
		} `json:"producer"`
		Nodes []struct {
			Path       string `json:"path"`
			Text       string `json:"text"`
			SourceHash string `json:"source_hash"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(out, &bundle); err != nil {
		t.Fatalf("the bundle is not JSON: %v\n---\n%s", err, out)
	}
	if bundle.BundleVersion != 1 {
		t.Errorf("bundle_version = %d, want 1", bundle.BundleVersion)
	}
	if bundle.Project != "py-probe-test" {
		t.Errorf("project = %q, want py-probe-test", bundle.Project)
	}
	if len(bundle.ImagePackages) != 2 || bundle.ImagePackages["pyarrow"] != "26.0.0" || bundle.ImagePackages["duckdb"] != "1.4.0" {
		t.Errorf("image_packages = %v, want the list Pack was given, %v", bundle.ImagePackages, packages)
	}
	if bundle.Producer.ContinuoPythonRuntime == "" || bundle.Producer.ContinuoContracts == "" {
		t.Errorf("producer = %+v, want both versions", bundle.Producer)
	}
	if len(bundle.Nodes) != 1 {
		t.Fatalf("nodes = %d entries, want 1", len(bundle.Nodes))
	}
	n := bundle.Nodes[0]
	if n.Path != "contracts/py_probe.odcs.yaml" {
		t.Errorf("path = %q, want contracts/py_probe.odcs.yaml", n.Path)
	}
	if n.Text != probeContract {
		t.Errorf("text is not the contract file verbatim:\n%s", n.Text)
	}
	if n.SourceHash == "" {
		t.Error("source_hash is empty, want the script's sha256")
	}
}

// TestCLIPackager_PackRefusesABrokenContract runs pack over a contract that
// names a script the repository does not hold: the tool reads it and refuses
// it, which the packager reports as ErrContractRejected with the tool's words.
func TestCLIPackager_PackRefusesABrokenContract(t *testing.T) {
	requirePackCLI(t)
	repoRoot := t.TempDir()
	writeTree(t, repoRoot, map[string]string{"contracts/py_probe.odcs.yaml": probeContract})
	p, err := NewCLIPackager()
	if err != nil {
		t.Fatalf("NewCLIPackager: %v", err)
	}

	_, err = p.Pack(context.Background(), filepath.Join(repoRoot, "contracts"), repoRoot, "py-probe-test", "postgres",
		map[string]string{"pyarrow": "26.0.0"})
	if !errors.Is(err, ports.ErrContractRejected) {
		t.Fatalf("Pack error = %v, want it to wrap ErrContractRejected", err)
	}
	if !strings.Contains(err.Error(), "probe.py") {
		t.Errorf("error = %v, want the tool's complaint naming the missing script", err)
	}
}
