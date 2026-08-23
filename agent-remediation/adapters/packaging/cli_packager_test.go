//go:build integration

package packaging

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestMerge_ProducesHashFoldedWireContract runs the real continuo-runtime CLI
// (present in the built image, not on a bare host) against a minimal
// contract dir shaped like tests/e2e/fixtures/py-probe/contracts/*.yml, and
// asserts the merged output is a parseable wire contract whose every node
// entry carries the hash fold topology-controller recomputes and checks. A
// wrong subprocess argument order, a wrong --out path, or a CLI that never
// ran at all would each surface here as a missing field or a parse failure —
// the same rejection topology-controller would give the shadow artifact.
func TestMerge_ProducesHashFoldedWireContract(t *testing.T) {
	if _, err := exec.LookPath("continuo-runtime"); err != nil {
		t.Skipf("continuo-runtime not on PATH (%v): this binary ships only inside the agent-remediation image, not on a bare host or CI runner. It is exercised in CI by the dedicated in-container step that runs this package's integration test inside the running agent-remediation container.", err)
	}

	repoRoot := t.TempDir()
	contractDir := filepath.Join(repoRoot, "contracts")
	scriptsDir := filepath.Join(repoRoot, "scripts")
	if err := os.MkdirAll(contractDir, 0o755); err != nil {
		t.Fatalf("mkdir contracts: %v", err)
	}
	if err := os.MkdirAll(scriptsDir, 0o755); err != nil {
		t.Fatalf("mkdir scripts: %v", err)
	}

	script := `import pyarrow as pa


def run(ctx):
    ctx.read("tables")
    return pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "label": pa.array(["a", "b", "c"], type=pa.string()),
        }
    )
`
	if err := os.WriteFile(filepath.Join(scriptsDir, "probe.py"), []byte(script), 0o644); err != nil {
		t.Fatalf("write probe.py: %v", err)
	}

	contract := `nodes:
  - schema: e2e_schema
    table: py_probe
    owner: e2e
    schedule: daily
    criticality: SECONDARY
    script: scripts/probe.py
    reads:
      tables: select schemaname from pg_catalog.pg_tables
    output_columns:
      - {name: id, type: INTEGER, nullable: false}
      - {name: label, type: VARCHAR(16)}
`
	if err := os.WriteFile(filepath.Join(contractDir, "probe.yml"), []byte(contract), 0o644); err != nil {
		t.Fatalf("write probe.yml: %v", err)
	}

	p, err := NewCLIPackager()
	if err != nil {
		t.Fatalf("NewCLIPackager: %v", err)
	}
	out, err := p.Merge(context.Background(), contractDir, repoRoot, "py-probe-test", "postgres")
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}

	var doc struct {
		ContractVersion int    `yaml:"contract_version"`
		Service         string `yaml:"service"`
		Nodes           []struct {
			Schema         string `yaml:"schema"`
			Table          string `yaml:"table"`
			SourceHash     string `yaml:"source_hash"`
			SharedCodeHash string `yaml:"shared_code_hash"`
			ConfigHash     string `yaml:"config_hash"`
			ContentHash    string `yaml:"content_hash"`
		} `yaml:"nodes"`
	}
	if err := yaml.Unmarshal(out, &doc); err != nil {
		t.Fatalf("merged output does not parse as yaml: %v\n---\n%s", err, out)
	}

	if doc.ContractVersion != 1 {
		t.Errorf("contract_version = %d, want 1", doc.ContractVersion)
	}
	if doc.Service != "py-probe-test" {
		t.Errorf("service = %q, want %q", doc.Service, "py-probe-test")
	}
	if len(doc.Nodes) != 1 {
		t.Fatalf("nodes = %d entries, want 1", len(doc.Nodes))
	}

	n := doc.Nodes[0]
	if n.Schema != "e2e_schema" || n.Table != "py_probe" {
		t.Errorf("node relation = %s.%s, want e2e_schema.py_probe", n.Schema, n.Table)
	}
	if n.SourceHash == "" {
		t.Error("source_hash is empty, want a sha256 hex digest")
	}
	if n.ConfigHash == "" {
		t.Error("config_hash is empty, want a sha256 hex digest")
	}
	if n.ContentHash == "" {
		t.Error("content_hash is empty, want a sha256: fold")
	}
	// shared_code_hash is legitimately "" when the script has no in-repo
	// import closure (probe.py imports only the third-party pyarrow), so it
	// is deliberately not asserted non-empty here.
	_ = n.SharedCodeHash
}

// TestMerge_AcceptsAPythonCsvContract runs the real continuo-runtime CLI
// against a script-less python-csv contract shaped like
// tests/e2e/fixtures/py-probe/contracts/py_csv.yml. A csv fix proposes no
// script at all — only a "kind: python-csv" entry and a "reads: {csv: ...}"
// uri — so this pins that the image's pinned CLI actually understands "kind"
// and accepts an empty script for it. continuo-python-runtime==0.2.0 (the
// pin this test caught stale) predates "kind" entirely and rejects this exact
// shape with "unknown key(s) ['kind']"; regressing the image's pin back below
// the csv-capable release would fail this test the same way, instead of only
// failing silently once a real csv fix reaches this packager in production.
func TestMerge_AcceptsAPythonCsvContract(t *testing.T) {
	if _, err := exec.LookPath("continuo-runtime"); err != nil {
		t.Skipf("continuo-runtime not on PATH (%v): this binary ships only inside the agent-remediation image, not on a bare host or CI runner. It is exercised in CI by the dedicated in-container step that runs this package's integration test inside the running agent-remediation container.", err)
	}

	repoRoot := t.TempDir()
	contractDir := filepath.Join(repoRoot, "contracts")
	if err := os.MkdirAll(contractDir, 0o755); err != nil {
		t.Fatalf("mkdir contracts: %v", err)
	}

	contract := `nodes:
  - schema: e2e_schema
    table: py_csv
    owner: e2e
    schedule: daily
    criticality: SECONDARY
    kind: python-csv
    description: "csv-sourced python node for the e2e run path"
    reads:
      csv: s3://continuo/fixtures/orders.csv
    output_columns:
      - {name: order_id, type: INTEGER, nullable: false}
      - {name: amount, type: DOUBLE PRECISION}
`
	if err := os.WriteFile(filepath.Join(contractDir, "py_csv.yml"), []byte(contract), 0o644); err != nil {
		t.Fatalf("write py_csv.yml: %v", err)
	}

	p, err := NewCLIPackager()
	if err != nil {
		t.Fatalf("NewCLIPackager: %v", err)
	}
	out, err := p.Merge(context.Background(), contractDir, repoRoot, "py-csv-test", "postgres")
	if err != nil {
		t.Fatalf("Merge rejected a python-csv contract (the CLI on PATH predates \"kind\" support): %v", err)
	}

	var doc struct {
		Nodes []struct {
			Schema string            `yaml:"schema"`
			Table  string            `yaml:"table"`
			Kind   string            `yaml:"kind"`
			Script string            `yaml:"script"`
			Reads  map[string]string `yaml:"reads"`
		} `yaml:"nodes"`
	}
	if err := yaml.Unmarshal(out, &doc); err != nil {
		t.Fatalf("merged output does not parse as yaml: %v\n---\n%s", err, out)
	}
	if len(doc.Nodes) != 1 {
		t.Fatalf("nodes = %d entries, want 1", len(doc.Nodes))
	}

	n := doc.Nodes[0]
	if n.Schema != "e2e_schema" || n.Table != "py_csv" {
		t.Errorf("node relation = %s.%s, want e2e_schema.py_csv", n.Schema, n.Table)
	}
	if n.Kind != "python-csv" {
		t.Errorf("kind = %q, want %q", n.Kind, "python-csv")
	}
	if n.Script != "" {
		t.Errorf("script = %q, want empty: a python-csv node has no script", n.Script)
	}
	if n.Reads["csv"] != "s3://continuo/fixtures/orders.csv" {
		t.Errorf("reads[csv] = %q, want the declared uri preserved", n.Reads["csv"])
	}
}
