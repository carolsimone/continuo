package pythonlocate

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// writeFile creates path (and its parent directories) under root with the
// given content.
func writeFile(t *testing.T, root, relPath, content string) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(relPath))
	if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
		t.Fatalf("mkdir for %q: %v", relPath, err)
	}
	if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
		t.Fatalf("write %q: %v", relPath, err)
	}
}

// TestLocate_FindsSecondOfTwoFiles verifies that Locate correctly identifies
// the file declaring the target node even when it is not the first yaml file
// the walk encounters, and that the returned Located fields are derived
// correctly from where that file lives in the tree.
func TestLocate_FindsSecondOfTwoFiles(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "services/service-py/contracts/other.yml", "nodes:\n  - schema: analytics\n    table: other_kpis\n    script: scripts/other.py\n")
	writeFile(t, root, "services/service-py/contracts/py_daily_kpis.yml", "nodes:\n  - schema: analytics\n    table: py_daily_kpis\n    script: scripts/py_daily_kpis.py\n")

	got, err := Locate(root, "analytics", "py_daily_kpis")
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}
	if got.YAMLPath != "services/service-py/contracts/py_daily_kpis.yml" {
		t.Errorf("YAMLPath = %q", got.YAMLPath)
	}
	if got.ContractDir != "services/service-py/contracts" {
		t.Errorf("ContractDir = %q", got.ContractDir)
	}
	if got.RepoRoot != "services/service-py" {
		t.Errorf("RepoRoot = %q", got.RepoRoot)
	}
	wantText := "nodes:\n  - schema: analytics\n    table: py_daily_kpis\n    script: scripts/py_daily_kpis.py\n"
	if got.YAMLText != wantText {
		t.Errorf("YAMLText = %q, want %q", got.YAMLText, wantText)
	}
}

// TestLocate_MultiNodeFileTargetInMiddle verifies that a single dbt-style
// schema.yml file declaring several nodes is still matched correctly when
// the target entry is neither first nor last in the list.
func TestLocate_MultiNodeFileTargetInMiddle(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "services/service-py/contracts/schema.yml", `nodes:
  - schema: analytics
    table: first_node
    script: scripts/first.py
  - schema: analytics
    table: middle_node
    script: scripts/middle.py
  - schema: analytics
    table: last_node
    script: scripts/last.py
`)

	got, err := Locate(root, "analytics", "middle_node")
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}
	if got.YAMLPath != "services/service-py/contracts/schema.yml" {
		t.Errorf("YAMLPath = %q", got.YAMLPath)
	}
}

// TestLocate_NoMatchIsErrNodeNotDeclared verifies that a schema/table with no
// declaring entry anywhere in the tree returns the distinguishable sentinel
// error, so a caller can match it with errors.Is and persist a skip rationale
// rather than treating it as a transient failure.
func TestLocate_NoMatchIsErrNodeNotDeclared(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "services/service-py/contracts/py_daily_kpis.yml", "nodes:\n  - schema: analytics\n    table: py_daily_kpis\n")

	_, err := Locate(root, "analytics", "does_not_exist")
	if !errors.Is(err, ErrNodeNotDeclared) {
		t.Fatalf("err = %v, want ErrNodeNotDeclared", err)
	}
}

// TestLocate_DuplicateDeclarationIsErrAmbiguous verifies that the same
// (schema, table) declared in two different files — a state the system's
// duplicate-node gate should make impossible — is reported as the distinct
// ambiguous-declaration sentinel rather than either file being picked
// silently.
func TestLocate_DuplicateDeclarationIsErrAmbiguous(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "services/service-py/contracts/a.yml", "nodes:\n  - schema: analytics\n    table: py_daily_kpis\n")
	writeFile(t, root, "services/other/contracts/b.yml", "nodes:\n  - schema: analytics\n    table: py_daily_kpis\n")

	_, err := Locate(root, "analytics", "py_daily_kpis")
	if !errors.Is(err, ErrAmbiguousDeclaration) {
		t.Fatalf("err = %v, want ErrAmbiguousDeclaration", err)
	}
}

// TestLocate_SkipsOversizeFile verifies that a yaml file over the 1 MiB cap
// is skipped by the search rather than parsed, even when it would otherwise
// match — matching only files within the size cap.
func TestLocate_SkipsOversizeFile(t *testing.T) {
	root := t.TempDir()
	padding := make([]byte, maxContractYAMLBytes+1)
	for i := range padding {
		padding[i] = '#'
	}
	oversized := "nodes:\n  - schema: analytics\n    table: py_daily_kpis\n# " + string(padding) + "\n"
	writeFile(t, root, "services/service-py/contracts/py_daily_kpis.yml", oversized)

	_, err := Locate(root, "analytics", "py_daily_kpis")
	if !errors.Is(err, ErrNodeNotDeclared) {
		t.Fatalf("err = %v, want ErrNodeNotDeclared (oversize file should be skipped, not matched)", err)
	}
}

// TestLocate_SkipsUnparseableYAML verifies that a file that fails to parse as
// yaml is skipped (logged, not surfaced as an error), so one malformed file
// elsewhere in the tree cannot break the search for a real match.
func TestLocate_SkipsUnparseableYAML(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "services/broken/contracts/broken.yml", "nodes: [this is not: valid: yaml")
	writeFile(t, root, "services/service-py/contracts/py_daily_kpis.yml", "nodes:\n  - schema: analytics\n    table: py_daily_kpis\n")

	got, err := Locate(root, "analytics", "py_daily_kpis")
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}
	if got.YAMLPath != "services/service-py/contracts/py_daily_kpis.yml" {
		t.Errorf("YAMLPath = %q", got.YAMLPath)
	}
}

// TestLocate_IgnoresNonYAMLFiles verifies that files without a .yml/.yaml
// extension are never parsed as candidates, even if their content happens to
// look like a contract yaml.
func TestLocate_IgnoresNonYAMLFiles(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "services/service-py/scripts/py_daily_kpis.py", "nodes:\n  - schema: analytics\n    table: py_daily_kpis\n")

	_, err := Locate(root, "analytics", "py_daily_kpis")
	if !errors.Is(err, ErrNodeNotDeclared) {
		t.Fatalf("err = %v, want ErrNodeNotDeclared", err)
	}
}
