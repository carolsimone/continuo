package repofs

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// newLocator builds the Locator under test with a discarding logger: the
// skipped-file warnings it emits are not what these tests assert.
func newLocator() *Locator {
	return NewLocator(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

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

// odcsContract renders a minimal ODCS contract for the node id, whose table
// is the part after its last dot.
func odcsContract(id string) string {
	table := id[strings.LastIndex(id, ".")+1:]
	return "apiVersion: v3.2.0\nkind: DataContract\nid: " + id + "\nname: " + table + "\nversion: \"0\"\n" +
		"team:\n  name: service-py\n" +
		"customProperties:\n" +
		"  - {vendor: continuo, property: schedule, value: daily}\n" +
		"  - {vendor: continuo, property: criticality, value: SECONDARY}\n" +
		"  - {vendor: continuo, property: script, value: scripts/" + table + ".py}\n" +
		"schema:\n  - name: " + table + "\n    physicalType: table\n    properties:\n" +
		"      - {name: day, physicalType: DATE, required: true}\n"
}

// TestLocate_FindsTheODCSFileByID verifies that Locate returns the ODCS file
// whose id names the node, even when it is not the first contract the walk
// reads, and derives the contract directory and the application root from
// where that file lives.
func TestLocate_FindsTheODCSFileByID(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "services/service-py/contracts/other_kpis.odcs.yaml", odcsContract("analytics.other_kpis"))
	writeFile(t, root, "services/service-py/contracts/py_daily_kpis.odcs.yaml", odcsContract("analytics.py_daily_kpis"))

	got, err := newLocator().Locate(root, "analytics", "py_daily_kpis")
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}
	want := ports.Located{
		YAMLPath:    "services/service-py/contracts/py_daily_kpis.odcs.yaml",
		YAMLText:    odcsContract("analytics.py_daily_kpis"),
		ContractDir: "services/service-py/contracts",
		RepoRoot:    "services/service-py",
	}
	if got != want {
		t.Errorf("Locate = %+v, want %+v", got, want)
	}
}

// TestLocate_MatchesTheIDNotTheFileName verifies that the file name plays no
// part in the search: a contract is found by the id it declares.
func TestLocate_MatchesTheIDNotTheFileName(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "svc/contracts/misnamed.odcs.yaml", odcsContract("analytics.py_daily_kpis"))
	writeFile(t, root, "svc/contracts/py_daily_kpis.odcs.yaml", odcsContract("analytics.something_else"))

	got, err := newLocator().Locate(root, "analytics", "py_daily_kpis")
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}
	if got.YAMLPath != "svc/contracts/misnamed.odcs.yaml" {
		t.Errorf("YAMLPath = %q", got.YAMLPath)
	}
}

// TestLocate_NoMatchIsErrNodeNotDeclared verifies that a node no ODCS file
// declares returns the distinguishable sentinel error, so a caller can match
// it with errors.Is and record why it stopped.
func TestLocate_NoMatchIsErrNodeNotDeclared(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "svc/contracts/py_daily_kpis.odcs.yaml", odcsContract("analytics.py_daily_kpis"))

	_, err := newLocator().Locate(root, "analytics", "does_not_exist")
	if !errors.Is(err, ports.ErrNodeNotDeclared) {
		t.Fatalf("err = %v, want ports.ErrNodeNotDeclared", err)
	}
}

// TestLocate_TwoFilesWithOneIDAreAmbiguous verifies that one id declared by
// two ODCS files is reported as ambiguous rather than either file being
// picked.
func TestLocate_TwoFilesWithOneIDAreAmbiguous(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "services/service-py/contracts/py_daily_kpis.odcs.yaml", odcsContract("analytics.py_daily_kpis"))
	writeFile(t, root, "services/other/contracts/copy.odcs.yaml", odcsContract("analytics.py_daily_kpis"))

	_, err := newLocator().Locate(root, "analytics", "py_daily_kpis")
	if !errors.Is(err, ports.ErrAmbiguousDeclaration) {
		t.Fatalf("err = %v, want ports.ErrAmbiguousDeclaration", err)
	}
}

// TestLocate_ATreeWithNoODCSFileDeclaresNoNode verifies that a tree holding no
// ODCS file, only a yaml file with a "nodes:" list naming the node, is a tree
// no contract declares the node in.
func TestLocate_ATreeWithNoODCSFileDeclaresNoNode(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "services/service-py/contracts/contract.yaml",
		"nodes:\n  - schema: analytics\n    table: py_daily_kpis\n")

	_, err := newLocator().Locate(root, "analytics", "py_daily_kpis")
	if !errors.Is(err, ports.ErrNodeNotDeclared) {
		t.Fatalf("err = %v, want ports.ErrNodeNotDeclared", err)
	}
}

// TestLocate_OnlyContractsDirectoriesHoldContracts verifies that an ODCS file
// counts only directly inside a directory named contracts: a copy under a
// test fixture tree, or nested below contracts/, is never matched.
func TestLocate_OnlyContractsDirectoriesHoldContracts(t *testing.T) {
	for name, rel := range map[string]string{
		"a test fixture copy":    "svc/tests/fixtures/py_daily_kpis.odcs.yaml",
		"nested below contracts": "svc/contracts/old/py_daily_kpis.odcs.yaml",
		"a project root":         "svc/py_daily_kpis.odcs.yaml",
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			writeFile(t, root, "svc/contracts/py_daily_kpis.odcs.yaml", odcsContract("analytics.py_daily_kpis"))
			writeFile(t, root, rel, odcsContract("analytics.py_daily_kpis"))

			got, err := newLocator().Locate(root, "analytics", "py_daily_kpis")
			if err != nil {
				t.Fatalf("Locate: %v (the copy outside contracts/ must not make the node ambiguous)", err)
			}
			if got.YAMLPath != "svc/contracts/py_daily_kpis.odcs.yaml" {
				t.Errorf("YAMLPath = %q", got.YAMLPath)
			}

			alone := t.TempDir()
			writeFile(t, alone, rel, odcsContract("analytics.py_daily_kpis"))
			if _, err := newLocator().Locate(alone, "analytics", "py_daily_kpis"); !errors.Is(err, ports.ErrNodeNotDeclared) {
				t.Fatalf("err = %v, want ports.ErrNodeNotDeclared for a contract outside contracts/", err)
			}
		})
	}
}

// TestLocate_SkipsSymlinks verifies that the walk never follows or reads a
// symbolic link, whether it points at a contract file or at a directory of
// them, so a checkout cannot make the search read outside itself.
func TestLocate_SkipsSymlinks(t *testing.T) {
	outside := t.TempDir()
	writeFile(t, outside, "contracts/py_daily_kpis.odcs.yaml", odcsContract("analytics.py_daily_kpis"))

	for name, link := range map[string]struct{ target, at string }{
		"a linked file":      {filepath.Join(outside, "contracts", "py_daily_kpis.odcs.yaml"), "svc/contracts/py_daily_kpis.odcs.yaml"},
		"a linked directory": {filepath.Join(outside, "contracts"), "svc/contracts"},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			full := filepath.Join(root, filepath.FromSlash(link.at))
			if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(link.target, full); err != nil {
				t.Fatal(err)
			}

			_, err := newLocator().Locate(root, "analytics", "py_daily_kpis")
			if !errors.Is(err, ports.ErrNodeNotDeclared) {
				t.Fatalf("err = %v, want ports.ErrNodeNotDeclared (a symlink must not be followed)", err)
			}
		})
	}
}

// TestLocate_SkipsOversizeFile verifies that an ODCS file over the 1 MiB cap
// is skipped by the search rather than parsed, even when it would otherwise
// match.
func TestLocate_SkipsOversizeFile(t *testing.T) {
	root := t.TempDir()
	padding := strings.Repeat("#", maxContractYAMLBytes+1)
	writeFile(t, root, "svc/contracts/py_daily_kpis.odcs.yaml", odcsContract("analytics.py_daily_kpis")+"# "+padding+"\n")

	_, err := newLocator().Locate(root, "analytics", "py_daily_kpis")
	if !errors.Is(err, ports.ErrNodeNotDeclared) {
		t.Fatalf("err = %v, want ports.ErrNodeNotDeclared (oversize file should be skipped, not matched)", err)
	}
}

// TestLocate_SkipsUnparseableYAML verifies that an ODCS file that fails to
// parse is skipped (logged, not surfaced as an error), so one malformed file
// elsewhere in the tree cannot break the search for a real match.
func TestLocate_SkipsUnparseableYAML(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "services/broken/contracts/broken.odcs.yaml", "id: [this is not: valid: yaml")
	writeFile(t, root, "services/service-py/contracts/py_daily_kpis.odcs.yaml", odcsContract("analytics.py_daily_kpis"))

	got, err := newLocator().Locate(root, "analytics", "py_daily_kpis")
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}
	if got.YAMLPath != "services/service-py/contracts/py_daily_kpis.odcs.yaml" {
		t.Errorf("YAMLPath = %q", got.YAMLPath)
	}
}

// TestLocate_ReadsOnlyODCSFiles verifies that a file whose name does not end
// in .odcs.yaml is never a candidate, even when its content is a contract
// naming the node.
func TestLocate_ReadsOnlyODCSFiles(t *testing.T) {
	for name, rel := range map[string]string{
		"plain yaml": "svc/contracts/py_daily_kpis.yaml",
		"yml":        "svc/contracts/py_daily_kpis.odcs.yml",
		"script":     "svc/scripts/py_daily_kpis.py",
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			writeFile(t, root, "svc/contracts/other_kpis.odcs.yaml", odcsContract("analytics.other_kpis"))
			writeFile(t, root, rel, odcsContract("analytics.py_daily_kpis"))

			_, err := newLocator().Locate(root, "analytics", "py_daily_kpis")
			if !errors.Is(err, ports.ErrNodeNotDeclared) {
				t.Fatalf("err = %v, want ports.ErrNodeNotDeclared", err)
			}
		})
	}
}

// TestLocate_MatchesTheIDCaseInsensitively verifies that a contract whose id
// is written with capitals is still found for the lowercased schema and table
// every caller derives from a node id. Only the comparison folds: the returned
// text is exactly what is on disk.
func TestLocate_MatchesTheIDCaseInsensitively(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "svc/contracts/orders.odcs.yaml", odcsContract("Analytics.Orders"))

	got, err := newLocator().Locate(root, "analytics", "orders")
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}
	if !strings.Contains(got.YAMLText, "id: Analytics.Orders") {
		t.Errorf("YAMLText must be the file verbatim, with the author's case intact: %q", got.YAMLText)
	}
}

// TestLocate_CaseVariantsAreOneAmbiguity verifies that two files whose ids
// differ only in case are reported as ambiguous rather than one of them being
// picked: they name the same node.
func TestLocate_CaseVariantsAreOneAmbiguity(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "contracts/a.odcs.yaml", odcsContract("Analytics.Orders"))
	writeFile(t, root, "contracts/b.odcs.yaml", odcsContract("analytics.orders"))

	_, err := newLocator().Locate(root, "analytics", "orders")
	if !errors.Is(err, ports.ErrAmbiguousDeclaration) {
		t.Fatalf("Locate error = %v, want ports.ErrAmbiguousDeclaration", err)
	}
}

// TestDeclarations_ReadsTheIdentityAndTheReadKeysInOrder verifies that
// Declarations returns the one node an ODCS document declares: its schema
// (the id without the table), its table (the schema object's name), the
// continuo properties that identify it, its team as the owner, and the keys of
// readQueries in the order the file writes them. The caller compares these
// across an edit, so a field this adapter dropped would be a field an answer
// could change unseen.
func TestDeclarations_ReadsTheIdentityAndTheReadKeysInOrder(t *testing.T) {
	const doc = `apiVersion: v3.2.0
kind: DataContract
id: analytics.py_daily_kpis
name: py_daily_kpis
version: "0"
team:
  name: data-platform
customProperties:
  - {vendor: continuo, property: schedule, value: daily}
  - {vendor: continuo, property: criticality, value: SECONDARY}
  - {vendor: continuo, property: script, value: scripts/py_daily_kpis.py}
  - {vendor: continuo, property: secretRef, value: continuo-api-fx}
  - {vendor: other, property: script, value: not/this.py}
  - vendor: continuo
    property: readQueries
    value:
      orders: select id from analytics.orders
      customers: select id from analytics.customers
      accounts: select id from analytics.accounts
schema:
  - name: py_daily_kpis
    physicalType: table
    properties:
      - {name: revenue, physicalType: INTEGER}
`
	got, err := newLocator().Declarations(doc)
	if err != nil {
		t.Fatalf("Declarations: %v", err)
	}
	want := ports.NodeDeclaration{
		Identity: ports.NodeIdentity{ //nolint:gosec // G101: continuo-api-* is a Secret name, not a value
			Schema: "analytics", Table: "py_daily_kpis", Script: "scripts/py_daily_kpis.py",
			Kind: "python-node", Owner: "data-platform", Schedule: "daily", Criticality: "SECONDARY",
			SecretRef: "continuo-api-fx",
		},
		ReadKeys: []string{"orders", "customers", "accounts"},
	}
	if len(got) != 1 {
		t.Fatalf("Declarations returned %d entries, want 1: %+v", len(got), got)
	}
	if got[0].Identity != want.Identity {
		t.Errorf("identity = %+v, want %+v", got[0].Identity, want.Identity)
	}
	if !slices.Equal(got[0].ReadKeys, want.ReadKeys) {
		t.Errorf("read keys = %v, want %v", got[0].ReadKeys, want.ReadKeys)
	}
}

// TestDeclarations_TheKindIsReadAsTheReleaseReadsIt verifies that a contract
// with no kind property is a python-node and that the python-model alias reads
// as python-node, so an answer that writes the kind the release already used
// is not taken for a change of kind.
func TestDeclarations_TheKindIsReadAsTheReleaseReadsIt(t *testing.T) {
	for written, want := range map[string]string{
		"":             "python-node",
		"python-model": "python-node",
		"python-api":   "python-api",
	} {
		t.Run(want+"/"+written, func(t *testing.T) {
			doc := odcsContract("analytics.py_daily_kpis")
			if written != "" {
				doc = strings.Replace(doc, "customProperties:\n",
					"customProperties:\n  - {vendor: continuo, property: kind, value: "+written+"}\n", 1)
			}
			got, err := newLocator().Declarations(doc)
			if err != nil {
				t.Fatalf("Declarations: %v", err)
			}
			if len(got) != 1 || got[0].Identity.Kind != want {
				t.Fatalf("Declarations = %+v, want one node of kind %s", got, want)
			}
		})
	}
}

// TestDeclarations_ACsvNodesReadIsItsCsvSource verifies that a python-csv
// contract's one read is its csvSource, under the key "csv", as the release
// records a csv node's reads.
func TestDeclarations_ACsvNodesReadIsItsCsvSource(t *testing.T) {
	const doc = `apiVersion: v3.2.0
kind: DataContract
id: analytics.demo_orders_csv
team:
  name: service-py
customProperties:
  - {vendor: continuo, property: kind, value: python-csv}
  - {vendor: continuo, property: csvSource, value: "s3://bucket/orders.csv"}
schema:
  - name: demo_orders_csv
    physicalType: table
`
	got, err := newLocator().Declarations(doc)
	if err != nil {
		t.Fatalf("Declarations: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Declarations returned %d entries, want 1: %+v", len(got), got)
	}
	if got[0].Identity.Kind != "python-csv" || got[0].Identity.Table != "demo_orders_csv" {
		t.Errorf("identity = %+v", got[0].Identity)
	}
	if !slices.Equal(got[0].ReadKeys, []string{"csv"}) {
		t.Errorf("read keys = %v, want [csv]", got[0].ReadKeys)
	}
}

// TestDeclarations_AnIDThatDoesNotEndInTheTableKeepsTheWholeID verifies that
// an id that does not name the schema object's table is kept whole as the
// schema, so an answer that renames the table without the id (or the id
// without the table) reads as a different node.
func TestDeclarations_AnIDThatDoesNotEndInTheTableKeepsTheWholeID(t *testing.T) {
	doc := strings.Replace(odcsContract("analytics.py_daily_kpis"), "  - name: py_daily_kpis\n", "  - name: renamed\n", 1)
	got, err := newLocator().Declarations(doc)
	if err != nil {
		t.Fatalf("Declarations: %v", err)
	}
	if len(got) != 1 || got[0].Identity.Schema != "analytics.py_daily_kpis" || got[0].Identity.Table != "renamed" {
		t.Fatalf("Declarations = %+v", got)
	}
}

// TestDeclarations_UnexpectedShapesStillDeclareTheNode verifies that a
// readQueries or customProperties holding something other than what ODCS
// expects costs the fields it carries and nothing else. Failing the whole
// document instead would read as "this file declares nothing", which excuses
// the node from the comparison made across an edit.
func TestDeclarations_UnexpectedShapesStillDeclareTheNode(t *testing.T) {
	for name, doc := range map[string]string{
		"readQueries a list": strings.Replace(odcsContract("analytics.py_daily_kpis"), "customProperties:\n",
			"customProperties:\n  - {vendor: continuo, property: readQueries, value: [select 1]}\n", 1),
		"customProperties a mapping": "id: analytics.py_daily_kpis\ncustomProperties: {script: x}\nschema:\n  - name: py_daily_kpis\n",
		"team a string":              "id: analytics.py_daily_kpis\nteam: data\nschema:\n  - name: py_daily_kpis\n",
	} {
		t.Run(name, func(t *testing.T) {
			got, err := newLocator().Declarations(doc)
			if err != nil {
				t.Fatalf("Declarations: %v", err)
			}
			if len(got) != 1 || got[0].Identity.Table != "py_daily_kpis" {
				t.Fatalf("Declarations = %+v, want the node it declares", got)
			}
			if len(got[0].ReadKeys) != 0 {
				t.Errorf("read keys = %v, want none", got[0].ReadKeys)
			}
		})
	}
}

// TestDeclarations_DocumentDeclaringNoNodeIsNotAnError verifies that a yaml
// file that is not an ODCS contract yields no declarations and no error: a
// contract directory may hold such files, and they declare nothing to
// preserve across an edit.
func TestDeclarations_DocumentDeclaringNoNodeIsNotAnError(t *testing.T) {
	for name, doc := range map[string]string{
		"empty":            "",
		"unrelated object": "version: 2\nmodels:\n  - name: orders\n",
		"a nodes list":     "nodes:\n  - schema: analytics\n    table: py_daily_kpis\n",
		"a list":           "- a\n- b\n",
	} {
		t.Run(name, func(t *testing.T) {
			got, err := newLocator().Declarations(doc)
			if err != nil {
				t.Fatalf("Declarations: %v", err)
			}
			if len(got) != 0 {
				t.Errorf("Declarations returned %+v, want no entries", got)
			}
		})
	}
}

// TestDeclarations_UnparseableTextIsAnError verifies that text which is not
// yaml at all is reported rather than read as "declares nothing", so a broken
// answer cannot erase a declaration and be recorded as having changed nothing.
func TestDeclarations_UnparseableTextIsAnError(t *testing.T) {
	if _, err := newLocator().Declarations("id: analytics.x\n\t- name: x\n"); err == nil {
		t.Fatal("Declarations accepted text that is not valid yaml; an unreadable document must not read as an empty one")
	}
}
