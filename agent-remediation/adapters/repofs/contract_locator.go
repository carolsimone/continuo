// Package repofs reads and patches a repository checkout on local disk. It
// implements the ports whose subject is the tree an archive fetch already
// extracted, keeping the directory walk, the file reads and writes, and the
// yaml deserialization out of the application layer.
package repofs

import (
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// maxContractYAMLBytes caps the size of a yaml file this search will parse.
// A contract file is always small; a file over this cap is skipped rather than
// read, so an unrelated large yaml file elsewhere in the repository is never
// loaded into memory on this path.
const maxContractYAMLBytes = 1 << 20 // 1 MiB

// odcsSuffix ends the name of every ODCS contract file. A Python node's
// contract is one such file, and the packager reads only these.
const odcsSuffix = ".odcs.yaml"

// contractsDir is the name of the directory a service keeps its ODCS
// contracts in, directly: <service>/contracts/<table>.odcs.yaml.
const contractsDir = "contracts"

// continuoVendor is the vendor of the customProperties items continuo reads.
const continuoVendor = "continuo"

// csvReadKey is the read name a python-csv node's csvSource is recorded under
// by the release, which reads a csv node's one read as {"csv": csvSource}.
const csvReadKey = "csv"

// kindAliases maps a kind a contract may still write to the kind the release
// reads it as.
var kindAliases = map[string]string{"python-model": string(pkg_model.NodeTypePythonNode)}

// Locator searches a repository checkout for the ODCS contract file declaring
// a python node.
type Locator struct {
	logger *slog.Logger
}

var _ ports.ContractLocator = (*Locator)(nil)
var _ ports.ContractInspector = (*Locator)(nil)

// NewLocator builds a Locator that reports skipped files through logger.
func NewLocator(logger *slog.Logger) *Locator {
	return &Locator{logger: logger}
}

// match pairs a candidate file's repo-relative path with the content already
// read from disk while checking it, so a hit does not require re-reading the
// file.
type match struct {
	relPath string
	text    string
}

// Locate walks rootDir for every *.odcs.yaml file of at most
// maxContractYAMLBytes directly inside a directory named contracts, and
// returns the location of the one whose id names schema and table
// ("<schema>.<table>"). The file name plays no part in the match, and an ODCS
// file anywhere else (a test fixture copy, say) is not a contract. Symbolic
// links are never followed or read.
//
// The match folds case on both sides. A node's identity across this system is
// its lowercased "<schema>.<table>", so a caller derives the schema and table
// it searches for from an already-lowercased id. Only the comparison folds:
// the returned paths and text are exactly what is on disk.
//
// Zero matching files returns ports.ErrNodeNotDeclared. More than one matching
// file returns ports.ErrAmbiguousDeclaration, including two ids that differ
// only in case. A file over the size cap, or one that fails to parse as yaml,
// is skipped (logged) rather than treated as a search error, so one malformed
// or oversized file elsewhere in the tree does not prevent finding the real
// match.
func (l *Locator) Locate(rootDir, schema, table string) (ports.Located, error) {
	want := schema + "." + table
	var matches []match

	err := filepath.WalkDir(rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&fs.ModeSymlink != 0 || d.IsDir() {
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, odcsSuffix) || filepath.Base(filepath.Dir(path)) != contractsDir {
			return nil
		}

		raw, ok := l.readCapped(path)
		if !ok {
			return nil
		}
		root, err := parseRoot(raw)
		if err != nil {
			l.logger.Warn("contract search: unparseable yaml; skipping", "path", path, "error", err)
			return nil
		}
		if id, ok := scalar(mappingValue(root, "id")); ok && strings.EqualFold(id, want) {
			rel, relErr := filepath.Rel(rootDir, path)
			if relErr != nil {
				return fmt.Errorf("relativize %q: %w", path, relErr)
			}
			matches = append(matches, match{relPath: filepath.ToSlash(rel), text: string(raw)})
		}
		return nil
	})
	if err != nil {
		return ports.Located{}, fmt.Errorf("search %q for schema %q table %q: %w", rootDir, schema, table, err)
	}

	switch len(matches) {
	case 0:
		return ports.Located{}, fmt.Errorf("schema %q table %q: %w", schema, table, ports.ErrNodeNotDeclared)
	case 1:
		return buildLocated(matches[0]), nil
	default:
		paths := make([]string, len(matches))
		for i, m := range matches {
			paths[i] = m.relPath
		}
		return ports.Located{}, fmt.Errorf("schema %q table %q found in %v: %w", schema, table, paths, ports.ErrAmbiguousDeclaration)
	}
}

// readCapped reads path when it is at most maxContractYAMLBytes. ok is false,
// with the reason logged, for a file it skipped.
func (l *Locator) readCapped(path string) (raw []byte, ok bool) {
	info, err := os.Stat(path)
	if err != nil {
		l.logger.Warn("contract search: stat failed; skipping", "path", path, "error", err)
		return nil, false
	}
	if info.Size() > maxContractYAMLBytes {
		l.logger.Warn("contract search: file exceeds size cap; skipping",
			"path", path, "size", info.Size(), "cap", maxContractYAMLBytes)
		return nil, false
	}
	raw, err = os.ReadFile(path) //nolint:gosec // G304: path is produced by filepath.WalkDir over rootDir, the caller-supplied repository checkout root — not external/user input.
	if err != nil {
		l.logger.Warn("contract search: read failed; skipping", "path", path, "error", err)
		return nil, false
	}
	return raw, true
}

// Declarations parses yamlText as an ODCS contract and returns the one node it
// declares: the fields that identify it and the names of the reads it
// declares.
//
// The node's table is the name of the first schema object and its schema is
// the id without the ".<table>" that ends it (compared case-insensitively), or
// the whole id when the id does not end in the table. Script, kind, schedule,
// criticality and secretRef are the continuo customProperties of those names
// (a missing kind is python-node, and python-model reads as python-node), and
// the owner is team.name. The read names are the keys of the readQueries
// property in file order; a python-csv node's one read is its csvSource, named
// "csv", as the release records it.
//
// A document that parses but is not a contract (no id) yields no declaration
// and no error: a contract directory may hold other yaml files, and they
// declare nothing to compare. A field of an unexpected shape is read as absent
// rather than failing the document, because a caller reads "declares nothing"
// as "protects no node". Text that is not valid yaml returns an error, because
// a caller comparing declarations across an edit must not treat "could not be
// read" as "declares nothing".
func (l *Locator) Declarations(yamlText string) ([]ports.NodeDeclaration, error) {
	root, err := parseRoot([]byte(yamlText))
	if err != nil {
		return nil, fmt.Errorf("parse contract document: %w", err)
	}
	id, ok := scalar(mappingValue(root, "id"))
	if !ok || id == "" {
		return nil, nil
	}

	table, _ := scalar(mappingValue(firstItem(mappingValue(root, "schema")), "name"))
	owner, _ := scalar(mappingValue(mappingValue(root, "team"), "name"))
	props := continuoProperties(mappingValue(root, "customProperties"))

	kind, _ := scalar(props["kind"])
	if kind == "" {
		kind = string(pkg_model.NodeTypePythonNode)
	}
	if alias, ok := kindAliases[kind]; ok {
		kind = alias
	}
	script, _ := scalar(props["script"])
	schedule, _ := scalar(props["schedule"])
	criticality, _ := scalar(props["criticality"])
	secretRef, _ := scalar(props["secretRef"])

	readKeys := mappingKeys(props["readQueries"])
	if kind == string(pkg_model.NodeTypePythonCsv) {
		readKeys = nil
		if props["csvSource"] != nil {
			readKeys = []string{csvReadKey}
		}
	}

	return []ports.NodeDeclaration{{
		Identity: ports.NodeIdentity{
			Schema:      schemaOf(id, table),
			Table:       table,
			Script:      script,
			Kind:        kind,
			Owner:       owner,
			Schedule:    schedule,
			Criticality: criticality,
			SecretRef:   secretRef,
		},
		ReadKeys: readKeys,
	}}, nil
}

// schemaOf returns the id without the ".<table>" that ends it, comparing the
// two case-insensitively, or the whole id when it does not end in the table.
func schemaOf(id, table string) string {
	tail := "." + table
	if table != "" && len(id) > len(tail) && strings.EqualFold(id[len(id)-len(tail):], tail) {
		return id[:len(id)-len(tail)]
	}
	return id
}

// parseRoot parses raw as one yaml document and returns its top-level node:
// the mapping a contract is, or any other node, which declares nothing. An
// empty document yields nil.
func parseRoot(raw []byte) (*yaml.Node, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return nil, nil
	}
	return doc.Content[0], nil
}

// mappingValue returns the value of key in the mapping n, or nil when n is not
// a mapping or has no such key.
func mappingValue(n *yaml.Node, key string) *yaml.Node {
	if n == nil || n.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return n.Content[i+1]
		}
	}
	return nil
}

// mappingKeys returns the keys of the mapping n in file order, or none when n
// is not a mapping.
func mappingKeys(n *yaml.Node) []string {
	if n == nil || n.Kind != yaml.MappingNode {
		return nil
	}
	out := make([]string, 0, len(n.Content)/2)
	for i := 0; i+1 < len(n.Content); i += 2 {
		out = append(out, n.Content[i].Value)
	}
	return out
}

// firstItem returns the first item of the sequence n, or nil.
func firstItem(n *yaml.Node) *yaml.Node {
	if n == nil || n.Kind != yaml.SequenceNode || len(n.Content) == 0 {
		return nil
	}
	return n.Content[0]
}

// scalar returns the value of the scalar node n. ok is false for a missing
// node or one that is not a scalar.
func scalar(n *yaml.Node) (string, bool) {
	if n == nil || n.Kind != yaml.ScalarNode {
		return "", false
	}
	return n.Value, true
}

// continuoProperties returns the value node of each continuo item of a
// customProperties list, by property name. An item of another vendor is
// skipped, and so is one with no string property or no value; a name that
// appears twice keeps its last value, as the release reads it.
func continuoProperties(n *yaml.Node) map[string]*yaml.Node {
	out := map[string]*yaml.Node{}
	if n == nil || n.Kind != yaml.SequenceNode {
		return out
	}
	for _, item := range n.Content {
		vendor, _ := scalar(mappingValue(item, "vendor"))
		if vendor != continuoVendor {
			continue
		}
		name, ok := scalar(mappingValue(item, "property"))
		value := mappingValue(item, "value")
		if !ok || value == nil {
			continue
		}
		out[name] = value
	}
	return out
}

// buildLocated derives ContractDir and RepoRoot from where the matched file
// lives: ContractDir is the file's own directory, and RepoRoot is that
// directory's parent — the python node's application root.
func buildLocated(m match) ports.Located {
	contractDir := filepath.ToSlash(filepath.Dir(m.relPath))
	repoRoot := filepath.ToSlash(filepath.Dir(contractDir))
	return ports.Located{
		YAMLPath:    m.relPath,
		YAMLText:    m.text,
		ContractDir: contractDir,
		RepoRoot:    repoRoot,
	}
}
