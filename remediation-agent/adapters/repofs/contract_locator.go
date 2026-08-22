// Package repofs reads a repository checkout on local disk. It implements the
// ports whose subject is the tree an archive fetch already extracted, keeping
// the directory walk, the file reads, and the yaml deserialization out of the
// application layer.
package repofs

import (
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// maxContractYAMLBytes caps the size of a yaml file this search will parse.
// A contract file declaring one or many python nodes is always small; a file
// over this cap is skipped rather than read, so an unrelated large yaml file
// elsewhere in the repository is never loaded into memory on this path.
const maxContractYAMLBytes = 1 << 20 // 1 MiB

// Locator searches a repository checkout for the contract yaml declaring a
// python node.
type Locator struct {
	logger *slog.Logger
}

var _ ports.ContractLocator = (*Locator)(nil)
var _ ports.ContractInspector = (*Locator)(nil)

// NewLocator builds a Locator that reports skipped files through logger.
func NewLocator(logger *slog.Logger) *Locator {
	return &Locator{logger: logger}
}

// contractDoc is the subset of a contract yaml's shape this package needs: a
// top-level "nodes:" list. A filename convention is deliberately never assumed
// — a single file can declare many nodes (a dbt-schema.yml-style layout), and
// nothing about a file's name is guaranteed to relate to any one of them.
type contractDoc struct {
	Nodes []contractNode `yaml:"nodes"`
}

// contractNode is one entry of that list, narrowed to the fields that identify
// the node plus the reads it declares. The search matches on schema and table;
// the remaining identity fields are read so a caller can tell an entry that was
// repaired apart from one that was re-identified, and the reads so it can tell
// a repaired read apart from a deleted one.
type contractNode struct {
	Schema      string `yaml:"schema"`
	Table       string `yaml:"table"`
	Script      string `yaml:"script"`
	Kind        string `yaml:"kind"`
	Owner       string `yaml:"owner"`
	Schedule    string `yaml:"schedule"`
	Criticality string `yaml:"criticality"`
	// Reads is the entry's "reads:" mapping kept as a raw node, because only its
	// keys are needed and its values are whatever the author wrote. Decoding it
	// into a typed map would make one entry with an unexpected value shape fail
	// the whole document, which every caller here reads as "this file declares
	// nothing" — skipping it in the contract search and excusing it from every
	// comparison made across an edit.
	Reads yaml.Node `yaml:"reads"`
}

// readKeys returns the names an entry gives the relations it reads, in
// declaration order. A "reads:" that is absent, empty, or not a mapping yields
// none: there is no key in it to preserve.
func readKeys(n yaml.Node) []string {
	if n.Kind != yaml.MappingNode {
		return nil
	}
	out := make([]string, 0, len(n.Content)/2)
	for i := 0; i+1 < len(n.Content); i += 2 {
		out = append(out, n.Content[i].Value)
	}
	return out
}

// match pairs a candidate file's repo-relative path with the content already
// read from disk while checking it, so a hit does not require re-reading the
// file.
type match struct {
	relPath string
	text    string
}

// Locate walks rootDir for every *.yml/*.yaml file, parses each as a
// contractDoc, and returns the location of the file whose "nodes:" list
// contains an entry matching schema and table.
//
// The match folds case on both sides. A node's identity across this system is
// its lowercased "<schema>.<table>", so a caller derives the schema and table
// it searches for from an already-lowercased id, while the contract file keeps
// whatever case its author wrote — the declared spelling is what renders into
// SQL and DDL, so nothing normalizes it in the repository. Comparing the two
// verbatim would report a declared node as undeclared for every team that
// capitalizes a name. Only the comparison folds: the returned paths and text
// are exactly what is on disk.
//
// Zero matching files returns ports.ErrNodeNotDeclared. More than one matching
// file returns ports.ErrAmbiguousDeclaration — including two files whose
// declarations differ only in case, which name one relation and one node
// identity, so neither may be picked over the other. A file over
// maxContractYAMLBytes, or one that fails to parse as yaml, is skipped
// (logged) rather than treated as a search error — one malformed or oversized
// file elsewhere in the tree must not prevent finding the real match.
func (l *Locator) Locate(rootDir, schema, table string) (ports.Located, error) {
	var matches []match

	err := filepath.WalkDir(rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		ext := filepath.Ext(d.Name())
		if ext != ".yml" && ext != ".yaml" {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			l.logger.Warn("contract search: stat failed; skipping", "path", path, "error", err)
			return nil
		}
		if info.Size() > maxContractYAMLBytes {
			l.logger.Warn("contract search: file exceeds size cap; skipping",
				"path", path, "size", info.Size(), "cap", maxContractYAMLBytes)
			return nil
		}

		raw, err := os.ReadFile(path) //nolint:gosec // G304: path is produced by filepath.WalkDir over rootDir, the caller-supplied repository checkout root — not external/user input.
		if err != nil {
			l.logger.Warn("contract search: read failed; skipping", "path", path, "error", err)
			return nil
		}

		var doc contractDoc
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			l.logger.Warn("contract search: unparseable yaml; skipping", "path", path, "error", err)
			return nil
		}

		for _, n := range doc.Nodes {
			if strings.EqualFold(n.Schema, schema) && strings.EqualFold(n.Table, table) {
				rel, relErr := filepath.Rel(rootDir, path)
				if relErr != nil {
					return fmt.Errorf("relativize %q: %w", path, relErr)
				}
				matches = append(matches, match{
					relPath: filepath.ToSlash(rel),
					text:    string(raw),
				})
				break
			}
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

// Declarations parses yamlText as a contract document and returns every node
// under its "nodes:" list, in declaration order: the fields that identify it and
// the keys of the reads it declares.
//
// A document that parses but declares no nodes yields an empty result and no
// error: a yaml file holding something else entirely is a normal member of a
// contract directory, and it declares nothing to compare. Text that is not
// valid yaml returns an error, because a caller comparing declarations across
// an edit cannot tell "declares nothing" apart from "could not be read" and
// must not treat the second as the first.
func (l *Locator) Declarations(yamlText string) ([]ports.NodeDeclaration, error) {
	var doc contractDoc
	if err := yaml.Unmarshal([]byte(yamlText), &doc); err != nil {
		return nil, fmt.Errorf("parse contract document: %w", err)
	}
	out := make([]ports.NodeDeclaration, 0, len(doc.Nodes))
	for _, n := range doc.Nodes {
		// Absent kind means python-model — the same default every parser of this
		// wire format applies (manifest-controller's python_contract_parser.py,
		// e.g.). Every legacy contract omits "kind:", so without this default an
		// answer that writes it explicitly (a plausible, correct normalization)
		// would read as an identity change ("" -> "python-model") rather than as
		// the no-op it actually is.
		kind := n.Kind
		if kind == "" {
			kind = "python-model"
		}
		out = append(out, ports.NodeDeclaration{
			Identity: ports.NodeIdentity{
				Schema:      n.Schema,
				Table:       n.Table,
				Script:      n.Script,
				Kind:        kind,
				Owner:       n.Owner,
				Schedule:    n.Schedule,
				Criticality: n.Criticality,
			},
			ReadKeys: readKeys(n.Reads),
		})
	}
	return out, nil
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
