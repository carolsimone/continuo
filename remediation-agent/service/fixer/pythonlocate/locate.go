// Package pythonlocate searches a repository's working tree for the contract
// yaml file that declares a given python node, since — unlike a dbt model's
// source path, which the control plane already carries end to end — a python
// node's declaring yaml path is recorded nowhere: manifest-controller reads
// it once at candidate-build time and discards the path, and the release's
// code bundle stores only the parsed contract entry, not its file location.
package pythonlocate

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ErrNodeNotDeclared reports that no yaml file under the searched tree
// declares a node matching the given schema and table.
var ErrNodeNotDeclared = errors.New("node not declared in any contract yaml")

// ErrAmbiguousDeclaration reports that more than one yaml file declares a
// node with the same schema and table. The system's duplicate-node gate
// should make this state impossible upstream, so seeing it here means the
// repository has drifted from what the control plane believes — not that the
// search should just pick one.
var ErrAmbiguousDeclaration = errors.New("node declared in more than one contract yaml")

// maxContractYAMLBytes caps the size of a yaml file this search will parse.
// A contract file declaring one or many python nodes is always small; a file
// over this cap is skipped rather than read, so an unrelated large yaml file
// elsewhere in the repository is never loaded into memory on this path.
const maxContractYAMLBytes = 1 << 20 // 1 MiB

// Located is the contract yaml file that declares a python node, and the
// repository locations derived from where that file lives.
type Located struct {
	// YAMLPath is the repo-relative path of the declaring file.
	YAMLPath string
	// YAMLText is the file's verbatim content, unparsed.
	YAMLText string
	// ContractDir is the repo-relative directory holding the file — the
	// directory a fix is merged into.
	ContractDir string
	// RepoRoot is the repo-relative parent of ContractDir: the python node's
	// application root, matching the runtime image's APP_ROOT convention.
	RepoRoot string
}

// contractDoc is the subset of a contract yaml's shape this search needs: a
// top-level "nodes:" list, each entry matched on schema and table only. A
// filename convention is deliberately never assumed — a single file can
// declare many nodes (a dbt-schema.yml-style layout), and nothing about a
// file's name is guaranteed to relate to any one of them.
type contractDoc struct {
	Nodes []contractNode `yaml:"nodes"`
}

type contractNode struct {
	Schema string `yaml:"schema"`
	Table  string `yaml:"table"`
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
// Zero matching files returns ErrNodeNotDeclared. More than one matching file
// returns ErrAmbiguousDeclaration — including two files whose declarations
// differ only in case, which name one relation and one node identity, so
// neither may be picked over the other. A file over maxContractYAMLBytes, or
// one that fails to parse as yaml, is skipped (logged) rather than treated as
// a search error — one malformed or oversized file elsewhere in the tree must
// not prevent finding the real match.
func Locate(rootDir, schema, table string) (Located, error) {
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
			slog.Default().Warn("pythonlocate: stat failed; skipping", "path", path, "error", err)
			return nil
		}
		if info.Size() > maxContractYAMLBytes {
			slog.Default().Warn("pythonlocate: file exceeds size cap; skipping",
				"path", path, "size", info.Size(), "cap", maxContractYAMLBytes)
			return nil
		}

		raw, err := os.ReadFile(path) //nolint:gosec // G304: path is produced by filepath.WalkDir over rootDir, the caller-supplied repository checkout root — not external/user input.
		if err != nil {
			slog.Default().Warn("pythonlocate: read failed; skipping", "path", path, "error", err)
			return nil
		}

		var doc contractDoc
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			slog.Default().Warn("pythonlocate: unparseable yaml; skipping", "path", path, "error", err)
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
		return Located{}, fmt.Errorf("search %q for schema %q table %q: %w", rootDir, schema, table, err)
	}

	switch len(matches) {
	case 0:
		return Located{}, fmt.Errorf("schema %q table %q: %w", schema, table, ErrNodeNotDeclared)
	case 1:
		return buildLocated(matches[0]), nil
	default:
		paths := make([]string, len(matches))
		for i, m := range matches {
			paths[i] = m.relPath
		}
		return Located{}, fmt.Errorf("schema %q table %q found in %v: %w", schema, table, paths, ErrAmbiguousDeclaration)
	}
}

// buildLocated derives ContractDir and RepoRoot from where the matched file
// lives: ContractDir is the file's own directory, and RepoRoot is that
// directory's parent — the python node's application root.
func buildLocated(m match) Located {
	contractDir := filepath.ToSlash(filepath.Dir(m.relPath))
	repoRoot := filepath.ToSlash(filepath.Dir(contractDir))
	return Located{
		YAMLPath:    m.relPath,
		YAMLText:    m.text,
		ContractDir: contractDir,
		RepoRoot:    repoRoot,
	}
}
