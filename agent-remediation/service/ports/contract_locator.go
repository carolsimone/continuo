package ports

import "errors"

// ErrNodeNotDeclared reports that no contract yaml under the searched tree
// declares a node matching the given schema and table.
var ErrNodeNotDeclared = errors.New("node not declared in any contract yaml")

// ErrAmbiguousDeclaration reports that more than one contract yaml declares a
// node with the same schema and table. The system's duplicate-node gate should
// make this state impossible upstream, so seeing it here means the repository
// has drifted from what the control plane believes — not that the search
// should just pick one.
var ErrAmbiguousDeclaration = errors.New("node declared in more than one contract yaml")

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

// ContractLocator finds which contract yaml in a repository checkout declares
// a given python node. Unlike a dbt model's source path, which the control
// plane carries end to end, a python node's declaring yaml path is recorded
// nowhere: topology-controller reads it once at candidate-build time and
// discards it, and the release's code bundle stores only the parsed contract
// entry, not its file location. So the tree is searched instead.
type ContractLocator interface {
	// Locate searches rootDir — a repository checkout, as returned by
	// RepoArchive.Fetch — for the file declaring the node identified by schema
	// and table, and returns where that file lives.
	//
	// Zero declaring files returns ErrNodeNotDeclared and more than one
	// returns ErrAmbiguousDeclaration; both are permanent for a given
	// checkout, so a caller records them rather than retrying. Any other error
	// is a failure of the search itself.
	Locate(rootDir, schema, table string) (Located, error)
}
