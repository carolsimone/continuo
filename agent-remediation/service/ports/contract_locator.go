package ports

import "errors"

// ErrNodeNotDeclared reports that no ODCS contract file under the searched
// tree declares a node matching the given schema and table.
var ErrNodeNotDeclared = errors.New("node not declared in any ODCS contract file")

// ErrAmbiguousDeclaration reports that more than one ODCS contract file
// declares a node with the same schema and table. The system's duplicate-node
// gate should make this state impossible upstream, so seeing it here means the
// repository has drifted from what the control plane believes — not that the
// search should just pick one.
var ErrAmbiguousDeclaration = errors.New("node declared in more than one ODCS contract file")

// Located is the ODCS contract file that declares a python node, and the
// repository locations derived from where that file lives.
type Located struct {
	// YAMLPath is the repo-relative path of the declaring file.
	YAMLPath string
	// YAMLText is the file's verbatim content, unparsed.
	YAMLText string
	// ContractDir is the repo-relative directory holding the file — the
	// directory a fix is packaged from.
	ContractDir string
	// RepoRoot is the repo-relative parent of ContractDir: the python node's
	// application root, matching the runtime image's APP_ROOT convention.
	RepoRoot string
}

// ContractLocator finds which ODCS contract file in a repository checkout
// declares a given python node. Each python node has one contract file,
// directly inside its service's contracts/ directory
// (contracts/<table>.odcs.yaml), matched by the id it declares
// ("<schema>.<table>") rather than by its name. The control plane records no
// path for that file, so the tree is searched.
type ContractLocator interface {
	// Locate searches rootDir — a repository checkout, as returned by
	// RepoArchive.Fetch — for the ODCS file declaring the node identified by
	// schema and table, and returns where that file lives.
	//
	// Zero declaring files returns ErrNodeNotDeclared and more than one
	// returns ErrAmbiguousDeclaration. Both are permanent for a given
	// checkout, so a caller records them rather than retrying. Any other
	// error is a failure of the search itself.
	Locate(rootDir, schema, table string) (Located, error)
}
