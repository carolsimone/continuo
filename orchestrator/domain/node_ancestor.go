package domain

import (
	"errors"
	"time"
)

// ErrNodeNotFound is returned by ancestry reads when the requested unique_id is
// not an active :Table node.
var ErrNodeNotFound = errors.New("node not found")

// ErrUnitNotFound is returned by shared-code-unit reads when the requested
// unit_id has no recorded :CodeUnit and no recorded :CodeUnitVersion —
// i.e. the unit was never referenced by any promoted node version.
var ErrUnitNotFound = errors.New("code unit not found")

// NodeAncestor is one node in a GetNodeAncestry result: either the queried node
// (Depth 0) or one of its transitive upstreams. Provenance fields are zero/nil
// when unknown (the node has not changed since provenance tracking began).
type NodeAncestor struct {
	UniqueID      string
	SchemaName    string
	TableName     string
	ServiceName   string
	NodeType      string
	Depth         int
	FilePath      string
	LastCommitSHA string
	LastRepo      string
	LastChangedAt *time.Time // nil when unknown
	LastReleaseID string
}

// NodeLocation is where a node's source lives: the project-relative file path
// captured from the manifest, and the service that owns the node.
type NodeLocation struct {
	FilePath    string
	ServiceName string
}
