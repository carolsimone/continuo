package domain

import (
	"errors"
	"time"
)

// ErrNodeNotFound is returned by ancestry reads when the requested unique_id is
// not an active :Table node.
var ErrNodeNotFound = errors.New("node not found")

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
