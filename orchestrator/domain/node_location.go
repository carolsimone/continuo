package domain

import "errors"

// ErrNodeNotFound is returned by node reads (GetNode, GetNodeLocation,
// GetNodeVersions, and the other code-version queries) when the requested
// unique_id is not an active :Table node.
var ErrNodeNotFound = errors.New("node not found")

// ErrUnitNotFound is returned by shared-code-unit reads when the requested
// unit_id has no recorded :CodeUnit and no recorded :CodeUnitVersion —
// i.e. the unit was never referenced by any promoted node version.
var ErrUnitNotFound = errors.New("code unit not found")

// NodeLocation is where a node's source lives: the project-relative file path
// captured from the manifest, and the service that owns the node.
type NodeLocation struct {
	FilePath    string
	ServiceName string
}
