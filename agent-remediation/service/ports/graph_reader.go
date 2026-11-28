package ports

import (
	"context"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
)

// NodeLocator resolves a node's source location from the catalog graph.
type NodeLocator interface {
	// Locate returns the node's project-relative source path and owning
	// project. An unknown node degrades to ("", "", nil) — absence is a
	// degraded answer, not an error.
	Locate(ctx context.Context, uniqueID string) (filePath, projectName string, err error)
}

// UpstreamChangeReader returns a node's recently-changed upstream ancestry.
type UpstreamChangeReader interface {
	// UpstreamChanges returns the node's most-recently-changed ancestors with
	// their latest code and config diffs, most-recent first. Server-capped
	// (5 ancestors, 8 KiB per diff). An unknown node degrades to (nil, nil).
	UpstreamChanges(ctx context.Context, uniqueID string) ([]prompt.UpstreamChange, error)
}

// CurrentVersion is the code a node runs now, used as the last-known-good
// diff baseline.
type CurrentVersion struct {
	RawCode     string
	ContentHash string
	PromotedAt  string // RFC3339
}

// VersionReader returns a node's recorded code-version history.
type VersionReader interface {
	// CurrentVersion returns the node's running version. ok=false when the
	// node is unknown or has no recorded current version.
	CurrentVersion(ctx context.Context, uniqueID string) (v CurrentVersion, ok bool, err error)
}

// CurrentContractReader reads the contract a node has now in the live
// topology.
type CurrentContractReader interface {
	// CurrentContract returns the node's current contract as its published
	// ODCS YAML. A node with no contract, or one the catalog does not know,
	// degrades to ("", nil).
	CurrentContract(ctx context.Context, uniqueID string) (string, error)
}

// PrecedentQuery addresses failure precedents by exact signature, falling back
// to the broader (category, reason) class.
type PrecedentQuery struct {
	Signature string
	Category  string
	Reason    string
	Limit     int32
}

// PrecedentReader returns the failure precedents matching a PrecedentQuery.
type PrecedentReader interface {
	Precedents(ctx context.Context, q PrecedentQuery) ([]prompt.Precedent, error)
}
