package ports

import (
	"context"

	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
)

// NodeLocator resolves a node's source location from the orchestrator graph.
type NodeLocator interface {
	// Locate returns the node's project-relative source path and owning
	// service. An unknown node degrades to ("", "", nil) — absence is a
	// degraded answer, not an error.
	Locate(ctx context.Context, uniqueID string) (filePath, serviceName string, err error)
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

// PrecedentQuery addresses past rejections by exact signature, falling back
// to the broader (category, reason) class.
type PrecedentQuery struct {
	Signature string
	Category  string
	Reason    string
	Limit     int32
}

// PrecedentReader returns past rejections matching a PrecedentQuery.
type PrecedentReader interface {
	Precedents(ctx context.Context, q PrecedentQuery) ([]prompt.Precedent, error)
}
