package ports

import (
	"context"

	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
)

// AncestryClient returns the failed node's own file path plus its ranked
// upstream ancestry. Best-effort: the handler proceeds degraded on error.
type AncestryClient interface {
	NodeContext(ctx context.Context, nodeID string) (filePath string, ancestors []prompt.Ancestor, err error)
}
