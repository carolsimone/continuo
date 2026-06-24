package ports

import (
	"context"

	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
)

// AncestryClient returns the failed node's own file path, its service name,
// and its ranked upstream ancestry. Best-effort: the handler proceeds degraded
// on error.
type AncestryClient interface {
	NodeContext(ctx context.Context, nodeID string) (filePath string, serviceName string, ancestors []prompt.Ancestor, err error)
}
