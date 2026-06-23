package ports

import (
	"context"

	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
)

// AncestryClient returns the failed node's ranked upstream ancestry metadata.
// Best-effort: the handler proceeds without it on error.
type AncestryClient interface {
	Ancestors(ctx context.Context, nodeID string) ([]prompt.Ancestor, error)
}
