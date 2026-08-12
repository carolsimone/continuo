package ports

import (
	"context"
	"errors"

	"github.com/carolsimone/continuo/orchestrator/domain/codebundle"
)

// ErrBundleNotFound means the release's code-bundle object is absent from object
// storage — not written yet, or aged out by the bucket lifecycle rule. The
// caller retries: a bundle that is merely late becomes readable.
var ErrBundleNotFound = errors.New("code bundle not found")

// ErrBundleMalformed means the object exists but cannot be interpreted (bad
// JSON, unknown contract_version, a node missing content_hash). Re-reading it
// can never help, so the caller drops the message permanently and logs loudly.
var ErrBundleMalformed = errors.New("code bundle malformed")

// CodeBundleReader fetches a release's code-bundle contract document.
type CodeBundleReader interface {
	Fetch(ctx context.Context, uri string) (codebundle.Bundle, error)
}
