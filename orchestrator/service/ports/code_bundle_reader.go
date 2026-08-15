package ports

import (
	"context"
	"errors"

	"github.com/carolsimone/continuo/pkg/codebundle"
)

// ErrBundleNotFound means the release's code-bundle object is absent from object
// storage — not written yet, or aged out by the bucket lifecycle rule. The
// caller retries: a bundle that is merely late becomes readable.
var ErrBundleNotFound = errors.New("code bundle not found")

// ErrBundleMalformed means the object exists but cannot be interpreted (bad
// JSON, unknown contract_version, a node missing content_hash, or a document
// that does not belong to the release that referenced it). Re-reading it can
// never help, so the caller drops the message permanently and logs loudly.
var ErrBundleMalformed = errors.New("code bundle malformed")

// ErrBundleTooLarge means the object exceeds the size this build will hold in
// memory. Like a malformed bundle it is permanent — the object will not shrink
// on redelivery — but it is reported separately so an operator can tell a
// producer defect from a genuine capacity limit.
var ErrBundleTooLarge = errors.New("code bundle too large")

// CodeBundleReader fetches a release's code-bundle contract document.
type CodeBundleReader interface {
	Fetch(ctx context.Context, uri string) (codebundle.Bundle, error)
}
