package ports

import (
	"context"
	"errors"
)

// ErrNotFound signals the object referenced by a URI does not exist.
var ErrNotFound = errors.New("evidence not found")

// EvidenceReader fetches a textual S3 object (candidate SQL, dbt log) by URI.
type EvidenceReader interface {
	Fetch(ctx context.Context, uri string) (string, error)
}
