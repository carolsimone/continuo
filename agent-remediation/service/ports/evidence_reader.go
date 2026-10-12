package ports

import (
	"context"
	"errors"
)

// ErrNotFound signals an object that cannot be read for good: its URI names
// no object, the object does not exist, or it is larger than a reader holds.
// Callers test it with errors.Is and proceed without the object.
var ErrNotFound = errors.New("evidence not found")

// EvidenceReader fetches a textual S3 object (candidate SQL, dbt log) by URI.
type EvidenceReader interface {
	Fetch(ctx context.Context, uri string) (string, error)
}
