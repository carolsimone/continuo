// Package ports declares the technical collaborators the application layer
// depends on; adapters implement these.
package ports

import (
	"context"
	"errors"
)

// ErrLogNotFound signals a log that cannot be read for good: its URI names no
// object, the object does not exist, or it is larger than a reader holds.
// Distinct from a transient read failure: callers treat it as "log
// unavailable" (classify unknown) rather than retrying. It is returned bare;
// callers compare it by identity.
var ErrLogNotFound = errors.New("log not found")

// LogReader fetches the textual contents of a dbt log given its storage URI.
type LogReader interface {
	Fetch(ctx context.Context, uri string) (string, error)
}
