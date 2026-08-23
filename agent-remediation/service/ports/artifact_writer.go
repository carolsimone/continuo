package ports

import "context"

// ArtifactWriter writes a proposal artifact (SQL or diff) to object storage and
// returns its URI.
type ArtifactWriter interface {
	Write(ctx context.Context, key, body, contentType string) (uri string, err error)
}
