package ports

import "context"

// CandidateSource is one node's source as the release's code bundle records
// it. For a dbt node RawCode is the model file text; for a python node it is
// the normalized contract entry as canonical JSON.
type CandidateSource struct {
	RawCode string
	Runtime string
}

// CandidateSourceReader fetches one node's source from a release's code
// bundle in object storage.
type CandidateSourceReader interface {
	// NodeSource returns the node's entry from the bundle at bundleURI.
	// Empty URI, a missing object, a missing node, or an uninterpretable
	// bundle all yield ErrNotFound (the caller falls back to the repo read);
	// a transient fetch error is returned as-is so the trigger redelivers.
	NodeSource(ctx context.Context, bundleURI, uniqueID string) (CandidateSource, error)
}
