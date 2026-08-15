package ports

import "context"

// RuntimeDbt is the CandidateSource.Runtime value for a dbt node — the only
// runtime whose RawCode is model source text a fix can be proposed against.
// Every other value (including an absent one) marks RawCode as something else,
// and the caller must not send it to the LLM.
const RuntimeDbt = "dbt"

// CandidateSource is one node's source as the release's code bundle records
// it. For a dbt node (Runtime == RuntimeDbt) RawCode is the model file text;
// for a python node it is the normalized contract entry as canonical JSON.
type CandidateSource struct {
	RawCode string
	Runtime string
}

// CandidateSourceReader fetches one node's source from a release's code
// bundle in object storage.
type CandidateSourceReader interface {
	// NodeSource returns the node's entry from the bundle at bundleURI, after
	// confirming the bundle belongs to releaseID. Empty URI, a missing object,
	// a missing node, an uninterpretable bundle, or a bundle for a different
	// release (a stale or misrouted object naming the same node id) all yield
	// ErrNotFound (the caller falls back to the repo read); a transient fetch
	// error is returned as-is so the trigger redelivers.
	NodeSource(ctx context.Context, bundleURI, uniqueID, releaseID string) (CandidateSource, error)
}
