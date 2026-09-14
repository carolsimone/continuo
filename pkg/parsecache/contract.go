// Package parsecache holds the Go side of the cross-language parse-cache
// hydration contract. The hydrate-parse-cache initContainer (Python,
// s3-sidecar/parse_cache_fetcher.py) fetches the release-proven
// partial-parse artifact into a run pod and reports its outcome on the
// container's termination message; execution-controller
// (service/handlers/job_status_handler.go) parses that message to derive
// task_execution.parse_cache, and also names the initContainer it builds
// (adapters/k8s/jobs_create.go). These constants are the single Go source of
// truth for the three sentinel strings that stitch the Python fetcher and
// execution-controller together — the cross-language guard in
// contract_test.go binds them to the Python source so the sides cannot drift
// silently.
package parsecache

const (
	// ContainerName is the initContainer name execution-controller assigns to
	// the parse-cache hydration step, and the key it looks up in
	// K8sPodResult.InitTerminationMessages to find its outcome.
	ContainerName = "hydrate-parse-cache"

	// Hydrated is the termination message the fetcher writes on success (the
	// cache was fetched and written) and the parse_cache state
	// execution-controller records for it.
	Hydrated = "hydrated"

	// DegradedPrefix is the termination message prefix the fetcher writes
	// when it falls back to a full parse; the remainder of the message is a
	// human-readable reason, e.g. "degraded:fetch s3://... failed: ...".
	DegradedPrefix = "degraded:"
)
