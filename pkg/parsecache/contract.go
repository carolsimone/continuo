// Package parsecache holds the Go side of the cross-language parse-cache
// hydration contract. The hydrate-parse-cache initContainer (Python,
// dbt/s3-sidecar/parse_cache_fetcher.py) fetches the release-proven
// partial-parse artifact into a run pod and reports its outcome on the
// container's termination message; k8s-controller
// (service/handlers/check_status_handler.go) parses that message to derive
// task_execution.parse_cache, and executor-controller
// (adapters/k8s/client.go) names the initContainer it builds. These
// constants are the single Go source of truth for the three sentinel
// strings that stitch the Python fetcher and the two Go services together —
// the cross-language guard in contract_test.go binds them to the Python
// source so the sides cannot drift silently.
package parsecache

const (
	// ContainerName is the initContainer name executor-controller assigns to
	// the parse-cache hydration step, and the key k8s-controller looks up in
	// K8sPodResult.InitTerminationMessages to find its outcome.
	ContainerName = "hydrate-parse-cache"

	// Hydrated is the termination message the fetcher writes on success (the
	// cache was fetched and written) and the parse_cache state k8s-controller
	// records for it.
	Hydrated = "hydrated"

	// DegradedPrefix is the termination message prefix the fetcher writes
	// when it falls back to a full parse; the remainder of the message is a
	// human-readable reason, e.g. "degraded:fetch s3://... failed: ...".
	DegradedPrefix = "degraded:"
)
