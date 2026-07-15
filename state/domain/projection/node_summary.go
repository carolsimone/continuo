// Package projection holds read-only join shapes returned by query-side
// repositories. These are not aggregates and carry no invariants.
package projection

import "time"

// NodeSummary is one row of the node catalog: a node identity plus aggregate
// stats computed over that node's most recent runs (the stats window, capped at
// 50). Returned by the ListNodes gRPC method.
//
// A -1 on SuccessRatePct / AvgDurationSec / P95DurationSec means "undefined":
// no terminal runs in the window, or no run with a measurable duration. The
// repository converts these to nulls at the BFF boundary so the UI renders "—".
type NodeSummary struct {
	ServiceName    string
	SchemaName     string
	TableName      string
	RunCount       int
	SuccessRatePct int
	AvgDurationSec int
	P95DurationSec int
	FlakyRatePct   int
	LastStatus     string
	LastRunAt      time.Time
	// Operation is the run/test/build dimension this catalog row was scoped
	// to when queried: "run" | "test" | "build".
	Operation string
}
