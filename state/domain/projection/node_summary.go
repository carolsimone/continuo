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
	ServiceName    string    `db:"service_name"`
	SchemaName     string    `db:"schema_name"`
	TableName      string    `db:"table_name"`
	RunCount       int       `db:"run_count"`
	SuccessRatePct int       `db:"success_rate_pct"`
	AvgDurationSec int       `db:"avg_duration_sec"`
	P95DurationSec int       `db:"p95_duration_sec"`
	FlakyRatePct   int       `db:"flaky_rate_pct"`
	LastStatus     string    `db:"last_status"`
	LastRunAt      time.Time `db:"last_run_at"`
}
