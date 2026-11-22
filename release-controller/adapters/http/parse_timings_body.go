package http

import "github.com/carolsimone/continuo/release-controller/domain/release"

// addParseTimings names in body the timings a run's parse reported: how long
// the parse took and how long its contract stages took, and, for a parse that
// ended ok, how many contracts the bundle holds by family. A key is present only
// once the parse reported it, so a run the parse has not reached, or one parsed
// by a topology-controller that reports no timings, has none rather than zero.
// A release and a verification run name them alike.
func addParseTimings(body map[string]any, t release.ParseTimings) {
	if t.ParseMs != nil {
		body["parse_ms"] = *t.ParseMs
	}
	if t.ContractsMs != nil {
		body["contracts_ms"] = *t.ContractsMs
	}
	if t.ContractsByFamily != nil {
		body["contracts_by_family"] = t.ContractsByFamily
	}
}
