// Package ports holds orchestrator application-layer (technical) ports —
// collaborators that are not domain concepts. Adapters implement these.
package ports

import "context"

// RunStatusReader reads a run's authoritative lifecycle status from the state
// service. The reconciler uses it to converge the Neo4j :Run projection to
// state's decision when an event-driven finalization was missed or raced ahead
// of the run's snapshot.
type RunStatusReader interface {
	// GetTerminalStatus returns (status, true, nil) when the run is terminal in
	// state (one of "succeeded" | "failed" | "cancelled"), and ("", false, nil)
	// when the run is still in-flight or unknown to state.
	GetTerminalStatus(ctx context.Context, runID string) (status string, terminal bool, err error)
}
