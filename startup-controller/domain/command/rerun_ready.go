package command

import "github.com/google/uuid"

// RerunReady carries the target nodes from the orchestrator after it has
// reset FAILED nodes and prepared them for re-execution.
type RerunReady struct {
	RunID       uuid.UUID
	TargetNodes []NodePayload
}

func (RerunReady) isCommand() {}
