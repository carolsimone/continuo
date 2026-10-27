package events

import (
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

// AttemptReported is the typed form of one task.status.updated:v2 report: the
// tenant its envelope names, the run it belongs to and what
// execution-controller reported of the attempt. The run is that tenant's.
type AttemptReported struct {
	TenantID string
	RunID    uuid.UUID
	Report   run.AttemptReport
}
