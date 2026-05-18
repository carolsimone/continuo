package catalog

import (
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
)

// Entry is one row of schedule_catalog. RemovedAt is nil for active entries.
type Entry struct {
	ScheduleName    string
	RemovedAt       *time.Time
	ServiceMetadata map[string]run.ServiceMetadata
}

// IsActive returns true when RemovedAt is nil.
func (e Entry) IsActive() bool { return e.RemovedAt == nil }
