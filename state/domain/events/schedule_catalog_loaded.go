package events

import (
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

// ScheduleCatalogLoaded is the typed form of schedules.loaded:v1.
type ScheduleCatalogLoaded struct {
	EventID         uuid.UUID
	ScheduleNames   []string
	ServiceMetadata map[string]run.ServiceMetadata
}
