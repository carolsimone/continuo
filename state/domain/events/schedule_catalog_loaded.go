package events

import (
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
)

// ScheduleCatalogLoaded is the typed form of schedules.loaded:v1.
type ScheduleCatalogLoaded struct {
	EventID         uuid.UUID
	ScheduleNames   []string
	ServiceMetadata map[string]model.ServiceMetadata
}
