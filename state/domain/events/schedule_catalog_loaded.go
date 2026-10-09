package events

import (
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

// ScheduleCatalogLoaded is the typed form of schedules.loaded:v1.
// PromotionSeq is the promotion seq of the release the payload describes; 0
// when the payload carries none.
type ScheduleCatalogLoaded struct {
	EventID         uuid.UUID
	ScheduleNames   []string
	ServiceMetadata map[string]run.ServiceMetadata
	PromotionSeq    int64
}
