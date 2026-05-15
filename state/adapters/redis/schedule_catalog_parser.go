package redis

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

type schedulesLoadedPayload struct {
	EventID         string                       `json:"event_id"`
	ScheduleNames   []string                     `json:"schedule_names"`
	ServiceMetadata map[string]map[string]string `json:"service_metadata"`
}

// ParseScheduleCatalogLoaded translates a schedules.loaded:v1 Redis XMessage
// into a typed events.ScheduleCatalogLoaded. Errors are parse-permanent —
// callers wrap with events.ErrPermanent at the binding layer.
func ParseScheduleCatalogLoaded(msg goredis.XMessage) (events.ScheduleCatalogLoaded, error) {
	payloadStr, _ := msg.Values["payload"].(string)
	if payloadStr == "" {
		return events.ScheduleCatalogLoaded{}, fmt.Errorf("missing payload field")
	}
	var p schedulesLoadedPayload
	if err := json.Unmarshal([]byte(payloadStr), &p); err != nil {
		return events.ScheduleCatalogLoaded{}, fmt.Errorf("invalid JSON: %w", err)
	}
	if p.EventID == "" {
		return events.ScheduleCatalogLoaded{}, fmt.Errorf("missing event_id")
	}
	eventID, err := uuid.Parse(p.EventID)
	if err != nil {
		return events.ScheduleCatalogLoaded{}, fmt.Errorf("invalid event_id UUID: %w", err)
	}
	meta := make(map[string]model.ServiceMetadata, len(p.ServiceMetadata))
	for svc, m := range p.ServiceMetadata {
		meta[svc] = model.ServiceMetadata{
			ManifestVersion: m["manifest_version"],
			ImageTag:        m["image_tag"],
		}
	}
	return events.ScheduleCatalogLoaded{
		EventID:         eventID,
		ScheduleNames:   p.ScheduleNames,
		ServiceMetadata: meta,
	}, nil
}
