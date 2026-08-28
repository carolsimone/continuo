// executor-controller/adapters/redis/compile_requested_parser.go
package redis

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// compileRequestedDTO mirrors the flat JSON body release-controller emits in
// the "payload" field of a compile.requested:v1 message.
type compileRequestedDTO struct {
	ReleaseID        string `json:"release_id"`
	Service          string `json:"service"`
	ImageTag         string `json:"image_tag"`
	Bucket           string `json:"bucket"`
	CandidateSchema  string `json:"candidate_schema"`
	SourceOverlayURI string `json:"source_overlay_uri"`
}

// ParseCompileRequested translates a compile.requested:v1 XMessage into a
// typed domain event. All required fields (release_id, service, image_tag,
// bucket) must be present; any absence is a permanent parse error.
//
// All errors are permanent (malformed input never becomes valid on retry); the
// binding wraps them with events.ErrPermanent so the consumer ACKs rather than
// redelivering forever.
func ParseCompileRequested(msg goredis.XMessage) (events.CompileRequested, error) {
	raw := stringField(msg.Values, "payload")
	if raw == "" {
		return events.CompileRequested{}, fmt.Errorf("missing payload field")
	}

	var dto compileRequestedDTO
	if err := json.Unmarshal([]byte(raw), &dto); err != nil {
		return events.CompileRequested{}, fmt.Errorf("unmarshal payload: %w", err)
	}

	switch {
	case dto.ReleaseID == "":
		return events.CompileRequested{}, fmt.Errorf("missing release_id")
	case dto.Service == "":
		return events.CompileRequested{}, fmt.Errorf("missing service")
	case dto.ImageTag == "":
		return events.CompileRequested{}, fmt.Errorf("missing image_tag")
	case dto.Bucket == "":
		return events.CompileRequested{}, fmt.Errorf("missing bucket")
	}

	var outboxEntryID uuid.UUID
	if s := stringField(msg.Values, "outbox_entry_id"); s != "" {
		var err error
		outboxEntryID, err = uuid.Parse(s)
		if err != nil {
			return events.CompileRequested{}, fmt.Errorf("invalid outbox_entry_id: %w", err)
		}
	}

	return events.CompileRequested{
		OutboxEntryID:    outboxEntryID,
		ReleaseID:        dto.ReleaseID,
		Service:          dto.Service,
		ImageTag:         dto.ImageTag,
		Bucket:           dto.Bucket,
		CandidateSchema:  dto.CandidateSchema,
		SourceOverlayURI: dto.SourceOverlayURI,
	}, nil
}
