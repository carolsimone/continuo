// executor-controller/adapters/redis/seed_build_requested_parser.go
package redis

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// seedBuildRequestedDTO mirrors the flat JSON body release-controller emits in
// the "payload" field of a seed.build.requested:v1 message.
type seedBuildRequestedDTO struct {
	ReleaseID       string               `json:"release_id"`
	Mode            string               `json:"mode"`
	Seeds           []seedBuildNodeDTO   `json:"seeds"`
	SeedIDsInOrder  []string             `json:"seed_ids_in_order"`
	ImageTags       map[string]string    `json:"image_tags"`
	CandidateSchema string               `json:"candidate_schema"`
}

type seedBuildNodeDTO struct {
	UniqueID    string `json:"unique_id"`
	ServiceName string `json:"service_name"`
	NodeType    string `json:"node_type"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
	ImageTag    string `json:"image_tag"`
}

// ParseSeedBuildRequested translates a seed.build.requested:v1 XMessage into a
// typed domain event. Seeds have no upstream_node_ids or candidate_artifact_uri;
// each seed's node_type must be "dbt-seed".
//
// All errors are permanent (malformed input never becomes valid on retry); the
// binding wraps them with events.ErrPermanent so the consumer ACKs rather than
// redelivering forever.
func ParseSeedBuildRequested(msg goredis.XMessage) (events.SeedBuildRequested, error) {
	raw := stringField(msg.Values, "payload")
	if raw == "" {
		return events.SeedBuildRequested{}, fmt.Errorf("missing payload field")
	}

	var dto seedBuildRequestedDTO
	if err := json.Unmarshal([]byte(raw), &dto); err != nil {
		return events.SeedBuildRequested{}, fmt.Errorf("unmarshal payload: %w", err)
	}

	if dto.ReleaseID == "" {
		return events.SeedBuildRequested{}, fmt.Errorf("missing release_id")
	}
	if dto.Mode != "seed_build" {
		return events.SeedBuildRequested{},
			fmt.Errorf("unexpected mode %q (want %q)", dto.Mode, "seed_build")
	}
	if len(dto.Seeds) == 0 {
		return events.SeedBuildRequested{}, fmt.Errorf("seeds is empty: nothing to build")
	}

	seeds := make([]events.SeedBuildNode, 0, len(dto.Seeds))
	seedIDSet := make(map[string]struct{}, len(dto.Seeds))
	for i, s := range dto.Seeds {
		switch {
		case s.UniqueID == "":
			return events.SeedBuildRequested{}, fmt.Errorf("seed[%d] missing unique_id", i)
		case s.ServiceName == "":
			return events.SeedBuildRequested{}, fmt.Errorf("seed[%d] (%s) missing service_name", i, s.UniqueID)
		case s.SchemaName == "":
			return events.SeedBuildRequested{}, fmt.Errorf("seed[%d] (%s) missing schema_name", i, s.UniqueID)
		case s.TableName == "":
			return events.SeedBuildRequested{}, fmt.Errorf("seed[%d] (%s) missing table_name", i, s.UniqueID)
		case s.ImageTag == "":
			return events.SeedBuildRequested{}, fmt.Errorf("seed[%d] (%s) missing image_tag", i, s.UniqueID)
		}
		nodeType, err := pkg_model.ParseNodeType(s.NodeType)
		if err != nil {
			return events.SeedBuildRequested{},
				fmt.Errorf("seed[%d] (%s) invalid node_type: %w", i, s.UniqueID, err)
		}
		if nodeType != pkg_model.NodeTypeDbtSeed {
			return events.SeedBuildRequested{},
				fmt.Errorf("seed[%d] (%s) node_type %q must be %q in seed build event",
					i, s.UniqueID, s.NodeType, string(pkg_model.NodeTypeDbtSeed))
		}
		seeds = append(seeds, events.SeedBuildNode{
			NodeID:      s.UniqueID,
			ServiceName: s.ServiceName,
			SchemaName:  s.SchemaName,
			TableName:   s.TableName,
			NodeType:    nodeType,
			ImageTag:    s.ImageTag,
		})
		seedIDSet[s.UniqueID] = struct{}{}
	}

	if err := validateNodeIDsMatch(seedIDSet, dto.SeedIDsInOrder); err != nil {
		return events.SeedBuildRequested{}, err
	}

	var outboxEntryID uuid.UUID
	if s := stringField(msg.Values, "outbox_entry_id"); s != "" {
		var err error
		outboxEntryID, err = uuid.Parse(s)
		if err != nil {
			return events.SeedBuildRequested{}, fmt.Errorf("invalid outbox_entry_id: %w", err)
		}
	}

	return events.SeedBuildRequested{
		OutboxEntryID:   outboxEntryID,
		ReleaseID:       dto.ReleaseID,
		Mode:            dto.Mode,
		Seeds:           seeds,
		SeedIDsInOrder:  dto.SeedIDsInOrder,
		ImageTags:       dto.ImageTags,
		CandidateSchema: dto.CandidateSchema,
	}, nil
}
