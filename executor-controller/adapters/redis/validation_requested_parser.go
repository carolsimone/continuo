// executor-controller/adapters/redis/validation_requested_parser.go
package redis

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// validationRequestedDTO mirrors the flat JSON body release-controller emits
// in the "payload" field of a validation.requested:v1 message. Per-node
// identity arrives under the "unique_id" key.
type validationRequestedDTO struct {
	ReleaseID       string                    `json:"release_id"`
	Mode            string                    `json:"mode"`
	Nodes           []validationRequestedNode `json:"nodes"`
	NodeIDsInOrder  []string                  `json:"node_ids_in_order"`
	ImageTags       map[string]string         `json:"image_tags"`
	CandidateSchema string                    `json:"candidate_schema"`
	DBTFlags        []string                  `json:"dbt_flags"`
}

type validationRequestedNode struct {
	UniqueID        string   `json:"unique_id"`
	ServiceName     string   `json:"service_name"`
	NodeType        string   `json:"node_type"`
	SchemaName      string   `json:"schema_name"`
	TableName       string   `json:"table_name"`
	ImageTag        string   `json:"image_tag"`
	UpstreamNodeIDs []string `json:"upstream_node_ids"`
	CandidateSQLURI string   `json:"candidate_sql_uri"`
}

// ParseValidationRequested translates a validation.requested:v1 XMessage into
// a typed domain event. The wire format is a single "payload" field carrying
// the JSON body (matching the outbox publisher); outbox_entry_id rides
// alongside as provenance.
//
// All errors are permanent (malformed input never becomes valid on retry); the
// binding wraps them with events.ErrPermanent so the consumer ACKs rather than
// redelivering forever.
func ParseValidationRequested(msg goredis.XMessage) (events.ValidationRequested, error) {
	raw := stringField(msg.Values, "payload")
	if raw == "" {
		return events.ValidationRequested{}, fmt.Errorf("missing payload field")
	}

	var dto validationRequestedDTO
	if err := json.Unmarshal([]byte(raw), &dto); err != nil {
		return events.ValidationRequested{}, fmt.Errorf("unmarshal payload: %w", err)
	}

	if dto.ReleaseID == "" {
		return events.ValidationRequested{}, fmt.Errorf("missing release_id")
	}
	if dto.Mode != "validation" {
		return events.ValidationRequested{},
			fmt.Errorf("unexpected mode %q (want %q)", dto.Mode, "validation")
	}
	if len(dto.Nodes) == 0 {
		return events.ValidationRequested{}, fmt.Errorf("nodes is empty: nothing to validate")
	}

	nodes := make([]events.ValidationNode, 0, len(dto.Nodes))
	nodeIDSet := make(map[string]struct{}, len(dto.Nodes))
	for i, n := range dto.Nodes {
		switch {
		case n.UniqueID == "":
			return events.ValidationRequested{}, fmt.Errorf("node[%d] missing unique_id", i)
		case n.ServiceName == "":
			return events.ValidationRequested{}, fmt.Errorf("node[%d] (%s) missing service_name", i, n.UniqueID)
		case n.SchemaName == "":
			return events.ValidationRequested{}, fmt.Errorf("node[%d] (%s) missing schema_name", i, n.UniqueID)
		case n.TableName == "":
			return events.ValidationRequested{}, fmt.Errorf("node[%d] (%s) missing table_name", i, n.UniqueID)
		case n.ImageTag == "":
			return events.ValidationRequested{}, fmt.Errorf("node[%d] (%s) missing image_tag", i, n.UniqueID)
		}
		nodeType, err := pkg_model.ParseNodeType(n.NodeType)
		if err != nil {
			return events.ValidationRequested{},
				fmt.Errorf("node[%d] (%s) invalid node_type: %w", i, n.UniqueID, err)
		}
		nodes = append(nodes, events.ValidationNode{
			NodeID:          n.UniqueID,
			ServiceName:     n.ServiceName,
			SchemaName:      n.SchemaName,
			TableName:       n.TableName,
			NodeType:        nodeType,
			ImageTag:        n.ImageTag,
			UpstreamNodeIDs: n.UpstreamNodeIDs,
			CandidateSQLURI: n.CandidateSQLURI,
		})
		nodeIDSet[n.UniqueID] = struct{}{}
	}

	if err := validateNodeIDsMatch(nodeIDSet, dto.NodeIDsInOrder); err != nil {
		return events.ValidationRequested{}, err
	}

	// outbox_entry_id is the release-controller outbox row ID, carried as
	// provenance only. The binding does not dedup on it; dedup keys on a
	// deterministic release-derived UUID (see validationDedupKey). Absent or
	// empty → uuid.Nil. Present-but-malformed → permanent error.
	var outboxEntryID uuid.UUID
	if s := stringField(msg.Values, "outbox_entry_id"); s != "" {
		var err error
		outboxEntryID, err = uuid.Parse(s)
		if err != nil {
			return events.ValidationRequested{}, fmt.Errorf("invalid outbox_entry_id: %w", err)
		}
	}

	return events.ValidationRequested{
		OutboxEntryID:   outboxEntryID,
		ReleaseID:       dto.ReleaseID,
		Mode:            dto.Mode,
		Nodes:           nodes,
		NodeIDsInOrder:  dto.NodeIDsInOrder,
		ImageTags:       dto.ImageTags,
		CandidateSchema: dto.CandidateSchema,
		DBTFlags:        dto.DBTFlags,
	}, nil
}

// validateNodeIDsMatch ensures the set of nodes[].unique_id equals the set of
// node_ids_in_order. A divergence means producer and consumer disagree on the
// validation closure — permanently bad input.
func validateNodeIDsMatch(nodeIDSet map[string]struct{}, order []string) error {
	if len(order) != len(nodeIDSet) {
		return fmt.Errorf("node_ids_in_order has %d entries but nodes has %d",
			len(order), len(nodeIDSet))
	}
	seen := make(map[string]struct{}, len(order))
	for _, id := range order {
		if _, ok := nodeIDSet[id]; !ok {
			return fmt.Errorf("node_ids_in_order references %q not present in nodes", id)
		}
		if _, dup := seen[id]; dup {
			return fmt.Errorf("node_ids_in_order has duplicate entry %q", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}
