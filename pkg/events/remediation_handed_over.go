package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

// RemediationHandedOverSchemaVersion is the schema_version of
// remediation.handed_over:v1 entries.
const RemediationHandedOverSchemaVersion = 1

// RemediationHandedOver is the payload of a remediation.handed_over:v1 entry:
// a remediation ended with nodes a human must fix. Tries is how many tries the
// remediation ran, 0 when it handed nodes over before any try.
type RemediationHandedOver struct {
	RemediationID    string           `json:"remediation_id"`
	ReleaseID        string           `json:"release_id"`
	RemediationRound int              `json:"remediation_round"`
	Flow             string           `json:"flow"`
	Tries            int              `json:"tries"`
	Nodes            []HandedOverNode `json:"nodes"`
}

// HandedOverNode is one node left to a human. Reason is a value of the
// handover_reason vocabulary and Message says in words what the operator
// finds and does.
type HandedOverNode struct {
	NodeID  string `json:"node_id"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// RemediationHandedOverFields renders p as the Redis fields of one
// remediation.handed_over:v1 entry. eventID is the id of the outbox row that
// carries the entry. The payload always holds a nodes array, empty when p
// names none.
func RemediationHandedOverFields(eventID, tenantID, producer string, occurredAt time.Time, p RemediationHandedOver) (map[string]any, error) {
	if p.Nodes == nil {
		p.Nodes = []HandedOverNode{}
	}
	env := Envelope{
		EventID:       eventID,
		TenantID:      tenantID,
		OccurredAt:    occurredAt,
		Producer:      producer,
		SchemaVersion: RemediationHandedOverSchemaVersion,
	}
	return env.Fields(p)
}

// DecodeRemediationHandedOver reads one remediation.handed_over:v1 entry.
// Every error wraps ErrMalformedEnvelope: an unreadable envelope, another
// schema_version, a payload that is not JSON, an empty remediation_id,
// release_id or flow, a remediation_round below 1, negative tries, or a node
// without a node_id or with a reason outside the handover_reason vocabulary.
func DecodeRemediationHandedOver(fields map[string]string) (Envelope, RemediationHandedOver, error) {
	env, payload, err := ParseEnvelope(fields)
	if err != nil {
		return Envelope{}, RemediationHandedOver{}, err
	}
	if env.SchemaVersion != RemediationHandedOverSchemaVersion {
		return Envelope{}, RemediationHandedOver{}, fmt.Errorf("%w: schema_version %d", ErrMalformedEnvelope, env.SchemaVersion)
	}
	var p RemediationHandedOver
	if err := json.Unmarshal(payload, &p); err != nil {
		return Envelope{}, RemediationHandedOver{}, fmt.Errorf("%w: payload: %v", ErrMalformedEnvelope, err)
	}
	if err := validRemediationHandedOver(p); err != nil {
		return Envelope{}, RemediationHandedOver{}, err
	}
	return env, p, nil
}

func validRemediationHandedOver(p RemediationHandedOver) error {
	switch {
	case p.RemediationID == "":
		return fmt.Errorf("%w: no remediation_id", ErrMalformedEnvelope)
	case p.ReleaseID == "":
		return fmt.Errorf("%w: no release_id for remediation %s", ErrMalformedEnvelope, p.RemediationID)
	case p.Flow == "":
		return fmt.Errorf("%w: no flow for remediation %s", ErrMalformedEnvelope, p.RemediationID)
	case p.RemediationRound < 1:
		return fmt.Errorf("%w: remediation_round %d for remediation %s", ErrMalformedEnvelope, p.RemediationRound, p.RemediationID)
	case p.Tries < 0:
		return fmt.Errorf("%w: tries %d for remediation %s", ErrMalformedEnvelope, p.Tries, p.RemediationID)
	}
	for i, n := range p.Nodes {
		if n.NodeID == "" {
			return fmt.Errorf("%w: node %d of remediation %s has no node_id", ErrMalformedEnvelope, i, p.RemediationID)
		}
		if !model.HandoverReason(n.Reason).IsValid() {
			return fmt.Errorf("%w: reason %q for node %s of remediation %s", ErrMalformedEnvelope, n.Reason, n.NodeID, p.RemediationID)
		}
	}
	return nil
}
