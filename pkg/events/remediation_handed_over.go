package events

import (
	"fmt"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

// RemediationHandedOverSchemaVersion is the schema_version of
// remediation.handed_over:v1 entries.
const RemediationHandedOverSchemaVersion = 1

// RemediationHandedOver is the payload of a remediation.handed_over:v1 entry:
// a remediation ended with nodes a human must fix. Tries is the number of the
// last try the remediation ran, which is how many tries its round made, since
// a round numbers its tries from 1; 0 when it handed nodes over before any try.
type RemediationHandedOver struct {
	RemediationID    string           `json:"remediation_id"`
	ReleaseID        string           `json:"release_id"`
	RemediationRound int              `json:"remediation_round"`
	Flow             string           `json:"flow"`
	Tries            int              `json:"tries"`
	Nodes            []HandedOverNode `json:"nodes"`
}

// HandedOverNode is one node left to a human. Reason is a value of the
// handover_reason vocabulary and Message is the detail the runner recorded
// with that code, cleaned of credentials and email addresses (for a classifier
// hand-over, the failure's key error line).
// Owner is who owns the node, the person or team to hand it to, as its code
// bundle or its contract file names them: free text, empty and then left out of
// the payload when no owner was found.
type HandedOverNode struct {
	NodeID  string `json:"node_id"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
	Owner   string `json:"owner,omitempty"`
}

// RemediationHandedOverFields renders p as the Redis fields of one
// remediation.handed_over:v1 entry. eventID is the id of the outbox row that
// carries the entry. The payload always holds a nodes array, empty when p
// names none. A payload the contract forbids is refused with an error that
// wraps ErrMalformedEnvelope: an empty remediation_id, release_id or flow, a
// remediation_round below 1, negative tries, or a node without a node_id or
// with a reason outside the handover_reason vocabulary. A node's owner is not
// checked.
func RemediationHandedOverFields(eventID, tenantID, producer string, occurredAt time.Time, p RemediationHandedOver) (map[string]any, error) {
	if err := validRemediationHandedOver(p); err != nil {
		return nil, err
	}
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
