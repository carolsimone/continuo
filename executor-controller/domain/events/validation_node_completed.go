// executor-controller/domain/events/validation_node_completed.go
package events

import "github.com/google/uuid"

// ValidationNodeCompleted is the parsed validation.node.completed:v1 stream
// payload — a typed, in-process representation of one candidate-validation
// node's terminal result, emitted by k8s-controller when a validation Job ends.
// The executor attaches the outcome to the matching (ReleaseID, NodeID)
// validation deployment, then runs the per-release aggregate-emit gate.
//
// OutboxEntryID is the k8s-controller outbox row ID, carried through as the
// message_processing provenance for standard (msg.ID, stream_name) dedup with
// an outbox_entry_id fallback. Zero value (uuid.Nil) means the inbound message
// did not carry the field.
//
// Outcome is the terminal node status, one of "ok" or "failed".
type ValidationNodeCompleted struct {
	OutboxEntryID uuid.UUID
	ReleaseID     string
	NodeID        string
	Outcome       string
	DBTLogURI     string
}
