// executor-controller/domain/events/compile_node_completed.go
package events

import "github.com/google/uuid"

// CompileNodeCompleted is the parsed compile.node.completed:v1 stream
// payload — a typed, in-process representation of a compile node's terminal
// result, emitted by k8s-controller when a compile Job ends.
// It mirrors SeedBuildNodeCompleted field-for-field (the two legs report a
// per-node terminal outcome identically). The executor attaches the outcome to
// the matching (ReleaseID, NodeID) compile deployment, then runs the
// per-release compile aggregate-emit gate.
//
// OutboxEntryID is the k8s-controller outbox row ID, carried through as the
// message_processing provenance for standard (msg.ID, stream_name) dedup with
// an outbox_entry_id fallback. Zero value (uuid.Nil) means the inbound message
// did not carry the field.
//
// Outcome is the terminal node status, one of "ok" or "failed".
//
// FailedContainer is the name of the pod container that terminated non-zero
// (compile | parse-prod | parse-candidate | upload), carried by k8s-controller
// only on a failed outcome; "" when absent (including every "ok" outcome).
type CompileNodeCompleted struct {
	OutboxEntryID   uuid.UUID
	ReleaseID       string
	NodeID          string
	Outcome         string
	DBTLogURI       string
	RunResultsURI   string // S3 key of the structured result; "" when absent
	FailedContainer string
}
