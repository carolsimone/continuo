// executor-controller/domain/events/seed_build_node_completed.go
package events

import "github.com/google/uuid"

// SeedBuildNodeCompleted is the parsed seed.build.node.completed:v1 stream
// payload — a typed, in-process representation of one candidate seed-build
// node's terminal result, emitted by k8s-controller when a seed-build Job ends.
// It mirrors ValidationNodeCompleted field-for-field (the two legs report a
// per-node terminal outcome identically). The executor attaches the outcome to
// the matching (ReleaseID, NodeID) seed-build deployment, then runs the
// per-release seed-build aggregate-emit gate.
//
// OutboxEntryID is the k8s-controller outbox row ID, carried through as the
// message_processing provenance for standard (msg.ID, stream_name) dedup with
// an outbox_entry_id fallback. Zero value (uuid.Nil) means the inbound message
// did not carry the field.
//
// Outcome is the terminal node status, one of "ok" or "failed".
type SeedBuildNodeCompleted struct {
	OutboxEntryID uuid.UUID
	ReleaseID     string
	NodeID        string
	Outcome       string
	DBTLogURI     string
	RunResultsURI string // S3 key of the structured result; "" when absent
}
