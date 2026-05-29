// executor-controller/domain/events/validation_requested.go
package events

import (
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// ValidationNode is one entry in a ValidationRequested.Nodes slice — the
// flat, per-node spec executor-controller needs to enqueue one candidate
// validation deployment.
//
// NodeID carries the dbt unique_id of the node (the producer emits it under
// the per-node "unique_id" key). It is the node's identity throughout the
// executor: the K8s Job name, the executor_deployments.node_id column, and
// the (release_id, node_id) lookup key all derive from this value.
type ValidationNode struct {
	NodeID      string // dbt unique_id
	ServiceName string
	SchemaName  string
	TableName   string
	NodeType    pkg_model.NodeType
	ImageTag    string
}

// ValidationRequested is the parsed validation.requested:v1 stream payload —
// a typed, in-process representation of a candidate-release validation request
// emitted by release-controller. The executor enqueues one validation
// deployment per node in Nodes.
//
// OutboxEntryID is the release-controller outbox row ID, carried through as
// the message_processing_id provenance field on the executor's outbox row.
// Zero value (uuid.Nil) means the inbound message did not carry the field;
// dedup then relies solely on the shared (msg.ID, stream_name) layer.
//
// NodeIDsInOrder lists the same dbt unique_ids as Nodes[].NodeID, in
// topological order. The parser guarantees the two agree as a set.
type ValidationRequested struct {
	OutboxEntryID   uuid.UUID
	ReleaseID       string
	Mode            string
	Nodes           []ValidationNode
	NodeIDsInOrder  []string
	ImageTags       map[string]string
	CandidateSchema string
	DeferStateURI   string
	DBTFlags        []string
}
