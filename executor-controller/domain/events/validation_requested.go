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
//
// UpstreamNodeIDs lists the dbt unique_ids of in-set nodes (intra- AND
// cross-service) that must complete successfully before this node may be
// dispatched. An empty slice means the node is a root and can be dispatched
// immediately.
//
// CandidateSQLURI is an S3 URI (s3://bucket/key) pointing to the node's
// compiled SQL with every schema-qualified reference already rewritten to the
// candidate schema. For model/snapshot nodes it is passed as the
// CANDIDATE_SQL_URI env var on the validation Job's fetch init container, which
// downloads the object into the shared file the runner reads (CANDIDATE_SQL_PATH)
// and builds an empty CTAS table without a dbt recompile. Empty for seed nodes
// (unchanged seeds are cloned from prod; new/changed seeds are pre-built).
type ValidationNode struct {
	NodeID          string // dbt unique_id
	ServiceName     string
	SchemaName      string
	TableName       string
	NodeType        pkg_model.NodeType
	ImageTag        string
	UpstreamNodeIDs []string
	CandidateSQLURI string
	// ValidationOp selects the runner operation for this node:
	// "build_from_sql" (default) | "clone_from_prod". ProdSchema is the source
	// schema for clone_from_prod (empty otherwise). Set per node by
	// release-controller from the changed-closure membership.
	ValidationOp string
	ProdSchema   string
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
	DBTFlags        []string
}
