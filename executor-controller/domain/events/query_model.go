// executor-controller/domain/events/query_model.go
package events

import (
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// QueryModel is the parsed query.model:v1 stream payload — a typed,
// in-process representation of a deploy request emitted by the orchestrator.
//
// OutboxEntryID is the orchestrator's outbox row ID, carried through
// as the message_processing_id provenance field on the executor's outbox
// row. Zero value (uuid.Nil) means the inbound message did not carry the
// field; dedup relies solely on the shared (msg.ID, stream_name) layer.
type QueryModel struct {
	OutboxEntryID uuid.UUID
	TaskID        uuid.UUID
	ScheduleID    uuid.UUID
	ScheduleName  string
	ServiceName   string
	SchemaName    string
	TableName     string
	JobName       string
	NodeType      pkg_model.NodeType
	ImageTag      string
	// Operation selects the dbt verb the executor runs for this node.
	// pkg_model.OperationRun (empty) is the default: dbt run/seed/snapshot by
	// NodeType. pkg_model.OperationTest runs `dbt test --select <node>`.
	Operation pkg_model.Operation
}
