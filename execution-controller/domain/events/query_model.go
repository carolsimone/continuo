// execution-controller/domain/events/query_model.go
package events

import (
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// QueryModel is the parsed query.model:v1 stream payload — a typed,
// in-process representation of a deploy request emitted by state.
//
// OutboxEntryID is state's outbox row ID, carried through
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
	// SecretRef names the continuo-api-* Secret a python-api node's pod
	// receives as env vars; empty for every other node.
	SecretRef string
	// Operation selects the dbt verb the executor runs for this node.
	// pkg_model.OperationRun (empty) is the default: dbt run/seed/snapshot by
	// NodeType — a seed load is plain, non-destructive `dbt seed`.
	// pkg_model.OperationTest runs `dbt test --select <node>`.
	// pkg_model.OperationFullRefresh rebuilds a model (full_refresh command)
	// or seed (seed_full_refresh command) from scratch.
	Operation pkg_model.Operation
	// Mode names the Job mode a message carries explicitly. state sets none, so
	// it is empty for every message state writes; events.ModePromoteSeed is the
	// one value still honoured.
	Mode string
	// MaxRetries is the task's retry budget state stamped on the
	// dispatch. Zero means the message carried none; the queued deployment
	// then takes pkgevents.DefaultTaskMaxRetries.
	MaxRetries int32
}
