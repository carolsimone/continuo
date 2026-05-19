// executor-controller/domain/events/query_model.go
package events

import (
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// QueryModel is the parsed query.model:v1 stream payload — a typed,
// in-process representation of a deploy request emitted by the orchestrator.
//
// OutboxEntryID is the application-level idempotency key set by the
// orchestrator's outbox processor (the ID of the orchestrator's own
// outbox row). It is used as the unique key on
// deployment_outbox.outbox_entry_id so that an ambiguous publisher retry
// (same payload, new Redis msg.ID) does not produce a duplicate K8s
// deploy. Zero value (uuid.Nil) means the inbound message did not carry
// the field; the dedup degrades to the shared (msg.ID, stream_name)
// layer only.
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
}
