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
	// Mode carries the optional dispatch mode from the query.model:v1 payload.
	// Empty for normal production jobs; events.ModePromoteSeed for promote-seed
	// jobs. The executor stamps it as a k8s Job label so k8s-controller can
	// suppress the production lifecycle events for modes that have no real run.
	Mode string
}
