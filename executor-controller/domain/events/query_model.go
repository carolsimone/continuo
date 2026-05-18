// executor-controller/domain/events/query_model.go
package events

import (
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// QueryModel is the parsed query.model:v1 stream payload — a typed,
// in-process representation of a deploy request emitted by the orchestrator.
type QueryModel struct {
	TaskID       uuid.UUID
	ScheduleID   uuid.UUID
	ScheduleName string
	ServiceName  string
	SchemaName   string
	TableName    string
	JobName      string
	NodeType     pkg_model.NodeType
	ImageTag     string
}
